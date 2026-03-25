/**
 * This service provides operations of statistics.
 */

const _ = require('lodash')
const Joi = require('joi')
const config = require('config')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prismaManager = require('../common/prisma')
const { Prisma } = prismaManager
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()
const prismaHelper = require('../common/prismaHelper')
const reviewDb = require('../common/reviewDb')
const { resolveChallengeResultRelation } = require('../common/reviewDbHelper')
const { rerateDevTrack } = require('../ratings/developRatingEngine')
const { rerateMmTrack } = require('../ratings/mmRatingEngine')
const {
  TRACK_NAMES,
  TYPE_NAMES,
  getCanonicalTrackName,
  getCanonicalTypeName,
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup,
  resolveTrackNameFromLookup,
  resolveTypeNameFromLookup
} = require('../common/statsDimensionHelper')

const DISTRIBUTION_FIELDS = ['track', 'subTrack', 'distribution', 'createdAt', 'updatedAt',
  'createdBy', 'updatedBy']
const DISTRIBUTION_FIELDS_NO_DATE = ['track', 'subTrack', 'distribution']

const HISTORY_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE',
  'createdAt', 'updatedAt', 'createdBy', 'updatedBy']

const MEMBER_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'maxRating',
  'challenges', 'wins', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT', 'createdAt',
  'updatedAt', 'createdBy', 'updatedBy']

const LEGACY_STATS_READ_SOURCE = 'legacy'
const SUPPORTED_STATS_READ_SOURCES = ['unified', LEGACY_STATS_READ_SOURCE]
const DISTRIBUTION_RANGES = _.range(0, 4000, 100)
const configuredStatsReadSource = _.toLower(String(config.STATS_READ_SOURCE || 'unified').trim())
if (!_.includes(SUPPORTED_STATS_READ_SOURCES, configuredStatsReadSource)) {
  logger.warn(`Invalid STATS_READ_SOURCE='${config.STATS_READ_SOURCE}'. Falling back to 'unified'.`)
}
const USE_LEGACY_STATS_READS = configuredStatsReadSource === LEGACY_STATS_READ_SOURCE

/**
 * Join Prisma SQL condition fragments with a literal AND separator.
 * Prisma joins with a Prisma.sql separator stringify that separator to [object Object].
 * @param {Array<Object>} conditions Prisma SQL condition fragments
 * @returns {Object} joined Prisma SQL fragment
 */
function joinSqlConditions (conditions) {
  return Prisma.join(conditions, ' AND ')
}

function toOptionalInt (value) {
  if (_.isNil(value) || value === '') {
    return undefined
  }
  return _.toInteger(value)
}

function toOptionalFloat (value) {
  if (_.isNil(value) || value === '') {
    return undefined
  }
  return Number(value)
}

function toOptionalDate (value) {
  if (_.isNil(value)) {
    return undefined
  }
  return prismaHelper.convertDate(value)
}

/**
 * Normalize request challenge identifiers into the string form documented by the API.
 * Numeric compatibility inputs are echoed back as strings, while omitted values remain null.
 * @param {*} value request challenge identifier
 * @returns {string|null} normalized challenge identifier
 */
function normalizeChallengeIdForResponse (value) {
  if (_.isNil(value)) {
    return null
  }

  return String(value)
}

let challengeDimensionLookupPromise

/**
 * Load the shared challenge track/type lookup used by unified stats reads and writes.
 * The lookup translates between stored UUID ids and the canonical API labels used
 * by request payloads, filters, and response builders.
 * @returns {Promise<Object>} cached challenge dimension lookup
 */
async function getChallengeDimensionLookup () {
  if (!challengeDimensionLookupPromise) {
    challengeDimensionLookupPromise = loadChallengeDimensionLookup(prismaManager.getChallengesClient())
  }

  return challengeDimensionLookupPromise
}

/**
 * Normalize a track label into the canonical API name used by rerate endpoints.
 * @param {*} trackId raw track label
 * @returns {string|undefined} canonical track name when recognized
 */
function resolveTrackName (trackId) {
  return getCanonicalTrackName(trackId)
}

/**
 * Normalize a type label into the canonical API name used by rerate endpoints.
 * @param {*} typeId raw type label
 * @returns {string|undefined} canonical type name when recognized
 */
function resolveTypeName (typeId) {
  return getCanonicalTypeName(typeId)
}

function isLegacyMaxRatingPayload (value) {
  return _.isPlainObject(value) && !_.isNil(value.rating) && !_.isNil(value.ratingColor)
}

function normalizeUnifiedRecord (record, isPrivate, dimensionLookup) {
  if (!record || !record.trackId || !record.typeId) {
    return null
  }

  const normalized = _.omitBy({
    trackId: resolveTrackIdFromLookup(dimensionLookup, record.trackId),
    typeId: resolveTypeIdFromLookup(dimensionLookup, record.typeId),
    challenges: toOptionalInt(record.challenges),
    wins: toOptionalInt(record.wins),
    mostRecentSubmission: toOptionalDate(record.mostRecentSubmission),
    mostRecentEventDate: toOptionalDate(record.mostRecentEventDate),
    rating: toOptionalInt(record.rating),
    avgRank: toOptionalFloat(record.avgRank),
    avgNumSubmissions: toOptionalInt(record.avgNumSubmissions),
    bestRank: toOptionalInt(record.bestRank),
    globalRank: toOptionalInt(record.globalRank),
    countryRank: toOptionalInt(record.countryRank),
    schoolRank: toOptionalInt(record.schoolRank),
    volatility: toOptionalInt(record.volatility),
    maxRating: toOptionalInt(record.maxRating),
    minRating: toOptionalInt(record.minRating),
    topFiveFinishes: toOptionalInt(record.topFiveFinishes),
    topTenFinishes: toOptionalInt(record.topTenFinishes),
    isPrivate
  }, _.isUndefined)

  if (!normalized.trackId || !normalized.typeId) {
    return null
  }

  return normalized
}

function pushUnifiedRecord (collection, record, isPrivate, dimensionLookup) {
  const normalized = normalizeUnifiedRecord(record, isPrivate, dimensionLookup)
  if (normalized) {
    collection.push(normalized)
  }
}

function buildUnifiedStatsRecordsFromPayload (payload, isPrivate, dimensionLookup, options = {}) {
  const data = payload || {}
  const records = []
  const isPartial = !!options.partial
  const unifiedMaxRating = isLegacyMaxRatingPayload(data.maxRating) ? undefined : data.maxRating

  const rootPayload = {
    trackId: data.trackId,
    typeId: data.typeId,
    challenges: data.challenges,
    wins: data.wins,
    mostRecentSubmission: data.mostRecentSubmission,
    mostRecentEventDate: data.mostRecentEventDate,
    rating: data.rating,
    avgRank: data.avgRank,
    avgNumSubmissions: data.avgNumSubmissions,
    bestRank: data.bestRank,
    globalRank: data.globalRank,
    countryRank: data.countryRank,
    schoolRank: data.schoolRank,
    volatility: data.volatility,
    maxRating: unifiedMaxRating,
    minRating: data.minRating,
    topFiveFinishes: data.topFiveFinishes,
    topTenFinishes: data.topTenFinishes
  }

  if (rootPayload.trackId && rootPayload.typeId) {
    pushUnifiedRecord(records, rootPayload, isPrivate, dimensionLookup)
  }

  if (_.isArray(data.records)) {
    _.forEach(data.records, (record) => {
      pushUnifiedRecord(records, record, isPrivate, dimensionLookup)
    })
  }

  if (!isPartial && records.length === 0 && (!_.isNil(data.challenges) || !_.isNil(data.wins))) {
    pushUnifiedRecord(records, {
      trackId: data.trackId || TRACK_NAMES.DEVELOP,
      typeId: data.typeId || TYPE_NAMES.CHALLENGE,
      challenges: data.challenges,
      wins: data.wins,
      mostRecentSubmission: data.mostRecentSubmission,
      mostRecentEventDate: data.mostRecentEventDate,
      rating: data.rating,
      avgRank: data.avgRank,
      avgNumSubmissions: data.avgNumSubmissions,
      bestRank: data.bestRank,
      globalRank: data.globalRank,
      countryRank: data.countryRank,
      schoolRank: data.schoolRank,
      volatility: data.volatility,
      maxRating: unifiedMaxRating,
      minRating: data.minRating,
      topFiveFinishes: data.topFiveFinishes,
      topTenFinishes: data.topTenFinishes
    }, isPrivate, dimensionLookup)
  }

  // Last record wins for duplicate (trackId, typeId) keys.
  return _.values(_.keyBy(records, record => `${record.trackId}::${record.typeId}`))
}

function buildStatsTrackTypeKey (trackId, typeId) {
  return `${trackId}::${typeId}`
}

function getReviewDbClientOrThrow () {
  if (!reviewDb) {
    throw new Error('REVIEW_DB_URL must be configured to refresh or rerate member stats')
  }

  return reviewDb
}

async function fetchReviewChallengeResultsForMember (reviewDbClient, userId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "userId" = $1
      ORDER BY "createdAt" ASC
    `,
    [userId.toString()]
  )

  return result.rows
}

/**
 * Load placement winners for one member from challenge-api.
 * These rows provide a fallback history source for unrated tracks such as
 * First2Finish when review-api results are unavailable.
 * @param {Object} challengeClient prisma challenge client
 * @param {BigInt} userId member user id
 * @returns {Promise<Array<Object>>} placement winner rows with embedded challenge metadata
 */
async function fetchChallengeWinnerResultsForMember (challengeClient, userId) {
  try {
    return await challengeClient.ChallengeWinner.findMany({
      where: {
        userId: helper.bigIntToNumber(userId),
        type: 'PLACEMENT'
      },
      select: {
        challengeId: true,
        placement: true,
        createdAt: true,
        challenge: {
          select: {
            id: true,
            name: true,
            status: true,
            trackId: true,
            typeId: true,
            endDate: true
          }
        }
      }
    })
  } catch (error) {
    logger.warn(`Unable to load challenge winner fallback rows for userId=${userId.toString()}: ${error.message}`)
    return []
  }
}

/**
 * Load challenge metadata keyed by both canonical UUID id and legacy numeric id.
 * Unified history rows may still carry legacy challenge identifiers from migrated
 * data, so callers can resolve names and canonical UUIDs without mutating storage.
 * @param {Object} challengeClient prisma challenge client
 * @param {Array<*>} challengeIds challenge identifiers from stats/history rows
 * @returns {Promise<Map<string, Object>>} metadata keyed by UUID and legacy id strings
 */
async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  const normalizedChallengeIds = _.chain(challengeIds)
    .map(challengeId => (_.isNil(challengeId) ? null : String(challengeId).trim()))
    .filter(Boolean)
    .uniq()
    .value()

  if (normalizedChallengeIds.length === 0) {
    return new Map()
  }

  const numericChallengeIds = _.chain(normalizedChallengeIds)
    .filter(challengeId => /^\d+$/.test(challengeId))
    .map(challengeId => Number(challengeId))
    .filter(Number.isSafeInteger)
    .uniq()
    .value()

  const whereClauses = [{
    id: {
      in: normalizedChallengeIds
    }
  }]

  if (numericChallengeIds.length > 0) {
    whereClauses.push({
      legacyId: {
        in: numericChallengeIds
      }
    })
    whereClauses.push({
      legacyRecord: {
        is: {
          legacySystemId: {
            in: numericChallengeIds
          }
        }
      }
    })
  }

  const challenges = await challengeClient.challenge.findMany({
    where: whereClauses.length === 1 ? whereClauses[0] : { OR: whereClauses },
    select: {
      id: true,
      legacyId: true,
      name: true,
      status: true,
      trackId: true,
      typeId: true,
      endDate: true,
      track: {
        select: {
          name: true
        }
      },
      type: {
        select: {
          name: true
        }
      },
      metadata: {
        where: {
          name: {
            in: ['rated', 'isRated', 'unrated']
          }
        },
        select: {
          name: true,
          value: true
        }
      },
      legacyRecord: {
        select: {
          legacySystemId: true
        }
      }
    }
  })

  const metadataByChallengeId = new Map()
  _.forEach(challenges, (challenge) => {
    metadataByChallengeId.set(String(challenge.id), challenge)
    if (!_.isNil(challenge.legacyId)) {
      metadataByChallengeId.set(String(challenge.legacyId), challenge)
    }
    if (!_.isNil(_.get(challenge, 'legacyRecord.legacySystemId'))) {
      metadataByChallengeId.set(String(challenge.legacyRecord.legacySystemId), challenge)
    }
  })

  return metadataByChallengeId
}

/**
 * Determine whether challenge metadata represents a completed challenge.
 * @param {Object} challenge challenge metadata row
 * @returns {boolean} true when the challenge status is COMPLETED
 */
function isCompletedChallenge (challenge) {
  return String(_.get(challenge, 'status') || '').trim().toUpperCase() === 'COMPLETED'
}

/**
 * Drop persisted history rows when challenge metadata proves the challenge is not completed.
 * Rows without matching challenge metadata are kept so legacy history can still surface.
 * @param {Array<Object>} rows unified history rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @returns {Array<Object>} history rows limited to completed challenges when status is known
 */
function filterUnifiedHistoryRowsToCompletedChallenges (rows, challengeMetadataById) {
  return _.filter(rows || [], (row) => {
    const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
    if (!challengeId) {
      return true
    }

    const challenge = challengeMetadataById.get(challengeId)
    if (!challenge) {
      return true
    }

    return isCompletedChallenge(challenge)
  })
}

/**
 * Attach canonical challenge ids and names to unified history rows before shaping
 * the response payload consumed by the profiles UI.
 * @param {Array<Object>} rows unified history rows loaded from members.memberStatsHistory
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @returns {Array<Object>} rows enriched with canonical challenge ids and names when available
 */
function enrichUnifiedHistoryRowsWithChallengeMetadata (rows, challengeMetadataById) {
  return _.map(rows || [], (row) => {
    const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
    if (!challengeId) {
      return row
    }

    const challenge = challengeMetadataById.get(challengeId)
    if (!challenge) {
      return row
    }

    return {
      ...row,
      challengeId: String(challenge.id),
      challengeName: row.challengeName || challenge.name
    }
  })
}

/**
 * Normalize placement values so only positive integer rankings are surfaced.
 * A zero placement is not meaningful in the profile history UI and is treated
 * as missing data that can be backfilled from challenge winners.
 * @param {*} value raw placement value
 * @returns {number|undefined} positive placement when available
 */
function toVisiblePlacement (value) {
  const placement = toOptionalInt(value)

  return Number.isInteger(placement) && placement > 0 ? placement : undefined
}

/**
 * Determine whether any history rows are missing a usable placement value.
 * @param {Array<Object>} rows history rows already shaped for response building
 * @returns {boolean} true when a row still needs placement enrichment
 */
function historyRowsNeedPlacementEnrichment (rows) {
  return _.some(rows || [], row => !_.isNil(row.challengeId) && !toVisiblePlacement(row.placement))
}

/**
 * Build a canonical challengeId -> placement lookup from challenge winner rows.
 * When duplicate winner rows exist, keep the best available placement.
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Map<string, number>} canonical challenge placements by challenge id
 */
function buildChallengeWinnerPlacementLookup (winnerRows) {
  const placementByChallengeId = new Map()

  _.forEach(winnerRows || [], (row) => {
    const placement = toVisiblePlacement(row.placement)
    const challengeId = _.get(row, 'challenge.id') || row.challengeId
    const challengeKey = _.isNil(challengeId) ? null : String(challengeId).trim()

    if (!placement || !challengeKey) {
      return
    }

    const existingPlacement = placementByChallengeId.get(challengeKey)
    if (_.isNil(existingPlacement) || placement < existingPlacement) {
      placementByChallengeId.set(challengeKey, placement)
    }
  })

  return placementByChallengeId
}

/**
 * Fill missing or zeroed persisted placements from challenge-api winner rows.
 * This keeps the profile challenge cards accurate while older history rows are
 * backfilled with authoritative placement data.
 * @param {Array<Object>} rows persisted and/or synthesized history rows
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Array<Object>} history rows with corrected placements when available
 */
function mergeHistoryPlacementsFromChallengeWinners (rows, winnerRows) {
  const placementByChallengeId = buildChallengeWinnerPlacementLookup(winnerRows)

  if (placementByChallengeId.size === 0) {
    return rows || []
  }

  return _.map(rows || [], (row) => {
    const challengeKey = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
    const placement = challengeKey ? placementByChallengeId.get(challengeKey) : undefined

    if (toVisiblePlacement(row.placement) || !placement) {
      return row
    }

    return {
      ...row,
      placement
    }
  })
}

function buildAggregatedStatsFromReviewResults (reviewRows, challengeMetadataById) {
  const aggregateByKey = new Map()

  _.forEach(reviewRows, (row) => {
    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const trackId = String(challenge.trackId)
    const typeId = String(challenge.typeId)
    if (!trackId || !typeId) {
      return
    }

    const key = buildStatsTrackTypeKey(trackId, typeId)
    const existing = aggregateByKey.get(key) || {
      trackId,
      typeId,
      challenges: 0,
      wins: 0,
      mostRecentSubmission: null,
      mostRecentEventDate: null
    }

    existing.challenges += 1
    if (_.toInteger(row.placement) === 1) {
      existing.wins += 1
    }

    const submissionDate = row.createdAt ? new Date(row.createdAt) : null
    const eventDate = challenge.endDate ? new Date(challenge.endDate) : submissionDate

    if (submissionDate && !Number.isNaN(submissionDate.getTime()) &&
      (!existing.mostRecentSubmission || submissionDate > existing.mostRecentSubmission)) {
      existing.mostRecentSubmission = submissionDate
    }

    if (eventDate && !Number.isNaN(eventDate.getTime()) &&
      (!existing.mostRecentEventDate || eventDate > existing.mostRecentEventDate)) {
      existing.mostRecentEventDate = eventDate
    }

    aggregateByKey.set(key, existing)
  })

  return Array.from(aggregateByKey.values())
}

/**
 * Check whether the unified history response should surface the supplied track.
 * The public history contract currently exposes DEVELOPMENT, DESIGN, and DATA_SCIENCE groups.
 * @param {string|undefined} trackName canonical track label
 * @returns {boolean} true when the track should be included in history responses
 */
function isSupportedUnifiedHistoryTrack (trackName) {
  return _.includes([TRACK_NAMES.DEVELOP, TRACK_NAMES.DESIGN, TRACK_NAMES.DATA_SCIENCE], trackName)
}

/**
 * Identify aggregate track/type pairs that are visible in memberStats for the
 * current request scope.
 * @param {Array<Object>} aggregateRows unified memberStats rows for one member
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Set<string>} visible track/type pair keys
 */
function getVisibleUnifiedHistoryPairKeys (aggregateRows, dimensionLookup) {
  return new Set(
    _.chain(annotateUnifiedDimensionRows(aggregateRows || [], dimensionLookup))
      .filter(row => isSupportedUnifiedHistoryTrack(row.trackName))
      .map(row => buildStatsTrackTypeKey(row.trackId, row.typeId))
      .uniq()
      .value()
  )
}

/**
 * Identify aggregate track/type pairs that are visible in memberStats but missing from
 * memberStatsHistory for the current request scope.
 * @param {Array<Object>} aggregateRows unified memberStats rows for one member
 * @param {Array<Object>} historyRows unified memberStatsHistory rows for one member
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Set<string>} missing track/type pair keys
 */
function getMissingUnifiedHistoryPairKeys (aggregateRows, historyRows, dimensionLookup) {
  const persistedPairKeys = new Set(
    _.map(historyRows || [], row => buildStatsTrackTypeKey(row.trackId, row.typeId))
  )

  return new Set(
    _.chain(Array.from(getVisibleUnifiedHistoryPairKeys(aggregateRows, dimensionLookup)))
      .filter(pairKey => !persistedPairKeys.has(pairKey))
      .value()
  )
}

/**
 * Build transient unified history rows from completed review-api challenge results for
 * aggregate track/type pairs that do not yet have authoritative memberStatsHistory rows.
 * These fallback rows preserve challenge cards for non-rated tracks such as First2Finish
 * until a persistent backfill is written.
 * @param {Array<Object>} reviewRows review-api challenge results for the member
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {Set<string>} missingPairKeys track/type pairs that should be synthesized
 * @returns {Array<Object>} transient unified history rows ordered per pair
 */
function buildFallbackHistoryRowsFromReviewResults (reviewRows, challengeMetadataById, dimensionLookup, missingPairKeys) {
  const fallbackRowsByChallengeKey = new Map()

  _.forEach(reviewRows || [], (row) => {
    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const trackId = String(challenge.trackId)
    const typeId = String(challenge.typeId)
    const pairKey = buildStatsTrackTypeKey(trackId, typeId)
    if (missingPairKeys && !missingPairKeys.has(pairKey)) {
      return
    }

    const trackName = resolveTrackNameFromLookup(dimensionLookup, trackId)
    if (!isSupportedUnifiedHistoryTrack(trackName)) {
      return
    }

    const typeName = resolveTypeNameFromLookup(dimensionLookup, typeId)
    const eventDate = toOptionalDate(challenge.endDate || row.createdAt)
    if (!eventDate) {
      return
    }

    const createdAt = toOptionalDate(row.createdAt) || eventDate
    const placement = toVisiblePlacement(row.placement)
    const challengeId = String(challenge.id)
    const challengeKey = `${pairKey}::${challengeId}`
    const existing = fallbackRowsByChallengeKey.get(challengeKey)

    if (existing && createdAt <= existing.createdAt) {
      return
    }

    fallbackRowsByChallengeKey.set(challengeKey, {
      trackId,
      typeId,
      trackName,
      typeName,
      challengeId,
      challengeName: challenge.name || null,
      eventDate,
      placement,
      createdAt
    })
  })

  const fallbackRows = []
  const rowsByPairKey = _.groupBy(Array.from(fallbackRowsByChallengeKey.values()), row => buildStatsTrackTypeKey(row.trackId, row.typeId))

  _.forEach(rowsByPairKey, (pairRows) => {
    const orderedRows = _.orderBy(pairRows, [
      row => row.eventDate.getTime(),
      row => row.createdAt.getTime(),
      row => row.challengeId
    ], ['desc', 'desc', 'desc'])

    _.forEach(orderedRows, (row, index) => {
      fallbackRows.push(_.omit({
        ...row,
        mostRecent: index === 0
      }, ['createdAt']))
    })
  })

  return fallbackRows
}

/**
 * Build transient unified history rows from completed challenge winner placements.
 * This fallback is used when review-api does not expose challengeResult rows for
 * a member but challenge-api still records the member's placements.
 * @param {Array<Object>} winnerRows placement winner rows with embedded challenge metadata
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {Set<string>} missingPairKeys track/type pairs that should be synthesized
 * @returns {Array<Object>} transient unified history rows ordered per pair
 */
function buildFallbackHistoryRowsFromChallengeWinners (winnerRows, dimensionLookup, missingPairKeys) {
  const fallbackRowsByChallengeKey = new Map()

  _.forEach(winnerRows || [], (row) => {
    const challenge = row.challenge
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const trackId = String(challenge.trackId)
    const typeId = String(challenge.typeId)
    const pairKey = buildStatsTrackTypeKey(trackId, typeId)
    if (missingPairKeys && !missingPairKeys.has(pairKey)) {
      return
    }

    const trackName = resolveTrackNameFromLookup(dimensionLookup, trackId)
    if (!isSupportedUnifiedHistoryTrack(trackName)) {
      return
    }

    const typeName = resolveTypeNameFromLookup(dimensionLookup, typeId)
    const eventDate = toOptionalDate(challenge.endDate || row.createdAt)
    if (!eventDate) {
      return
    }

    const createdAt = toOptionalDate(row.createdAt) || eventDate
    const placement = toVisiblePlacement(row.placement)
    const challengeId = String(challenge.id || row.challengeId)
    const challengeKey = `${pairKey}::${challengeId}`
    const existing = fallbackRowsByChallengeKey.get(challengeKey)

    if (existing && createdAt <= existing.createdAt) {
      return
    }

    fallbackRowsByChallengeKey.set(challengeKey, {
      trackId,
      typeId,
      trackName,
      typeName,
      challengeId,
      challengeName: challenge.name || null,
      eventDate,
      placement,
      createdAt
    })
  })

  const fallbackRows = []
  const rowsByPairKey = _.groupBy(Array.from(fallbackRowsByChallengeKey.values()), row => buildStatsTrackTypeKey(row.trackId, row.typeId))

  _.forEach(rowsByPairKey, (pairRows) => {
    const orderedRows = _.orderBy(pairRows, [
      row => row.eventDate.getTime(),
      row => row.createdAt.getTime(),
      row => row.challengeId
    ], ['desc', 'desc', 'desc'])

    _.forEach(orderedRows, (row, index) => {
      fallbackRows.push(_.omit({
        ...row,
        mostRecent: index === 0
      }, ['createdAt']))
    })
  })

  return fallbackRows
}

/**
 * Remove track/type pairs from the pending fallback set once rows have been synthesized.
 * @param {Set<string>} pairKeys pending track/type pairs
 * @param {Array<Object>} rows synthesized history rows
 * @returns {Set<string>} unresolved pair keys
 */
function getUnresolvedHistoryPairKeys (pairKeys, rows) {
  const unresolvedPairKeys = new Set(pairKeys || [])
  _.forEach(rows || [], (row) => {
    unresolvedPairKeys.delete(buildStatsTrackTypeKey(row.trackId, row.typeId))
  })
  return unresolvedPairKeys
}

function buildHistoryChallengeKey (row) {
  return `${buildStatsTrackTypeKey(row.trackId, row.typeId)}::${row.challengeId}`
}

/**
 * Merge synthesized history rows without duplicating existing challenge cards.
 * @param {Array<Object>} existingRows persisted and/or synthesized history rows
 * @param {Array<Object>} fallbackRows synthesized candidate rows
 * @returns {Array<Object>} merged history rows
 */
function mergeMissingHistoryRows (existingRows, fallbackRows) {
  const mergedRows = existingRows ? existingRows.slice() : []
  const existingKeys = new Set(_.map(mergedRows, row => buildHistoryChallengeKey(row)))

  _.forEach(fallbackRows || [], (row) => {
    const key = buildHistoryChallengeKey(row)
    if (existingKeys.has(key)) {
      return
    }

    existingKeys.add(key)
    mergedRows.push(row)
  })

  return mergedRows
}

/**
 * Apply the stable ordering expected by the unified history response builders.
 * @param {Array<Object>} rows persisted and/or transient history rows
 * @returns {Array<Object>} rows ordered by mostRecent and event recency
 */
function orderUnifiedHistoryRows (rows) {
  return _.orderBy(rows || [], [
    row => (row.mostRecent ? 1 : 0),
    row => (row.eventDate ? row.eventDate.getTime() : 0),
    row => row.challengeId
  ], ['desc', 'desc', 'desc'])
}

function getDistributionRangeKey (rangeStart) {
  if (rangeStart < 0 || rangeStart > 3900) {
    return null
  }
  if (rangeStart === 0) {
    return 'ratingRange0To099'
  }
  return `ratingRange${rangeStart}To${rangeStart + 99}`
}

function createEmptyDistribution () {
  const distribution = {}
  _.forEach(DISTRIBUTION_RANGES, (rangeStart) => {
    distribution[getDistributionRangeKey(rangeStart)] = 0
  })
  return distribution
}

function toInteger (value) {
  if (_.isNil(value)) {
    return undefined
  }
  const parsed = Number(value)
  if (!Number.isInteger(parsed)) {
    return undefined
  }
  return parsed
}

/**
 * Convert legacy history stats data to response structure.
 * @param {Object} member member data
 * @param {Object} historyStats stats history row with nested develop/dataScience history
 * @param {Array} fields fields to return in response
 * @returns response
 */
function buildLegacyStatsHistoryResponse (member, historyStats, fields) {
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(historyStats.groupId),
    handle: member.handle,
    handleLower: member.handleLower
  }

  if (historyStats.develop && historyStats.develop.length > 0) {
    item.DEVELOP = { subTracks: [] }
    const subTrackGroupData = _.groupBy(historyStats.develop, 'subTrackId')
    _.forEach(subTrackGroupData, (trackHistory, subTrackId) => {
      const subTrackItem = {
        id: subTrackId,
        name: trackHistory[0].subTrack
      }
      subTrackItem.history = _.map(trackHistory, h => ({
        ..._.pick(h, ['challengeName', 'newRating']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        ratingDate: h.ratingDate ? h.ratingDate.getTime() : null
      }))
      item.DEVELOP.subTracks.push(subTrackItem)
    })
  }

  if (historyStats.dataScience && historyStats.dataScience.length > 0) {
    item.DATA_SCIENCE = {}
    const srmHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'SRM')
    const marathonHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'MARATHON_MATCH')
    if (srmHistory.length > 0) {
      item.DATA_SCIENCE.SRM = {}
      item.DATA_SCIENCE.SRM.history = _.map(srmHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
    if (marathonHistory.length > 0) {
      item.DATA_SCIENCE.MARATHON_MATCH = {}
      item.DATA_SCIENCE.MARATHON_MATCH.history = _.map(marathonHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
  }
  return fields ? _.pick(item, fields) : item
}

/**
 * Get distribution statistics from legacy table.
 * @param {Object} query the query parameters
 * @param {Array} fields selected fields
 * @returns {Object} the distribution statistics
 */
async function getLegacyDistribution (query, fields) {
  const whereConditions = []
  if (query.track) {
    whereConditions.push(Prisma.sql`UPPER("track") LIKE ${`%${query.track.toUpperCase()}%`}`)
  }
  if (query.subTrack) {
    whereConditions.push(Prisma.sql`UPPER("subTrack") LIKE ${`%${query.subTrack.toUpperCase()}%`}`)
  }

  const whereClause = whereConditions.length > 0
    ? Prisma.sql`WHERE ${joinSqlConditions(whereConditions)}`
    : Prisma.empty

  const items = await prisma.$queryRaw`
    SELECT *
    FROM "members"."distributionStats"
    ${whereClause}
  `

  if (!items || items.length === 0) {
    throw new errors.NotFoundError('No member distribution statistics is found.')
  }

  const records = []
  _.forEach(items, row => {
    const record = _.pick(row, DISTRIBUTION_FIELDS)
    record.distribution = createEmptyDistribution()
    _.forEach(DISTRIBUTION_RANGES, (rangeStart) => {
      const key = getDistributionRangeKey(rangeStart)
      record.distribution[key] = Number(row[key] || 0)
    })
    records.push(record)
  })

  let result = { track: query.track, subTrack: query.subTrack, distribution: {} }
  _.forEach(records, (record) => {
    _.forIn(record.distribution, (value, key) => {
      if (!result.distribution[key]) {
        result.distribution[key] = 0
      }
      result.distribution[key] += Number(value)
    })
    if (record.createdAt && (!result.createdAt || new Date(record.createdAt) < result.createdAt)) {
      result.createdAt = new Date(record.createdAt)
      result.createdBy = record.createdBy
    }
    if (record.updatedAt && (!result.updatedAt || new Date(record.updatedAt) > result.updatedAt)) {
      result.updatedAt = new Date(record.updatedAt)
      result.updatedBy = record.updatedBy
    }
  })

  if (fields) {
    result = _.pick(result, fields)
  }
  return result
}

/**
 * Load one legacy member stats aggregate row with nested legacy stats details.
 * @param {BigInt} userId member user id
 * @param {String|Number} groupId requested group id
 * @returns {Object|null} member stats row in legacy shape
 */
async function getLegacyMemberStatsRow (userId, groupId) {
  const isPrivate = String(groupId) !== String(config.PUBLIC_GROUP_ID)
  const statsRows = await prisma.$queryRaw`
    SELECT ms.*
    FROM "members"."memberStats" ms
    WHERE ms."userId" = ${userId}
      AND ms."isPrivate" = ${isPrivate}
    ORDER BY
      CASE
        WHEN EXISTS (SELECT 1 FROM "members"."memberDevelopStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberDesignStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberCopilotStats" cs WHERE cs."memberStatsId" = ms."id")
          THEN 0
        ELSE 1
      END,
      ms."id" ASC
    LIMIT 1
  `

  const stat = _.head(statsRows)
  if (!stat) {
    return null
  }

  const numericGroupId = _.toNumber(groupId)
  if (!Number.isNaN(numericGroupId)) {
    stat.groupId = numericGroupId
  }

  const [designRows, developRows, dataScienceRows, copilotRows] = await Promise.all([
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDesignStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDataScienceStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberCopilotStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `
  ])

  const design = _.head(designRows)
  if (design) {
    const designItems = await prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDesignStatsItem"
      WHERE "designStatsId" = ${design.id}
      ORDER BY "subTrackId" ASC, "id" ASC
    `
    design.items = designItems
  }
  stat.design = design

  const develop = _.head(developRows)
  if (develop) {
    const developItems = await prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopStatsItem"
      WHERE "developStatsId" = ${develop.id}
      ORDER BY "subTrackId" ASC, "id" ASC
    `
    develop.items = developItems
  }
  stat.develop = develop

  const dataScience = _.head(dataScienceRows)
  if (dataScience) {
    const [srmRows, marathonRows] = await Promise.all([
      prisma.$queryRaw`
        SELECT *
        FROM "members"."memberSrmStats"
        WHERE "dataScienceStatsId" = ${dataScience.id}
        ORDER BY "id" ASC
        LIMIT 1
      `,
      prisma.$queryRaw`
        SELECT *
        FROM "members"."memberMarathonStats"
        WHERE "dataScienceStatsId" = ${dataScience.id}
        ORDER BY "id" ASC
        LIMIT 1
      `
    ])
    const srm = _.head(srmRows)
    if (srm) {
      const [challengeDetails, divisions] = await Promise.all([
        prisma.$queryRaw`
          SELECT *
          FROM "members"."memberSrmChallengeDetail"
          WHERE "srmStatsId" = ${srm.id}
          ORDER BY "id" ASC
        `,
        prisma.$queryRaw`
          SELECT *
          FROM "members"."memberSrmDivisionDetail"
          WHERE "srmStatsId" = ${srm.id}
          ORDER BY "divisionName" ASC, "levelName" ASC, "id" ASC
        `
      ])
      srm.challengeDetails = challengeDetails
      srm.divisions = divisions
    }
    dataScience.srm = srm
    dataScience.marathon = _.head(marathonRows)
  }
  stat.dataScience = dataScience
  stat.copilot = _.head(copilotRows)

  return stat
}

/**
 * Load one legacy history stats aggregate row with nested history details.
 * @param {BigInt} userId member user id
 * @param {String|Number} groupId requested group id
 * @returns {Object|null} history stats row in legacy shape
 */
async function getLegacyHistoryStatsRow (userId, groupId) {
  const isPrivate = String(groupId) !== String(config.PUBLIC_GROUP_ID)
  const groupIdValue = toInteger(groupId)

  if (isPrivate && _.isNil(groupIdValue)) {
    return null
  }

  const whereConditions = [
    Prisma.sql`"userId" = ${userId}`,
    Prisma.sql`"isPrivate" = ${isPrivate}`
  ]
  if (isPrivate) {
    whereConditions.push(Prisma.sql`"groupId" = ${groupIdValue}`)
  }

  const historyRows = await prisma.$queryRaw`
    SELECT *
    FROM "members"."memberHistoryStats"
    WHERE ${joinSqlConditions(whereConditions)}
    ORDER BY "id" ASC
    LIMIT 1
  `

  const history = _.head(historyRows)
  if (!history) {
    return null
  }

  if (!isPrivate) {
    history.groupId = _.toNumber(groupId)
  }

  const [developRows, dataScienceRows] = await Promise.all([
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopHistoryStats"
      WHERE "historyStatsId" = ${history.id}
      ORDER BY "subTrackId" ASC, "ratingDate" DESC, "id" DESC
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDataScienceHistoryStats"
      WHERE "historyStatsId" = ${history.id}
      ORDER BY "subTrack" ASC, "date" DESC, "id" DESC
    `
  ])

  history.develop = developRows
  history.dataScience = dataScienceRows
  return history
}

function buildUnifiedHistoryRecordsFromPayload (payload, dimensionLookup) {
  const data = payload || {}
  const records = []
  const pushHistoryRecord = (item, fallbackTrackId, fallbackTypeId) => {
    const trackId = resolveTrackIdFromLookup(dimensionLookup, item.trackId || fallbackTrackId)
    const typeId = resolveTypeIdFromLookup(dimensionLookup, item.typeId || fallbackTypeId)
    if (!trackId || !typeId) {
      return
    }
    const eventDate = toOptionalDate(item.eventDate || item.date || item.ratingDate)
    if (!eventDate || _.isNil(item.challengeId)) {
      return
    }
    records.push(_.omitBy({
      trackId,
      typeId,
      challengeId: String(item.challengeId),
      oldRating: toOptionalInt(item.oldRating),
      newRating: toOptionalInt(item.newRating),
      oldGlobalRank: toOptionalInt(item.oldGlobalRank),
      newGlobalRank: toOptionalInt(item.newGlobalRank),
      oldCountryRank: toOptionalInt(item.oldCountryRank),
      newCountryRank: toOptionalInt(item.newCountryRank),
      oldSchoolRank: toOptionalInt(item.oldSchoolRank),
      newSchoolRank: toOptionalInt(item.newSchoolRank),
      eventDate
    }, _.isUndefined))
  }

  if (!_.isNil(data.challengeId)) {
    pushHistoryRecord(data, data.trackId, data.typeId)
  }

  if (_.isArray(data.history) && data.history.length > 0) {
    _.forEach(data.history, (item) => {
      pushHistoryRecord(item, data.trackId, data.typeId)
    })
  }

  return _.values(_.keyBy(records, record => `${record.trackId}::${record.typeId}::${record.challengeId}`))
}

/**
 * Attach resolved canonical track/type labels to unified stats rows before response building.
 * @param {Array<Object>} rows unified stats or history rows from the database
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Array<Object>} rows annotated with trackName and typeName
 */
function annotateUnifiedDimensionRows (rows, dimensionLookup) {
  return _.map(rows || [], row => ({
    ...row,
    trackName: resolveTrackNameFromLookup(dimensionLookup, row.trackId),
    typeName: resolveTypeNameFromLookup(dimensionLookup, row.typeId)
  }))
}

/**
 * Resolve optional unified stats filter parameters into stored UUID ids.
 * A missing filter remains undefined; an invalid non-empty filter resolves to undefined
 * with its corresponding has* flag still true so callers can short-circuit to no results.
 * @param {Object} query request query params
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Object} resolved filter payload
 */
function resolveUnifiedDimensionFilters (query, dimensionLookup) {
  const hasTrackFilter = !_.isNil(query.trackId) && String(query.trackId).trim() !== ''
  const hasTypeFilter = !_.isNil(query.typeId) && String(query.typeId).trim() !== ''

  return {
    hasTrackFilter,
    hasTypeFilter,
    trackId: hasTrackFilter ? resolveTrackIdFromLookup(dimensionLookup, query.trackId) : undefined,
    typeId: hasTypeFilter ? resolveTypeIdFromLookup(dimensionLookup, query.typeId) : undefined
  }
}

function getUniqueTrackTypePairs (records) {
  return _.values(_.keyBy(_.map(records, record => ({
    trackId: record.trackId,
    typeId: record.typeId
  })), pair => `${pair.trackId}::${pair.typeId}`))
}

/**
 * Recompute the mostRecent marker for each affected (trackId, typeId) pair.
 * Exactly one row per pair is marked as mostRecent=true when rows exist.
 * The latest row's newRating is aligned with the current memberStats rating when available.
 * The latest row's oldRating is aligned with the prior history event for the same pair.
 *
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId user id
 * @param {Array} records history records that determine affected pairs
 * @param {String} operatorId operator id
 */
async function refreshMostRecentHistoryFlags (tx, userId, records, operatorId) {
  const pairs = getUniqueTrackTypePairs(records)
  for (const pair of pairs) {
    await tx.memberStatsHistory.updateMany({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId,
        mostRecent: true
      },
      data: {
        mostRecent: false,
        updatedBy: operatorId
      }
    })

    const latest = await tx.memberStatsHistory.findFirst({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId
      },
      orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
      select: { id: true }
    })

    if (latest) {
      const currentStats = await tx.memberStats.findFirst({
        where: {
          userId,
          trackId: pair.trackId,
          typeId: pair.typeId
        },
        select: {
          rating: true
        }
      })
      const previous = await tx.memberStatsHistory.findFirst({
        where: {
          userId,
          trackId: pair.trackId,
          typeId: pair.typeId
        },
        orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
        skip: 1,
        select: {
          newRating: true
        }
      })

      const latestUpdateData = {
        mostRecent: true,
        oldRating: previous ? previous.newRating : null,
        updatedBy: operatorId
      }
      if (currentStats) {
        latestUpdateData.newRating = currentStats.rating
      }

      await tx.memberStatsHistory.update({
        where: { id: latest.id },
        data: latestUpdateData
      })
    }
  }
}

/**
 * Synchronize newRating on the most recent history row per (trackId, typeId) pair
 * with the current value in memberStats.rating.
 *
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId user id
 * @param {Array} records stats records that determine affected pairs
 * @param {String} operatorId operator id
 */
async function syncMostRecentHistoryRatings (tx, userId, records, operatorId) {
  const pairs = getUniqueTrackTypePairs(records)
  for (const pair of pairs) {
    const currentStats = await tx.memberStats.findFirst({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId
      },
      select: {
        rating: true
      }
    })
    if (!currentStats) {
      continue
    }

    await tx.memberStatsHistory.updateMany({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId,
        mostRecent: true
      },
      data: {
        newRating: currentStats.rating,
        updatedBy: operatorId
      }
    })
  }
}

/**
 * Get distribution statistics.
 * @param {Object} query the query parameters
 * @returns {Object} the distribution statistics
 */
async function getDistribution (query) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, DISTRIBUTION_FIELDS_NO_DATE) || DISTRIBUTION_FIELDS_NO_DATE
  if (USE_LEGACY_STATS_READS) {
    return getLegacyDistribution(query, fields)
  }

  logger.info(`Calculating distribution on-the-fly for track='${query.track || ''}' subTrack='${query.subTrack || ''}'`)
  const dimensionLookup = await getChallengeDimensionLookup()
  const hasTrackFilter = !_.isNil(query.track) && String(query.track).trim() !== ''
  const hasTypeFilter = !_.isNil(query.subTrack) && String(query.subTrack).trim() !== ''
  const trackId = hasTrackFilter ? resolveTrackIdFromLookup(dimensionLookup, query.track) : undefined
  const typeId = hasTypeFilter ? resolveTypeIdFromLookup(dimensionLookup, query.subTrack) : undefined

  if ((hasTrackFilter && !trackId) || (hasTypeFilter && !typeId)) {
    throw new errors.NotFoundError('No member distribution statistics is found.')
  }

  const whereConditions = [Prisma.sql`"rating" IS NOT NULL`]
  if (trackId) {
    whereConditions.push(Prisma.sql`"trackId" = ${trackId}`)
  }
  if (typeId) {
    whereConditions.push(Prisma.sql`"typeId" = ${typeId}`)
  }

  const rows = await prisma.$queryRaw`
    SELECT
      (FLOOR("rating" / 100.0)::int * 100) AS "rangeStart",
      COUNT(*)::int AS "count"
    FROM "members"."memberStats"
    WHERE ${joinSqlConditions(whereConditions)}
    GROUP BY (FLOOR("rating" / 100.0)::int * 100)
    ORDER BY "rangeStart" ASC
  `

  if (!rows || rows.length === 0) {
    const matchingStatsRow = await prisma.memberStats.findFirst({
      where: _.omitBy({
        trackId,
        typeId
      }, _.isUndefined),
      select: {
        id: true
      }
    })

    if (!matchingStatsRow) {
      throw new errors.NotFoundError('No member distribution statistics is found.')
    }

    let emptyResult = {
      track: query.track,
      subTrack: query.subTrack,
      distribution: createEmptyDistribution()
    }

    if (fields) {
      emptyResult = _.pick(emptyResult, fields)
    }

    return emptyResult
  }

  const distribution = createEmptyDistribution()
  _.forEach(rows, (row) => {
    const rangeStart = _.toInteger(row.rangeStart)
    const key = getDistributionRangeKey(rangeStart)
    if (key) {
      distribution[key] = Number(row.count)
    }
  })

  let result = {
    track: query.track,
    subTrack: query.subTrack,
    distribution
  }

  if (fields) {
    result = _.pick(result, fields)
  }
  return result
}

getDistribution.schema = {
  query: Joi.object().keys({
    track: Joi.string(),
    subTrack: Joi.string(),
    fields: Joi.string()
  })
}

/**
 * Get history statistics for completed challenges.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the history statistics
 */
async function getHistoryStats (currentUser, handle, query) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, HISTORY_STATS_FIELDS) || HISTORY_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)
  let result = []

  if (USE_LEGACY_STATS_READS) {
    const overallStat = []
    for (const groupId of groupIds) {
      const statsDb = await getLegacyHistoryStatsRow(member.userId, groupId)
      if (!_.isNil(statsDb)) {
        overallStat.push(statsDb)
      }
    }
    result = _.map(overallStat, t => buildLegacyStatsHistoryResponse(member, t, fields))
  } else {
    const dimensionLookup = await getChallengeDimensionLookup()
    const challengeClient = prismaManager.getChallengesClient()
    const where = {
      userId: member.userId
    }
    const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
    if (hasTrackFilter && !trackId) {
      return []
    }
    if (hasTypeFilter && !typeId) {
      return []
    }
    if (trackId) {
      where.trackId = trackId
    }
    if (typeId) {
      where.typeId = typeId
    }

    const historyRows = await prisma.memberStatsHistory.findMany({
      where,
      orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
    })
    const aggregateRows = await prisma.memberStats.findMany({
      where,
      select: {
        trackId: true,
        typeId: true
      }
    })

    const overallStat = []
    const visiblePairKeys = getVisibleUnifiedHistoryPairKeys(aggregateRows, dimensionLookup)
    const missingPairKeys = getMissingUnifiedHistoryPairKeys(aggregateRows, historyRows, dimensionLookup)

    if (historyRows.length > 0 || missingPairKeys.size > 0) {
      let reviewRows = []
      let unresolvedPairKeys = new Set(missingPairKeys)
      if (unresolvedPairKeys.size > 0 && reviewDb) {
        reviewRows = await fetchReviewChallengeResultsForMember(reviewDb, member.userId)
      }

      const challengeMetadataById = await fetchChallengeMetadataMap(
        challengeClient,
        _.uniq(_.map(historyRows, row => row.challengeId).concat(_.map(reviewRows, row => row.challengeId)))
      )

      let annotatedRows = filterUnifiedHistoryRowsToCompletedChallenges(
        enrichUnifiedHistoryRowsWithChallengeMetadata(
          annotateUnifiedDimensionRows(historyRows, dimensionLookup),
          challengeMetadataById
        ),
        challengeMetadataById
      )

      if (unresolvedPairKeys.size > 0 && reviewRows.length > 0) {
        const reviewFallbackRows = buildFallbackHistoryRowsFromReviewResults(
          reviewRows,
          challengeMetadataById,
          dimensionLookup,
          unresolvedPairKeys
        )
        annotatedRows = mergeMissingHistoryRows(annotatedRows, reviewFallbackRows)
        unresolvedPairKeys = getUnresolvedHistoryPairKeys(unresolvedPairKeys, reviewFallbackRows)
      }

      if (missingPairKeys.size > 0 || historyRowsNeedPlacementEnrichment(annotatedRows)) {
        const winnerRows = await fetchChallengeWinnerResultsForMember(challengeClient, member.userId)

        annotatedRows = mergeHistoryPlacementsFromChallengeWinners(annotatedRows, winnerRows)
        const winnerFallbackPairKeys = new Set(
          Array.from(visiblePairKeys).concat(
            _.map(annotatedRows, row => buildStatsTrackTypeKey(row.trackId, row.typeId))
          )
        )

        const winnerFallbackRows = buildFallbackHistoryRowsFromChallengeWinners(
          winnerRows,
          dimensionLookup,
          winnerFallbackPairKeys
        )
        annotatedRows = mergeMissingHistoryRows(annotatedRows, winnerFallbackRows)
        unresolvedPairKeys = getUnresolvedHistoryPairKeys(unresolvedPairKeys, winnerFallbackRows)
      }

      const orderedRows = orderUnifiedHistoryRows(annotatedRows)
      if (orderedRows.length > 0) {
        _.forEach(groupIds, (groupId) => {
          const scopedRows = _.map(orderedRows, row => ({ ...row, groupId: _.toNumber(groupId) }))
          overallStat.push(scopedRows)
        })
      }
    }

    result = _.map(overallStat, rows => prismaHelper.buildUnifiedStatsHistoryResponse(member, rows, fields))
  }

  if (!helper.canManageMember(currentUser, member)) {
    result = _.map(result, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return result
}

getHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    groupIds: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    fields: Joi.string()
  })
}

/**
 * Create history stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the history stats data to create
 * @returns {Object} the created history stats
 */
async function createHistoryStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const operatorId = currentUser.userId || currentUser.sub
  const dimensionLookup = await getChallengeDimensionLookup()
  const challengeClient = prismaManager.getChallengesClient()
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data, dimensionLookup)
  if (!unifiedHistoryRecords || unifiedHistoryRecords.length === 0) {
    throw new errors.BadRequestError('No valid history records provided for unified history stats.')
  }

  logger.info(`Creating unified history stats for userId=${member.userId.toString()} with ${unifiedHistoryRecords.length} record(s)`)

  await prisma.$transaction(async (tx) => {
    const existingClauses = _.map(unifiedHistoryRecords, record => ({
      userId: member.userId,
      trackId: record.trackId,
      typeId: record.typeId,
      challengeId: record.challengeId
    }))

    const existingCount = await tx.memberStatsHistory.count({
      where: { OR: existingClauses }
    })
    if (existingCount > 0) {
      throw new errors.BadRequestError('History stats already exists')
    }

    await tx.memberStatsHistory.createMany({
      data: _.map(unifiedHistoryRecords, record => ({
        ...record,
        userId: member.userId,
        createdBy: operatorId,
        updatedBy: operatorId
      }))
    })

    await refreshMostRecentHistoryFlags(tx, member.userId, unifiedHistoryRecords, operatorId)
  })

  const createdRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId: member.userId,
      OR: _.map(unifiedHistoryRecords, record => ({
        trackId: record.trackId,
        typeId: record.typeId,
        challengeId: record.challengeId
      }))
    },
    orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
  })

  const challengeMetadataById = await fetchChallengeMetadataMap(challengeClient, _.map(createdRows, row => row.challengeId))
  const scopedRows = _.map(enrichUnifiedHistoryRowsWithChallengeMetadata(
    annotateUnifiedDimensionRows(createdRows, dimensionLookup),
    challengeMetadataById
  ), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsHistoryResponse(member, scopedRows, HISTORY_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

createHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challengeId: Joi.alternatives().try(Joi.string(), Joi.number()),
    mostRecent: Joi.boolean(),
    oldRating: Joi.number(),
    newRating: Joi.number(),
    oldGlobalRank: Joi.number(),
    newGlobalRank: Joi.number(),
    oldCountryRank: Joi.number(),
    newCountryRank: Joi.number(),
    oldSchoolRank: Joi.number(),
    newSchoolRank: Joi.number(),
    eventDate: Joi.number().positive(),
    date: Joi.number().positive(),
    ratingDate: Joi.number().positive(),
    history: Joi.array().items(Joi.object().keys({
      trackId: Joi.string(),
      typeId: Joi.string(),
      challengeId: Joi.alternatives().try(Joi.string(), Joi.number()).required(),
      mostRecent: Joi.boolean(),
      oldRating: Joi.number(),
      newRating: Joi.number(),
      oldGlobalRank: Joi.number(),
      newGlobalRank: Joi.number(),
      oldCountryRank: Joi.number(),
      newCountryRank: Joi.number(),
      oldSchoolRank: Joi.number(),
      newSchoolRank: Joi.number(),
      eventDate: Joi.number().positive(),
      date: Joi.number().positive(),
      ratingDate: Joi.number().positive()
    }))
  }).required()
}

/**
 * Partially update history stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the history stats data to update
 * @returns {Object} the updated history stats
 */
async function partiallyUpdateHistoryStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const operatorId = currentUser.userId || currentUser.sub
  const dimensionLookup = await getChallengeDimensionLookup()
  const challengeClient = prismaManager.getChallengesClient()
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data, dimensionLookup)
  if (!unifiedHistoryRecords || unifiedHistoryRecords.length === 0) {
    throw new errors.BadRequestError('No valid history records provided for unified history stats.')
  }

  await prisma.$transaction(async (tx) => {
    logger.info(`Upserting unified history stats for userId=${member.userId.toString()} with ${unifiedHistoryRecords.length} record(s)`)
    for (const record of unifiedHistoryRecords) {
      const existingRecord = await tx.memberStatsHistory.findFirst({
        where: {
          userId: member.userId,
          trackId: record.trackId,
          typeId: record.typeId,
          challengeId: record.challengeId
        }
      })
      if (existingRecord) {
        await tx.memberStatsHistory.update({
          where: { id: existingRecord.id },
          data: {
            ..._.omit(record, ['trackId', 'typeId', 'challengeId']),
            updatedBy: operatorId
          }
        })
      } else {
        await tx.memberStatsHistory.create({
          data: {
            ...record,
            userId: member.userId,
            createdBy: operatorId,
            updatedBy: operatorId
          }
        })
      }
    }

    await refreshMostRecentHistoryFlags(tx, member.userId, unifiedHistoryRecords, operatorId)
  })

  const updatedRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId: member.userId,
      OR: _.map(unifiedHistoryRecords, record => ({
        trackId: record.trackId,
        typeId: record.typeId,
        challengeId: record.challengeId
      }))
    },
    orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
  })

  const challengeMetadataById = await fetchChallengeMetadataMap(challengeClient, _.map(updatedRows, row => row.challengeId))
  const scopedRows = _.map(enrichUnifiedHistoryRowsWithChallengeMetadata(
    annotateUnifiedDimensionRows(updatedRows, dimensionLookup),
    challengeMetadataById
  ), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsHistoryResponse(member, scopedRows, HISTORY_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

partiallyUpdateHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challengeId: Joi.alternatives().try(Joi.string(), Joi.number()),
    mostRecent: Joi.boolean(),
    oldRating: Joi.number(),
    newRating: Joi.number(),
    oldGlobalRank: Joi.number(),
    newGlobalRank: Joi.number(),
    oldCountryRank: Joi.number(),
    newCountryRank: Joi.number(),
    oldSchoolRank: Joi.number(),
    newSchoolRank: Joi.number(),
    eventDate: Joi.number().positive(),
    date: Joi.number().positive(),
    ratingDate: Joi.number().positive(),
    history: Joi.array().items(Joi.object().keys({
      trackId: Joi.string(),
      typeId: Joi.string(),
      challengeId: Joi.alternatives().try(Joi.string(), Joi.number()).required(),
      mostRecent: Joi.boolean(),
      oldRating: Joi.number(),
      newRating: Joi.number(),
      oldGlobalRank: Joi.number(),
      newGlobalRank: Joi.number(),
      oldCountryRank: Joi.number(),
      newCountryRank: Joi.number(),
      oldSchoolRank: Joi.number(),
      newSchoolRank: Joi.number(),
      eventDate: Joi.number().positive(),
      date: Joi.number().positive(),
      ratingDate: Joi.number().positive()
    }))
  }).required()
}

/**
 * Load member statistics from unified table.
 * @param {Object} member member row
 * @param {Array} groupIds requested group ids
 * @param {Object} query the query parameters
 * @param {Array} fields fields to return in response
 * @returns {Array} member statistics
 */
async function getUnifiedMemberStats (member, groupIds, query, fields) {
  const dimensionLookup = await getChallengeDimensionLookup()
  const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
  const stats = []

  if (hasTrackFilter && !trackId) {
    return stats
  }
  if (hasTypeFilter && !typeId) {
    return stats
  }

  for (const groupId of groupIds) {
    const where = {
      userId: member.userId,
      isPrivate: String(groupId) !== String(config.PUBLIC_GROUP_ID)
    }
    if (trackId) {
      where.trackId = trackId
    }
    if (typeId) {
      where.typeId = typeId
    }

    const unifiedStats = await prisma.memberStats.findMany({
      where,
      include: prismaHelper.unifiedStatsIncludeParams
    })

    if (unifiedStats && unifiedStats.length > 0) {
      const scopedStats = _.map(annotateUnifiedDimensionRows(unifiedStats, dimensionLookup), stat => ({
        ...stat,
        groupId: _.toNumber(groupId)
      }))
      stats.push(prismaHelper.buildUnifiedStatsResponse(member, scopedStats, fields))
    }
  }

  return stats
}

/**
 * Load member statistics using legacy mapper from memberStats and nested legacy tables.
 * @param {Object} member member row
 * @param {Array} groupIds requested group ids
 * @param {Array} fields fields to return in response
 * @returns {Array} member statistics
 */
async function getLegacyMemberStats (member, groupIds, fields) {
  const stats = []
  for (const groupId of groupIds) {
    const stat = await getLegacyMemberStatsRow(member.userId, groupId)
    if (!_.isNil(stat)) {
      stats.push(prismaHelper.buildStatsResponse(member, stat, fields))
    }
  }
  return stats
}

/**
 * Get member statistics.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member statistics
 */
async function getMemberStats (currentUser, handle, query, throwError) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, MEMBER_STATS_FIELDS) || MEMBER_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)
  let stats = []
  if (USE_LEGACY_STATS_READS) {
    stats = await getLegacyMemberStats(member, groupIds, fields)
    if (stats.length === 0) {
      logger.warn(`Legacy member stats lookup returned no rows for handle='${handle}', groupIds='${groupIds}'. Falling back to unified memberStats lookup.`)
      stats = await getUnifiedMemberStats(member, groupIds, query, fields)
    }
  } else {
    const dimensionLookup = await getChallengeDimensionLookup()
    const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
    if (hasTrackFilter && !trackId) {
      return []
    }
    if (hasTypeFilter && !typeId) {
      return []
    }
    for (const groupId of groupIds) {
      const where = {
        userId: member.userId,
        isPrivate: groupId !== config.PUBLIC_GROUP_ID
      }
      if (trackId) {
        where.trackId = trackId
      }
      if (typeId) {
        where.typeId = typeId
      }

      const unifiedStats = await prisma.memberStats.findMany({
        where,
        include: prismaHelper.unifiedStatsIncludeParams
      })

      if (unifiedStats && unifiedStats.length > 0) {
        const scopedStats = _.map(annotateUnifiedDimensionRows(unifiedStats, dimensionLookup), stat => ({
          ...stat,
          groupId: _.toNumber(groupId)
        }))
        stats.push(prismaHelper.buildUnifiedStatsResponse(member, scopedStats, fields))
      }
    }
  }

  if (throwError && stats.length === 0) {
    throw new errors.NotFoundError('Member stats not found')
  }

  if (!helper.canManageMember(currentUser, member)) {
    stats = _.map(stats, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return stats
}

getMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    groupIds: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    fields: Joi.string()
  }),
  throwError: Joi.boolean()
}

/**
 * Create member stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the stats data to create
 * @returns {Object} the updated member stats
 */
async function createMemberStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const isPrivate = groupIds[0] !== config.PUBLIC_GROUP_ID
  const rawData = _.cloneDeep(data)
  const dimensionLookup = await getChallengeDimensionLookup()
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate, dimensionLookup)
  const legacyMaxRatingData = isLegacyMaxRatingPayload(rawData.maxRating) ? rawData.maxRating : null

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    // get statistics by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: false }
    })
  } else {
    // get statistics private by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: true }
    })
  }

  if (existingStat) {
    throw new errors.BadRequestError('Member stats already exists')
  }

  if (!unifiedRecords || unifiedRecords.length === 0) {
    throw new errors.BadRequestError('No valid unified member stats payload provided.')
  }
  const operatorId = currentUser.userId || currentUser.sub
  logger.info(`Creating unified memberStats rows for userId=${member.userId.toString()} with ${unifiedRecords.length} row(s)`)
  await prisma.$transaction(async (tx) => {
    for (const record of unifiedRecords) {
      await tx.memberStats.create({
        data: {
          ...record,
          userId: member.userId,
          createdBy: operatorId,
          updatedBy: operatorId
        }
      })
    }
    await syncMostRecentHistoryRatings(tx, member.userId, unifiedRecords, operatorId)

    if (legacyMaxRatingData) {
      await prismaHelper.updateOrCreateModel(legacyMaxRatingData, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }
  })

  const allStats = await prisma.memberStats.findMany({
    where: { userId: member.userId, isPrivate },
    include: prismaHelper.unifiedStatsIncludeParams
  })
  const scopedStats = _.map(annotateUnifiedDimensionRows(allStats, dimensionLookup), stat => ({
    ...stat,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsResponse(member, scopedStats, MEMBER_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  if (legacyMaxRatingData) {
    result.maxRating = {
      ...result.maxRating,
      ...legacyMaxRatingData
    }
  }
  return result
}

createMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challenges: Joi.number().positive(),
    wins: Joi.number().positive(),
    mostRecentSubmission: Joi.number().positive(),
    mostRecentEventDate: Joi.number().positive(),
    rating: Joi.number(),
    avgRank: Joi.number(),
    avgNumSubmissions: Joi.number(),
    bestRank: Joi.number(),
    globalRank: Joi.number(),
    countryRank: Joi.number(),
    schoolRank: Joi.number(),
    volatility: Joi.number(),
    minRating: Joi.number(),
    topFiveFinishes: Joi.number(),
    topTenFinishes: Joi.number(),
    records: Joi.array().items(Joi.object().keys({
      trackId: Joi.string().required(),
      typeId: Joi.string().required(),
      challenges: Joi.number(),
      wins: Joi.number(),
      mostRecentSubmission: Joi.number().positive(),
      mostRecentEventDate: Joi.number().positive(),
      rating: Joi.number(),
      avgRank: Joi.number(),
      avgNumSubmissions: Joi.number(),
      bestRank: Joi.number(),
      globalRank: Joi.number(),
      countryRank: Joi.number(),
      schoolRank: Joi.number(),
      volatility: Joi.number(),
      maxRating: Joi.number(),
      minRating: Joi.number(),
      topFiveFinishes: Joi.number(),
      topTenFinishes: Joi.number()
    })),
    maxRating: Joi.alternatives().try(
      Joi.object().keys({
        rating: Joi.number().positive().required(),
        track: Joi.string(),
        subTrack: Joi.string(),
        ratingColor: Joi.string().required()
      }),
      Joi.number()
    )
  }).required()
}

/**
 * Partially update member stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the stats data to update
 * @returns {Object} the updated member stats
 */
async function partiallyUpdateMemberStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const isPrivate = groupIds[0] !== config.PUBLIC_GROUP_ID
  const rawData = _.cloneDeep(data)
  const dimensionLookup = await getChallengeDimensionLookup()
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate, dimensionLookup, { partial: true })
  const legacyMaxRatingData = isLegacyMaxRatingPayload(rawData.maxRating) ? rawData.maxRating : null

  if ((!unifiedRecords || unifiedRecords.length === 0) && !legacyMaxRatingData) {
    throw new errors.BadRequestError('No valid unified member stats update payload provided.')
  }

  const operatorId = currentUser.userId || currentUser.sub

  await prisma.$transaction(async (tx) => {
    logger.info(`Upserting unified memberStats rows for userId=${member.userId.toString()} with ${unifiedRecords.length} row(s)`)
    for (const record of unifiedRecords) {
      await tx.memberStats.upsert({
        where: {
          userId_trackId_typeId: {
            userId: member.userId,
            trackId: record.trackId,
            typeId: record.typeId
          }
        },
        create: {
          ...record,
          userId: member.userId,
          createdBy: operatorId,
          updatedBy: operatorId
        },
        update: {
          ..._.omit(record, ['trackId', 'typeId', 'isPrivate']),
          isPrivate: record.isPrivate,
          updatedBy: operatorId
        }
      })
    }
    if (unifiedRecords.length > 0) {
      await syncMostRecentHistoryRatings(tx, member.userId, unifiedRecords, operatorId)
    }

    if (legacyMaxRatingData) {
      await prismaHelper.updateOrCreateModel(legacyMaxRatingData, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }
  })

  const updatedRows = await prisma.memberStats.findMany({
    where: { userId: member.userId, isPrivate },
    include: prismaHelper.unifiedStatsIncludeParams
  })
  const scopedRows = _.map(annotateUnifiedDimensionRows(updatedRows, dimensionLookup), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsResponse(member, scopedRows, MEMBER_STATS_FIELDS)
  if (legacyMaxRatingData) {
    result.maxRating = {
      ...result.maxRating,
      ...legacyMaxRatingData
    }
  }
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

partiallyUpdateMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challenges: Joi.number().positive(),
    wins: Joi.number().positive(),
    mostRecentSubmission: Joi.number().positive(),
    mostRecentEventDate: Joi.number().positive(),
    rating: Joi.number(),
    avgRank: Joi.number(),
    avgNumSubmissions: Joi.number(),
    bestRank: Joi.number(),
    globalRank: Joi.number(),
    countryRank: Joi.number(),
    schoolRank: Joi.number(),
    volatility: Joi.number(),
    minRating: Joi.number(),
    topFiveFinishes: Joi.number(),
    topTenFinishes: Joi.number(),
    records: Joi.array().items(Joi.object().keys({
      trackId: Joi.string().required(),
      typeId: Joi.string().required(),
      challenges: Joi.number(),
      wins: Joi.number(),
      mostRecentSubmission: Joi.number().positive(),
      mostRecentEventDate: Joi.number().positive(),
      rating: Joi.number(),
      avgRank: Joi.number(),
      avgNumSubmissions: Joi.number(),
      bestRank: Joi.number(),
      globalRank: Joi.number(),
      countryRank: Joi.number(),
      schoolRank: Joi.number(),
      volatility: Joi.number(),
      maxRating: Joi.number(),
      minRating: Joi.number(),
      topFiveFinishes: Joi.number(),
      topTenFinishes: Joi.number()
    })),
    maxRating: Joi.alternatives().try(
      Joi.object().keys({
        rating: Joi.number().positive().required(),
        track: Joi.string(),
        subTrack: Joi.string(),
        ratingColor: Joi.string().required()
      }),
      Joi.number()
    )
  }).required()
}

/**
 * Refresh unified memberStats aggregates for a member from completed review-api challenge
 * results. Challenge metadata is resolved from challenge-api so counts and timestamps are
 * grouped by the existing unified track/type identifiers used in memberStats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data optional payload echoed in the summary response with a string challengeId
 * @returns {Object} summary describing the refresh work that was completed
 * @throws {errors.ForbiddenError} if the caller is not allowed to manage the member
 */
async function refreshMemberStats (currentUser, handle, data) {
  const payload = data || {}
  const member = await helper.getMemberByHandle(handle)
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const operatorId = currentUser.userId || currentUser.sub
  const reviewDbClient = getReviewDbClientOrThrow()
  const challengeClient = prismaManager.getChallengesClient()
  const reviewRows = await fetchReviewChallengeResultsForMember(reviewDbClient, member.userId)

  if (reviewRows.length === 0) {
    return {
      handle,
      refreshed: true,
      challengeId: normalizeChallengeIdForResponse(payload.challengeId),
      challengeResultsProcessed: 0,
      statsUpdated: 0
    }
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengeClient,
    _.uniq(_.map(reviewRows, row => String(row.challengeId)))
  )
  const aggregateRows = buildAggregatedStatsFromReviewResults(reviewRows, challengeMetadataById)

  if (aggregateRows.length > 0) {
    await prisma.$transaction(async (tx) => {
      for (const aggregateRow of aggregateRows) {
        await tx.memberStats.upsert({
          where: {
            userId_trackId_typeId: {
              userId: member.userId,
              trackId: aggregateRow.trackId,
              typeId: aggregateRow.typeId
            }
          },
          create: {
            userId: member.userId,
            trackId: aggregateRow.trackId,
            typeId: aggregateRow.typeId,
            challenges: aggregateRow.challenges,
            wins: aggregateRow.wins,
            mostRecentSubmission: aggregateRow.mostRecentSubmission,
            mostRecentEventDate: aggregateRow.mostRecentEventDate,
            isPrivate: false,
            createdBy: operatorId,
            updatedBy: operatorId
          },
          update: {
            challenges: aggregateRow.challenges,
            wins: aggregateRow.wins,
            mostRecentSubmission: aggregateRow.mostRecentSubmission,
            mostRecentEventDate: aggregateRow.mostRecentEventDate,
            isPrivate: false,
            updatedBy: operatorId
          }
        })
      }

      await refreshMostRecentHistoryFlags(tx, member.userId, aggregateRows, operatorId)
    })
  }

  return {
    handle,
    refreshed: true,
    challengeId: normalizeChallengeIdForResponse(payload.challengeId),
    challengeResultsProcessed: reviewRows.length,
    statsUpdated: aggregateRows.length
  }
}

refreshMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    challengeId: Joi.alternatives().try(Joi.string().uuid(), Joi.number().integer().strict())
  })
}

/**
 * Trigger a DEVELOPMENT / Challenge or DATA_SCIENCE / MARATHON_MATCH re-rating pass
 * beginning with the supplied challenge. The relevant review-api results are
 * reprocessed in chronological order and persisted into the existing unified
 * rating tables for the member.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the rerate payload whose challengeId is echoed back as a string
 * @returns {Object} summary describing the rerate work that was completed
 * @throws {errors.ForbiddenError} if the caller is not allowed to manage the member
 */
async function rerateMemberStats (currentUser, handle, data) {
  const payload = data || {}
  const member = await helper.getMemberByHandle(handle)
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const trackId = resolveTrackName(payload.trackId || TRACK_NAMES.DEVELOP)
  const typeId = resolveTypeName(payload.typeId || TYPE_NAMES.CHALLENGE)
  const challengeClient = prismaManager.getChallengesClient()
  const reviewDbClient = getReviewDbClientOrThrow()

  let result
  if (trackId === TRACK_NAMES.DEVELOP && typeId === TYPE_NAMES.CHALLENGE) {
    result = await rerateDevTrack(
      prisma,
      challengeClient,
      reviewDbClient,
      member.userId,
      payload.challengeId
    )
  } else if (trackId === TRACK_NAMES.DATA_SCIENCE && typeId === TYPE_NAMES.MARATHON_MATCH) {
    result = await rerateMmTrack(
      prisma,
      challengeClient,
      prismaManager.getMmClient(),
      reviewDbClient,
      member.userId,
      payload.challengeId
    )
  } else {
    throw new errors.BadRequestError('Only DEVELOP / Challenge and DATA_SCIENCE / MARATHON_MATCH rerates are currently supported.')
  }

  return {
    handle,
    rerated: true,
    challengeId: normalizeChallengeIdForResponse(payload.challengeId),
    trackId,
    typeId,
    challengesRerated: Math.max(result.challengesProcessed - 1, 0),
    challengesProcessed: result.challengesProcessed,
    ratingsUpdated: result.ratingsUpdated
  }
}

rerateMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    challengeId: Joi.alternatives().try(Joi.string().uuid(), Joi.number().integer().strict()).required(),
    trackId: Joi.string().valid(TRACK_NAMES.DEVELOP, TRACK_NAMES.DATA_SCIENCE).insensitive(),
    typeId: Joi.string().valid(TYPE_NAMES.CHALLENGE, TYPE_NAMES.MARATHON_MATCH).insensitive()
  }).required()
}

/**
 * Get member skills.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member skills
 */
async function getMemberSkills (handle) {
  // validate member
  const member = await helper.getMemberByHandle(handle)
  const skillList = await skillsPrisma.userSkill.findMany({
    where: {
      userId: helper.bigIntToNumber(member.userId)
    },
    include: prismaHelper.skillsIncludeParams
  })
  // convert to response format
  return prismaHelper.buildMemberSkills(skillList)
}

getMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required()
}

/**
 * Check create/update member skill data
 * @param {Object} data request body
 */
async function validateMemberSkillData (data) {
  // Check displayMode
  if (data.displayModeId) {
    const modeCount = await skillsPrisma.userSkillDisplayMode.count({
      where: { id: data.displayModeId }
    })
    if (modeCount <= 0) {
      throw new errors.BadRequestError(`Display mode ${data.displayModeId} does not exist`)
    }
  }
  if (data.levels && data.levels.length > 0) {
    const levelCount = await skillsPrisma.userSkillLevel.count({
      where: { id: { in: data.levels } }
    })
    if (levelCount < data.levels.length) {
      throw new errors.BadRequestError(`Please make sure skill level exists`)
    }
  }
}

async function createMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate request
  const existingCount = await skillsPrisma.userSkill.count({
    where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId }
  })
  if (existingCount > 0) {
    throw new errors.BadRequestError('This member skill exists')
  }
  await validateMemberSkillData(data)

  // save to db
  // Determine target levels: provided, or default to 'self-declared'
  let levelIds = data.levels && data.levels.length > 0 ? data.levels : null
  if (!levelIds) {
    const selfDeclared = await skillsPrisma.userSkillLevel.findFirst({ where: { name: 'self-declared' } })
    if (!selfDeclared) {
      throw new errors.NotFoundError('Default skill level "self-declared" not found')
    }
    levelIds = [selfDeclared.id]
  }
  const modeId = data.displayModeId || (await (async () => {
    const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
    return principal ? principal.id : undefined
  })())
  if (!modeId) {
    throw new errors.BadRequestError('Display mode is required and default mode not found')
  }

  for (const levelId of levelIds) {
    await skillsPrisma.userSkill.create({
      data: {
        userId: helper.bigIntToNumber(member.userId),
        skillId: data.skillId,
        userSkillLevelId: levelId,
        userSkillDisplayModeId: modeId
      }
    })
  }

  // get skills by member handle
  const memberSkill = await this.getMemberSkills(handle)
  return memberSkill
}

createMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillId: Joi.string().uuid().required(),
    displayModeId: Joi.string().uuid(),
    levels: Joi.array().items(Joi.string().uuid())
  }).required()
}

/**
 * Partially update member skills.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the skills data to update
 * @returns {Object} the updated member skills
 */
async function partiallyUpdateMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate request
  const existingUserSkills = await skillsPrisma.userSkill.findMany({
    where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId }
  })
  if (!existingUserSkills || existingUserSkills.length === 0) {
    throw new errors.NotFoundError('Member skill not found')
  }
  await validateMemberSkillData(data)

  if (data.levels && data.levels.length > 0) {
    // Replace all existing with new set
    await skillsPrisma.userSkill.deleteMany({ where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId } })
    const modeId = data.displayModeId || (existingUserSkills[0] && existingUserSkills[0].userSkillDisplayModeId) || (await (async () => {
      const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
      return principal ? principal.id : undefined
    })())
    if (!modeId) {
      throw new errors.BadRequestError('Display mode is required and default mode not found')
    }
    for (const levelId of data.levels) {
      await skillsPrisma.userSkill.create({
        data: {
          userId: helper.bigIntToNumber(member.userId),
          skillId: data.skillId,
          userSkillLevelId: levelId,
          userSkillDisplayModeId: modeId
        }
      })
    }
  } else if (data.displayModeId) {
    // Update display mode on all existing records for this skill
    await skillsPrisma.userSkill.updateMany({
      where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId },
      data: { userSkillDisplayModeId: data.displayModeId }
    })
  }

  // get skills by member handle
  const memberSkill = await this.getMemberSkills(handle)
  return memberSkill
}

partiallyUpdateMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillId: Joi.string().uuid().required(),
    displayModeId: Joi.string().uuid(),
    levels: Joi.array().items(Joi.string().uuid())
  }).required()
}

/**
 * Bulk verify member skills for a completed course.
 * Ensures each provided skill is associated with the member and has level 'verified'.
 * Replaces any existing levels for those skills with 'verified'.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the payload containing skillIds: string[]
 * @returns {Object} the updated member skills
 */
async function verifyMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate input
  if (!data || !Array.isArray(data.skillIds) || data.skillIds.length === 0) {
    throw new errors.BadRequestError('skillIds is required and must be a non-empty array')
  }

  // ensure all skills exist
  const skillsCount = await skillsPrisma.skill.count({ where: { id: { in: data.skillIds } } })
  if (skillsCount < data.skillIds.length) {
    throw new errors.BadRequestError('One or more provided skills do not exist')
  }

  // find the 'verified' skill level id
  const verifiedLevel = await skillsPrisma.userSkillLevel.findFirst({ where: { name: 'verified' } })
  if (!verifiedLevel || !verifiedLevel.id) {
    throw new errors.NotFoundError('Verified skill level not found')
  }

  // process each skill: upsert memberSkill and set levels to verified only
  for (const skillId of data.skillIds) {
    const existing = await skillsPrisma.userSkill.findMany({
      where: { userId: helper.bigIntToNumber(member.userId), skillId }
    })
    // preserve display mode if any existing record
    let modeId = existing[0] ? existing[0].userSkillDisplayModeId : undefined
    if (!modeId) {
      const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
      modeId = principal ? principal.id : undefined
    }
    if (!modeId) {
      throw new errors.BadRequestError('Display mode is required and default mode not found')
    }
    // replace all with a single verified record
    await skillsPrisma.userSkill.deleteMany({ where: { userId: helper.bigIntToNumber(member.userId), skillId } })
    await skillsPrisma.userSkill.create({
      data: {
        userId: helper.bigIntToNumber(member.userId),
        skillId,
        userSkillLevelId: verifiedLevel.id,
        userSkillDisplayModeId: modeId
      }
    })
  }

  // return the updated skills set
  return this.getMemberSkills(handle)
}

verifyMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillIds: Joi.array().items(Joi.string().uuid()).required()
  }).required()
}

module.exports = {
  getDistribution,
  getHistoryStats,
  createHistoryStats,
  partiallyUpdateHistoryStats,
  getMemberStats,
  createMemberStats,
  partiallyUpdateMemberStats,
  refreshMemberStats,
  rerateMemberStats,
  getMemberSkills,
  createMemberSkills,
  partiallyUpdateMemberSkills,
  verifyMemberSkills
}

logger.buildService(module.exports)

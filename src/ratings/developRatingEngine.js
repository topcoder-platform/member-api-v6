/**
 * Re-rate unified member stats for rated DEVELOPMENT / Challenge results.
 *
 * The engine reads challengeResult rows from review-api, resolves challenge
 * metadata from challenge-api, applies the Qubits math one challenge at a
 * time, and persists the target member's rating state into memberStats,
 * memberStatsHistory, and memberMaxRating.
 */

'use strict'

const errors = require('../common/errors')
const { resolveChallengeResultRelation } = require('../common/reviewDbHelper')
const {
  TYPE_NAMES,
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup
} = require('../common/statsDimensionHelper')
const { recalculateRatingRanks } = require('../common/ratingRankHelper')
const { runQubitsRating, DEFAULT_VOLATILITY } = require('./qubitsAlgorithm')
const { syncCurrentMemberMaxRating } = require('./memberMaxRatingSync')
const {
  RATING_METADATA_SELECT,
  isChallengeRated
} = require('./challengeRatingStatus')

const TRACK_NAME = 'DEVELOP'
const TYPE_NAME = 'Challenge'
const CHALLENGE_TRACK_NAME = 'DEVELOPMENT'
const CHALLENGE_TYPE_NAMES = [TYPE_NAMES.CHALLENGE, TYPE_NAMES.CODE]
const RERATE_ACTOR = 'rerate-member-stats'
const COMPLETED_CHALLENGE_STATUS = 'COMPLETED'

function isBigIntValue (value) {
  return Object.prototype.toString.call(value) === '[object BigInt]'
}

function toBigIntUserId (value) {
  if (isBigIntValue(value)) {
    return value
  }

  if (typeof global.BigInt !== 'function') {
    throw new Error('BigInt is not supported in this runtime')
  }

  return global.BigInt(String(value).trim())
}

function normalizeDate (value, fallbackValue) {
  const date = value ? new Date(value) : new Date(fallbackValue)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date
}

/**
 * Normalize challenge track/type labels into stable uppercase identifiers.
 * @param {*} value raw challenge dimension label
 * @returns {string} normalized identifier
 */
function normalizeChallengeDimension (value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, '_')
}

/**
 * Add one non-empty challenge id candidate to the supplied set.
 * @param {Set<string>} candidates mutable challenge id candidate set
 * @param {*} value raw challenge id candidate
 * @returns {void}
 */
function addChallengeIdCandidate (candidates, value) {
  if (value === null || value === undefined) {
    return
  }

  const normalized = String(value).trim()
  if (normalized) {
    candidates.add(normalized)
  }
}

/**
 * Build every review-api challenge id that can identify a challenge.
 * Challenge-api stores canonical UUID ids, while historical review rows may
 * still use legacy numeric ids.
 * @param {*} challengeRef challenge id, challenge metadata row, or history entry
 * @returns {Array<string>} unique challenge id candidates
 */
function buildChallengeIdCandidates (challengeRef) {
  const candidates = new Set()

  if (Array.isArray(challengeRef)) {
    challengeRef.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    return Array.from(candidates)
  }

  if (challengeRef && typeof challengeRef === 'object' && !(challengeRef instanceof Date)) {
    addChallengeIdCandidate(candidates, challengeRef.challengeId)
    addChallengeIdCandidate(candidates, challengeRef.id)
    addChallengeIdCandidate(candidates, challengeRef.legacyId)

    if (Array.isArray(challengeRef.reviewChallengeIds)) {
      challengeRef.reviewChallengeIds.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (Array.isArray(challengeRef.challengeIds)) {
      challengeRef.challengeIds.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (Array.isArray(challengeRef.challengeIdCandidates)) {
      challengeRef.challengeIdCandidates.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (challengeRef.legacyRecord) {
      addChallengeIdCandidate(candidates, challengeRef.legacyRecord.legacySystemId)
    }

    return Array.from(candidates)
  }

  addChallengeIdCandidate(candidates, challengeRef)
  return Array.from(candidates)
}

/**
 * Merge challenge metadata aliases with the source id observed in review-api.
 * @param {Object} challenge challenge-api metadata row
 * @param {*} [sourceChallengeId] challenge id from the review-api row
 * @returns {Array<string>} unique review challenge id candidates
 */
function buildReviewChallengeIds (challenge, sourceChallengeId) {
  const candidates = new Set()
  addChallengeIdCandidate(candidates, sourceChallengeId)
  buildChallengeIdCandidates(challenge).forEach((candidate) => candidates.add(candidate))
  return Array.from(candidates)
}

/**
 * Test whether a history entry can be addressed by the supplied challenge id.
 * @param {Object} historyEntry rerate history entry
 * @param {*} challengeId requested challenge id
 * @returns {boolean} true when the entry has the challenge id as an alias
 */
function historyEntryMatchesChallengeId (historyEntry, challengeId) {
  const normalizedChallengeId = String(challengeId).trim()
  return buildChallengeIdCandidates(historyEntry).includes(normalizedChallengeId)
}

/**
 * Resolve whether challenge metadata belongs to the Development Challenge rating stream.
 * Development CODE challenge rows are rated into the same DEVELOP / Challenge
 * stream as standard Development Challenge rows.
 * @param {Object} challenge challenge metadata record
 * @returns {boolean} true when the challenge should be replayed by this engine
 */
function isDevelopmentRatingChallenge (challenge) {
  if (!challenge || !challenge.track || !challenge.type) {
    return false
  }

  const normalizedTrackName = normalizeChallengeDimension(challenge.track.name)
  const normalizedTypeName = normalizeChallengeDimension(challenge.type.name)
  const supportedTypeNames = CHALLENGE_TYPE_NAMES.map(normalizeChallengeDimension)

  return normalizedTrackName === CHALLENGE_TRACK_NAME && supportedTypeNames.includes(normalizedTypeName)
}

function isCompletedChallenge (challenge) {
  const status = String(challenge && challenge.status ? challenge.status : '').trim().toUpperCase()
  return !status || status === COMPLETED_CHALLENGE_STATUS
}

function createDefaultState () {
  return {
    rating: 0,
    volatility: 0,
    numRatings: 0
  }
}

function hasRatingCheckpoint (historyRow) {
  return historyRow && Number.isFinite(Number(historyRow.newRating)) && Number(historyRow.newRating) > 0
}

function findRatingCheckpointIndex (historyRows, seedIndex) {
  if (!Array.isArray(historyRows) || seedIndex < 0) {
    return -1
  }

  for (let index = Math.min(seedIndex, historyRows.length - 1); index >= 0; index -= 1) {
    if (hasRatingCheckpoint(historyRows[index])) {
      return index
    }
  }

  return -1
}

function countRatingCheckpointsThroughIndex (historyRows, endIndex) {
  if (!Array.isArray(historyRows) || endIndex < 0) {
    return 0
  }

  let count = 0
  for (let index = 0; index <= endIndex && index < historyRows.length; index += 1) {
    if (hasRatingCheckpoint(historyRows[index])) {
      count += 1
    }
  }

  return count
}

/**
 * Clone a rating state into the normalized shape expected by the Qubits engine.
 * @param {Object} state source state
 * @returns {Object} normalized rating state
 */
function cloneState (state) {
  if (!state) {
    return createDefaultState()
  }

  return {
    rating: Number.isFinite(Number(state.rating)) ? Number(state.rating) : 0,
    volatility: Number.isFinite(Number(state.volatility)) ? Number(state.volatility) : 0,
    numRatings: Number.isFinite(Number(state.numRatings)) ? Math.max(0, Number(state.numRatings)) : 0
  }
}

function buildUserStateKey (userId) {
  return String(userId)
}

/**
 * Resolve the unified track/type UUIDs used for DEVELOPMENT / Challenge rows.
 * @param {Object} challengeClient prisma challenge client
 * @returns {Promise<{trackId: string, typeId: string, trackName: string, typeName: string, dimensionLookup: Object}>} resolved unified ids
 */
async function resolveUnifiedDimensionIds (challengeClient) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const trackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAME)
  const typeId = resolveTypeIdFromLookup(dimensionLookup, TYPE_NAME)

  if (!trackId || !typeId) {
    throw new Error(`Unable to resolve unified dimension ids for ${TRACK_NAME}/${TYPE_NAME}`)
  }

  return {
    trackId,
    typeId,
    trackName: TRACK_NAME,
    typeName: TYPE_NAME,
    dimensionLookup
  }
}

/**
 * Build a rerate seed state from the latest authoritative rating checkpoint at or
 * before a history index. Aggregate-only history rows can have null ratings and
 * must not count as prior rated events. Historical volatility is used when available.
 * Older history rows that predate volatility checkpoints fall back to the default
 * Qubits volatility.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {number} seedIndex index of the last history row before the target challenge
 * @returns {Object} seeded rating state
 */
function createHistorySeedState (historyRows, seedIndex) {
  const checkpointIndex = findRatingCheckpointIndex(historyRows, seedIndex)
  if (checkpointIndex < 0 || !historyRows[checkpointIndex]) {
    return createDefaultState()
  }

  return {
    rating: Number(historyRows[checkpointIndex].newRating),
    volatility: Number.isFinite(Number(historyRows[checkpointIndex].newVolatility))
      ? Number(historyRows[checkpointIndex].newVolatility)
      : DEFAULT_VOLATILITY,
    numRatings: countRatingCheckpointsThroughIndex(historyRows, checkpointIndex)
  }
}

/**
 * Find the history row index for a challenge within one participant timeline.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {string} challengeId challenge identifier
 * @returns {number} index of the matching row or -1 when absent
 */
function findHistoryIndexForChallenge (historyRows, challengeId) {
  const normalizedChallengeId = String(challengeId)

  for (let index = historyRows.length - 1; index >= 0; index -= 1) {
    if (String(historyRows[index].challengeId) === normalizedChallengeId) {
      return index
    }
  }

  return -1
}

/**
 * Find the last history row that is safely before the supplied challenge boundary.
 * When the current challenge already has a history row, the immediately preceding row is
 * authoritative. Otherwise the unified history table only exposes eventDate ordering, so
 * same-day rows without the current challenge checkpoint are excluded conservatively.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {Object} challengeEntry rerate challenge metadata
 * @returns {number} index of the latest pre-challenge history row or -1
 */
function findHistorySeedIndexForChallenge (historyRows, challengeEntry) {
  if (!Array.isArray(historyRows) || historyRows.length === 0) {
    return -1
  }

  const currentChallengeIndex = findHistoryIndexForChallenge(historyRows, challengeEntry.challengeId)
  if (currentChallengeIndex >= 0) {
    return currentChallengeIndex - 1
  }

  const challengeEventDate = challengeEntry && challengeEntry.eventDate
  if (!challengeEventDate) {
    return -1
  }

  const challengeTimestamp = challengeEventDate.getTime()
  let seedIndex = -1

  for (let index = 0; index < historyRows.length; index += 1) {
    if (historyRows[index].eventDate.getTime() < challengeTimestamp) {
      seedIndex = index
      continue
    }

    break
  }

  return seedIndex
}

function normalizeScore (row) {
  const finalScore = Number(row.finalScore)
  if (Number.isFinite(finalScore)) {
    return finalScore
  }

  const placement = Number(row.placement)
  if (Number.isFinite(placement)) {
    return -placement
  }

  return 0
}

/**
 * Resolve whether one participant row should count toward Development rerating.
 * review-api challengeResult.rated can be false for backfilled rows when the
 * producer lacked explicit challenge metadata, so rerates rely on a usable
 * positive score or placement while challenge-level rated intent is enforced
 * separately.
 * @param {Object} row challengeResult row
 * @returns {boolean} true when the participant should be included in rerating
 */
function isParticipantEligibleForRating (row) {
  if (!row || row.validSubmission === false) {
    return false
  }

  const placement = Number(row && row.placement)
  if (Number.isInteger(placement) && placement > 0) {
    return true
  }

  const finalScore = Number(row && row.finalScore)
  if (Number.isFinite(finalScore) && finalScore > 0) {
    return true
  }

  return false
}

async function fetchReviewResultsForUser (reviewDbClient, userId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "passedReview", "validSubmission", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "userId" = $1
      ORDER BY "createdAt" ASC
    `,
    [String(userId)]
  )

  return result.rows
}

async function fetchParticipantsForChallenge (reviewDbClient, challengeRef) {
  const challengeIds = buildChallengeIdCandidates(challengeRef)
  if (challengeIds.length === 0) {
    return []
  }

  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const placeholders = challengeIds.map((_, index) => `$${index + 1}`).join(', ')
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "passedReview", "validSubmission", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "challengeId" IN (${placeholders})
      ORDER BY "placement" ASC, "finalScore" DESC, "createdAt" ASC
    `,
    challengeIds
  )

  return result.rows
}

/**
 * Load challenge metadata required for Development rerating from challenge-api.
 * Rated metadata is loaded defensively from challenge metadata key/value rows
 * because the Challenge Prisma model does not expose a dedicated rated field.
 * @param {Object} challengeClient prisma challenge client
 * @param {Array<string>} challengeIds challenge identifiers
 * @returns {Promise<Map<string, Object>>} challenge metadata keyed by challenge id
 */
async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  const normalizedChallengeIds = buildChallengeIdCandidates(challengeIds)
  if (normalizedChallengeIds.length === 0) {
    return new Map()
  }

  const numericChallengeIds = normalizedChallengeIds
    .filter((challengeId) => /^\d+$/.test(challengeId))
    .map((challengeId) => Number(challengeId))
    .filter(Number.isSafeInteger)

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
      endDate: true,
      status: true,
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
      metadata: RATING_METADATA_SELECT,
      legacyRecord: {
        select: {
          legacySystemId: true
        }
      }
    }
  })

  const metadataByChallengeId = new Map()
  challenges.forEach((challenge) => {
    buildReviewChallengeIds(challenge).forEach((candidate) => {
      metadataByChallengeId.set(candidate, challenge)
    })
  })

  return metadataByChallengeId
}

function buildTargetHistory (reviewRows, challengeMetadataById) {
  const historyByChallengeId = new Map()

  reviewRows.forEach((row) => {
    if (!isParticipantEligibleForRating(row)) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.endDate) {
      return
    }

    if (!isCompletedChallenge(challenge)) {
      return
    }

    if (!challenge.track || !challenge.type) {
      return
    }

    if (!isChallengeRated(challenge)) {
      return
    }

    if (!isDevelopmentRatingChallenge(challenge)) {
      return
    }

    const eventDate = normalizeDate(challenge.endDate, row.createdAt)
    if (!eventDate) {
      return
    }

    const canonicalChallengeId = String(challenge.id)
    historyByChallengeId.set(canonicalChallengeId, {
      challengeId: canonicalChallengeId,
      reviewChallengeIds: buildReviewChallengeIds(challenge, row.challengeId),
      createdAt: normalizeDate(row.createdAt, challenge.endDate),
      endDate: normalizeDate(challenge.endDate, row.createdAt),
      eventDate
    })
  })

  const history = Array.from(historyByChallengeId.values())

  history.sort((left, right) => {
    const leftEventDate = left.eventDate ? left.eventDate.getTime() : 0
    const rightEventDate = right.eventDate ? right.eventDate.getTime() : 0
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    const leftCreatedAt = left.createdAt ? left.createdAt.getTime() : 0
    const rightCreatedAt = right.createdAt ? right.createdAt.getTime() : 0
    if (leftCreatedAt !== rightCreatedAt) {
      return leftCreatedAt - rightCreatedAt
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return history
}

/**
 * Cache the unified history timeline for participants needed during the rerate span.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds participant identifiers
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 */
async function loadParticipantHistoryCache (membersClient, participantIds, historyByUserId, dimensionIds) {
  const idsToLoad = participantIds.filter((participantId) => !historyByUserId.has(buildUserStateKey(participantId)))
  if (idsToLoad.length === 0) {
    return
  }

  const historyRows = await membersClient.memberStatsHistory.findMany({
    where: {
      userId: {
        in: idsToLoad
      },
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ userId: 'asc' }, { eventDate: 'asc' }, { id: 'asc' }],
    select: {
      id: true,
      userId: true,
      challengeId: true,
      newRating: true,
      newVolatility: true,
      eventDate: true
    }
  })

  idsToLoad.forEach((participantId) => {
    historyByUserId.set(buildUserStateKey(participantId), [])
  })

  historyRows.forEach((row) => {
    const stateKey = buildUserStateKey(row.userId)
    const cachedRows = historyByUserId.get(stateKey)
    const eventDate = normalizeDate(row.eventDate, row.eventDate)

    if (!cachedRows || !eventDate) {
      return
    }

    cachedRows.push({
      id: row.id,
      challengeId: String(row.challengeId),
      newRating: row.newRating,
      newVolatility: row.newVolatility,
      eventDate
    })
  })
}

/**
 * Build per-challenge participant seed states from authoritative history rows.
 * The target user's in-memory state is taken from the rerate replay so later challenges
 * in the rerate span see prior rerated values, while every other participant is loaded
 * fresh from history as of the current challenge boundary.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds challenge participant identifiers
 * @param {BigInt} targetUserId target member id
 * @param {Object} targetState current rerated target state
 * @param {Object} challengeEntry rerate challenge metadata
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 * @returns {Map<string, Object>} seed state keyed by participant id
 */
async function loadParticipantStates (membersClient, participantIds, targetUserId, targetState, challengeEntry, historyByUserId, dimensionIds) {
  const targetStateKey = buildUserStateKey(targetUserId)
  const stateByUserId = new Map()
  const nonTargetParticipantIds = participantIds.filter((participantId) => buildUserStateKey(participantId) !== targetStateKey)

  if (nonTargetParticipantIds.length > 0) {
    await loadParticipantHistoryCache(membersClient, nonTargetParticipantIds, historyByUserId, dimensionIds)
  }

  participantIds.forEach((participantId) => {
    const stateKey = buildUserStateKey(participantId)
    if (stateKey === targetStateKey) {
      stateByUserId.set(stateKey, cloneState(targetState))
      return
    }

    const historyRows = historyByUserId.get(stateKey) || []
    const seedIndex = findHistorySeedIndexForChallenge(historyRows, challengeEntry)
    stateByUserId.set(stateKey, createHistorySeedState(historyRows, seedIndex))
  })

  return stateByUserId
}

async function upsertHistoryRow (tx, userId, challengeId, previousState, updatedState, eventDate, dimensionIds) {
  const hasPreviousRating = previousState && previousState.numRatings > 0
  const oldRating = hasPreviousRating ? previousState.rating : null
  const oldVolatility = hasPreviousRating ? previousState.volatility : null
  const existingHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      challengeId
    },
    select: {
      id: true
    }
  })

  if (existingHistory) {
    await tx.memberStatsHistory.update({
      where: {
        id: existingHistory.id
      },
      data: {
        oldRating,
        newRating: updatedState.rating,
        oldVolatility,
        newVolatility: updatedState.volatility,
        eventDate,
        updatedBy: RERATE_ACTOR
      }
    })
    return
  }

  await tx.memberStatsHistory.create({
    data: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      challengeId,
      mostRecent: false,
      oldRating,
      newRating: updatedState.rating,
      oldVolatility,
      newVolatility: updatedState.volatility,
      eventDate,
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    }
  })
}

async function deleteStaleHistoryRows (tx, userId, targetHistory, dimensionIds) {
  const retainedChallengeIds = targetHistory.map((entry) => entry.challengeId)
  if (retainedChallengeIds.length === 0) {
    return
  }

  await tx.memberStatsHistory.deleteMany({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      challengeId: {
        notIn: retainedChallengeIds
      }
    }
  })
}

/**
 * Persist the member's global max rating row after a Develop rating update.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {Object} dimensionIds unified Develop rating dimensions
 * @returns {Promise<void>} resolves when max rating is stored
 */
async function updateMaxRating (tx, userId, dimensionIds) {
  await syncCurrentMemberMaxRating(tx, userId, {
    actor: RERATE_ACTOR,
    currentDimension: dimensionIds,
    dimensionLookup: dimensionIds.dimensionLookup
  })
}

async function refreshMostRecentHistoryFlag (tx, userId, dimensionIds) {
  await tx.memberStatsHistory.updateMany({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      mostRecent: true
    },
    data: {
      mostRecent: false,
      updatedBy: RERATE_ACTOR
    }
  })

  const latestHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    select: {
      id: true
    }
  })

  if (!latestHistory) {
    return
  }

  const currentStats = await tx.memberStats.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    select: {
      rating: true,
      volatility: true
    }
  })

  const orderedHistoryRows = await tx.memberStatsHistory.findMany({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    select: {
      id: true,
      newRating: true,
      newVolatility: true
    }
  })
  const latestHistoryIndex = orderedHistoryRows.findIndex((row) => String(row.id) === String(latestHistory.id))
  const previousHistory = orderedHistoryRows
    .slice(latestHistoryIndex >= 0 ? latestHistoryIndex + 1 : 1)
    .find(hasRatingCheckpoint)

  const latestUpdate = {
    mostRecent: true,
    oldRating: previousHistory ? previousHistory.newRating : null,
    oldVolatility: previousHistory ? previousHistory.newVolatility : null,
    updatedBy: RERATE_ACTOR
  }

  if (currentStats && currentStats.rating !== null) {
    latestUpdate.newRating = currentStats.rating
  }
  if (currentStats && currentStats.volatility !== null) {
    latestUpdate.newVolatility = currentStats.volatility
  }

  await tx.memberStatsHistory.update({
    where: {
      id: latestHistory.id
    },
    data: latestUpdate
  })
}

/**
 * Re-rate one member's Development Challenge timeline from the requested point.
 * The target member's challenge history is replayed in event order, while
 * opponent states are loaded from persisted history at each challenge boundary.
 * Rank recalculation is enabled by default for direct rerates, but bulk scripts
 * can disable it and run one final rank update after all member rows are stored.
 * @param {Object} membersClient member Prisma client
 * @param {Object} challengeClient challenge Prisma client
 * @param {Object} reviewDbClient review database client
 * @param {BigInt|string|number} userId target member id
 * @param {string|null} fromChallengeId optional challenge id to start from
 * @param {Object} [options] rerate controls
 * @param {boolean} [options.recalculateRanks=true] recompute Develop Challenge ranks after this member rerate
 * @returns {Promise<{challengesProcessed: number, ratingsUpdated: number}>} rerate counters
 * @throws {Error} when required review DB or dimension data is unavailable
 */
async function rerateDevTrack (membersClient, challengeClient, reviewDbClient, userId, fromChallengeId, options = {}) {
  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate development stats')
  }

  const normalizedUserId = toBigIntUserId(userId)
  const reviewRows = await fetchReviewResultsForUser(reviewDbClient, normalizedUserId)
  if (reviewRows.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengeClient,
    Array.from(new Set(reviewRows.map((row) => String(row.challengeId))))
  )

  const targetHistory = buildTargetHistory(reviewRows, challengeMetadataById)
  if (targetHistory.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  const dimensionIds = await resolveUnifiedDimensionIds(challengeClient)

  let startIndex = 0
  if (fromChallengeId) {
    startIndex = targetHistory.findIndex((entry) => historyEntryMatchesChallengeId(entry, fromChallengeId))
    if (startIndex < 0) {
      throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${TYPE_NAME} event for this member`)
    }
  }

  const participantHistoryByUserId = new Map()
  let targetState = createDefaultState()
  const shouldPruneStaleHistory = startIndex === 0 && !fromChallengeId

  if (startIndex > 0) {
    await loadParticipantHistoryCache(membersClient, [normalizedUserId], participantHistoryByUserId, dimensionIds)

    const targetHistoryRows = participantHistoryByUserId.get(buildUserStateKey(normalizedUserId)) || []
    let startSeedIndex = findHistoryIndexForChallenge(targetHistoryRows, targetHistory[startIndex - 1].challengeId)

    if (startSeedIndex < 0) {
      startSeedIndex = findHistorySeedIndexForChallenge(targetHistoryRows, targetHistory[startIndex])
    }

    targetState = createHistorySeedState(targetHistoryRows, startSeedIndex)
  }

  let challengesProcessed = 0
  let ratingsUpdated = 0
  const shouldRecalculateRanks = options.recalculateRanks !== false

  for (let index = startIndex; index < targetHistory.length; index += 1) {
    const historyEntry = targetHistory[index]
    const participantRows = (await fetchParticipantsForChallenge(reviewDbClient, historyEntry))
      .filter((row) => isParticipantEligibleForRating(row))
    if (participantRows.length === 0) {
      continue
    }

    const participantIds = participantRows.map((row) => toBigIntUserId(row.userId))
    const stateByUserId = await loadParticipantStates(
      membersClient,
      participantIds,
      normalizedUserId,
      targetState,
      historyEntry,
      participantHistoryByUserId,
      dimensionIds
    )

    const targetStateBeforeRun = cloneState(targetState)
    const participants = participantRows.map((row) => {
      const participantUserId = toBigIntUserId(row.userId)
      const participantState = stateByUserId.get(buildUserStateKey(participantUserId)) || createDefaultState()

      return {
        coderId: String(participantUserId),
        rating: participantState.rating,
        volatility: participantState.volatility,
        numRatings: participantState.numRatings,
        score: normalizeScore(row)
      }
    })

    runQubitsRating(participants)

    const updatedTarget = participants.find((participant) => participant.coderId === String(normalizedUserId))
    if (!updatedTarget) {
      continue
    }

    targetState = cloneState(updatedTarget)
    challengesProcessed += 1
    ratingsUpdated += 1

    await membersClient.$transaction(async (tx) => {
      await tx.memberStats.upsert({
        where: {
          userId_trackId_typeId: {
            userId: normalizedUserId,
            trackId: dimensionIds.trackId,
            typeId: dimensionIds.typeId
          }
        },
        create: {
          userId: normalizedUserId,
          trackId: dimensionIds.trackId,
          typeId: dimensionIds.typeId,
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          isPrivate: false,
          createdBy: RERATE_ACTOR,
          updatedBy: RERATE_ACTOR
        },
        update: {
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          isPrivate: false,
          updatedBy: RERATE_ACTOR
        }
      })

      await upsertHistoryRow(
        tx,
        normalizedUserId,
        historyEntry.challengeId,
        targetStateBeforeRun,
        updatedTarget,
        historyEntry.eventDate,
        dimensionIds
      )
    })
  }

  if (ratingsUpdated > 0) {
    await membersClient.$transaction(async (tx) => {
      if (shouldPruneStaleHistory) {
        await deleteStaleHistoryRows(tx, normalizedUserId, targetHistory, dimensionIds)
      }
      await refreshMostRecentHistoryFlag(tx, normalizedUserId, dimensionIds)
      await updateMaxRating(tx, normalizedUserId, dimensionIds)
    })

    if (shouldRecalculateRanks) {
      await recalculateRatingRanks(membersClient, dimensionIds, { updatedBy: RERATE_ACTOR })
    }
  }

  return {
    challengesProcessed,
    ratingsUpdated
  }
}

module.exports = {
  buildChallengeIdCandidates,
  buildReviewChallengeIds,
  fetchParticipantsForChallenge,
  isDevelopmentRatingChallenge,
  rerateDevTrack
}

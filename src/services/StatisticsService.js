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
const { rerateDevTrack } = require('../ratings/developRatingEngine')
const { rerateMmTrack } = require('../ratings/mmRatingEngine')

const DISTRIBUTION_FIELDS = ['track', 'subTrack', 'distribution', 'createdAt', 'updatedAt',
  'createdBy', 'updatedBy']
const DISTRIBUTION_FIELDS_NO_DATE = ['track', 'subTrack', 'distribution']

const HISTORY_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'DEVELOP', 'DATA_SCIENCE',
  'createdAt', 'updatedAt', 'createdBy', 'updatedBy']

const MEMBER_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'maxRating',
  'challenges', 'wins', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT', 'createdAt',
  'updatedAt', 'createdBy', 'updatedBy']

const TRACK_NAMES = {
  DEVELOP: 'DEVELOP',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE',
  COPILOT: 'COPILOT'
}

const TYPE_NAMES = {
  CHALLENGE: 'Challenge',
  FIRST2FINISH: 'First2Finish',
  TASK: 'Task',
  SRM: 'SRM',
  MARATHON_MATCH: 'MARATHON_MATCH'
}

const LEGACY_STATS_READ_SOURCE = 'legacy'
const SUPPORTED_STATS_READ_SOURCES = ['unified', LEGACY_STATS_READ_SOURCE]
const DISTRIBUTION_RANGES = _.range(0, 4000, 100)
const configuredStatsReadSource = _.toLower(String(config.STATS_READ_SOURCE || 'unified').trim())
if (!_.includes(SUPPORTED_STATS_READ_SOURCES, configuredStatsReadSource)) {
  logger.warn(`Invalid STATS_READ_SOURCE='${config.STATS_READ_SOURCE}'. Falling back to 'unified'.`)
}
const USE_LEGACY_STATS_READS = configuredStatsReadSource === LEGACY_STATS_READ_SOURCE

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

function resolveTrackId (trackId) {
  if (_.isNil(trackId)) {
    return undefined
  }
  const normalized = String(trackId).trim().toUpperCase()
  if (!normalized) {
    return undefined
  }
  if (normalized.includes('DATA') && normalized.includes('SCIENCE')) {
    return TRACK_NAMES.DATA_SCIENCE
  }
  if (normalized.includes('DEVELOP') || normalized === 'DEV') {
    return TRACK_NAMES.DEVELOP
  }
  if (normalized.includes('DESIGN') || normalized === 'DES') {
    return TRACK_NAMES.DESIGN
  }
  if (normalized.includes('COPILOT')) {
    return TRACK_NAMES.COPILOT
  }
  return trackId
}

function resolveTypeId (typeId) {
  if (_.isNil(typeId)) {
    return undefined
  }
  const normalized = String(typeId).trim().toUpperCase()
  if (!normalized) {
    return undefined
  }
  if (normalized.includes('MARATHON')) {
    return TYPE_NAMES.MARATHON_MATCH
  }
  if (normalized.includes('FIRST') || normalized.includes('F2F')) {
    return TYPE_NAMES.FIRST2FINISH
  }
  if (normalized.includes('TASK')) {
    return TYPE_NAMES.TASK
  }
  if (normalized.includes('SRM')) {
    return TYPE_NAMES.SRM
  }
  if (normalized.includes('CHALLENGE')) {
    return TYPE_NAMES.CHALLENGE
  }
  return typeId
}

function isLegacyMaxRatingPayload (value) {
  return _.isPlainObject(value) && !_.isNil(value.rating) && !_.isNil(value.ratingColor)
}

function normalizeUnifiedRecord (record, isPrivate) {
  if (!record || !record.trackId || !record.typeId) {
    return null
  }

  const normalized = _.omitBy({
    trackId: resolveTrackId(record.trackId),
    typeId: resolveTypeId(record.typeId),
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

function pushUnifiedRecord (collection, record, isPrivate) {
  const normalized = normalizeUnifiedRecord(record, isPrivate)
  if (normalized) {
    collection.push(normalized)
  }
}

function buildUnifiedStatsRecordsFromPayload (payload, isPrivate, options = {}) {
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
    pushUnifiedRecord(records, rootPayload, isPrivate)
  }

  if (_.isArray(data.records)) {
    _.forEach(data.records, (record) => {
      pushUnifiedRecord(records, record, isPrivate)
    })
  }

  if (!isPartial && records.length === 0 && (!_.isNil(data.challenges) || !_.isNil(data.wins))) {
    pushUnifiedRecord(records, {
      trackId: resolveTrackId(data.trackId || TRACK_NAMES.DEVELOP),
      typeId: resolveTypeId(data.typeId || TYPE_NAMES.CHALLENGE),
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
    }, isPrivate)
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
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "createdAt"
      FROM "challengeResult"
      WHERE "userId" = $1
      ORDER BY "createdAt" ASC
    `,
    [userId.toString()]
  )

  return result.rows
}

async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  if (!challengeIds || challengeIds.length === 0) {
    return new Map()
  }

  const challenges = await challengeClient.challenge.findMany({
    where: {
      id: {
        in: challengeIds
      }
    },
    select: {
      id: true,
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
      }
    }
  })

  return new Map(challenges.map(challenge => [challenge.id, challenge]))
}

function buildAggregatedStatsFromReviewResults (reviewRows, challengeMetadataById) {
  const aggregateByKey = new Map()

  _.forEach(reviewRows, (row) => {
    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.track || !challenge.type) {
      return
    }

    const trackId = resolveTrackId(challenge.track.name)
    const typeId = resolveTypeId(challenge.type.name)
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
    ? Prisma.sql`WHERE ${Prisma.join(whereConditions, Prisma.sql` AND `)}`
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
    WHERE ${Prisma.join(whereConditions, Prisma.sql` AND `)}
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

function buildUnifiedHistoryRecordsFromPayload (payload) {
  const data = payload || {}
  const records = []
  const pushHistoryRecord = (item, fallbackTrackId, fallbackTypeId) => {
    const trackId = resolveTrackId(item.trackId || fallbackTrackId)
    const typeId = resolveTypeId(item.typeId || fallbackTypeId)
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
  const trackId = resolveTrackId(query.track)
  const typeId = resolveTypeId(query.subTrack)

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
    WHERE ${Prisma.join(whereConditions, Prisma.sql` AND `)}
    GROUP BY (FLOOR("rating" / 100.0)::int * 100)
    ORDER BY "rangeStart" ASC
  `

  if (!rows || rows.length === 0) {
    throw new errors.NotFoundError('No member distribution statistics is found.')
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
 * Get history statistics.
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
    const where = {
      userId: member.userId
    }
    const trackId = resolveTrackId(query.trackId)
    const typeId = resolveTypeId(query.typeId)
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

    const overallStat = []
    if (historyRows.length > 0) {
      _.forEach(groupIds, (groupId) => {
        const scopedRows = _.map(historyRows, row => ({ ...row, groupId: _.toNumber(groupId) }))
        overallStat.push(scopedRows)
      })
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
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data)
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

  const scopedRows = _.map(createdRows, row => ({ ...row, groupId: _.toNumber(groupIds[0]) }))
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
    eventDate: Joi.positive(),
    date: Joi.positive(),
    ratingDate: Joi.positive(),
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
      eventDate: Joi.positive(),
      date: Joi.positive(),
      ratingDate: Joi.positive()
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
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data)
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

  const scopedRows = _.map(updatedRows, row => ({ ...row, groupId: _.toNumber(groupIds[0]) }))
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
    eventDate: Joi.positive(),
    date: Joi.positive(),
    ratingDate: Joi.positive(),
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
      eventDate: Joi.positive(),
      date: Joi.positive(),
      ratingDate: Joi.positive()
    }))
  }).required()
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
    for (const groupId of groupIds) {
      const stat = await getLegacyMemberStatsRow(member.userId, groupId)
      if (!_.isNil(stat)) {
        stats.push(prismaHelper.buildStatsResponse(member, stat, fields))
      }
    }
  } else {
    const trackId = resolveTrackId(query.trackId)
    const typeId = resolveTypeId(query.typeId)
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
        const scopedStats = _.map(unifiedStats, stat => ({ ...stat, groupId: _.toNumber(groupId) }))
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
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate)
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
  const scopedStats = _.map(allStats, stat => ({ ...stat, groupId: _.toNumber(groupIds[0]) }))
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
    challenges: Joi.positive(),
    wins: Joi.positive(),
    mostRecentSubmission: Joi.positive(),
    mostRecentEventDate: Joi.positive(),
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
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
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
        rating: Joi.positive().required(),
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
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate, { partial: true })
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
  const scopedRows = _.map(updatedRows, row => ({ ...row, groupId: _.toNumber(groupIds[0]) }))
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
    challenges: Joi.positive(),
    wins: Joi.positive(),
    mostRecentSubmission: Joi.positive(),
    mostRecentEventDate: Joi.positive(),
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
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
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
        rating: Joi.positive().required(),
        track: Joi.string(),
        subTrack: Joi.string(),
        ratingColor: Joi.string().required()
      }),
      Joi.number()
    )
  }).required()
}

/**
 * Refresh unified memberStats aggregates for a member from review-api challenge results.
 * Challenge metadata is resolved from challenge-api so counts and timestamps are
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

  const trackId = resolveTrackId(payload.trackId || TRACK_NAMES.DEVELOP)
  const typeId = resolveTypeId(payload.typeId || TYPE_NAMES.CHALLENGE)
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

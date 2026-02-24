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

const DISTRIBUTION_RANGES = _.range(0, 4000, 100)

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

      const latestUpdateData = {
        mostRecent: true,
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

  let result = _.map(overallStat, rows => prismaHelper.buildUnifiedStatsHistoryResponse(member, rows, fields))
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
  const trackId = resolveTrackId(query.trackId)
  const typeId = resolveTypeId(query.typeId)
  let stats = []
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
  getMemberSkills,
  createMemberSkills,
  partiallyUpdateMemberSkills,
  verifyMemberSkills
}

logger.buildService(module.exports)

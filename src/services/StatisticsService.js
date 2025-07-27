/**
 * This service provides operations of statistics.
 */

const _ = require('lodash')
const Joi = require('joi')
const config = require('config')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prisma = require('../common/prisma').getClient()
const prismaHelper = require('../common/prismaHelper')
const { v4: uuidv4 } = require('uuid')

const DISTRIBUTION_FIELDS = ['track', 'subTrack', 'distribution', 'createdAt', 'updatedAt',
  'createdBy', 'updatedBy']

const HISTORY_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'DEVELOP', 'DATA_SCIENCE',
  'createdAt', 'updatedAt', 'createdBy', 'updatedBy']

const MEMBER_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'maxRating',
  'challenges', 'wins', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT', 'createdAt',
  'updatedAt', 'createdBy', 'updatedBy']

/**
 * Get distribution statistics.
 * @param {Object} query the query parameters
 * @returns {Object} the distribution statistics
 */
async function getDistribution (query) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, DISTRIBUTION_FIELDS) || DISTRIBUTION_FIELDS

  // find matched distribution records
  const prismaFilter = { where: {} }
  if (query.track || query.subTrack) {
    prismaFilter.where = { AND: [] }
    if (query.track) {
      prismaFilter.where.AND.push({
        track: { contains: query.track.toUpperCase() }
      })
    }
    if (query.subTrack) {
      prismaFilter.where.AND.push({
        subTrack: { contains: query.subTrack.toUpperCase() }
      })
    }
  }
  const items = await prisma.distributionStats.findMany(prismaFilter)
  if (!items || items.length === 0) {
    throw new errors.NotFoundError(`No member distribution statistics is found.`)
  }
  // convert result to response structure
  const records = []
  _.forEach(items, t => {
    const r = _.pick(t, DISTRIBUTION_FIELDS)
    r.distribution = {}
    _.forEach(t, (value, key) => {
      if (key.startsWith('ratingRange')) {
        r.distribution[key] = value
      }
    })
    records.push(r)
  })

  // aggregate the statistics
  let result = { track: query.track, subTrack: query.subTrack, distribution: {} }
  _.forEach(records, (record) => {
    if (record.distribution) {
      // sum the statistics
      _.forIn(record.distribution, (value, key) => {
        if (!result.distribution[key]) {
          result.distribution[key] = 0
        }
        result.distribution[key] += Number(value)
      })
      // use earliest createdAt
      if (record.createdAt && (!result.createdAt || new Date(record.createdAt) < result.createdAt)) {
        result.createdAt = new Date(record.createdAt)
        result.createdBy = record.createdBy
      }
      // use latest updatedAt
      if (record.updatedAt && (!result.updatedAt || new Date(record.updatedAt) > result.updatedAt)) {
        result.updatedAt = new Date(record.updatedAt)
        result.updatedBy = record.updatedBy
      }
    }
  })
  // select fields if provided
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
  let overallStat = []
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, HISTORY_STATS_FIELDS) || HISTORY_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)

  for (const groupId of groupIds) {
    let statsDb
    if (groupId === config.PUBLIC_GROUP_ID) {
      // get statistics by member user id from db
      statsDb = await prisma.memberHistoryStats.findFirst({
        where: { userId: member.userId, isPrivate: false },
        include: { develop: true, dataScience: true }
      })
      if (!_.isNil(statsDb)) {
        statsDb.groupId = _.toNumber(groupId)
      }
    } else {
      // get statistics private by member user id from db
      statsDb = await prisma.memberHistoryStats.findFirst({
        where: { userId: member.userId, groupId, isPrivate: true },
        include: { develop: true, dataScience: true }
      })
    }
    if (!_.isNil(statsDb)) {
      overallStat.push(statsDb)
    }
  }
  // build stats history response
  let result = _.map(overallStat, t => prismaHelper.buildStatsHistoryResponse(member, t, fields))
  // remove identifiable info fields if user is not admin, not M2M and not member himself
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

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    data.isPrivate = false
    // get statistics by member user id from db
    existingStat = await prisma.memberHistoryStats.findFirst({
      where: { userId: member.userId, isPrivate: false },
      include: { develop: true, dataScience: true }
    })
    if (!_.isNil(existingStat)) {
      existingStat = _.assign(existingStat, { groupId: _.toNumber(groupIds[0]) })
    }
  } else {
    data.isPrivate = true
    // get statistics private by member user id from db
    existingStat = await prisma.memberHistoryStats.findFirst({
      where: { userId: member.userId, groupId: groupIds[0], isPrivate: true },
      include: { develop: true, dataScience: true }
    })
  }

  if (existingStat) {
    throw new errors.BadRequestError('History stats already exists')
  }

  const operatorId = currentUser.userId || currentUser.sub

  if (data.DEVELOP && data.DEVELOP.subTracks && data.DEVELOP.subTracks.length > 0) {
    prismaHelper.validateSubTrackData(data.DEVELOP.subTracks, [], 'Member develop history subTrack')

    data.develop = []
    data.DEVELOP.subTracks.forEach(item => {
      if (item.history && item.history.length > 0) {
        const historyItems = item.history.map(item2 => ({
          ...item2,
          ratingDate: prismaHelper.convertDate(item2.ratingDate),
          subTrackId: item.id,
          subTrack: item.name,
          createdBy: operatorId
        }))
        prismaHelper.validateHistoryData(historyItems, [], 'Member develop history stats')

        data.develop = data.develop.concat(historyItems)
      }
    })
  }

  if (data.DATA_SCIENCE) {
    data.dataScience = []
    if (data.DATA_SCIENCE.SRM && data.DATA_SCIENCE.SRM.history && data.DATA_SCIENCE.SRM.history.length > 0) {
      const historyItems = data.DATA_SCIENCE.SRM.history.map(item => ({
        ...item,
        date: prismaHelper.convertDate(item.date),
        subTrack: 'SRM',
        createdBy: operatorId
      }))
      prismaHelper.validateHistoryData(historyItems, [], 'Member dataScience history srm stats')

      data.dataScience = historyItems
    }
    if (data.DATA_SCIENCE.MARATHON_MATCH && data.DATA_SCIENCE.MARATHON_MATCH.history && data.DATA_SCIENCE.MARATHON_MATCH.history.length > 0) {
      const historyItems = data.DATA_SCIENCE.MARATHON_MATCH.history.map(item => ({
        ...item,
        date: prismaHelper.convertDate(item.date),
        subTrack: 'MARATHON_MATCH',
        createdBy: operatorId
      }))
      prismaHelper.validateHistoryData(historyItems, [], 'Member dataScience history marathon stats')

      data.dataScience = data.dataScience.concat(historyItems)
    }
  }

  // create model memberHistoryStats
  const statsRes = await prisma.memberHistoryStats.create({
    data: {
      isPrivate: data.isPrivate,
      createdBy: operatorId,
      userId: member.userId,
      develop: {
        create: data.develop
      },
      dataScience: {
        create: data.dataScience
      }
    },
    include: { develop: true, dataScience: true }
  })

  if (!data.isPrivate) {
    statsRes.groupId = _.toNumber(groupIds[0])
  }

  // build stats history response
  let result = prismaHelper.buildStatsHistoryResponse(member, statsRes, HISTORY_STATS_FIELDS)
  // remove identifiable info fields if user is not admin, not M2M and not member himself
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
    DEVELOP: Joi.object().keys({
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string().required(),
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string().required(),
          ratingDate: Joi.positive().required(),
          newRating: Joi.positive().required()
        }))
      }))
    }),
    DATA_SCIENCE: Joi.object().keys({
      SRM: Joi.object().keys({
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string().required(),
          date: Joi.positive().required(),
          rating: Joi.positive().required(),
          placement: Joi.positive().required(),
          percentile: Joi.number().required()
        }))
      }),
      MARATHON_MATCH: Joi.object().keys({
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string().required(),
          date: Joi.positive().required(),
          rating: Joi.positive().required(),
          placement: Joi.positive().required(),
          percentile: Joi.number().required()
        }))
      })
    })
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

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    // get statistics by member user id from db
    existingStat = await prisma.memberHistoryStats.findFirst({
      where: { userId: member.userId, isPrivate: false },
      include: { develop: true, dataScience: true }
    })
    if (!_.isNil(existingStat)) {
      existingStat = _.assign(existingStat, { groupId: _.toNumber(groupIds[0]) })
    }
  } else {
    // get statistics private by member user id from db
    existingStat = await prisma.memberHistoryStats.findFirst({
      where: { userId: member.userId, groupId: groupIds[0], isPrivate: true },
      include: { develop: true, dataScience: true }
    })
  }

  if (!existingStat || !existingStat.id) {
    throw new errors.NotFoundError('History stats not found')
  }

  if (data.DEVELOP && data.DEVELOP.subTracks && data.DEVELOP.subTracks.length > 0) {
    prismaHelper.validateSubTrackData(data.DEVELOP.subTracks, existingStat.develop || [], 'Member develop history subTrack')

    data.DEVELOP.subTracks.forEach(item => {
      if (item.history && item.history.length > 0) {
        const historyItems = item.history.map(item2 => ({
          ...item2,
          subTrackId: item.id,
          subTrack: item.name
        }))
        const toCreateItems = prismaHelper.validateHistoryData(historyItems, existingStat.develop || [], 'Member develop history stats')

        if (toCreateItems.length > 0) {
          const validateRes = DevelopHistoryStatsSchema.validate(toCreateItems)

          if (validateRes.error) {
            throw new errors.BadRequestError(validateRes.error.error)
          }
        }
      }
    })
  }

  if (data.DATA_SCIENCE) {
    if (data.DATA_SCIENCE.SRM && data.DATA_SCIENCE.SRM.history && data.DATA_SCIENCE.SRM.history.length > 0) {
      const historyItems = data.DATA_SCIENCE.SRM.history.map(item => ({
        ...item,
        subTrack: 'SRM'
      }))
      const toCreateItems = prismaHelper.validateHistoryData(historyItems, existingStat.dataScience || [], 'Member dataScience history srm stats')

      if (toCreateItems.length > 0) {
        const validateRes = DataScienceHistoryStatsSchema.validate(toCreateItems)

        if (validateRes.error) {
          throw new errors.BadRequestError(validateRes.error.error)
        }
      }
    }
    if (data.DATA_SCIENCE.MARATHON_MATCH && data.DATA_SCIENCE.MARATHON_MATCH.history && data.DATA_SCIENCE.MARATHON_MATCH.history.length > 0) {
      const historyItems = data.DATA_SCIENCE.MARATHON_MATCH.history.map(item => ({
        ...item,
        subTrack: 'MARATHON_MATCH'
      }))
      const toCreateItems = prismaHelper.validateHistoryData(historyItems, existingStat.dataScience || [], 'Member dataScience history marathon stats')

      if (toCreateItems.length > 0) {
        const validateRes = DataScienceHistoryStatsSchema.validate(toCreateItems)

        if (validateRes.error) {
          throw new errors.BadRequestError(validateRes.error.error)
        }
      }
    }
  }

  const operatorId = currentUser.userId || currentUser.sub
  const historyStatsId = existingStat.id

  // open a transaction to handle update
  let result = await prisma.$transaction(async (tx) => {
    // update DEVELOP subTracks history
    if (data.DEVELOP && data.DEVELOP.subTracks && data.DEVELOP.subTracks.length > 0) {
      let developHistory = []
      data.DEVELOP.subTracks.forEach(item => {
        const baseItem = {
          subTrackId: item.id,
          subTrack: item.name
        }
        developHistory = developHistory.concat((item.history || []).map(h => ({
          ...baseItem,
          ...h,
          ratingDate: prismaHelper.convertDate(h.ratingDate)
        })))
      })

      const existingItems = existingStat.develop || []

      await prismaHelper.updateHistoryItems(developHistory, existingItems, tx.memberDevelopHistoryStats, { historyStatsId }, operatorId)
    }

    // update DATA_SCIENCE history
    if (data.DATA_SCIENCE) {
      let dataScienceHistory = []
      if (data.DATA_SCIENCE.SRM && data.DATA_SCIENCE.SRM.history && data.DATA_SCIENCE.SRM.history.length > 0) {
        dataScienceHistory = data.DATA_SCIENCE.SRM.history.map(h => ({
          ...h,
          date: prismaHelper.convertDate(h.date),
          subTrack: 'SRM'
        }))
      }

      if (data.DATA_SCIENCE.MARATHON_MATCH && data.DATA_SCIENCE.MARATHON_MATCH.history && data.DATA_SCIENCE.MARATHON_MATCH.history.length > 0) {
        dataScienceHistory = dataScienceHistory.concat(data.DATA_SCIENCE.MARATHON_MATCH.history.map(h => ({
          ...h,
          date: prismaHelper.convertDate(h.date),
          subTrack: 'MARATHON_MATCH'
        })))
      }

      const existingItems = existingStat.dataScience || []

      await prismaHelper.updateHistoryItems(dataScienceHistory, existingItems, tx.memberDataScienceHistoryStats, { historyStatsId }, operatorId)
    }

    const updatedHistoryStats = await tx.memberHistoryStats.findUnique({
      where: { id: existingStat.id },
      include: { develop: true, dataScience: true }
    })

    return updatedHistoryStats
  })

  // build stats history response
  result = prismaHelper.buildStatsHistoryResponse(member, result, HISTORY_STATS_FIELDS)
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  if (!helper.canManageMember(currentUser, member)) {
    result = _.map(result, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return result
}

const DevelopHistoryStatsSchema = Joi.array().items(Joi.object().keys({
  challengeId: Joi.positive().required(),
  challengeName: Joi.string().required(),
  ratingDate: Joi.positive().required(),
  newRating: Joi.positive().required(),
  subTrackId: Joi.positive(),
  subTrack: Joi.string()
}))

const DataScienceHistoryStatsSchema = Joi.array().items(Joi.object().keys({
  challengeId: Joi.positive().required(),
  challengeName: Joi.string().required(),
  date: Joi.positive().required(),
  rating: Joi.positive().required(),
  placement: Joi.positive().required(),
  percentile: Joi.number().required(),
  subTrack: Joi.string()
}))

partiallyUpdateHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    DEVELOP: Joi.object().keys({
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string().required(),
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string(),
          ratingDate: Joi.positive(),
          newRating: Joi.positive()
        }))
      }))
    }),
    DATA_SCIENCE: Joi.object().keys({
      SRM: Joi.object().keys({
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string(),
          date: Joi.positive(),
          rating: Joi.positive(),
          placement: Joi.positive(),
          percentile: Joi.number()
        }))
      }),
      MARATHON_MATCH: Joi.object().keys({
        history: Joi.array().items(Joi.object().keys({
          challengeId: Joi.positive().required(),
          challengeName: Joi.string(),
          date: Joi.positive(),
          rating: Joi.positive(),
          placement: Joi.positive(),
          percentile: Joi.number()
        }))
      })
    })
  }).required()
}

/**
 * Get member statistics.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member statistics
 */
async function getMemberStats (currentUser, handle, query, throwError) {
  let stats = []
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, MEMBER_STATS_FIELDS) || MEMBER_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)

  const includeParams = prismaHelper.statsIncludeParams

  for (const groupId of groupIds) {
    let stat
    if (groupId === config.PUBLIC_GROUP_ID) {
      // get statistics by member user id from db
      stat = await prisma.memberStats.findFirst({
        where: { userId: member.userId, isPrivate: false },
        include: includeParams
      })
      if (!_.isNil(stat)) {
        stat = _.assign(stat, { groupId: _.toNumber(groupId) })
      }
    } else {
      // get statistics private by member user id from db
      stat = await prisma.memberStats.findFirst({
        where: { userId: member.userId, isPrivate: true, groupId },
        include: includeParams
      })
    }
    if (!_.isNil(stat)) {
      stats.push(stat)
    }
  }

  let result = _.map(stats, t => prismaHelper.buildStatsResponse(member, t, fields))
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  if (!helper.canManageMember(currentUser, member)) {
    result = _.map(result, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return result
}

getMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    groupIds: Joi.string(),
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

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    data.isPrivate = false
    // get statistics by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: false }
    })
  } else {
    data.isPrivate = true
    // get statistics private by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: true, groupId: groupIds[0] }
    })
  }

  if (existingStat) {
    throw new errors.BadRequestError('Member stats already exists')
  }

  // validate request data
  if (data.DEVELOP && data.DEVELOP.subTracks && data.DEVELOP.subTracks.length > 0) {
    prismaHelper.validateSubTrackData(data.DEVELOP.subTracks, [], 'Member stats develop')
  }

  if (data.DESIGN && data.DESIGN.subTracks && data.DESIGN.subTracks.length > 0) {
    prismaHelper.validateSubTrackData(data.DESIGN.subTracks, [], 'Member stats design')
  }

  if (data.DATA_SCIENCE && data.DATA_SCIENCE.SRM) {
    if (data.DATA_SCIENCE.SRM.challengeDetails) {
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.challengeDetails, [], 'Member stats dataScience srm', 'challengeDetail', MemberStatsSrmChallengeDetailsSchema)
    }

    if (data.DATA_SCIENCE.SRM.division1) {
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.division1, [], 'Member stats dataScience srm', 'division1', MemberStatsSrmDivisionsSchema)
    }

    if (data.DATA_SCIENCE.SRM.division2) {
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.division2, [], 'Member stats dataScience srm', 'division2', MemberStatsSrmDivisionsSchema)
    }
  }

  const operatorId = currentUser.userId || currentUser.sub

  // prepare insert data
  if (data.DEVELOP) {
    data.develop = {
      challenges: data.DEVELOP.challenges,
      wins: data.DEVELOP.wins,
      mostRecentSubmission: prismaHelper.convertDate(data.DEVELOP.mostRecentSubmission),
      mostRecentEventDate: prismaHelper.convertDate(data.DEVELOP.mostRecentEventDate),
      createdBy: operatorId
    }

    if (data.DEVELOP.subTracks) {
      const developItems = data.DEVELOP.subTracks.map(item => ({
        subTrackId: item.id,
        name: item.name,
        challenges: item.challenges,
        wins: item.wins,
        mostRecentSubmission: prismaHelper.convertDate(item.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(item.mostRecentEventDate),
        ...(item.submissions ? item.submissions : {}),
        ...(item.rank ? item.rank : {}),
        createdBy: operatorId
      }))

      data.develop.items = {
        create: developItems
      }
    }
  }

  if (data.DESIGN) {
    data.design = {
      challenges: data.DESIGN.challenges,
      wins: data.DESIGN.wins,
      mostRecentSubmission: prismaHelper.convertDate(data.DESIGN.mostRecentSubmission),
      mostRecentEventDate: prismaHelper.convertDate(data.DESIGN.mostRecentEventDate),
      createdBy: operatorId
    }

    if (data.DESIGN.subTracks) {
      const designItems = data.DESIGN.subTracks.map(item => ({
        ...(_.omit(item, ['id', 'mostRecentSubmission', 'mostRecentEventDate'])),
        subTrackId: item.id,
        mostRecentSubmission: prismaHelper.convertDate(item.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(item.mostRecentEventDate),
        createdBy: operatorId
      }))

      data.design.items = {
        create: designItems
      }
    }
  }

  if (data.DATA_SCIENCE) {
    data.dataScience = {
      challenges: data.DATA_SCIENCE.challenges,
      wins: data.DATA_SCIENCE.wins,
      mostRecentEventName: data.DATA_SCIENCE.mostRecentEventName,
      mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.mostRecentSubmission),
      mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.mostRecentEventDate),
      createdBy: operatorId
    }

    if (data.DATA_SCIENCE.SRM) {
      const dataScienceSrmData = {
        challenges: data.DATA_SCIENCE.SRM.challenges,
        wins: data.DATA_SCIENCE.SRM.wins,
        mostRecentEventName: data.DATA_SCIENCE.SRM.mostRecentEventName,
        mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.SRM.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.SRM.mostRecentEventDate),
        ...(data.DATA_SCIENCE.SRM.rank),
        createdBy: operatorId
      }

      data.dataScience.srm = {
        create: dataScienceSrmData
      }

      if (data.DATA_SCIENCE.SRM.challengeDetails) {
        const srmChallengeDetailData = data.DATA_SCIENCE.SRM.challengeDetails.map(item => ({
          ...item,
          createdBy: operatorId
        }))

        data.dataScience.srm.create.challengeDetails = {
          create: srmChallengeDetailData
        }
      }

      if (data.DATA_SCIENCE.SRM.division1 || data.DATA_SCIENCE.SRM.division2) {
        const srmDivision1Data = (data.DATA_SCIENCE.SRM.division1 || []).map(item => ({
          ...item,
          divisionName: 'division1',
          createdBy: operatorId
        }))

        const srmDivision2Data = (data.DATA_SCIENCE.SRM.division2 || []).map(item => ({
          ...item,
          divisionName: 'division2',
          createdBy: operatorId
        }))

        data.dataScience.srm.create.divisions = {
          create: _.concat(srmDivision1Data, srmDivision2Data)
        }
      }
    }

    if (data.DATA_SCIENCE.MARATHON_MATCH) {
      const dataScienceMarathonData = {
        challenges: data.DATA_SCIENCE.MARATHON_MATCH.challenges,
        wins: data.DATA_SCIENCE.MARATHON_MATCH.wins,
        mostRecentEventName: data.DATA_SCIENCE.MARATHON_MATCH.mostRecentEventName,
        mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.MARATHON_MATCH.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.MARATHON_MATCH.mostRecentEventDate),
        ...(data.DATA_SCIENCE.MARATHON_MATCH.rank),
        createdBy: operatorId
      }

      data.dataScience.marathon = {
        create: dataScienceMarathonData
      }
    }

    if (data.COPILOT) {
      data.copilot = {
        ...data.COPILOT,
        createdBy: operatorId
      }
    }
  }

  // open a transaction to handle create
  let result = await prisma.$transaction(async (tx) => {
    // create model memberStats
    const statsRes = await tx.memberStats.create({
      data: {
        challenges: data.challenges,
        wins: data.wins,
        isPrivate: data.isPrivate,
        createdBy: operatorId,
        userId: member.userId,
        develop: {
          create: data.develop
        },
        design: {
          create: data.design
        },
        dataScience: {
          create: data.dataScience
        },
        copilot: {
          create: data.copilot
        }
      },
      include: prismaHelper.statsIncludeParams
    })

    if (!data.isPrivate) {
      statsRes.groupId = _.toNumber(groupIds[0])
    }

    // create maxRating
    if (data.maxRating) {
      await prismaHelper.updateOrCreateModel(data.maxRating, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }

    return statsRes
  })

  result = prismaHelper.buildStatsResponse(member, result, MEMBER_STATS_FIELDS)
  // update maxRating
  if (data.maxRating) {
    result.maxRating = {
      ...result.maxRating,
      ...data.maxRating
    }
  }
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }

  return result
}

createMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    challenges: Joi.positive(),
    wins: Joi.positive(),
    maxRating: Joi.object().keys({
      rating: Joi.positive().required(),
      track: Joi.string(),
      subTrack: Joi.string(),
      ratingColor: Joi.string().required()
    }),
    DEVELOP: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string().required(),
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        submissions: Joi.object().keys({
          numInquiries: Joi.positive(),
          submissions: Joi.positive(),
          submissionRate: Joi.number(),
          passedScreening: Joi.positive(),
          screeningSuccessRate: Joi.number(),
          passedReview: Joi.positive(),
          reviewSuccessRate: Joi.number(),
          appeals: Joi.positive(),
          appealSuccessRate: Joi.number(),
          maxScore: Joi.number(),
          minScore: Joi.number(),
          avgScore: Joi.number(),
          avgPlacement: Joi.number(),
          winPercent: Joi.number()
        }),
        rank: Joi.object().keys({
          rating: Joi.positive(),
          activePercentile: Joi.number(),
          activeRank: Joi.positive(),
          activeCountryRank: Joi.positive(),
          activeSchoolRank: Joi.positive(),
          overallPercentile: Joi.number(),
          overallRank: Joi.positive(),
          overallCountryRank: Joi.positive(),
          overallSchoolRank: Joi.positive(),
          volatility: Joi.positive(),
          reliability: Joi.number(),
          maxRating: Joi.positive(),
          minRating: Joi.positive()
        })
      }))
    }),
    DESIGN: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string().required(),
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        numInquiries: Joi.positive().required(),
        submissions: Joi.positive().required(),
        passedScreening: Joi.positive().required(),
        avgPlacement: Joi.number().required(),
        screeningSuccessRate: Joi.number().required(),
        submissionRate: Joi.number().required(),
        winPercent: Joi.number().required()
      }))
    }),
    DATA_SCIENCE: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      mostRecentEventName: Joi.string(),
      SRM: Joi.object().keys({
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        mostRecentEventName: Joi.string(),
        rank: Joi.object().keys({
          rating: Joi.positive().required(),
          percentile: Joi.number().required(),
          rank: Joi.positive().required(),
          countryRank: Joi.positive().required(),
          schoolRank: Joi.positive().required(),
          volatility: Joi.positive().required(),
          maximumRating: Joi.positive().required(),
          minimumRating: Joi.positive().required(),
          defaultLanguage: Joi.string().required(),
          competitions: Joi.positive().required()
        }).required(),
        challengeDetails: Joi.array().items(Joi.object().keys({
          challenges: Joi.positive().required(),
          levelName: Joi.string().required(),
          failedChallenges: Joi.positive().required()
        })),
        division1: Joi.array().items(Joi.object().keys({
          problemsSubmitted: Joi.positive().required(),
          problemsSysByTest: Joi.positive().required(),
          problemsFailed: Joi.positive().required(),
          levelName: Joi.string().required()
        })),
        division2: Joi.array().items(Joi.object().keys({
          problemsSubmitted: Joi.positive().required(),
          problemsSysByTest: Joi.positive().required(),
          problemsFailed: Joi.positive().required(),
          levelName: Joi.string().required()
        }))
      }),
      MARATHON_MATCH: Joi.object().keys({
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        mostRecentEventName: Joi.string(),
        rank: Joi.object().keys({
          rating: Joi.positive().required(),
          competitions: Joi.positive().required(),
          avgRank: Joi.number().required(),
          avgNumSubmissions: Joi.positive().required(),
          bestRank: Joi.positive().required(),
          topFiveFinishes: Joi.positive().required(),
          topTenFinishes: Joi.positive().required(),
          rank: Joi.positive().required(),
          percentile: Joi.number().required(),
          volatility: Joi.positive().required(),
          minimumRating: Joi.positive().required(),
          maximumRating: Joi.positive().required(),
          countryRank: Joi.positive().required(),
          schoolRank: Joi.positive().required(),
          defaultLanguage: Joi.string().required()
        }).required()
      })
    }),
    COPILOT: Joi.object().keys({
      contests: Joi.positive().required(),
      projects: Joi.positive().required(),
      failures: Joi.positive().required(),
      reposts: Joi.positive().required(),
      activeContests: Joi.positive().required(),
      activeProjects: Joi.positive().required(),
      fulfillment: Joi.number().required()
    })
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

  const includeParams = prismaHelper.statsIncludeParams

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    // get statistics by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: false },
      include: includeParams
    })
    if (!_.isNil(existingStat)) {
      existingStat = _.assign(existingStat, { groupId: _.toNumber(groupIds[0]) })
    }
  } else {
    // get statistics private by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: true, groupId: groupIds[0] },
      include: includeParams
    })
  }

  if (!existingStat || !existingStat.id) {
    throw new errors.NotFoundError('Member stats not found')
  }

  // validate request data
  if (data.DEVELOP && data.DEVELOP.subTracks && data.DEVELOP.subTracks.length > 0) {
    const developItemsDB = existingStat.develop ? (existingStat.develop.items || []) : []
    prismaHelper.validateSubTrackData(data.DEVELOP.subTracks, developItemsDB, 'Member stats develop')
  }

  if (data.DESIGN && data.DESIGN.subTracks && data.DESIGN.subTracks.length > 0) {
    const designItemsDB = existingStat.design ? (existingStat.design.items || []) : []
    const toCreateItems = prismaHelper.validateSubTrackData(data.DESIGN.subTracks, designItemsDB, 'Member stats design')
    if (toCreateItems.length > 0) {
      const validateRes = MemberStatsDesignSubTrackSchema.validate(toCreateItems)

      if (validateRes.error) {
        throw new errors.BadRequestError(validateRes.error.error)
      }
    }
  }

  if (data.DATA_SCIENCE && data.DATA_SCIENCE.SRM) {
    if (!existingStat.dataScience || !existingStat.dataScience.srm) {
      const validateRes1 = MemberStatsDataScienceSrmSchema.validate(data.DATA_SCIENCE.SRM)
      if (validateRes1.error) {
        throw new errors.BadRequestError(validateRes1.error.error)
      }
    }

    const dataScienceDB = existingStat.dataScience || {}
    const srmDB = dataScienceDB.srm || {}
    if (data.DATA_SCIENCE.SRM.challengeDetails) {
      const srmChallengeDetailsDB = srmDB.challengeDetails || []
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.challengeDetails, srmChallengeDetailsDB, 'Member stats dataScience srm', 'challengeDetail', MemberStatsSrmChallengeDetailsSchema)
    }

    const srmDivisionsDB = srmDB.divisions || []
    if (data.DATA_SCIENCE.SRM.division1) {
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.division1, srmDivisionsDB, 'Member stats dataScience srm', 'division1', MemberStatsSrmDivisionsSchema)
    }

    if (data.DATA_SCIENCE.SRM.division2) {
      prismaHelper.validateLevelItemsData(data.DATA_SCIENCE.SRM.division2, srmDivisionsDB, 'Member stats dataScience srm', 'division2', MemberStatsSrmDivisionsSchema)
    }
  }

  if (data.DATA_SCIENCE && data.DATA_SCIENCE.MARATHON_MATCH && !(existingStat.dataScience || {}).marathon) {
    const validateRes1 = MemberStatsDataScienceMarathonSchema.validate(data.DATA_SCIENCE.MARATHON_MATCH)
    if (validateRes1.error) {
      throw new errors.BadRequestError(validateRes1.error.error)
    }
  }

  if (data.COPILOT && data.COPILOT && !existingStat.copilot) {
    const validateRes1 = MemberStatsDataScienceCopilotSchema.validate(data.COPILOT)
    if (validateRes1.error) {
      throw new errors.BadRequestError(validateRes1.error.error)
    }
  }

  const operatorId = currentUser.userId || currentUser.sub

  // open a transaction to handle update
  const result = await prisma.$transaction(async (tx) => {
    // update model memberStats
    if (data.challenges || data.wins) {
      await tx.memberStats.update({
        where: {
          id: existingStat.id
        },
        data: {
          challenges: data.challenges,
          wins: data.wins,
          updatedBy: operatorId
        }
      })
    }

    // update maxRating
    if (data.maxRating) {
      await prismaHelper.updateOrCreateModel(data.maxRating, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }

    // update DEVELOP
    if (data.DEVELOP) {
      const developData = {
        challenges: data.DEVELOP.challenges,
        wins: data.DEVELOP.wins,
        mostRecentSubmission: prismaHelper.convertDate(data.DEVELOP.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(data.DEVELOP.mostRecentEventDate)
      }
      const newDevelop = await prismaHelper.updateOrCreateModel(developData, existingStat.develop, tx.memberDevelopStats, { memberStatsId: existingStat.id }, operatorId)
      if (newDevelop) {
        existingStat.develop = newDevelop
      }

      // update develop subTracks
      if (data.DEVELOP.subTracks) {
        const developItems = data.DEVELOP.subTracks.map(item => ({
          subTrackId: item.id,
          name: item.name,
          challenges: item.challenges,
          wins: item.wins,
          mostRecentSubmission: prismaHelper.convertDate(item.mostRecentSubmission),
          mostRecentEventDate: prismaHelper.convertDate(item.mostRecentEventDate),
          ...(item.submissions ? item.submissions : {}),
          ...(item.rank ? item.rank : {})
        }))

        const developStatsId = existingStat.develop.id
        const existingItems = existingStat.develop.items || []

        await prismaHelper.updateArrayItems(developItems, existingItems, tx.memberDevelopStatsItem, { developStatsId }, operatorId)
      }
    }

    // update DESIGN
    if (data.DESIGN) {
      const designData = {
        challenges: data.DESIGN.challenges,
        wins: data.DESIGN.wins,
        mostRecentSubmission: prismaHelper.convertDate(data.DESIGN.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(data.DESIGN.mostRecentEventDate)
      }
      const newDesign = await prismaHelper.updateOrCreateModel(designData, existingStat.design, tx.memberDesignStats, { memberStatsId: existingStat.id }, operatorId)
      if (newDesign) {
        existingStat.design = newDesign
      }

      // update design subTracks
      if (data.DESIGN.subTracks) {
        const designItems = data.DESIGN.subTracks.map(item => ({
          ...(_.omit(item, ['id', 'mostRecentSubmission', 'mostRecentEventDate'])),
          subTrackId: item.id,
          mostRecentSubmission: prismaHelper.convertDate(item.mostRecentSubmission),
          mostRecentEventDate: prismaHelper.convertDate(item.mostRecentEventDate)
        }))

        const designStatsId = existingStat.design.id
        const existingItems = existingStat.design.items || []

        await prismaHelper.updateArrayItems(designItems, existingItems, tx.memberDesignStatsItem, { designStatsId }, operatorId)
      }
    }

    // update DATA_SCIENCE
    if (data.DATA_SCIENCE) {
      const dataScienceData = {
        challenges: data.DATA_SCIENCE.challenges,
        wins: data.DATA_SCIENCE.wins,
        mostRecentEventName: data.DATA_SCIENCE.mostRecentEventName,
        mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.mostRecentSubmission),
        mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.mostRecentEventDate)
      }
      const newDataScience = await prismaHelper.updateOrCreateModel(dataScienceData, existingStat.dataScience, tx.memberDataScienceStats, { memberStatsId: existingStat.id }, operatorId)
      if (newDataScience) {
        existingStat.dataScience = newDataScience
      }

      // update data science srm
      if (data.DATA_SCIENCE.SRM) {
        const dataScienceSrmData = {
          challenges: data.DATA_SCIENCE.SRM.challenges,
          wins: data.DATA_SCIENCE.SRM.wins,
          mostRecentEventName: data.DATA_SCIENCE.SRM.mostRecentEventName,
          mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.SRM.mostRecentSubmission),
          mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.SRM.mostRecentEventDate),
          ...(data.DATA_SCIENCE.SRM.rank)
        }
        const newDataScienceSrm = await prismaHelper.updateOrCreateModel(dataScienceSrmData, existingStat.dataScience.srm, tx.memberSrmStats, { dataScienceStatsId: existingStat.dataScience.id }, operatorId)
        if (newDataScienceSrm) {
          existingStat.dataScience.srm = newDataScienceSrm
        }

        const srmStatsId = existingStat.dataScience.srm.id
        if (data.DATA_SCIENCE.SRM.challengeDetails) {
          const existingItems = existingStat.dataScience.srm.challengeDetails || []

          await prismaHelper.updateArrayLevelItems(data.DATA_SCIENCE.SRM.challengeDetails, existingItems, tx.memberSrmChallengeDetail, { srmStatsId }, operatorId)
        }

        if (data.DATA_SCIENCE.SRM.division1 || data.DATA_SCIENCE.SRM.division2) {
          const existingItems = existingStat.dataScience.srm.divisions || []

          await prismaHelper.updateArrayDivisionItems(data.DATA_SCIENCE.SRM.division1, data.DATA_SCIENCE.SRM.division2, existingItems, tx.memberSrmDivisionDetail, { srmStatsId }, operatorId)
        }
      }

      // update data science marathon
      if (data.DATA_SCIENCE.MARATHON_MATCH) {
        const dataScienceMarathonData = {
          challenges: data.DATA_SCIENCE.MARATHON_MATCH.challenges,
          wins: data.DATA_SCIENCE.MARATHON_MATCH.wins,
          mostRecentEventName: data.DATA_SCIENCE.MARATHON_MATCH.mostRecentEventName,
          mostRecentSubmission: prismaHelper.convertDate(data.DATA_SCIENCE.MARATHON_MATCH.mostRecentSubmission),
          mostRecentEventDate: prismaHelper.convertDate(data.DATA_SCIENCE.MARATHON_MATCH.mostRecentEventDate),
          ...(data.DATA_SCIENCE.MARATHON_MATCH.rank)
        }
        await prismaHelper.updateOrCreateModel(dataScienceMarathonData, existingStat.dataScience.marathon, tx.memberMarathonStats, { dataScienceStatsId: existingStat.dataScience.id }, operatorId)
      }
    }

    // update COPILOT
    if (data.COPILOT) {
      await prismaHelper.updateOrCreateModel(data.COPILOT, existingStat.copilot, tx.memberCopilotStats, { memberStatsId: existingStat.id }, operatorId)
    }

    // Fetch updated stats
    let updatedStats = await tx.memberStats.findUnique({
      where: { id: existingStat.id },
      include: includeParams
    })
    updatedStats.groupId = existingStat.groupId
    updatedStats = prismaHelper.buildStatsResponse(member, updatedStats, MEMBER_STATS_FIELDS)
    // remove identifiable info fields if user is not admin, not M2M and not member himself
    if (!helper.canManageMember(currentUser, member)) {
      updatedStats = _.omit(updatedStats, config.STATISTICS_SECURE_FIELDS)
    }

    return updatedStats
  })

  return result
}

const MemberStatsDesignSubTrackSchema = Joi.array().items(Joi.object().keys({
  id: Joi.positive().required(),
  name: Joi.string(),
  challenges: Joi.positive(),
  wins: Joi.positive(),
  mostRecentSubmission: Joi.positive(),
  mostRecentEventDate: Joi.positive(),
  numInquiries: Joi.positive().required(),
  submissions: Joi.positive().required(),
  passedScreening: Joi.positive().required(),
  avgPlacement: Joi.number().required(),
  screeningSuccessRate: Joi.number().required(),
  submissionRate: Joi.number().required(),
  winPercent: Joi.number().required()
}))

const MemberStatsDataScienceSrmSchema = Joi.object().keys({
  challenges: Joi.positive(),
  wins: Joi.positive(),
  mostRecentSubmission: Joi.positive(),
  mostRecentEventDate: Joi.positive(),
  mostRecentEventName: Joi.string(),
  rank: Joi.object().keys({
    rating: Joi.positive().required(),
    percentile: Joi.number().required(),
    rank: Joi.positive().required(),
    countryRank: Joi.positive().required(),
    schoolRank: Joi.positive().required(),
    volatility: Joi.positive().required(),
    maximumRating: Joi.positive().required(),
    minimumRating: Joi.positive().required(),
    defaultLanguage: Joi.string().required(),
    competitions: Joi.positive().required()
  }).required(),
  challengeDetails: Joi.array(),
  division1: Joi.array(),
  division2: Joi.array()
})

const MemberStatsSrmChallengeDetailsSchema = Joi.array().items(Joi.object().keys({
  challenges: Joi.positive().required(),
  levelName: Joi.string().required(),
  failedChallenges: Joi.positive().required()
}))

const MemberStatsSrmDivisionsSchema = Joi.array().items(Joi.object().keys({
  problemsSubmitted: Joi.positive().required(),
  problemsSysByTest: Joi.positive().required(),
  problemsFailed: Joi.positive().required(),
  levelName: Joi.string().required()
}))

const MemberStatsDataScienceMarathonSchema = Joi.object().keys({
  challenges: Joi.positive(),
  wins: Joi.positive(),
  mostRecentSubmission: Joi.positive(),
  mostRecentEventDate: Joi.positive(),
  mostRecentEventName: Joi.string(),
  rank: Joi.object().keys({
    rating: Joi.positive().required(),
    competitions: Joi.positive().required(),
    avgRank: Joi.number().required(),
    avgNumSubmissions: Joi.positive().required(),
    bestRank: Joi.positive().required(),
    topFiveFinishes: Joi.positive().required(),
    topTenFinishes: Joi.positive().required(),
    rank: Joi.positive().required(),
    percentile: Joi.number().required(),
    volatility: Joi.positive().required(),
    minimumRating: Joi.positive().required(),
    maximumRating: Joi.positive().required(),
    countryRank: Joi.positive().required(),
    schoolRank: Joi.positive().required(),
    defaultLanguage: Joi.string().required()
  }).required()
})

const MemberStatsDataScienceCopilotSchema = Joi.object().keys({
  contests: Joi.positive().required(),
  projects: Joi.positive().required(),
  failures: Joi.positive().required(),
  reposts: Joi.positive().required(),
  activeContests: Joi.positive().required(),
  activeProjects: Joi.positive().required(),
  fulfillment: Joi.number().required()
})

partiallyUpdateMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    challenges: Joi.positive(),
    wins: Joi.positive(),
    maxRating: Joi.object().keys({
      rating: Joi.positive().required(),
      track: Joi.string(),
      subTrack: Joi.string(),
      ratingColor: Joi.string().required()
    }),
    DEVELOP: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string(),
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        submissions: Joi.object().keys({
          numInquiries: Joi.positive(),
          submissions: Joi.positive(),
          submissionRate: Joi.number(),
          passedScreening: Joi.positive(),
          screeningSuccessRate: Joi.number(),
          passedReview: Joi.positive(),
          reviewSuccessRate: Joi.number(),
          appeals: Joi.positive(),
          appealSuccessRate: Joi.number(),
          maxScore: Joi.number(),
          minScore: Joi.number(),
          avgScore: Joi.number(),
          avgPlacement: Joi.number(),
          winPercent: Joi.number()
        }),
        rank: Joi.object().keys({
          rating: Joi.positive(),
          activePercentile: Joi.number(),
          activeRank: Joi.positive(),
          activeCountryRank: Joi.positive(),
          activeSchoolRank: Joi.positive(),
          overallPercentile: Joi.number(),
          overallRank: Joi.positive(),
          overallCountryRank: Joi.positive(),
          overallSchoolRank: Joi.positive(),
          volatility: Joi.positive(),
          reliability: Joi.number(),
          maxRating: Joi.positive(),
          minRating: Joi.positive()
        })
      }))
    }),
    DESIGN: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      subTracks: Joi.array().items(Joi.object().keys({
        id: Joi.positive().required(),
        name: Joi.string(),
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        numInquiries: Joi.positive(),
        submissions: Joi.positive(),
        passedScreening: Joi.positive(),
        avgPlacement: Joi.number(),
        screeningSuccessRate: Joi.number(),
        submissionRate: Joi.number(),
        winPercent: Joi.number()
      }))
    }),
    DATA_SCIENCE: Joi.object().keys({
      challenges: Joi.positive(),
      wins: Joi.positive(),
      mostRecentSubmission: Joi.positive(),
      mostRecentEventDate: Joi.positive(),
      mostRecentEventName: Joi.string(),
      SRM: Joi.object().keys({
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        mostRecentEventName: Joi.string(),
        rank: Joi.object().keys({
          rating: Joi.positive(),
          percentile: Joi.number(),
          rank: Joi.positive(),
          countryRank: Joi.positive(),
          schoolRank: Joi.positive(),
          volatility: Joi.positive(),
          maximumRating: Joi.positive(),
          minimumRating: Joi.positive(),
          defaultLanguage: Joi.string(),
          competitions: Joi.positive()
        }),
        challengeDetails: Joi.array().items(Joi.object().keys({
          challenges: Joi.positive(),
          levelName: Joi.string().required(),
          failedChallenges: Joi.positive()
        })),
        division1: Joi.array().items(Joi.object().keys({
          problemsSubmitted: Joi.positive(),
          problemsSysByTest: Joi.positive(),
          problemsFailed: Joi.positive(),
          levelName: Joi.string().required()
        })),
        division2: Joi.array().items(Joi.object().keys({
          problemsSubmitted: Joi.positive(),
          problemsSysByTest: Joi.positive(),
          problemsFailed: Joi.positive(),
          levelName: Joi.string().required()
        }))
      }),
      MARATHON_MATCH: Joi.object().keys({
        challenges: Joi.positive(),
        wins: Joi.positive(),
        mostRecentSubmission: Joi.positive(),
        mostRecentEventDate: Joi.positive(),
        mostRecentEventName: Joi.string(),
        rank: Joi.object().keys({
          rating: Joi.positive(),
          competitions: Joi.positive(),
          avgRank: Joi.number(),
          avgNumSubmissions: Joi.positive(),
          bestRank: Joi.positive(),
          topFiveFinishes: Joi.positive(),
          topTenFinishes: Joi.positive(),
          rank: Joi.positive(),
          percentile: Joi.number(),
          volatility: Joi.positive(),
          minimumRating: Joi.positive(),
          maximumRating: Joi.positive(),
          countryRank: Joi.positive(),
          schoolRank: Joi.positive(),
          defaultLanguage: Joi.string()
        })
      })
    }),
    COPILOT: Joi.object().keys({
      contests: Joi.positive(),
      projects: Joi.positive(),
      failures: Joi.positive(),
      reposts: Joi.positive(),
      activeContests: Joi.positive(),
      activeProjects: Joi.positive(),
      fulfillment: Joi.number()
    })
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
  const skillList = await prisma.memberSkill.findMany({
    where: {
      userId: member.userId
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
    const modeCount = await prisma.displayMode.count({
      where: { id: data.displayModeId }
    })
    if (modeCount <= 0) {
      throw new errors.BadRequestError(`Display mode ${data.displayModeId} does not exist`)
    }
  }
  if (data.levels && data.levels.length > 0) {
    const levelCount = await prisma.skillLevel.count({
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
  const existingCount = await prisma.memberSkill.count({
    where: { userId: member.userId, skillId: data.skillId }
  })
  if (existingCount > 0) {
    throw new errors.BadRequestError('This member skill exists')
  }
  await validateMemberSkillData(data)

  // save to db
  const createdBy = currentUser.handle || currentUser.sub
  const memberSkillData = {
    id: uuidv4(),
    userId: member.userId,
    skillId: data.skillId,
    createdBy
  }
  if (data.displayModeId) {
    memberSkillData.displayModeId = data.displayModeId
  }
  if (data.levels && data.levels.length > 0) {
    memberSkillData.levels = {
      createMany: { data:
        _.map(data.levels, levelId => ({
          skillLevelId: levelId,
          createdBy
        }))
      }
    }
  }
  await prisma.memberSkill.create({ data: memberSkillData })

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
  const existing = await prisma.memberSkill.findFirst({
    where: { userId: member.userId, skillId: data.skillId }
  })
  if (!existing || !existing.id) {
    throw new errors.NotFoundError('Member skill not found')
  }
  await validateMemberSkillData(data)

  const updatedBy = currentUser.handle || currentUser.sub
  const memberSkillData = {
    updatedBy
  }
  if (data.displayModeId) {
    memberSkillData.displayModeId = data.displayModeId
  }
  if (data.levels && data.levels.length > 0) {
    await prisma.memberSkillLevel.deleteMany({
      where: { memberSkillId: existing.id }
    })
    memberSkillData.levels = {
      createMany: { data:
        _.map(data.levels, levelId => ({
          skillLevelId: levelId,
          createdBy: updatedBy,
          updatedBy
        }))
      }
    }
  }
  await prisma.memberSkill.update({
    data: memberSkillData,
    where: { id: existing.id }
  })

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
  partiallyUpdateMemberSkills
}

logger.buildService(module.exports)

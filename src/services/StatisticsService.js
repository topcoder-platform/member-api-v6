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
  getMemberStats,
  getMemberSkills,
  createMemberSkills,
  partiallyUpdateMemberSkills
}

logger.buildService(module.exports)

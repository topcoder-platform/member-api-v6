/**
 * This service provides operations of statistics.
 */

const _ = require('lodash')
const Joi = require('joi')
const config = require('config')
const Prisma = require('@prisma/client')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prismaHelper = require('../common/prismaHelper')
const prisma = require('../common/prisma').getClient()

const stringifyForLog = (value) => {
  try {
    const serialized = JSON.stringify(value, (_, val) => {
      if (typeof val === 'bigint') {
        return val.toString()
      }
      if (val instanceof Date) {
        return val.toISOString()
      }
      return val
    })
    return serialized === undefined ? 'undefined' : serialized
  } catch (err) {
    return '[unserializable payload]'
  }
}

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName',
  'status', 'addresses', 'photoURL', 'homeCountryCode', 'competitionCountryCode',
  'description', 'email', 'tracks', 'maxRating', 'wins', 'createdAt', 'createdBy',
  'updatedAt', 'updatedBy', 'skills', 'stats', 'verified', 'loginCount', 'lastLoginDate',
  'numberOfChallengesWon', 'skillScore', 'numberOfChallengesPlaced', 'availableForGigs', 'namesAndHandleAppearance']

const MEMBER_SORT_BY_FIELDS = ['userId', 'country', 'handle', 'firstName', 'lastName',
  'numberOfChallengesWon', 'numberOfChallengesPlaced', 'skillScore']

const MEMBER_AUTOCOMPLETE_FIELDS = ['userId', 'handle', 'handleLower',
  'status', 'email', 'createdAt', 'updatedAt']

var MEMBER_STATS_FIELDS = ['userId', 'handle', 'handleLower', 'maxRating',
  'numberOfChallengesWon', 'numberOfChallengesPlaced',
  'challenges', 'wins', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT']

function omitMemberAttributes (currentUser, query, allowedValues) {
  // validate and parse fields param
  let fields = helper.parseCommaSeparatedString(query.fields, allowedValues) || allowedValues
  // if current user is not admin and not M2M, then exclude the admin/M2M only fields
  if (!currentUser || (!currentUser.isMachine && !helper.hasAdminRole(currentUser))) {
    fields = _.without(fields, ...config.MEMBER_SECURE_FIELDS)
  }
  // If the current user does not have an autocompleterole, remove the communication fields
  if (!currentUser || (!currentUser.isMachine && !helper.hasAutocompleteRole(currentUser))) {
    fields = _.without(fields, ...config.COMMUNICATION_SECURE_FIELDS)
  }
  return fields
}
/**
 * Search members.
 * @param {Object} currentUser the user who performs operation
 * @param {Object} query the query parameters
 * @returns {Object} the search result
 */
async function searchMembers (currentUser, query) {
  const fields = omitMemberAttributes(currentUser, query, MEMBER_FIELDS)

  const logContext = _.omitBy({
    handle: query.handle,
    handleLower: query.handleLower,
    handlesCount: _.isArray(query.handles) ? query.handles.length : undefined,
    handlesLowerCount: _.isArray(query.handlesLower) ? query.handlesLower.length : undefined,
    userId: query.userId,
    userIdsCount: _.isArray(query.userIds) ? query.userIds.length : undefined,
    emailProvided: _.has(query, 'email') ? !!query.email : undefined,
    term: query.term,
    page: query.page,
    perPage: query.perPage,
    sort: query.sort,
    includeStats: query.includeStats
  }, _.isUndefined)
  logger.debug(`searchMembers: received query ${stringifyForLog(logContext)}`)

  if (query.email != null && query.email.length > 0) {
    if (currentUser == null) {
      throw new errors.UnauthorizedError('Authentication token is required to query users by email')
    }
    if (!helper.hasSearchByEmailRole(currentUser)) {
      throw new errors.BadRequestError('Admin role is required to query users by email')
    }
  }

  // search for the members based on query
  const prismaFilter = prismaHelper.buildSearchMemberFilter(query)
  logger.debug(`searchMembers: prisma filter ${stringifyForLog(prismaFilter)}`)
  const searchData = await fillMembers(prismaFilter, query, fields)

  // secure address data
  const canManageMember = currentUser && (currentUser.isMachine || helper.hasAdminRole(currentUser))
  if (!canManageMember) {
    searchData.result = _.map(searchData.result, res => helper.secureMemberAddressData(res))
    searchData.result = _.map(searchData.result, res => helper.truncateLastName(res))
  }

  logger.debug(`searchMembers: returning total=${searchData.total} resultCount=${_.size(searchData.result)} page=${searchData.page} perPage=${searchData.perPage}`)

  return searchData
}

searchMembers.schema = {
  currentUser: Joi.any(),
  query: Joi.object().keys({
    handleLower: Joi.string(),
    handlesLower: Joi.array(),
    handle: Joi.string(),
    handles: Joi.array(),
    email: Joi.string(),
    userId: Joi.number(),
    userIds: Joi.array(),
    term: Joi.string(),
    fields: Joi.string(),
    page: Joi.page(),
    perPage: Joi.perPage(),
    sort: Joi.sort()
  })
}

async function addStats (results) {
  if (!results || results.length === 0) {
    return []
  }
  const userIds = _.map(results, 'userId')
  // get member stats
  const memberStatsList = await prisma.memberStats.findMany({
    where: { userId: { in: userIds } },
    // include all tracks
    include: prismaHelper.statsIncludeParams
  })
  // merge overall members and stats
  const mbrsSkillsStatsKeys = _.keyBy(memberStatsList, 'userId')
  const resultsWithStats = _.map(results, item => {
    item.numberOfChallengesWon = 0
    item.numberOfChallengesPlaced = 0
    if (mbrsSkillsStatsKeys[item.userId]) {
      item.stats = []
      const statsData = prismaHelper.buildStatsResponse(item, mbrsSkillsStatsKeys[item.userId], MEMBER_STATS_FIELDS)
      if (statsData.wins > item.numberOfChallengesWon) {
        item.numberOfChallengesWon = statsData.wins
      }
      item.numberOfChallengesPlaced = statsData.challenges
      // clean up stats fields and filter on stats fields
      item.stats.push(statsData)
    } else {
      item.stats = []
    }
    return item
  })

  return resultsWithStats
}

/**
 * Get member skills and put skills into results
 * @param {Array} results member list
 */
async function addSkills (results) {
  if (!results || results.length === 0) {
    return
  }
  const userIds = _.map(results, 'userId')
  // get member skills
  const allSkillList = await prisma.memberSkill.findMany({
    where: { userId: { in: userIds } },
    include: prismaHelper.skillsIncludeParams
  })
  // group by user id
  const skillGroupData = _.groupBy(allSkillList, 'userId')
  // convert data and put into results
  _.forEach(results, member => {
    // find skill data
    const skillList = skillGroupData[member.userId]
    member.skills = prismaHelper.buildMemberSkills(skillList)
  })
}

async function addSkillScore (results, query) {
  // Pull out availableForGigs to add to the search results, for talent search
  let resultsWithScores = _.map(results, function (item) {
    if (!item.skills) {
      item.skillScore = 0
      return item
    }
    let score = 0.0
    let foundSkills = _.filter(item.skills, function (skill) { return query.skillIds.includes(skill.id) })
    for (const skill of foundSkills) {
      let challengeWin = false
      let selfPicked = false

      for (const level of skill.levels) {
        if (level.name === 'verified') {
          challengeWin = true
        } else if (level.name === 'self-declared') {
          selfPicked = true
        }
      }

      if (challengeWin) {
        score = score + 1.0
      } else if (selfPicked) {
        score = score + 0.5
      }
    }
    item.skillScore = Math.round(score / query.skillIds.length * 100) / 100

    if (item.availableForGigs == null) {
      // Deduct 1% if availableForGigs is not set on the user.
      item.skillScore = item.skillScore - 0.01
    }

    if (item.description == null || item.description === '') {
      // Deduct 1% if the description is not set on the user.
      item.skillScore = item.skillScore - 0.01
    }

    if (item.photoURL == null || item.photoURL === '') {
      // Deduct 4% if the photoURL is not set on the user.
      item.skillScore = item.skillScore - 0.04
    }

    // Use the pre-calculated skillScoreDeduction on the user profile
    if (item.skillScoreDeduction != null) {
      item.skillScore = item.skillScore + item.skillScoreDeduction
    } else {
      // The default skill score deduction is -4%, if it's not set on the user.
      item.skillScore = item.skillScore - 0.04
    }

    // 1696118400000 is the epoch value for Oct 1, 2023, which is when we deployed the change to set the last login date when a user logs in
    // So, we use this as the baseline for the user if they don't have a last login date.

    let lastLoginDate = 1696118400000
    if (item.lastLoginDate) {
      lastLoginDate = item.lastLoginDate
    }

    let loginDiff = Date.now() - lastLoginDate
    // For diff calculation (30 days, 24 hours, 60 minutes, 60 seconds, 1000 milliseconds)
    let monthLength = 30 * 24 * 60 * 60 * 1000

    // If logged in > 5 month ago
    if (loginDiff > (5 * monthLength)) {
      item.skillScore = item.skillScore - 0.5
    } else if (loginDiff > (4 * monthLength)) {
      // Logged in more than 4 months ago, but less than 5
      item.skillScore = item.skillScore - 0.4
    } else if (loginDiff > (3 * monthLength)) {
      // Logged in more than 3 months ago, but less than 4
      item.skillScore = item.skillScore - 0.3
    } else if (loginDiff > (2 * monthLength)) {
      // Logged in more than 2 months ago, but less than 3
      item.skillScore = item.skillScore - 0.2
    } else if (loginDiff > (1 * monthLength)) {
      // Logged in more than 1 month ago, but less than 2
      item.skillScore = item.skillScore - 0.1
    }
    if (item.skillScore < 0) {
      item.skillScore = 0
    }
    item.skillScore = Math.round(item.skillScore * 100) / 100
    // Default names and handle appearance
    // https://topcoder.atlassian.net/browse/MP-325
    if (!item.namesAndHandleAppearance) {
      item.namesAndHandleAppearance = 'namesAndHandle'
    }

    return item
  })

  return resultsWithScores
}

// The default search order, used by general handle searches
function handleSearchOrder (results, query) {
  // Sort the results for default searching
  results = _.orderBy(results, [query.sortBy, 'handleLower'], [query.sortOrder])
  return results
}

// The skill search order, which has a secondary sort of the number of
// Topcoder-verified skills, in descending order (where level.name===verified)
function skillSearchOrder (results, query) {
  results = _.orderBy(results, [query.sortBy, function (member) {
    const challengeWinSkills = _.filter(member.skills,
      function (skill) {
        skill.levels.forEach(level => {
          if (level.name === 'verified') {
            return true
          }
        })
      })
    return challengeWinSkills.length
  }], [query.sortOrder, 'desc'])
  return results
}

async function fillMembers (prismaFilter, query, fields, skillSearch = false) {
  // get the total
  let total = await prisma.member.count(prismaFilter)

  let results = []
  if (total === 0) {
    return { total: total, page: query.page, perPage: query.perPage, result: [] }
  }

  // get member data
  results = await prisma.member.findMany({
    ...prismaFilter,
    include: {
      maxRating: true,
      addresses: true
    },
    // sort by handle with given order
    skip: (query.page - 1) * query.perPage,
    take: query.perPage,
    orderBy: [{
      handle: query.sortOrder
    }]
  })

  // convert to response format
  _.forEach(results, r => prismaHelper.convertMember(r))

  // Include the stats by default, but allow them to be ignored with ?includeStats=false
  // This is for performance reasons - pulling the stats is a bit of a resource hog
  if (!query.includeStats || query.includeStats === 'true') {
    results = await addStats(results, query)
  }

  // add skills data
  await addSkills(results)

  // Sort in slightly different secondary orders, depending on if
  // this is a skill search or handle search
  if (skillSearch) {
    _.remove(results, (result) => (result.availableForGigs != null && result.availableForGigs === false))
    results = await addSkillScore(results, query)
    results = skillSearchOrder(results, query)
  } else {
    results = handleSearchOrder(results, query)
  }

  if (skillSearch) {
    // omit verified flag
    results = _.map(results, r => _.omit(r, 'verified'))
  }

  // filter member based on fields
  results = _.map(results, (item) => _.pick(item, fields))

  return { total: total, page: query.page, perPage: query.perPage, result: results }
}

/**
 * Search member with skill id list. Only return member id.
 * @param {Array} skillIds skill id array
 * @returns member id list
 */
async function searchMemberIdWithSkillIds (skillIds) {
  if (!skillIds || skillIds.length === 0) {
    return []
  }
  const members = await prisma.$queryRaw`
    SELECT m."userId"
    FROM "member" m
    JOIN "memberSkill" ms ON m."userId" = ms."userId"
    WHERE ms."skillId"::text IN (${Prisma.join(skillIds)})
    GROUP BY m."userId"
    HAVING COUNT(DISTINCT ms."skillId") = ${skillIds.length}
  `
  return _.map(members, 'userId')
}

// TODO - use some caching approach to replace these in-memory objects
/**
 * Search members by the given search query
 *
 * @param query The search query by which to search members
 *
 * @returns {Promise<[]>} The array of members matching the given query
 */
const searchMembersBySkills = async (currentUser, query) => {
  try {
    let skillIds = await helper.getParamsFromQueryAsArray(query, 'id')
    query.skillIds = skillIds
    if (_.isEmpty(skillIds)) {
      return { total: 0, page: query.page, perPage: query.perPage, result: [] }
    }
    // NOTE, we remove stats only because it's too much data at the current time for the talent search app
    // We can add stats back in at some point in the future if we want to expand the information shown on the
    // talent search app.
    const fields = omitMemberAttributes(currentUser, query, _.without(MEMBER_FIELDS, 'stats'))
    // build search member filter. Make sure member has every skill id in skillIds
    const memberIds = await searchMemberIdWithSkillIds(skillIds)
    const prismaFilter = {
      where: { userId: { in: memberIds } }
    }
    // build result
    let response = await fillMembers(prismaFilter, query, fields, true)

    // secure address data
    const canManageMember = currentUser && (currentUser.isMachine || helper.hasAdminRole(currentUser))
    if (!canManageMember) {
      response.result = _.map(response.result, res => helper.secureMemberAddressData(res))
      response.result = _.map(response.result, res => helper.truncateLastName(res))
    }

    return response
  } catch (e) {
    logger.error('ERROR WHEN SEARCHING')
    logger.error(e)
    return { total: 0, page: query.page, perPage: query.perPage, result: [] }
  }
}

searchMembersBySkills.schema = {
  currentUser: Joi.any(),
  query: Joi.object().keys({
    id: Joi.alternatives().try(Joi.string(), Joi.array().items(Joi.string())),
    page: Joi.page(),
    perPage: Joi.perPage(),
    includeStats: Joi.string(),
    sortBy: Joi.string().valid(MEMBER_SORT_BY_FIELDS).default('skillScore'),
    sortOrder: Joi.string().valid('asc', 'desc').default('desc')
  })
}

/**
 * members autocomplete.
 * @param {Object} currentUser the user who performs operation
 * @param {Object} query the query parameters
 * @returns {Object} the autocomplete result
 */
async function autocomplete (currentUser, query) {
  const fields = omitMemberAttributes(currentUser, query, MEMBER_AUTOCOMPLETE_FIELDS)

  if (!query.term || query.term.length === 0) {
    return { total: 0, page: query.page, perPage: query.perPage, result: [] }
  }
  const term = query.term.toLowerCase()
  const prismaFilter = {
    where: {
      handleLower: { startsWith: term },
      status: 'ACTIVE'
    }
  }
  const total = await prisma.member.count(prismaFilter)
  if (total === 0) {
    return { total: 0, page: query.page, perPage: query.perPage, result: [] }
  }
  const selectFields = {}
  _.forEach(fields, f => {
    selectFields[f] = true
  })

  let records = await prisma.member.findMany({
    ...prismaFilter,
    select: selectFields,
    skip: (query.page - 1) * query.perPage,
    take: query.perPage,
    orderBy: { handle: query.sortOrder }
  })
  records = _.map(records, item => {
    const t = _.pick(item, fields)
    if (t.userId) {
      t.userId = helper.bigIntToNumber(t.userId)
    }
    return t
  })

  return { total, page: query.page, perPage: query.perPage, result: records }
}

/**
 * Autocomplete members using handle prefix from path parameter.
 * @param {Object} currentUser the user who performs operation
 * @param {String} term the handle prefix
 * @returns {Array<Object>} autocomplete results
 */
async function autocompleteByHandlePrefix (currentUser, term) {
  if (!currentUser) {
    throw new errors.UnauthorizedError('Authentication token is required to access autocomplete')
  }

  if (currentUser.isMachine) {
    const allowedScopes = [config.SCOPES.MEMBERS.READ, config.SCOPES.MEMBERS.ALL]
    if (!helper.checkIfExists(allowedScopes, currentUser.scopes || [])) {
      throw new errors.ForbiddenError('read:user_profiles scope is required to access autocomplete')
    }
  } else {
    const hasCopilotRole = helper.checkIfExists(['copilot'], currentUser.roles || [])
    const hasAdminRole = helper.checkIfExists(['administrator', 'admin'], currentUser.roles || [])

    if (!hasCopilotRole && !hasAdminRole) {
      throw new errors.ForbiddenError('Copilot or administrator role is required to access autocomplete')
    }
  }

  const normalizedTerm = _.trim(term || '')
  if (!normalizedTerm) {
    return []
  }

  const members = await prisma.member.findMany({
    where: {
      handleLower: {
        startsWith: normalizedTerm.toLowerCase()
      },
    },
    select: {
      userId: true,
      handle: true,
      firstName: true,
      lastName: true,
      photoURL: true,
      maxRating: {
        select: {
          rating: true,
          track: true,
          subTrack: true,
          ratingColor: true
        }
      }
    },
    orderBy: {
      handleLower: 'asc'
    }
  })

  return _.map(members, member => ({
    userId: helper.bigIntToNumber(member.userId),
    handle: member.handle,
    photoURL: member.photoURL || '',
    firstName: member.firstName || '',
    lastName: member.lastName || '',
    maxRating: member.maxRating ? _.pick(member.maxRating, ['rating', 'track', 'subTrack', 'ratingColor']) : null
  }))
}

autocomplete.schema = {
  currentUser: Joi.any(),
  query: Joi.object().keys({
    term: Joi.string(),
    fields: Joi.string(),
    page: Joi.page(),
    perPage: Joi.perPage(),
    size: Joi.size(),
    sortOrder: Joi.string().valid('asc', 'desc').default('desc')
  })
}

autocompleteByHandlePrefix.schema = {
  currentUser: Joi.any(),
  term: Joi.string().allow('').required()
}

module.exports = {
  searchMembers,
  searchMembersBySkills,
  autocomplete,
  autocompleteByHandlePrefix
}

logger.buildService(module.exports)

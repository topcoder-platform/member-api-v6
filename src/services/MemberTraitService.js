/**
 * This service provides operations of member traits.
 */

const _ = require('lodash')
const Joi = require('joi')
const config = require('config')
const moment = require('moment')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const constants = require('../../app-constants')
const prisma = require('../common/prisma').getClient()

const TRAIT_IDS = ['basic_info', 'education', 'work', 'communities', 'languages', 'hobby', 'organization', 'device', 'software', 'service_provider', 'subscription', 'personalization', 'connect_info', 'onboarding_checklist']

const TRAIT_FIELDS = ['userId', 'traitId', 'categoryName', 'traits', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy']

const DeviceType = ['Console', 'Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Wearable', 'Other']
const SoftwareType = ['DeveloperTools', 'Browser', 'Productivity', 'GraphAndDesign', 'Utilities']
const ServiceProviderType = ['InternetServiceProvider', 'MobileCarrier', 'Television', 'FinancialInstitution', 'Other']
const WorkIndustryType = ['Banking', 'ConsumerGoods', 'Energy', 'Entertainment', 'HealthCare', 'Pharma', 'PublicSector', 'TechAndTechnologyService', 'Telecoms', 'TravelAndHospitality']

/**
 * Used to generate prisma query parameters
 */
const traitIdPrismaMap = {
  basic_info: 'basicInfo',
  education: 'education',
  work: 'work',
  communities: 'community',
  languages: 'language',
  device: 'device',
  software: 'software',
  service_provider: 'serviceProvider',
  onboarding_checklist: 'checklist',
  personalization: 'personalization'
}

const traitIdModelMap = {
  basic_info: 'memberTraitBasicInfo',
  education: 'memberTraitEducation',
  work: 'memberTraitWork',
  communities: 'memberTraitCommunity',
  languages: 'memberTraitLanguage',
  device: 'memberTraitDevice',
  software: 'memberTraitSoftware',
  service_provider: 'memberTraitServiceProvider',
  onboarding_checklist: 'memberTraitOnboardChecklist',
  personalization: 'memberTraitPersonalization'
}

const auditFields = [
  'createdAt', 'updatedAt', 'createdBy', 'updatedBy'
]

/**
 * Convert prisma data to response format
 * @param {Object} traitData prisma query result
 * @param {BigInt} userId member userId
 * @param {Array} traitIds trait id list
 * @returns trait data in response
 */
function convertPrismaToRes (traitData, userId, traitIds = TRAIT_IDS) {
  // reverse traitIdPrismaMap
  const prismaTraitIdMap = {}
  for (let key of Object.keys(traitIdPrismaMap)) {
    prismaTraitIdMap[traitIdPrismaMap[key]] = key
  }
  // read from prisma data
  const ret = []
  for (let key of Object.keys(prismaTraitIdMap)) {
    if (!traitData[key] || traitData[key].length === 0) {
      continue
    }
    // skip special data
    if (key === 'personalization') {
      continue
    }
    const prismaValues = traitData[key]
    const displayName = prismaTraitIdMap[key]
    const traitItem = {
      userId: helper.bigIntToNumber(userId),
      traitId: displayName,
      categoryName: _.startCase(displayName),
      ..._.pick(prismaValues[0], auditFields),
      traits: {
        traitId: displayName,
        data: []
      }
    }
    // Special case: return communities as a single object map for compatibility
    if (displayName === 'communities') {
      const communitiesMap = {}
      _.forEach(prismaValues, (t) => {
        const item = _.omit(t, ['id', 'memberTraitId', ...auditFields])
        if (item.communityName) {
          communitiesMap[item.communityName] = Boolean(item.status)
        }
      })
      traitItem.traits.data = [communitiesMap]
    } else {
      traitItem.traits.data = _.map(prismaValues,
        t => _.omit(t, ['id', 'memberTraitId', ...auditFields]))
    }

    ret.push(traitItem)
  }
  // handle subscription and hobby fields
  if (_.includes(traitIds, 'subscription') && !_.isEmpty(traitData.subscriptions)) {
    ret.push({
      userId: helper.bigIntToNumber(userId),
      traitId: 'subscription',
      categoryName: 'Subscription',
      ..._.pick(traitData, auditFields),
      traits: {
        traitId: 'subscription',
        data: traitData.subscriptions
      }
    })
  }
  if (_.includes(traitIds, 'hobby') && !_.isEmpty(traitData.hobby)) {
    ret.push({
      userId: helper.bigIntToNumber(userId),
      traitId: 'hobby',
      categoryName: 'Hobby',
      ..._.pick(traitData, auditFields),
      traits: {
        traitId: 'hobby',
        data: traitData.hobby
      }
    })
  }
  // handle special data
  if (_.includes(traitIds, 'personalization') &&
    !_.isEmpty(traitData.personalization)
  ) {
    const collectInfo = {}
    _.forEach(traitData.personalization, t => {
      collectInfo[t.key] = t.value
    })
    ret.push({
      userId: helper.bigIntToNumber(userId),
      traitId: 'personalization',
      categoryName: 'personalization',
      ..._.pick(traitData, auditFields),
      traits: {
        traitId: 'personalization',
        data: [collectInfo]
      }
    })
  }
  _.forEach(ret, r => {
    r.createdAt = r.createdAt ? r.createdAt.getTime() : null
    r.updatedAt = r.updatedAt ? r.updatedAt.getTime() : null
  })
  return ret
}

/**
 * Query trait data from db with traitIds
 * @param {BigInt} userId user id
 * @param {Array} traitIds string array
 * @returns member trait prisma data
 */
async function queryTraits (userId, traitIds = TRAIT_IDS) {
  // build prisma query
  const prismaFilter = {
    where: { userId },
    include: {}
  }
  // for each trait id, get prisma model and put it into "include"
  _.forEach(_.pick(traitIdPrismaMap, traitIds), t => {
    prismaFilter.include[t] = true
  })
  const traitData = await prisma.memberTraits.findUnique(prismaFilter)
  if (!traitData) {
    // trait data not found. Directly return.
    return { id: null, data: [] }
  }
  // convert trait data to response format
  return {
    id: traitData.id,
    data: convertPrismaToRes(traitData, userId, traitIds)
  }
}

/**
 * Get member traits.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member traits
 */
async function getTraits (currentUser, handle, query) {
  // get member
  const member = await helper.getMemberByHandle(handle)
  // parse query parameters
  const traitIds = helper.parseCommaSeparatedString(query.traitIds, TRAIT_IDS) || TRAIT_IDS
  const fields = helper.parseCommaSeparatedString(query.fields, TRAIT_FIELDS) || TRAIT_FIELDS
  // query trait from db and convert to response
  let queryResult = await queryTraits(member.userId, traitIds)
  let result = queryResult.data

  // keep only those of given trait ids
  if (traitIds) {
    result = _.filter(result, (item) => _.includes(traitIds, item.traitId))
  }
  // convert date time for traits data
  _.filter(result, (item) => _.forEach(item.traits.data, function (value) {
    if (value.hasOwnProperty('birthDate')) {
      if (value.birthDate) {
        value.birthDate = moment(value.birthDate).toDate().toISOString()
      }
    }
    if (value.hasOwnProperty('memberSince')) {
      if (value.memberSince) {
        value.memberSince = moment(value.memberSince).toDate().toISOString()
      }
    }
    if (value.hasOwnProperty('timePeriodFrom')) {
      if (value.timePeriodFrom) {
        value.timePeriodFrom = moment(value.timePeriodFrom).toDate().toISOString()
      }
    }
    if (value.hasOwnProperty('timePeriodTo')) {
      if (value.timePeriodTo) {
        value.timePeriodTo = moment(value.timePeriodTo).toDate().toISOString()
      }
    }
  }))

  // return only selected fields
  result = _.map(result, (item) => _.pick(item, fields))
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  if (!helper.canManageMember(currentUser, member)) {
    result = _.map(result, (item) => _.omit(item, config.MEMBER_TRAIT_SECURE_FIELDS))
  }
  // public traits access for anonymous users
  if (!currentUser) {
    result = _.filter(result, (item) => _.includes(config.MEMBER_PUBLIC_TRAITS, item.traitId))
  }
  return result
}

getTraits.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    traitIds: Joi.string(),
    fields: Joi.string()
  })
}

/**
 * Build prisma data for creating/updating traits
 * @param {Object} data query data
 * @param {Number} operatorId operator user id
 * @param {Array} result result
 * @returns prisma data
 */
function buildTraitPrismaData (data, operatorId, result) {
  const prismaData = {}
  _.forEach(data, (item) => {
    const traitId = item.traitId
    const modelKey = traitIdPrismaMap[traitId]
    if (modelKey && traitId !== 'personalization') {
      if (traitId === 'communities') {
        // Support payloads sent as an object map: { [communityName]: boolean }
        let communityData = []
        const inputArr = (item.traits && item.traits.data) ? item.traits.data : []
        _.forEach(inputArr, (piece) => {
          if (piece && _.isPlainObject(piece) && !piece.communityName && !_.has(piece, 'status')) {
            _.forEach(Object.keys(piece), (name) => {
              const val = piece[name]
              if (_.isBoolean(val)) {
                communityData.push({ communityName: name, status: val })
              }
            })
          } else if (piece && piece.communityName) {
            communityData.push({ communityName: piece.communityName, status: Boolean(piece.status) })
          }
        })
        _.forEach(communityData, t => {
          t.createdBy = operatorId
          t.updatedBy = operatorId
        })
        prismaData[modelKey] = {
          createMany: {
            data: communityData
          }
        }
      } else {
        _.forEach(item.traits.data, t => {
          t.createdBy = operatorId
          t.updatedBy = operatorId
        })
        prismaData[modelKey] = {
          createMany: {
            data: item.traits.data
          }
        }
      }
    } else if (traitId === 'subscription') {
      prismaData.subscriptions = item.traits.data || []
    } else if (traitId === 'hobby') {
      prismaData.hobby = item.traits.data || []
    } else if (traitId === 'personalization') {
      // convert to key & value
      const valuePairs = []
      _.forEach(item.traits.data, t => {
        for (let key of _.keys(t)) {
          valuePairs.push({
            key,
            value: t[key],
            createdBy: operatorId
          })
        }
      })
      prismaData['personalization'] = {
        createMany: {
          data: valuePairs
        }
      }
    }
    result.push(item)
  })
  return prismaData
}

/**
 * Create member traits.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Array} data the member traits data to be created
 * @returns {Array} the created member traits
 */
async function createTraits (currentUser, handle, data) {
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to create traits of the member.')
  }
  // get existing traits
  let queryResult = await queryTraits(member.userId, TRAIT_IDS)
  let existingTraits = queryResult.data
  // check if there is any conflict
  _.forEach(data, (item) => {
    if (_.find(existingTraits, (existing) => existing.traitId === item.traitId)) {
      throw new errors.BadRequestError(`The trait id ${item.traitId} already exists for the member.`)
    }
  })
  // create traits
  const result = []
  const operatorId = String(currentUser.userId || config.TC_WEBSERVICE_USERID)
  const prismaData = buildTraitPrismaData(data, operatorId, result)

  if (queryResult.id) {
    await prisma.memberTraits.update({
      where: { userId: member.userId },
      data: prismaData
    })
  } else {
    prismaData.userId = member.userId
    prismaData.createdBy = operatorId
    await prisma.memberTraits.create({
      data: prismaData
    })
  }
  // send data to event bus
  for (let item of data) {
    const trait = { ...item }
    trait.userId = helper.bigIntToNumber(member.userId)
    trait.createdBy = Number(currentUser.userId || config.TC_WEBSERVICE_USERID)
    if (trait.traits) {
      trait.traits = { 'traitId': trait.traitId, 'data': trait.traits.data }
    } else {
      trait.traits = { 'traitId': trait.traitId, 'data': [] }
    }
    // convert date time
    trait.createdAt = new Date().getTime()
    // post bus event
    await helper.postBusEvent(constants.TOPICS.MemberTraitCreated, trait)
  }

  // merge result
  existingTraits = _.concat(existingTraits, data)

  await updateSkillScoreDeduction(currentUser, member, existingTraits)
  return result
}

// define schema for each trait
const traitSchemas = {
  hobby: Joi.array().items(Joi.string()),
  subscription: Joi.array().items(Joi.string()),
  device: Joi.array().items(Joi.object({
    deviceType: Joi.string().valid(...DeviceType).required(),
    manufacturer: Joi.string().required(),
    model: Joi.string().required(),
    operatingSystem: Joi.string().required(),
    osLanguage: Joi.string(),
    osVersion: Joi.string()
  })),
  software: Joi.array().items(Joi.object({
    softwareType: Joi.string().valid(...SoftwareType).required(),
    name: Joi.string().required()
  })),
  service_provider: Joi.array().items(Joi.object({
    type: Joi.string().valid(...ServiceProviderType).required(),
    name: Joi.string().required()
  })),
  work: Joi.array().items(Joi.object({
    industry: Joi.string().valid(...WorkIndustryType).allow(null),
    companyName: Joi.string().required(),
    position: Joi.string().required(),
    startDate: Joi.date().iso().allow(null),
    endDate: Joi.date().iso().allow(null),
    working: Joi.boolean().allow(null)
  })),
  education: Joi.array().items(Joi.object({
    collegeName: Joi.string().required(),
    degree: Joi.string().required(),
    endYear: Joi.number().integer().min(1900).max(new Date().getFullYear()).allow(null)
  })),
  basic_info: Joi.array().items(Joi.object({
    country: Joi.string().required(),
    primaryInterestInTopcoder: Joi.string().required(),
    tshirtSize: Joi.string().allow(null, ''),
    gender: Joi.string().allow(null, ''),
    shortBio: Joi.string().required(),
    birthDate: Joi.date().iso().allow(null),
    currentLocation: Joi.string().allow(null, '')
  })),
  languages: Joi.array().items(Joi.object({
    language: Joi.string().required(),
    spokenLevel: Joi.string().allow(null, ''),
    writtenLevel: Joi.string().allow(null, '')
  })),
  onboarding_checklist: Joi.array().items(Joi.object({
    listItemType: Joi.string().required(),
    date: Joi.date().iso().required(),
    message: Joi.string().required(),
    status: Joi.string().required(),
    metadata: Joi.any().allow(null)
  })),
  personalization: Joi.array().items(Joi.object()),
  communities: Joi.array().items(Joi.object({
    communityName: Joi.string().required(),
    status: Joi.boolean().required()
  }))
}

const traitSchemaSwitch = _.keys(traitSchemas).map(k => ({ is: k, then: traitSchemas[k] }))

createTraits.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.array().items(Joi.object().keys({
    traitId: Joi.string().valid(...TRAIT_IDS).required(),
    categoryName: Joi.string(),
    traits: Joi.object().keys({
      // nested traitId optional for backward compatibility with existing clients
      traitId: Joi.string().valid(...TRAIT_IDS),
      // be permissive on data payload shape; detailed checks are handled downstream
      data: Joi.array().required()
    })
  }).required()).min(1).required()
}

/**
 * Update member traits.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Array} data the member traits data to be updated
 * @returns {Array} the updated member traits
 */
async function updateTraits (currentUser, handle, data) {
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update traits of the member.')
  }
  // get existing traits
  const queryResult = await queryTraits(member.userId, TRAIT_IDS)
  const existingTraits = queryResult.data
  // allow upserting traits: if a trait does not exist yet for the member,
  // create it instead of throwing a NotFound error

  const result = []
  const operatorId = String(currentUser.userId || config.TC_WEBSERVICE_USERID)
  const prismaData = buildTraitPrismaData(data, operatorId, result)
  // open transaction and update or create data
  if (queryResult.id) {
    await prisma.$transaction(async (tx) => {
      // clear existing traits for the specific models we are updating
      const traitIdList = _.map(data, item => item.traitId)
      // map trait ids to relational models and drop non-relational ones (e.g., subscription, hobby)
      const models = _.uniq(_.compact(_.map(traitIdList, t => traitIdModelMap[t])))
      // clear models data
      if (models.length > 0) {
        await Promise.all(_.map(models, m => tx[m].deleteMany({
          where: { memberTraitId: queryResult.id }
        })))
      }
      // update traits data
      await tx.memberTraits.update({
        where: { userId: member.userId },
        data: prismaData
      })
    })
  } else {
    // No traits record yet for this member: create it with the provided traits
    const createData = { ...prismaData, userId: member.userId, createdBy: operatorId }
    await prisma.memberTraits.create({ data: createData })
  }

  // post bus events: created for new traits, updated for existing ones
  const existingIds = new Set((existingTraits || []).map(t => t.traitId))
  for (let r of result) {
    if (!existingIds.has(r.traitId)) {
      const trait = { ...r }
      trait.userId = helper.bigIntToNumber(member.userId)
      trait.createdBy = Number(currentUser.userId || config.TC_WEBSERVICE_USERID)
      if (trait.traits) {
        trait.traits = { traitId: trait.traitId, data: trait.traits.data }
      } else {
        trait.traits = { traitId: trait.traitId, data: [] }
      }
      trait.createdAt = new Date().getTime()
      await helper.postBusEvent(constants.TOPICS.MemberTraitCreated, trait)
    } else {
      await helper.postBusEvent(constants.TOPICS.MemberTraitUpdated, r)
    }
  }
  return result
}

updateTraits.schema = createTraits.schema

/**
 * Remove member traits. If traitIds query parameter is not provided, then all member traits are removed.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 */
async function removeTraits (currentUser, handle, query) {
  // parse trait ids
  const traitIds = helper.parseCommaSeparatedString(query.traitIds, TRAIT_IDS) || TRAIT_IDS
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to remove traits of the member.')
  }
  // get existing traits
  const queryResult = await queryTraits(member.userId, TRAIT_IDS)
  let existingTraits = queryResult.data
  // check if any given trait id is not found
  _.forEach(traitIds || [], (id) => {
    if (!_.find(existingTraits, (existing) => existing.traitId === id)) {
      throw new errors.NotFoundError(`The trait id ${id} is not found for the member.`)
    }
  })
  // remove traits
  if (queryResult.id) {
    await prisma.$transaction(async (tx) => {
      // clear existing traits
      const models = _.uniq(_.compact(_.map(traitIds, t => traitIdModelMap[t])))
      // clear models data
      await Promise.all(_.map(models, m => tx[m].deleteMany({
        where: { memberTraitId: queryResult.id }
      })))
    })
  }
  // remove existingTraits data
  const memberProfileTraitIds = []
  _.forEach(existingTraits, t => {
    if (!traitIds || _.includes(traitIds, t.traitId)) {
      memberProfileTraitIds.push(t.traitId)
    }
  })

  existingTraits = _.filter(existingTraits, t => !traitIds.includes(t.traitId))

  await updateSkillScoreDeduction(currentUser, member, existingTraits)
  // post bus event
  if (memberProfileTraitIds.length > 0) {
    await helper.postBusEvent(constants.TOPICS.MemberTraitDeleted, {
      userId: helper.bigIntToNumber(member.userId),
      memberProfileTraitIds,
      updatedAt: new Date(),
      updatedBy: currentUser.userId || currentUser.sub
    })
  }
}

removeTraits.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    traitIds: Joi.string() // if not provided, then all member traits are removed
  })
}
/**
* This function is used to calculate a deduction to the skill score used in the talent search
* We have a calculation based on traits and if they are defined or not, including work history
* and education.  We keep the deduction at the member object level for efficiency when ranking
* members in search results.  See: TAL-77
* @param {Object} member - The member being updated
* @param {Array} traits - The updated traits for the given member
*/
async function updateSkillScoreDeduction (currentUser, member, existingTraits) {
  let skillScoreDeduction = 0
  let workHistory = false
  let education = false

  let traits = []
  if (existingTraits) {
    traits = existingTraits
  } else {
    traits = await getTraits(currentUser, member.handle, {})
  }

  let educationTrait = _.find(traits, function (trait) { return trait.traitId === 'education' })

  if (educationTrait && education === false) {
    education = true
  }

  let workTrait = _.find(traits, function (trait) { return trait.traitId === 'work' })

  if (workTrait && workHistory === false) {
    workHistory = true
  }

  // TAL-77 : missing experience, reduce match by 2%
  if (!workHistory) {
    skillScoreDeduction = skillScoreDeduction - 0.02
  }

  // TAL-77 : missing education, reduce match by 2%
  if (!education) {
    skillScoreDeduction = skillScoreDeduction - 0.02
  }

  // Only update if the value is new or has changed
  if (member.skillScoreDeduction === null || member.skillScoreDeduction !== skillScoreDeduction) {
    await prisma.member.update({
      where: { userId: member.userId },
      data: {
        skillScoreDeduction
      }
    })
  }
}

module.exports = {
  getTraits,
  createTraits,
  updateTraits,
  removeTraits
}

logger.buildService(module.exports)

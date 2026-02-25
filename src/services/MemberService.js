/**
 * This service provides operations of members.
 */

const _ = require('lodash')
const Joi = require('joi')
const crypto = require('crypto')
const uuid = require('uuid/v4')
const config = require('config')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const constants = require('../../app-constants')
const mailchimp = require('../common/mailchimp')
const hubspot = require('../common/hubspot')
const copilotEmailAccess = require('../common/copilotEmailAccess')
const memberTraitService = require('./MemberTraitService')
const mime = require('mime-types')
const fileType = require('file-type')
const fileTypeChecker = require('file-type-checker')
const sharp = require('sharp')
const { bufferContainsScript } = require('../common/image')
const { htmlToText } = require('../common/htmlUtils')
const countryCallingCodes = require('country-calling-code')
const prismaHelper = require('../common/prismaHelper')
const prismaManager = require('../common/prisma')
const identityPrismaManager = require('../common/identityPrisma')
const vanillaDb = require('../common/vanillaDb')
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()
const challengesPrisma = prismaManager.getChallengesClient()
const academyPrisma = prismaManager.getAcademyClient()
const resourcesPrisma = prismaManager.getResourcesClient()
const engagementsPrisma = prismaManager.getEngagementsClient()
const profilePDFService = require('./ProfilePDFService')
const request = require('request')
const cityTimezones = require('city-timezones')
const moment = require('moment-timezone')

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName', 'tracks', 'status',
  'addresses', 'description', 'email', 'country', 'homeCountryCode', 'competitionCountryCode', 'photoURL', 'verified', 'maxRating',
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'loginCount', 'lastLoginDate', 'skills', 'availableForGigs',
  'skillScoreDeduction', 'namesAndHandleAppearance', 'lastProfileConfirmationDate', 'availableForGigsLastUpdateDate', 'identityVerified', 'recentActivity']

const INTERNAL_MEMBER_FIELDS = ['newEmail', 'emailVerifyToken', 'emailVerifyTokenDate', 'newEmailVerifyToken',
  'newEmailVerifyTokenDate', 'handleSuggest', 'lastProfileConfirmationDate', 'availableForGigsLastUpdateDate']

const HANDLE_MIN_LENGTH = 3
const HANDLE_MAX_LENGTH = 64
const HANDLE_REGEX = /^[-A-Za-z0-9_.`{}[\]]{3,64}$/
const HANDLE_PUNCTUATION_ONLY_REGEX = /^[-_.{}[\]]+$/

/**
 * Clean member fields according to current user.
 * @param {Object} currentUser the user who performs operation
 * @param {Object} member the member profile data
 * @returns {Object} the cleaned member profile data
 */
function cleanMember (currentUser, member, selectFields) {
  let response = omitMemberAttributes(currentUser, member)
  // select fields
  if (selectFields) {
    response = _.pick(response, selectFields)
  }

  if (response.addresses) {
    response.addresses.forEach((address) => {
      if (address.stateCode === null) {
        address.stateCode = ''
      }
      if (address.streetAddr1 === null) {
        address.streetAddr1 = ''
      }
      if (address.streetAddr2 === null) {
        address.streetAddr2 = ''
      }
      if (address.city === null) {
        address.city = ''
      }
      if (address.zip === null) {
        address.zip = ''
      }
    })
  }

  if (response.skills) {
    response.skills.forEach((skill) => {
      skill.createdAt = undefined
      skill.updatedAt = undefined
    })
  }

  return response
}

function omitMemberAttributes (currentUser, mb) {
  // remove some internal fields
  let res = _.omit(mb, INTERNAL_MEMBER_FIELDS)
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  const canManageMember = helper.canManageMember(currentUser, mb)
  const hasSensitiveDataRole = helper.hasSensitiveDataRole(currentUser)
  const isM2M = currentUser && currentUser.isMachine
  const isSelf = currentUser && currentUser.handle && mb.handleLower &&
    currentUser.handle.trim().toLowerCase() === mb.handleLower.trim().toLowerCase()
  const canSeeIdentityVerified = isM2M || hasSensitiveDataRole || isSelf
  const canSeeRecentActivity = isM2M || hasSensitiveDataRole || isSelf
  const canSeeFullAddress = canManageMember || hasSensitiveDataRole

  if (!canManageMember ) {
    res = _.omit(res, config.MEMBER_SECURE_FIELDS)
    res = helper.truncateLastName(res)
  }

  // Show full address to admins and TMs
  if (!canSeeFullAddress) {
    res = helper.secureMemberAddressData(res)
  }

  if (!canManageMember && !hasSensitiveDataRole) {
    res = _.omit(res, config.COMMUNICATION_SECURE_FIELDS)
    if (res.phones) {
      delete res.phones
    }
  }
  // Remove identityVerified if user doesn't have permission
  if (!canSeeIdentityVerified && res.identityVerified !== undefined) {
    delete res.identityVerified
  }

  if (!canSeeRecentActivity && res.recentActivity !== undefined) {
    delete res.recentActivity
  }

  // Remove availableForGigs if user doesn't have permission
  if (!canManageMember && !hasSensitiveDataRole && res.availableForGigs !== undefined) {
    delete res.availableForGigs
  }

  return res
}

function validateHandleRules (handle, rawHandle) {
  if (handle.length < HANDLE_MIN_LENGTH || handle.length > HANDLE_MAX_LENGTH) {
    throw new errors.BadRequestError(
      `Length of Handle in character should be between ${HANDLE_MIN_LENGTH} and ${HANDLE_MAX_LENGTH}.`
    )
  }
  if ((rawHandle || handle).indexOf(' ') !== -1) {
    throw new errors.BadRequestError('Handle may not contain a space')
  }
  if (!HANDLE_REGEX.test(handle)) {
    throw new errors.BadRequestError(
      'Handle must be 3-64 characters long and can only contain alphanumeric characters and _.-`[]{} symbols.'
    )
  }
  if (HANDLE_PUNCTUATION_ONLY_REGEX.test(handle)) {
    throw new errors.BadRequestError('Handle may not contain only punctuation.')
  }
}

/**
 * Get member skills with user id
 * @param {BigInt} userId prisma BigInt userId
 */
async function getMemberSkills (userId) {
  const skillList = await skillsPrisma.userSkill.findMany({
    where: {
      userId: helper.bigIntToNumber(userId)
    },
    include: prismaHelper.skillsIncludeParams
  })
  return prismaHelper.buildMemberSkills(skillList)
}

/**
 * Compute member recent activity with user id
 * @param {BigInt} userId prisma BigInt userId
 * @returns {Boolean} true if member has recent activity in last 3 months
 */
async function getMemberRecentActivity (userId) {
  try {
    const threeMonthsAgo = new Date()
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3)

    const recent = await resourcesPrisma.resource.findFirst({
      where: {
        memberId: String(userId),
        resourceRole: {
          name: { in: ['Submitter', 'Copilot', 'Reviewer'] }
        },
        createdAt: { gte: threeMonthsAgo }
      }
    })

    return !!recent
  } catch (err) {
    console.error(`Failed to query recent activity for userId: ${userId}`, err)
    return false
  }
}

const countryCodes = countryCallingCodes.codes || []

/**
 * Get country display name from ISO 3166-1 alpha-3 code (e.g. ALB -> Albania)
 * @param {string} isoCode3 - 3-letter country code (e.g. homeCountryCode)
 * @returns {string|null} country name or null if not found
 */
function getCountryNameFromCode (isoCode3) {
  if (!isoCode3 || typeof isoCode3 !== 'string') return null
  const code = isoCode3.trim().toUpperCase()
  const item = countryCodes.find(c => (c.isoCode3 || '').toUpperCase() === code)
  return item ? item.country : null
}

/**
 * Get member profile data.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @param {Array} allowedFields optional array of allowed fields (defaults to MEMBER_FIELDS)
 * @returns {Object} the member profile data
 */
async function getMemberData (handle, query, allowedFields = MEMBER_FIELDS) {
  // validate and parse query parameter
  const selectFields = helper.parseCommaSeparatedString(query.fields, allowedFields) || allowedFields

  const prismaFilter = {
    where: {
      handleLower: handle.trim().toLowerCase()
    },
    include: {}
  }
  if (_.includes(selectFields, 'maxRating')) {
    prismaFilter.include.maxRating = true
  }
  if (_.includes(selectFields, 'addresses')) {
    prismaFilter.include.addresses = true
  }
  if (_.includes(selectFields, 'phones')) {
    prismaFilter.include.phones = true
  }

  // To keep original business logic, let's use findMany
  const member = await prisma.member.findUnique(prismaFilter)
  if (!member || !member.userId) {
    throw new errors.NotFoundError(`Member with handle: "${handle}" doesn't exist`)
  }

  // get member skills
  if (_.includes(selectFields, 'skills')) {
    member.skills = await getMemberSkills(member.userId)
  }

  return member
}

/**
 * Get member profile data.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member profile data
 */
async function getMember (currentUser, handle, query) {
  // Check if user has permission to see phones
  // Phones are visible to: self, users with sensitive data roles (Talent Manager, admin) and M2M
  const hasSensitiveDataRole = helper.hasSensitiveDataRole(currentUser)
  const isM2M = currentUser && currentUser.isMachine
  const isSelf = currentUser && currentUser.handle &&
    currentUser.handle.trim().toLowerCase() === handle.trim().toLowerCase()

  const canSeePhones = isM2M || hasSensitiveDataRole || isSelf
  const canSeeRecentActivity = isM2M || hasSensitiveDataRole || isSelf
  // Identity verified field has same access control as phones
  const canSeeIdentityVerified = isM2M || hasSensitiveDataRole || isSelf
  const allowedFields = canSeePhones ? [...MEMBER_FIELDS, 'phones'] : MEMBER_FIELDS

  const threeMonthsAgo = new Date()
  threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3)

  // Conditionally add phones and recent activity to query if user has permission
  const modifiedQuery = { ...query }
  if (canSeePhones) {
    // If fields are specified, check if phones is already included
    if (modifiedQuery.fields) {
      const fieldsArray = modifiedQuery.fields.split(',').map(f => f.trim())
      if (!_.includes(fieldsArray, 'phones')) {
        modifiedQuery.fields = `${modifiedQuery.fields},phones`
      }
    } else {
      // If no fields specified, add phones to the default fields
      modifiedQuery.fields = MEMBER_FIELDS.join(',') + ',phones'
    }
  }

  const member = await getMemberData(handle, modifiedQuery, allowedFields)

  if (!member || !member.userId) {
    throw new errors.NotFoundError(`Member with handle: "${handle}" doesn't exist`)
  }
  // convert members data structure to response
  prismaHelper.convertMember(member)

  // Query identity verification status from finance schema if user has permission
  if (canSeeIdentityVerified) {
    try {
      const financePrisma = prismaManager.getFinanceClient()
      const userIdString = String(helper.bigIntToNumber(member.userId))
      const verification = await financePrisma.user_identity_verification_associations.findFirst({
        where: {
          user_id: userIdString,
          verification_status: 'ACTIVE'
        }
      })
      member.identityVerified = verification !== null
    } catch (err) {
      // If finance schema query fails, log error but don't fail the request
      logger.error(`Failed to query identity verification for user ${member.userId}: ${err.message}`)
      member.identityVerified = false
    }
  }

  // get member recent activity
  if (canSeeRecentActivity) {
    member.recentActivity = await getMemberRecentActivity(member.userId)
  }

  // validate and parse query parameter
  const selectFields = helper.parseCommaSeparatedString(query.fields, allowedFields) || allowedFields
  // Add phones to selectFields if user has permission
  if (canSeePhones && !_.includes(selectFields, 'phones')) {
    selectFields.push('phones')
  }
  // add recent activity to selectFields if permitted user
  if (_.includes(selectFields, 'recentActivity') && canSeeRecentActivity) {
    selectFields.push('recentActivity')
  }
  // Add identityVerified to selectFields if user has permission
  if (canSeeIdentityVerified && !_.includes(selectFields, 'identityVerified')) {
    selectFields.push('identityVerified')
  }
  // clean member fields according to current user
  const response = cleanMember(currentUser, member, selectFields)

  // Copilots can only see member email when they share at least one challenge resource.
  if (response.email !== undefined) {
    const canAccessMemberEmail = await copilotEmailAccess.canCopilotAccessMemberEmail(
      currentUser,
      member.userId
    )
    if (!canAccessMemberEmail) {
      delete response.email
    }
  }

  return response
}

getMember.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    fields: Joi.string()
  })
}

/**
 * Get member profile completeness data.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters (not used currently)
 * @returns {Object} the member profile data
 */
async function getProfileCompleteness (currentUser, handle, query) {
  // Don't pass the query parameter to the trait service - we want *all* traits and member data
  // to come back for calculation of the completeness
  const memberTraits = await memberTraitService.getTraits(currentUser, handle, {})
  // Avoid getting the member stats, since we don't need them here, and performance is
  // better without them
  const memberFields = { 'fields': 'userId,handle,handleLower,photoURL,description,skills,verified,availableForGigs,availableForGigsLastUpdateDate,lastProfileConfirmationDate,updatedAt,addresses' }
  const member = await getMemberData(handle, memberFields)

  // Used for calculating the percentComplete
  let completeItems = 0

  let response = {}
  response.userId = helper.bigIntToNumber(member.userId)
  response.handle = member.handle
  let data = {}

  // We use this to hold the items not completed, and then randomly pick one
  // to use when showing the "toast" to prompt the user to complete an item in their profile
  let showToast = []
  // Set default values

  // TODO: Turn this back on once we have verification flow implemented elsewhere
  // data.verified = false
  data.skills = false
  data.gigAvailability = false
  data.bio = false
  data.workHistory = false
  data.education = false
  data.location = false

  const totalItems = Object.keys(data).length

  data.skillsLastUpdateDate = undefined
  data.gigAvailabilityLastUpdateDate = undefined
  data.workHistoryLastUpdateDate = undefined
  data.educationLastUpdateDate = undefined
  data.locationLastUpdateDate = undefined
  data.profileLastUpdateDate = new Date(member.updatedAt).toISOString()
  data.lastProfileConfirmationDate = member.lastProfileConfirmationDate ? new Date(member.lastProfileConfirmationDate).toISOString() : undefined

  if (member.availableForGigs != null) {
    completeItems += 1
    data.gigAvailability = true
    data.gigAvailabilityLastUpdateDate = member.availableForGigsLastUpdateDate || undefined
  }

  _.forEach(memberTraits, (item) => {
    if (item.traitId === 'education' && item.traits.data.length > 0 && !data.education) {
      completeItems += 1
      data.education = true
      data.educationLastUpdateDate = new Date(item.updatedAt).toISOString()
    }

    if (item.traitId === 'work' && item.traits.data.length > 0 && !data.workHistory) {
      completeItems += 1
      data.workHistory = true
      data.workHistoryLastUpdateDate = new Date(item.updatedAt).toISOString()
    }
  })
  // Push on the incomplete traits for picking a random toast to show
  if (!data.education) {
    showToast.push('education')
  }
  if (!data.workHistory) {
    showToast.push('workHistory')
  }
  if (!data.gigAvailability) {
    showToast.push('gigAvailability')
  }

  // TODO: Do we use the short bio or the "description" field of the member object?
  if (member.description && !data.bio) {
    completeItems += 1
    data.bio = true
  } else {
    showToast.push('bio')
  }

  // TODO: Turn this back on once verification is implemented
  // if(member.verified){
  //   completeItems += 1
  //   data.verified=true
  // }
  // else{
  //   showToast.push("verified")
  // }

  // Must have at least 3 skills entered
  if (member.skills && member.skills.length >= 3) {
    completeItems += 1
    data.skills = true

    const lastUpdateAt = member.skills.reduce((LastUpdateAt, skill) => (
      Math.max(LastUpdateAt, (skill.updatedAt || skill.createdAt).getTime())
    ), new Date(0))
    data.skillsLastUpdateDate = new Date(lastUpdateAt).toISOString()
  } else {
    showToast.push('skills')
  }

  const hasCountry = !!(member.homeCountryCode)

  const hasCity = !!(
    member.addresses
    && member.addresses.length
    && member.addresses.some(a => a && a.city && a.city.trim())
  )

  // Should have city and country in at least one address
  if (hasCity && hasCountry) {
    completeItems += 1
    data.location = true

    const addrDates = member.addresses
      .map(s => s.updatedAt || s.createdAt)
      .filter(Boolean)
      .map(d => new Date(d).getTime())

    if (addrDates.length > 0) {
      data.locationLastUpdateDate = new Date(Math.max(...addrDates)).toISOString()
    }
  }

  // Calculate the percent complete and round to 2 decimal places
  data.percentComplete = Math.round(completeItems / totalItems * 100) / 100
  response.data = data

  // Pick a random, unfinished item to show in the toast after the user logs in
  if (showToast.length > 0 && !query.toast) {
    response.showToast = showToast[Math.floor(Math.random() * showToast.length)]
  } else if (query.toast) {
    response.showToast = query.toast
  }

  return response
}

getProfileCompleteness.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    fields: Joi.string(),
    toast: Joi.string()
  })
}

/**
 * Compute the current user's userId
 * @param {Object} currentUser the user who performs operation
 * @param {Object} query the query parameters (not used currently)
 * @returns {Object} uid_signature: user's hashed userId
 */
async function getMemberUserIdSignature (currentUser, query) {
  const hashingSecret = config.HASHING_KEYS[(query.type || '').toUpperCase()]

  const userIdHash = crypto
    .createHmac('sha256', hashingSecret)
    .update(currentUser.userId)
    .digest('hex')

  return { uid_signature: userIdHash }
}

getMemberUserIdSignature.schema = {
  currentUser: Joi.any(),
  query: Joi.object().keys({
    type: Joi.string().valid('userflow').required()
  }).required()
}

/**
 * Update member profile data, only passed fields will be updated.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @param {Object} data the member data to be updated
 * @returns {Object} the updated member data
 */
async function updateMember (currentUser, handle, query, data) {
  const operatorId = currentUser.userId || currentUser.sub
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member.')
  }
  if (_.has(data, 'handle') || _.has(data, 'handleLower')) {
    throw new errors.BadRequestError('Handle updates must use the handle update endpoint.')
  }
  // validate and parse query parameter
  const selectFields = helper.parseCommaSeparatedString(query.fields, MEMBER_FIELDS) || MEMBER_FIELDS
  // check if email has changed
  const emailChanged = data.email &&
    (!member.email || data.email.trim().toLowerCase() !== member.email.trim().toLowerCase())

  if (emailChanged) {
    const emailCount = await prisma.member.count({
      where: { email: data.email }
    })
    if (emailCount > 0) {
      throw new errors.EmailRegisteredError(`Email "${data.email}" is already registered`)
    }
    data.newEmail = data.email
    delete data.email
    data.emailVerifyToken = uuid()
    data.emailVerifyTokenDate = new Date(new Date().getTime() + Number(config.VERIFY_TOKEN_EXPIRATION) * 60000).toISOString()
    data.newEmailVerifyToken = uuid()
    data.newEmailVerifyTokenDate = new Date(new Date().getTime() + Number(config.VERIFY_TOKEN_EXPIRATION) * 60000).toISOString()
  }
  const phoneRegex = constants.PHONE_REGEX
  if (data.phones !== undefined) {
    if (!Array.isArray(data.phones)) {
      throw new errors.BadRequestError('phones must be an array')
    }
    for (const phone of data.phones) {
      if (!phone.type || typeof phone.type !== 'string') {
        throw new errors.BadRequestError('Each phone must have a type (string)')
      }
      if (!phone.number || typeof phone.number !== 'string') {
        throw new errors.BadRequestError('Each phone must have a number (string)')
      }
      if (!phoneRegex.test(phone.number)) {
        throw new errors.BadRequestError(`Phone number "${phone.number}" is not in valid E.164 format (must start with + followed by 1-15 digits)`)
      }
    }
  }

  // set updated fields in data
  data.updatedAt = new Date()
  data.updatedBy = operatorId

  // Track availableForGigs changes
  if (data.availableForGigs !== undefined) {
    data.availableForGigsLastUpdateDate = new Date()
  }

  // open a transaction to handle update
  const result = await prisma.$transaction(async (tx) => {
    // check if address is present
    if (data.addresses && data.addresses.length > 0) {
      // clear current addresses
      await tx.memberAddress.deleteMany({
        where: { userId: member.userId }
      })
      // create new addresses
      await tx.memberAddress.createMany({
        data: _.map(data.addresses, t => ({
          ...t,
          // default address type to HOME if not provided
          type: t.type || 'HOME',
          userId: member.userId,
          createdBy: operatorId
        }))
      })
    }
    // clear addresses so it doesn't affect prisma.udpate
    delete data.addresses

    const phonesWereUpdated = data.phones !== undefined
    if (phonesWereUpdated) {
      await tx.memberPhone.deleteMany({
        where: { userId: member.userId }
      })
      if (data.phones.length > 0) {
        await tx.memberPhone.createMany({
          data: _.map(data.phones, t => ({
            type: t.type,
            number: t.number,
            userId: member.userId,
            createdBy: operatorId
          }))
        })
      }
    }
    delete data.phones

    const includeFields = { addresses: true }
    if (_.includes(selectFields, 'phones') || phonesWereUpdated) {
      includeFields.phones = true
    }

    return tx.member.update({
      where: { userId: member.userId },
      data,
      include: includeFields
    })
  })

  // convert prisma data to response format
  prismaHelper.convertMember(result)
  // send data to event bus
  await helper.postBusEvent(constants.TOPICS.MemberUpdated, result)
  if (emailChanged) {
    // send email verification to old email
    await helper.postBusEvent(constants.TOPICS.EmailChanged, {
      data: {
        subject: 'Topcoder - Email Change Verification',
        userHandle: member.handle,
        verificationAgreeUrl: (config.EMAIL_VERIFY_AGREE_URL).replace(
          '<emailVerifyToken>', data.emailVerifyToken),
        verificationDisagreeUrl: config.EMAIL_VERIFY_DISAGREE_URL
      },
      recipients: [member.email]
    })
    // send email verification to new email
    await helper.postBusEvent(constants.TOPICS.EmailChanged, {
      data: {
        subject: 'Topcoder - Email Change Verification',
        userHandle: member.handle,
        verificationAgreeUrl: (config.EMAIL_VERIFY_AGREE_URL).replace(
          '<emailVerifyToken>', data.newEmailVerifyToken),
        verificationDisagreeUrl: config.EMAIL_VERIFY_DISAGREE_URL
      },
      recipients: [data.newEmail]
    })
  }
  // clean member fields according to current user
  return cleanMember(currentUser, result, selectFields)
}

updateMember.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    fields: Joi.string()
  }),
  data: Joi.object().keys({
    handle: Joi.forbidden(),
    handleLower: Joi.forbidden(),
    handle: Joi.forbidden(),
    handleLower: Joi.forbidden(),
    firstName: Joi.string(),
    lastName: Joi.string(),
    description: Joi.string().allow(''),
    otherLangName: Joi.string(),
    status: Joi.string(),
    email: Joi.string().email(),
    addresses: Joi.array().items(Joi.object().keys({
      streetAddr1: Joi.string().allow('').allow(null),
      streetAddr2: Joi.string().allow('').allow(null),
      city: Joi.string().allow('').allow(null),
      zip: Joi.string().allow('').allow(null),
      stateCode: Joi.string().allow('').allow(null),
      type: Joi.string()
    })),
    phones: Joi.array().items(Joi.object().keys({
      type: Joi.string().required(),
      number: Joi.string().regex(constants.PHONE_REGEX, 'E.164 format').required()
    })),
    verified: Joi.bool(),
    country: Joi.string(),
    homeCountryCode: Joi.string(),
    competitionCountryCode: Joi.string(),
    photoURL: Joi.string().uri().allow('').allow(null),
    tracks: Joi.array().items(Joi.string()),
    availableForGigs: Joi.bool().allow(null),
    namesAndHandleAppearance: Joi.string().allow(null)
  }).required()
}

/**
 * Update member handle.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @param {Object} data the handle update payload
 * @returns {Object} the updated member data
 */
async function updateHandle (currentUser, handle, query, data) {
  const operatorId = currentUser.userId || currentUser.sub
  const member = await helper.getMemberByHandle(handle)

  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member handle.')
  }

  const newHandle = (data.newHandle || '').trim()
  if (!newHandle) {
    throw new errors.BadRequestError('newHandle is required')
  }

  const selectFields = helper.parseCommaSeparatedString(query.fields, MEMBER_FIELDS) || MEMBER_FIELDS

  if (newHandle === member.handle) {
    const currentMember = await getMemberData(handle, query, MEMBER_FIELDS)
    prismaHelper.convertMember(currentMember)
    return cleanMember(currentUser, currentMember, selectFields)
  }

  const newHandleLower = newHandle.toLowerCase()
  const existingMember = await prisma.member.findUnique({
    where: { handleLower: newHandleLower }
  })
  if (existingMember && String(existingMember.userId) !== String(member.userId)) {
    throw new errors.BadRequestError(`Handle "${newHandle}" is already registered`)
  }

  const identityPrisma = identityPrismaManager.getIdentityClient()
  const identityUserId = helper.bigIntToNumber(member.userId)
  const existingIdentity = await identityPrisma.user.findFirst({
    where: { handle_lower: newHandleLower }
  })
  if (existingIdentity && Number(existingIdentity.user_id) !== identityUserId) {
    throw new errors.BadRequestError(`Handle "${newHandle}" is already registered`)
  }

  const vanillaPool = vanillaDb.getVanillaPool()
  const now = new Date()
  let updatedMember
  let identityUpdated = false
  let memberUpdated = false
  let vanillaUpdated = false

  try {
    await updateIdentityHandle(identityUserId, newHandle, now)
    identityUpdated = true

    updatedMember = await prisma.member.update({
      where: { userId: member.userId },
      data: {
        handle: newHandle,
        handleLower: newHandleLower,
        updatedAt: now,
        updatedBy: operatorId
      },
      include: { addresses: true }
    })
    memberUpdated = true

    await updateVanillaHandle(member.handle, newHandle, vanillaPool)
    vanillaUpdated = true
  } catch (err) {
    if (vanillaUpdated) {
      try {
        await updateVanillaHandle(newHandle, member.handle, vanillaPool)
      } catch (rollbackErr) {
        logger.error(`Failed to rollback Vanilla handle update for ${member.userId}: ${rollbackErr.message}`)
      }
    }
    if (memberUpdated) {
      try {
        await prisma.member.update({
          where: { userId: member.userId },
          data: {
            handle: member.handle,
            handleLower: member.handleLower,
            updatedAt: new Date(),
            updatedBy: operatorId
          }
        })
      } catch (rollbackErr) {
        logger.error(`Failed to rollback member handle update for ${member.userId}: ${rollbackErr.message}`)
      }
    }
    if (identityUpdated) {
      try {
        await updateIdentityHandle(identityUserId, member.handle, new Date())
      } catch (rollbackErr) {
        logger.error(`Failed to rollback identity handle update for ${member.userId}: ${rollbackErr.message}`)
      }
    }
    throw err
  }

  prismaHelper.convertMember(updatedMember)
  await helper.postBusEvent(constants.TOPICS.MemberUpdated, updatedMember)
  return cleanMember(currentUser, updatedMember, selectFields)
}

updateHandle.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    fields: Joi.string()
  }),
  data: Joi.object().keys({
    newHandle: Joi.string().required()
  }).required()
}

/**
 * Verify email.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the verification result
 */
async function verifyEmail (currentUser, handle, query) {
  const member = await helper.getMemberByHandle(handle)
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member.')
  }
  let verifiedEmail
  if (member.emailVerifyToken === query.token) {
    if (new Date(member.emailVerifyTokenDate) < new Date()) {
      throw new errors.BadRequestError('Verification token expired.')
    }
    member.emailVerifyToken = 'VERIFIED'
    member.emailVerifyTokenDate = new Date(0).toISOString()
    verifiedEmail = member.email
  } else if (member.newEmailVerifyToken === query.token) {
    if (new Date(member.newEmailVerifyTokenDate) < new Date()) {
      throw new errors.BadRequestError('Verification token expired.')
    }
    member.newEmailVerifyToken = 'VERIFIED'
    member.newEmailVerifyTokenDate = new Date(0).toISOString()
    verifiedEmail = member.newEmail
  } else {
    throw new errors.BadRequestError('Wrong verification token.')
  }
  const emailChangeCompleted = (member.emailVerifyToken === 'VERIFIED' && member.newEmailVerifyToken === 'VERIFIED')
  if (emailChangeCompleted) {
    // emails are verified successfully, move new email to main email
    member.email = member.newEmail
    member.emailVerifyToken = null
    member.emailVerifyTokenDate = new Date(0).toISOString()
    member.newEmail = null
    member.newEmailVerifyToken = null
    member.newEmailVerifyTokenDate = new Date(0).toISOString()
  }
  member.updatedAt = new Date()
  member.updatedBy = currentUser.userId || currentUser.sub
  // update member in db
  const result = await prisma.member.update({
    where: { userId: member.userId },
    data: _.omit(member, ['maxRating'])
  })
  prismaHelper.convertMember(result)
  await helper.postBusEvent(constants.TOPICS.MemberUpdated, result)
  return { emailChangeCompleted, verifiedEmail }
}

verifyEmail.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    token: Joi.string().required()
  }).required()
}

/**
 * Upload photo.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} files the uploaded files
 * @returns {Object} the upload result
 */
async function uploadPhoto (currentUser, handle, files) {
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to upload photo for the member.')
  }
  const file = files.photo
  if (file.truncated) {
    throw new errors.BadRequestError(`The photo is too large, it should not exceed ${
      (config.FILE_UPLOAD_SIZE_LIMIT / 1024 / 1024).toFixed(2)
    } MB.`)
  }
  // name len validation
  if (file.name && file.name.length > config.FILE_UPLOAD_MAX_FILE_NAME_LENGTH) {
    throw new errors.BadRequestError(`The photo name is too long, it should not exceed ${
      config.FILE_UPLOAD_MAX_FILE_NAME_LENGTH
    } characters.`)
  }
  // mime type validation
  const type = await fileType.fromBuffer(file.data)
  const fileContentType = type.mime
  if (!fileContentType || !fileContentType.startsWith('image/')) {
    throw new errors.BadRequestError('The photo should be an image file.')
  }
  // content type validation
  const isImage = fileTypeChecker.validateFileType(
    file.data,
    ['jpeg', 'png']
  )
  if (!isImage) {
    throw new errors.BadRequestError('The photo should be an image file, either jpg, jpeg or png.')
  }
  const fileExt = mime.extension(fileContentType)
  var fileName = handle + '-' + new Date().getTime() + '.' + fileExt

  if (bufferContainsScript(file.data)) {
    throw new errors.BadRequestError('The photo should not contain any scripts or iframes.')
  }

  const sanitizedBuffer = await sharp(file.data)
    .toBuffer()

  if (bufferContainsScript(sanitizedBuffer)) {
    throw new errors.BadRequestError('Sanitized photo should not contain any scripts or iframes.')
  }

  // upload photo to S3
  // const photoURL = await helper.uploadPhotoToS3(file.data, file.mimetype, file.name)
  const photoURL = await helper.uploadPhotoToS3(sanitizedBuffer, file.mimetype, fileName)

  // update member's photoURL
  const result = await prisma.member.update({
    where: { userId: member.userId },
    data: {
      photoURL,
      updatedAt: new Date(),
      updatedBy: currentUser.userId || currentUser.sub
    }
  })
  prismaHelper.convertMember(result)
  // post bus event
  await helper.postBusEvent(constants.TOPICS.MemberUpdated, result)
  return { photoURL }
}

/**
 * Delete member profile data and scrub personal details.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @returns {Object} the deletion result
 */
async function deleteMember (currentUser, handle, data) {
  if (!currentUser || (!currentUser.isMachine && !helper.hasAdminRole(currentUser))) {
    throw new errors.ForbiddenError('You are not allowed to delete the member.')
  }

  if (!data || !data.ticketUrl) {
    throw new errors.BadRequestError('ticketUrl is required for deletion.')
  }

  const member = await helper.getMemberByHandle(handle)
  const originalEmail = member.email
  const operatorId = currentUser.userId || currentUser.sub || config.TC_WEBSERVICE_USERID
  const nanoId = generateNanoId()
  const deletedHandle = `DELETED_USER_${nanoId}`
  const deletedEmail = `${nanoId}@topcoder.com`
  const identityUserId = helper.bigIntToNumber(member.userId)
  const now = new Date()
  const ticketUrl = data.ticketUrl

  const updatedMember = await prisma.$transaction(async (tx) => {
    const traitsRecord = await tx.memberTraits.findUnique({
      where: { userId: member.userId },
      select: { id: true }
    })

    let memberTraitId = traitsRecord ? traitsRecord.id : null

    if (!memberTraitId) {
      const createdTraits = await tx.memberTraits.create({
        data: {
          userId: member.userId,
          createdBy: operatorId,
          updatedBy: operatorId
        }
      })
      memberTraitId = createdTraits.id
    }

    await tx.memberTraitWork.deleteMany({ where: { memberTraitId } })
    await tx.memberTraitEducation.deleteMany({ where: { memberTraitId } })
    await tx.memberTraitPersonalization.deleteMany({
      where: {
        memberTraitId,
        key: { in: ['quote', 'Quote'] }
      }
    })

    await tx.memberTraitPersonalization.create({
      data: {
        memberTraitId,
        key: 'delete_ticket',
        value: ticketUrl,
        createdBy: operatorId,
        updatedBy: operatorId
      }
    })

    await tx.memberTraits.update({
      where: { id: memberTraitId },
      data: {
        updatedAt: now,
        updatedBy: operatorId
      }
    })

    return tx.member.update({
      where: { userId: member.userId },
      data: {
        photoURL: null,
        homeCountryCode: null,
        competitionCountryCode: null,
        aggregatedSkills: null,
        enteredSkills: null,
        email: deletedEmail,
        newEmail: null,
        newEmailVerifyToken: null,
        newEmailVerifyTokenDate: null,
        emailVerifyToken: null,
        emailVerifyTokenDate: null,
        handle: deletedHandle,
        handleLower: deletedHandle.toLowerCase(),
        updatedAt: now,
        updatedBy: operatorId
      }
    })
  })

  await skillsPrisma.userSkill.deleteMany({ where: { userId: identityUserId } })
  await updateIdentityRecords(identityUserId, deletedHandle, deletedEmail, now)

  // Kick off MailChimp deletion without blocking the API response.
  ;(async () => {
    try {
      await mailchimp.deleteSubscriber(originalEmail)
    } catch (err) {
      logger.error(`MailChimp deletion failed for ${originalEmail}: ${err.message}`)
    }
  })()

  if (config.HUBSPOT_API_KEY) {
    // Kick off HubSpot deletion without blocking the API response.
    ;(async () => {
      try {
        await hubspot.deleteContactByEmail(originalEmail)
      } catch (err) {
        logger.error(`HubSpot deletion failed for ${originalEmail}: ${err.message}`)
      }
    })()
  }

  prismaHelper.convertMember(updatedMember)
  await helper.postBusEvent(constants.TOPICS.MemberUpdated, updatedMember)

  return {
    handle: deletedHandle,
    email: deletedEmail
  }
}

function generateNanoId (size = 21) {
  const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
  const bytes = crypto.randomBytes(size)
  let id = ''
  for (let i = 0; i < size; i += 1) {
    id += alphabet[bytes[i] % alphabet.length]
  }
  return id
}

async function updateVanillaHandle (oldHandle, newHandle, pool) {
  const vanillaPool = pool || vanillaDb.getVanillaPool()
  const [result] = await vanillaPool.execute(
    'UPDATE vanilla.GDN_User SET Name = ? WHERE Name = ?',
    [newHandle, oldHandle]
  )
  if (!result || result.affectedRows === 0) {
    throw new errors.NotFoundError(`Vanilla user with handle: "${oldHandle}" doesn't exist`)
  }
}

async function updateIdentityHandle (userId, handle, timestamp) {
  const identityPrisma = identityPrismaManager.getIdentityClient()
  const lowerHandle = handle.toLowerCase()
  const updatedAt = timestamp || new Date()

  let userResult
  try {
    userResult = await identityPrisma.$executeRaw`
      UPDATE identity."user"
      SET handle=${handle}, handle_lower=${lowerHandle}, modify_date=${updatedAt}
      WHERE user_id=${userId}
    `
  } catch (err) {
    logger.error(`Failed to update identity handle for user ${userId}: ${err.message}`)
    throw err
  }

  if (userResult === 0) {
    throw new Error(`Identity user not updated for user ${userId}`)
  }
}

async function updateIdentityRecords (userId, handle, email, timestamp) {
  const identityPrisma = identityPrismaManager.getIdentityClient()
  const lowerHandle = handle.toLowerCase()
  const updatedAt = timestamp || new Date()

  let userResult
  let emailResult
  try {
    userResult = await identityPrisma.$executeRaw`
      UPDATE identity."user"
      SET handle=${handle}, handle_lower=${lowerHandle}, modify_date=${updatedAt}
      WHERE user_id=${userId}
    `

    emailResult = await identityPrisma.$executeRaw`
      UPDATE identity.email
      SET address=${email}, modify_date=${updatedAt}
      WHERE user_id=${userId}
    `
  } catch (err) {
    logger.error(`Failed to update identity records for user ${userId}: ${err.message}`)
    throw err
  }

  if (userResult === 0 || emailResult === 0) {
    throw new Error(`Identity records not updated for user ${userId}`)
  }
}

uploadPhoto.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  files: Joi.object().keys({
    photo: Joi.object().required()
  }).required()
}

deleteMember.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    ticketUrl: Joi.string().uri().required()
  }).required()
}

/**
 * Confirm member profile data.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @returns {Object} the updated member profile data
 */
async function confirmProfileData (currentUser, handle) {
  const member = await helper.getMemberByHandle(handle)
  // check authorization - only the profile owner or admin can confirm
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to confirm this member profile.')
  }

  // Update the lastProfileConfirmationDate
  const result = await prisma.member.update({
    where: { userId: member.userId },
    data: {
      lastProfileConfirmationDate: new Date(),
      updatedAt: new Date(),
      updatedBy: currentUser.userId || currentUser.sub
    },
    include: { addresses: true }
  })

  // convert prisma data to response format
  prismaHelper.convertMember(result)

  // clean member fields according to current user
  return cleanMember(currentUser, result, MEMBER_FIELDS)
}

confirmProfileData.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required()
}

/**
 * Normalize badge name for grouping: strip year/digits after TCO (e.g. TCO18, TCO19 -> TCO)
 * so "TCO18 Marathon Champion" and "TCO19 Marathon Champion" group as "TCO Marathon Champion"
 * @param {string} badgeName - raw or html-stripped badge name
 * @returns {string} normalized name for map key and display
 */
function normalizeAchievementName (badgeName) {
  if (!badgeName || typeof badgeName !== 'string') return ''
  return badgeName
    .replace(/\bTCO\d+\b/gi, 'TCO')
    .replace(/\s+/g, ' ')
    .trim()
}

/**
 * Fetch gamification achievements for a member
 * @param {Number} userId the member userId
 * @returns {Promise<String>} formatted achievements string
 */
async function fetchGamificationAchievements (userId) {
  try {
    if (!config.GAMIFICATION_API_URL) {
      logger.warn(`GAMIFICATION_API_URL is not configured for user ${userId}`)
      return ''
    }
    const gamificationApiUrl = config.GAMIFICATION_API_URL
    let token
    try {
      token = await helper.getM2MToken()
    } catch (tokenError) {
      logger.warn(`Cannot get M2M token for gamification API for user ${userId}: ${tokenError.message}. Achievements will be empty.`)
      return ''
    }

    if (!token) {
      logger.warn(`M2M token is null/undefined for gamification API for user ${userId}`)
      return ''
    }

    const gamificationUrl = `${gamificationApiUrl}/badges/assigned/${userId}`

    if (!gamificationUrl || typeof gamificationUrl !== 'string' || !userId) {
      logger.error(`Invalid gamification URL for user ${userId}: gamificationUrl=${gamificationUrl}, userId=${userId}`)
      return ''
    }

    const finalGamificationUrl = String(gamificationUrl || '').trim()
    if (!finalGamificationUrl || finalGamificationUrl === 'undefined' || finalGamificationUrl.includes('undefined') || finalGamificationUrl.length === 0) {
      logger.error(`Invalid final gamification URL for user ${userId}: finalUrl="${finalGamificationUrl}", baseUrl="${gamificationApiUrl}", userId=${userId}`)
      return ''
    }

    return new Promise((resolve, reject) => {
      try {
        request({
          url: finalGamificationUrl,
          headers: {
            Authorization: `Bearer ${token}`
          }
        }, (error, response, body) => {
          if (error) {
            logger.warn(`Failed to fetch gamification achievements for user ${userId}: ${error.message}`)
            resolve('')
            return
          }
          if (response.statusCode !== 200) {
            logger.warn(`Gamification API returned status ${response.statusCode} for user ${userId}`)
            resolve('')
            return
          }
          try {
            const data = JSON.parse(body)
            // Format achievements: count multiples and join with " | "
            // Response structure: { rows: [...], count: ... }
            const achievementMap = {}
            const badges = data.rows || []

            logger.debug(`Gamification API response for user ${userId}: rows count=${badges.length}, hasRows=${!!data.rows}`)

            badges.forEach(badge => {
              const orgBadge = badge.org_badge
              if (orgBadge && orgBadge.badge_name) {
                // Check if badge is active - handle both boolean and string values
                const isActive = orgBadge.active === true || orgBadge.active === 'true' || String(orgBadge.active).toLowerCase() === 'true'
                // Check status - case insensitive
                const isActiveStatus = orgBadge.badge_status && String(orgBadge.badge_status).toLowerCase() === 'active'

                logger.debug(`Badge: ${orgBadge.badge_name}, active=${orgBadge.active} (${typeof orgBadge.active}), status=${orgBadge.badge_status}, isActive=${isActive}, isActiveStatus=${isActiveStatus}`)

                if (isActive && isActiveStatus) {
                  const name = htmlToText(orgBadge.badge_name)
                  const key = normalizeAchievementName(name)
                  achievementMap[key] = (achievementMap[key] || 0) + 1
                } else {
                  logger.debug(`Badge ${orgBadge.badge_name} filtered out: isActive=${isActive}, isActiveStatus=${isActiveStatus}`)
                }
              } else {
                logger.debug('Badge missing org_badge or badge_name:', { hasOrgBadge: !!badge.org_badge, hasBadgeName: !!(badge.org_badge && badge.org_badge.badge_name) })
              }
            })

            logger.debug(`Achievement map for user ${userId}:`, achievementMap)

            const achievements = Object.entries(achievementMap)
              .map(([name, count]) => count > 1 ? `${count}x ${name}` : name)
              .join(' | ')

            logger.debug(`Final achievements string for user ${userId}: "${achievements}"`)
            resolve(achievements)
          } catch (parseError) {
            logger.warn(`Failed to parse gamification response for user ${userId}: ${parseError.message}, body: ${body && body.substring(0, 200)}`)
            resolve('')
          }
        })
      } catch (requestError) {
        logger.error(`Error creating gamification request for user ${userId}: ${requestError.message}`)
        resolve('')
      }
    })
  } catch (error) {
    logger.warn(`Error fetching gamification achievements for user ${userId}: ${error.message}`)
    return ''
  }
}

/**
 * Fetch completed certifications and courses from learning-paths-api
 * @param {Number} userId the member userId
 * @returns {Promise<Object>} object with certifications and courses arrays
 */
async function fetchCertificationsAndCourses (userId) {
  try {
    if (!config.LEARNING_PATHS_API_URL) {
      logger.warn(`LEARNING_PATHS_API_URL is not configured for user ${userId}`)
      return { certifications: [], courses: [] }
    }
    const learningPathsApiUrl = config.LEARNING_PATHS_API_URL
    let token
    try {
      token = await helper.getM2MToken()
    } catch (tokenError) {
      logger.warn(`Cannot get M2M token for user ${userId}: ${tokenError.message}. Certifications and courses will be empty.`)
      return { certifications: [], courses: [] }
    }

    if (!token) {
      logger.warn(`M2M token is null/undefined for user ${userId}`)
      return { certifications: [], courses: [] }
    }

    const learningPathsUrl = `${learningPathsApiUrl}/completed-certifications/${userId}`

    if (!learningPathsUrl || typeof learningPathsUrl !== 'string' || !userId) {
      logger.error(`Invalid learning-paths URL for user ${userId}: learningPathsUrl=${learningPathsUrl}, userId=${userId}`)
      return { certifications: [], courses: [] }
    }

    // Double-check URL is valid before making request
    const finalUrl = String(learningPathsUrl || '').trim()
    if (!finalUrl || finalUrl === 'undefined' || finalUrl.includes('undefined') || finalUrl.length === 0) {
      logger.error(`Invalid final URL constructed for user ${userId}: finalUrl="${finalUrl}", baseUrl="${learningPathsApiUrl}", userId=${userId}`)
      return { certifications: [], courses: [] }
    }

    return new Promise((resolve, reject) => {
      try {
        request({
          url: finalUrl,
          headers: {
            Authorization: `Bearer ${token}`
          }
        }, (error, response, body) => {
          if (error) {
            logger.warn(`Failed to fetch certifications for user ${userId}: ${error.message}`)
            resolve({ certifications: [], courses: [] })
            return
          }
          if (response.statusCode !== 200) {
            logger.warn(`Learning-paths API returned status ${response.statusCode} for user ${userId}`)
            resolve({ certifications: [], courses: [] })
            return
          }
          try {
            const data = JSON.parse(body)

            // Process certifications
            const certifications = (data.enrollments || [])
              .filter(e => e.status === 'completed' && e.topcoderCertification)
              .map(e => `${e.topcoderCertification.title} - Topcoder Academy`)

            // Process courses
            const courses = (data.courses || [])
              .map(c => {
                const title = c.certificationTitle || c.certification || 'Course'
                return `${title} - Topcoder Academy`
              })

            resolve({ certifications, courses })
          } catch (parseError) {
            logger.warn(`Failed to parse learning-paths response for user ${userId}: ${parseError.message}`)
            resolve({ certifications: [], courses: [] })
          }
        })
      } catch (requestError) {
        logger.error(`Error creating request for user ${userId}: ${requestError.message}`)
        resolve({ certifications: [], courses: [] })
      }
    })
  } catch (error) {
    logger.warn(`Error fetching certifications for user ${userId}: ${error.message}`)
    return { certifications: [], courses: [] }
  }
}

/**
 * Get member timezone based on city
 * @param {Object} memberData the member data
 * @returns {String|null} timezone abbreviation or null if not found
 */
function getMemberTimezone (memberData) {
  const city = memberData && memberData.addresses && memberData.addresses[0]
    ? memberData.addresses[0].city
    : null
  if (!city) return null

  const cityTimezoneData = cityTimezones.lookupViaCity(city)
  let memberTimezone = null

  if (cityTimezoneData && cityTimezoneData.length) {
    memberTimezone = cityTimezoneData[0].timezone
  }

  // Validate timezone exists
  if (memberTimezone && moment.tz.zone(memberTimezone)) {
    // Get abbreviation for display
    return moment.tz(new Date(), memberTimezone).zoneAbbr()
  }

  return null
}

/**
 * Get skill names by their IDs
 * @param {Array<String>} skillIds array of skill UUIDs
 * @returns {Promise<Object>} map of skillId -> skillName
 */
async function getSkillNamesByIds (skillIds) {
  if (!skillIds || skillIds.length === 0) {
    return {}
  }

  const skills = await skillsPrisma.skill.findMany({
    where: { id: { in: skillIds } },
    select: { id: true, name: true }
  })

  const skillMap = {}
  skills.forEach(skill => {
    skillMap[skill.id] = skill.name
  })

  return skillMap
}

/**
 * Get member roles from identity database (role_assignment + role tables)
 * @param {Number} userId the member userId
 * @returns {Promise<String[]>} array of role names
 */
async function getMemberRoles (userId) {
  try {
    if (!config.IDENTITY_DB_URL) {
      logger.warn('IDENTITY_DB_URL is not configured; cannot fetch member roles')
      return []
    }
    const identityPrisma = identityPrismaManager.getIdentityClient()
    const assignments = await identityPrisma.roleAssignment.findMany({
      where: { subjectId: Number(userId), subjectType: 1 },
      include: { role: true }
    })
    return (assignments || [])
      .filter(a => a.role && a.role.name)
      .map(a => a.role.name)
  } catch (err) {
    logger.warn(`Failed to fetch roles for user ${userId}: ${err.message}`)
    return []
  }
}

/** Track enum to display name (wins, submissions, challenges) */
const TRACK_DISPLAY_NAMES = {
  DEVELOPMENT: 'Development',
  DESIGN: 'Design',
  DATA_SCIENCE: 'Data Science',
  QUALITY_ASSURANCE: 'Quality Assurance'
}

/**
 * Fetch member stats by challenge track for PDF: wins and submissions from ChallengeWinner,
 * registrations (challenges count) from resources schema, grouped by track.
 * @param {Number} userId member userId
 * @param {Object} challengesPrisma challenges Prisma client
 * @param {Object} resourcesPrisma resources Prisma client
 * @returns {Promise<Array<{ trackName: string, wins: number, submissions: number, challenges: number }>>}
 */
async function fetchMemberStatsByTrack (userId, challengesPrisma, resourcesPrisma) {
  const trackMap = {} // track enum -> { wins, submissions, challenges }

  try {
    const numUserId = typeof userId === 'bigint' ? helper.bigIntToNumber(userId) : userId

    const winnerRows = await challengesPrisma.ChallengeWinner.findMany({
      where: {
        userId: numUserId,
        type: { in: ['PLACEMENT', 'PASSED_REVIEW'] }
      },
      include: {
        challenge: {
          include: { track: true }
        }
      }
    })

    for (const w of winnerRows) {
      const trackEnum = w.challenge?.track?.track
      if (!trackEnum) continue
      if (!trackMap[trackEnum]) {
        trackMap[trackEnum] = { wins: 0, submissions: 0, challenges: 0 }
      }
      const row = trackMap[trackEnum]
      if (w.type === 'PLACEMENT') row.wins += 1
      if (w.type === 'PASSED_REVIEW') row.submissions += 1
    }

    // 2) Resources: registrations (distinct challenges) by track
    const memberIdStr = String(userId)
    const resources = await resourcesPrisma.resource.findMany({
      where: {
        memberId: memberIdStr,
        resourceRole: {
          nameLower: 'submitter'
        }
      },
      select: { challengeId: true }
    })
    const challengeIds = [...new Set(resources.map(r => r.challengeId).filter(Boolean))]
    if (challengeIds.length > 0) {
      const challenges = await challengesPrisma.Challenge.findMany({
        where: { id: { in: challengeIds } },
        include: { track: true }
      })
      const challengeIdToTrack = {}
      for (const c of challenges) {
        const trackEnum = c.track?.track
        if (trackEnum) challengeIdToTrack[c.id] = trackEnum
      }
      const challengesPerTrack = {}
      for (const cid of challengeIds) {
        const trackEnum = challengeIdToTrack[cid]
        if (!trackEnum) continue
        if (!challengesPerTrack[trackEnum]) challengesPerTrack[trackEnum] = 0
        challengesPerTrack[trackEnum] += 1
      }
      for (const [trackEnum, count] of Object.entries(challengesPerTrack)) {
        if (!trackMap[trackEnum]) {
          trackMap[trackEnum] = { wins: 0, submissions: 0, challenges: count }
        } else {
          trackMap[trackEnum].challenges = count
        }
      }
    }

    const statsByTrack = []
    for (const [trackEnum, counts] of Object.entries(trackMap)) {
      const trackName = TRACK_DISPLAY_NAMES[trackEnum] || trackEnum
      const hasAny = Object.values(counts).some(v => typeof v === 'number' && v > 0)
      if (!hasAny) continue
      statsByTrack.push({
        trackName,
        wins: counts.wins ?? 0,
        submissions: counts.submissions ?? 0,
        challenges: counts.challenges ?? 0
      })
    }
    return statsByTrack
  } catch (err) {
    logger.warn(`fetchMemberStatsByTrack failed for user ${userId}: ${err.message}`)
    return []
  }
}

/**
 * Aggregate all data needed for PDF generation
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @returns {Promise<Object>} aggregated PDF data
 */
async function aggregatePDFData (currentUser, handle) {
  // Get base member data
  const memberData = await getMember(currentUser, handle, {})
  const userId = helper.bigIntToNumber(memberData.userId)

  // Fetch traits (work, education, languages, basicInfo, personalization)
  const traits = (await memberTraitService.getTraits(currentUser, handle, {})) || []
  const workTraits = _.get(_.find(traits, { traitId: 'work' }), 'traits.data', [])
  const educationTraits = _.get(_.find(traits, { traitId: 'education' }), 'traits.data', [])
  const languageTraits = _.get(_.find(traits, { traitId: 'languages' }), 'traits.data', [])
  const basicInfoTraits = _.get(_.find(traits, { traitId: 'basic_info' }), 'traits.data', [])

  // Collect all skill GUIDs from work experiences for batch lookup
  const allSkillIds = []
  workTraits.forEach(work => {
    if (work.associatedSkills && work.associatedSkills.length > 0) {
      allSkillIds.push(...work.associatedSkills)
    }
  })

  // Batch lookup all skill names
  const skillNameMap = await getSkillNamesByIds([...new Set(allSkillIds)])

  // Extract personalization trait to get shortBio (profileSelfTitle)
  const personalizationTrait = _.find(traits, { traitId: 'personalization' })
  const personalizationData = _.get(personalizationTrait, 'traits.data[0]', {})
  const shortBio = personalizationData.profileSelfTitle || null

  // Fetch skills from standardized-skills-api
  const skills = await getMemberSkills(memberData.userId)

  // Separate skills by display mode and verification status
  const principalSkills = { verified: [], notVerified: [] }
  const additionalSkills = { verified: [], notVerified: [] }

  skills.forEach(skill => {
    const isPrincipal = _.get(skill, 'displayMode.name') === 'principal'
    const isVerified = _.some(_.get(skill, 'levels', []), level => level.name === 'verified')
    const skillName = skill.name

    if (isPrincipal) {
      if (isVerified) {
        principalSkills.verified.push(skillName)
      } else {
        principalSkills.notVerified.push(skillName)
      }
    } else {
      if (isVerified) {
        additionalSkills.verified.push(skillName)
      } else {
        additionalSkills.notVerified.push(skillName)
      }
    }
  })

  const specialRoles = []
  const roleMap = {
    'copilot': 'Copilot',
    'administrator': 'Administrator',
    'Talent Manager': 'Talent Manager',
    'Gamification Admin': 'Gamification Admin',
    'Self-Service Customer': 'Self-Service Customer',
    'Topcoder User': 'Topcoder User',
    'TCA Admin': 'TCA Admin',
    'Payment Admin': 'Payment Admin',
    'Payment Viewer': 'Payment Viewer',
    'PaymentProvider Admin': 'PaymentProvider Admin',
    'PaymentProvider Viewer': 'PaymentProvider Viewer',
    'TaxForm Admin': 'TaxForm Admin',
    'TaxForm Viewer': 'TaxForm Viewer',
    'Topcoder Staff': 'Topcoder Staff',
    'Project Manager': 'Project Manager',
    'Connect Manager': 'Connect Manager'
  }

  const currentUserId = currentUser && (currentUser.userId || currentUser.sub)
  const isSelf = currentUserId && String(currentUserId) === String(userId)

  if (currentUser && (isSelf || helper.hasAdminRole(currentUser))) {
    const memberRoles = await getMemberRoles(userId)
    memberRoles.forEach(role => {
      const roleName = roleMap[role.toLowerCase()]
      if (roleName && !specialRoles.includes(roleName)) {
        specialRoles.push(roleName)
      }
    })
  }

  // Fetch gamification achievements
  const achievements = await fetchGamificationAchievements(userId)

  // Fetch member stats by track (wins, submissions, challenges from ChallengeWinner + resources)
  let statsByTrack = []
  try {
    statsByTrack = await fetchMemberStatsByTrack(userId, challengesPrisma, resourcesPrisma)
  } catch (err) {
    logger.warn(`aggregatePDFData: statsByTrack failed for ${handle}: ${err.message}`)
  }

  // Fetch certifications and courses
  const { certifications, courses } = await fetchCertificationsAndCourses(userId)

  // Build status bar text (Active = has recent activity in last 3 months)
  const statusBarItems = []
  const hasRecentActivity = await getMemberRecentActivity(userId)
  if (hasRecentActivity) {
    statusBarItems.push('ACTIVE')
  }
  if (memberData.availableForGigs === true) {
    statusBarItems.push('OPEN TO WORK')
  }
  const statusBarText = statusBarItems.join(' • ')

  // Format dates
  const formatDate = (date) => {
    if (!date) return null
    const d = new Date(date)
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const year = d.getFullYear()
    return `${month}/${year}`
  }

  // Get member timezone
  const timezone = getMemberTimezone(memberData)

  const countryDisplayName = getCountryNameFromCode(memberData.homeCountryCode) || memberData.country || ''

  return {
    // Member basic info
    member: {
      ...memberData,
      country: countryDisplayName,
      statusBarText,
      generatedOn: new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }),
      timezone: timezone
    },
    // Work experience
    workExperience: workTraits.map(work => ({
      position: work.position,
      company: work.companyName,
      startDate: formatDate(work.startDate),
      endDate: formatDate(work.endDate),
      description: work.description,
      skills: (work.associatedSkills || [])
        .map(skillId => skillNameMap[skillId])
        .filter(Boolean) // Remove any undefined (GUIDs without names)
    })),
    // Education
    education: educationTraits.map(edu => ({
      degree: edu.degree,
      college: edu.collegeName,
      endYear: edu.endYear ? String(edu.endYear) : null
    })),
    // Languages
    languages: languageTraits.map(lang => lang.language).filter(Boolean),
    // Basic info (including shortBio from personalization)
    basicInfo: {
      ...(basicInfoTraits[0] || {}),
      shortBio: shortBio
    },
    // Skills
    skills: {
      principal: principalSkills,
      additional: additionalSkills
    },
    // Topcoder activity
    topcoderActivity: {
      specialRole: specialRoles.length > 0 ? `Topcoder Special Role: ${specialRoles.join(', ')}` : null,
      achievements: achievements,
      statsByTrack
    },
    // Certifications and courses
    certifications,
    courses
  }
}

/**
 * Download member profile as PDF
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @returns {Stream} PDF stream
 */
async function downloadProfile (currentUser, handle) {
  // Validate handle exists
  const member = await helper.getMemberByHandle(handle)

  // Check authorization
  if (!helper.canDownloadProfile(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to download this member profile.')
  }

  // Aggregate all PDF data
  const pdfData = await aggregatePDFData(currentUser, handle)

  // Generate PDF stream
  const pdfStream = await profilePDFService.generatePDF(pdfData)

  return pdfStream
}

downloadProfile.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required()
}

/**
 * Get a specific member skill by skill ID
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {String} skillId the skill ID
 * @returns {Object} the member skill data
 */
async function getMemberSkill (currentUser, handle, skillId) {
  // Get member data first to get userId
  const member = await getMemberData(handle, {})

  if (!member || !member.userId) {
    throw new errors.NotFoundError(`Member with handle: "${handle}" doesn't exist`)
  }

  // Check authorization
  if (!helper.canDownloadProfile(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to view this member profile.')
  }

  const dbSkill = await skillsPrisma.userSkill.findFirst({
    where: {
      userId: helper.bigIntToNumber(member.userId),
      skillId: skillId
    },
    include: {
      ...prismaHelper.skillsIncludeParams,
      skill: {
        include: {
          category: true,
          skillEvents: {
            where: {
              userId: helper.bigIntToNumber(member.userId)
            },
            select: {
              createdAt: true,
              sourceId: true,
              skillEventType : {
                select: { name: true }
              },
              sourceType: {
                select: { name: true }
              }
            }
          }
        }
      }
    }
  })

  if (!dbSkill) {
    throw new errors.NotFoundError(`Skill with ID: "${skillId}" not found for member: "${handle}"`)
  }

  // Build and return the skill data
  const [skill] = prismaHelper.buildMemberSkills([dbSkill])

  // Replace lastSources IDs with fetched details
  if (skill.activity) {
    const fetchPromises = []

    // Prepare challenge fetch
    const challengeSources = _.get(skill, 'activity.challenge.sources', [])
    if (challengeSources.length > 0) {
      const winMap = {challenge_2nd_place: 'challenge_win', challenge_3rd_place: 'challenge_win'};
      const challengeIds = _.uniqBy(challengeSources, 'sourceId').map(s => s.sourceId)
      const roleMap = new Map()
      challengeSources.forEach(source => {
        if (!roleMap.has(source.sourceId)) {
          roleMap.set(source.sourceId, new Set())
        }
        roleMap.get(source.sourceId).add(winMap[source.skillEventType.name] ?? source.skillEventType.name);
      })
      
      fetchPromises.push(
        challengesPrisma.Challenge.findMany({
          where: { id: { in: challengeIds } },
          select: { id: true, name: true }
        }).then(dbChallenges => {
          const challengeMap = new Map(dbChallenges.map(c => [c.id, c]))
          // Group challenges by role
          const groups = {}
          for (const challengeId of challengeIds) {
            const challenge = challengeMap.get(challengeId)
            if (challenge) {
              const roleNames = roleMap.get(challengeId)

              roleNames.forEach(roleName => {
                if (!groups[roleName]) groups[roleName] = []
                groups[roleName].push(challenge)
              })
            }
          }
          // For each role: sort by endDate desc, keep last 3, include total count
          skill.activity.challenge = Object.fromEntries(
            Object.entries(groups).map(([role, challenges]) => {
              const sorted = challenges.sort((a, b) =>
                new Date(b.endDate || 0) - new Date(a.endDate || 0)
              )
              return [role, {
                count: sorted.length,
                lastSources: sorted.slice(0, 3).map(c => ({
                  id: c.id,
                  name: c.name,
                  role
                }))
              }]
            })
          )
        })
      )
    }

    // Prepare certification fetch
    const certificationSources = _.get(skill, 'activity.certification.sources', [])
    if (certificationSources.length > 0) {
      const certificationIds = _.uniqBy(certificationSources, 'sourceId').map(s => s.sourceId).filter(Boolean)
      if (certificationIds.length > 0) {
        fetchPromises.push(
          academyPrisma.CertificationEnrollments.findMany({
            where: { completionEventId: { in: certificationIds.slice(0, 3) } },
            select: {
              completionEventId: true,
              TopcoderCertification: {
                select: { dashedName: true, title: true }
              }
            }
          }).then(dbCertifications => {
            const certificationMap = new Map(dbCertifications.map(c => [
              c.completionEventId,
              {
                completionEventId: c.completionEventId,
                dashedName: _.get(c, 'TopcoderCertification.dashedName'),
                title: _.get(c, 'TopcoderCertification.title')
              }
            ]))
            skill.activity.certification = {
              count: certificationIds.length,
              lastSources: certificationIds
                .map(id => certificationMap.get(id))
                .filter(Boolean)
            }
          })
        )
      }
    }

    // Prepare course fetch
    const courseSources = _.get(skill, 'activity.course.sources', [])
    if (courseSources.length > 0) {
      const courseIds = _.uniqBy(courseSources, 'sourceId').map(s => s.sourceId).filter(Boolean)
      if (courseIds.length > 0) {
        fetchPromises.push(
          academyPrisma.FccCertificationProgresses.findMany({
            where: { completionEventId: { in: courseIds.slice(0, 3) } },
            select: {
              certification: true,
              completionEventId: true,
              FccCourses: { select: { title: true } }
            }
          }).then(dbCourses => {
            const courseMap = new Map(dbCourses.map(c => [
              c.completionEventId,
              {
                completionEventId: c.completionEventId,
                certification: c.certification,
                title: _.get(c, 'FccCourses.title')
              }
            ]))
            skill.activity.course = {
              count: courseIds.length,
              lastSources: courseIds
                .map(id => courseMap.get(id))
                .filter(Boolean)
            }
          })
        )
      }
    }

    // Prepare engagement fetch
    const engagementSources = _.get(skill, 'activity.engagement.sources', [])
    if (engagementSources.length > 0) {
      const engagementIds = _.uniqBy(engagementSources, 'sourceId').map(s => s.sourceId).filter(Boolean)
      if (engagementIds.length > 0) {
        fetchPromises.push(
          engagementsPrisma.EngagementAssignment.findMany({
            where: { id: { in: engagementIds.slice(0, 3) } },
            select: {
              engagement: {
                select: { title: true, id: true }
              }
            }
          }).then(engagements => {
            skill.activity.engagement = {
              count: engagementIds.length,
              lastSources: engagements.map(assignment => ({
                id: assignment.engagement.id,
                title: assignment.engagement.title
              }))
            }
          })
        )
      }
    }

    // Fetch all sources in parallel
    await Promise.all(fetchPromises)
  }

  return skill
}

getMemberSkill.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  skillId: Joi.string().uuid().required()
}

module.exports = {
  getMember,
  getProfileCompleteness,
  getMemberUserIdSignature,
  getMemberSkill,
  updateMember,
  updateHandle,
  verifyEmail,
  uploadPhoto,
  deleteMember,
  confirmProfileData,
  downloadProfile
}

logger.buildService(module.exports)

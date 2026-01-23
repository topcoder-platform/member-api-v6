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
const memberTraitService = require('./MemberTraitService')
const mime = require('mime-types')
const fileType = require('file-type')
const fileTypeChecker = require('file-type-checker')
const sharp = require('sharp')
const { bufferContainsScript } = require('../common/image')
const prismaHelper = require('../common/prismaHelper')
const prismaManager = require('../common/prisma')
const { Prisma } = prismaManager
const identityPrismaManager = require('../common/identityPrisma')
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()
const resourcesPrisma = prismaManager.getResourcesClient()
const profilePDFService = require('./ProfilePDFService')

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName', 'tracks', 'status',
  'addresses', 'description', 'email', 'country', 'homeCountryCode', 'competitionCountryCode', 'photoURL', 'verified', 'maxRating',
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'loginCount', 'lastLoginDate', 'skills', 'availableForGigs',
  'skillScoreDeduction', 'namesAndHandleAppearance', 'lastProfileConfirmationDate', 'availableForGigsLastUpdateDate', 'identityVerified']

const INTERNAL_MEMBER_FIELDS = ['newEmail', 'emailVerifyToken', 'emailVerifyTokenDate', 'newEmailVerifyToken',
  'newEmailVerifyTokenDate', 'handleSuggest', 'lastProfileConfirmationDate', 'availableForGigsLastUpdateDate']

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
  const hasAutocompleteRole = helper.hasAutocompleteRole(currentUser)
  const isAdminOrM2M = currentUser && (currentUser.isMachine || helper.hasAdminRole(currentUser))
  const isSelf = currentUser && currentUser.handle && mb.handleLower &&
    currentUser.handle.trim().toLowerCase() === mb.handleLower.trim().toLowerCase()
  const canSeeIdentityVerified = isAdminOrM2M || hasAutocompleteRole || isSelf

  if (!canManageMember) {
    res = _.omit(res, config.MEMBER_SECURE_FIELDS)
    res = helper.secureMemberAddressData(res)
    res = helper.truncateLastName(res)
  }
  if (!canManageMember && !hasAutocompleteRole) {
    res = _.omit(res, config.COMMUNICATION_SECURE_FIELDS)
    if (res.phones) {
      delete res.phones
    }
  }
  // Remove identityVerified if user doesn't have permission
  if (!canSeeIdentityVerified && res.identityVerified !== undefined) {
    delete res.identityVerified
  }

  return res
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
        memberId: userId,
        roleName: { in: ['Submitter', 'Copilot', 'Reviewer'] },
        created: { gte: threeMonthsAgo }
      }
    })

    return !!recent
  } catch (err) {
    console.error(`Failed to query recent activity for userId: ${userId}`, err)
    return false
  }
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
  // Phones are visible to: self, admin, M2M, or users with autocomplete roles (Talent Manager, etc.)
  const hasAutocompleteRole = helper.hasAutocompleteRole(currentUser)
  const isAdminOrM2M = currentUser && (currentUser.isMachine || helper.hasAdminRole(currentUser))
  const isSelf = currentUser && currentUser.handle && 
    currentUser.handle.trim().toLowerCase() === handle.trim().toLowerCase()
  
  const canSeePhones = isAdminOrM2M || hasAutocompleteRole || isSelf
  const canSeeRecentActivity = hasAutocompleteRole || isSelf
  // Identity verified field has same access control as phones
  const canSeeIdentityVerified = isAdminOrM2M || hasAutocompleteRole || isSelf
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
  if(canSeeRecentActivity) {
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
  return cleanMember(currentUser, member, selectFields)
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
  data.profilePicture = false
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

  if (member.photoURL) {
    completeItems += 1
    data.profilePicture = true
  } else {
    showToast.push('profilePicture')
  }

  if (member.addresses && member.addresses.length) {
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

  // Fetch full member data for PDF
  const memberData = await getMember(currentUser, handle, {})

  // Generate PDF stream
  const pdfStream = await profilePDFService.generatePDF(memberData)

  return pdfStream
}

downloadProfile.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required()
}

module.exports = {
  getMember,
  getProfileCompleteness,
  getMemberUserIdSignature,
  updateMember,
  verifyEmail,
  uploadPhoto,
  deleteMember,
  confirmProfileData,
  downloadProfile
}

logger.buildService(module.exports)

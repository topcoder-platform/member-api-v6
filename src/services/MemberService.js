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
const memberTraitService = require('./MemberTraitService')
const mime = require('mime-types')
const fileType = require('file-type')
const fileTypeChecker = require('file-type-checker')
const sharp = require('sharp')
const { bufferContainsScript } = require('../common/image')
const prismaHelper = require('../common/prismaHelper')
const prisma = require('../common/prisma').getClient()

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName', 'tracks', 'status',
  'addresses', 'description', 'email', 'country', 'homeCountryCode', 'competitionCountryCode', 'photoURL', 'verified', 'maxRating',
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'loginCount', 'lastLoginDate', 'skills', 'availableForGigs',
  'skillScoreDeduction', 'namesAndHandleAppearance']

const INTERNAL_MEMBER_FIELDS = ['newEmail', 'emailVerifyToken', 'emailVerifyTokenDate', 'newEmailVerifyToken',
  'newEmailVerifyTokenDate', 'handleSuggest']

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

  return response
}

function omitMemberAttributes (currentUser, mb) {
  // remove some internal fields
  let res = _.omit(mb, INTERNAL_MEMBER_FIELDS)
  // remove identifiable info fields if user is not admin, not M2M and not member himself
  const canManageMember = helper.canManageMember(currentUser, mb)
  const hasAutocompleteRole = helper.hasAutocompleteRole(currentUser)

  if (!canManageMember) {
    res = _.omit(res, config.MEMBER_SECURE_FIELDS)
    res = helper.secureMemberAddressData(res)
    res = helper.truncateLastName(res)
  }
  if (!canManageMember && !hasAutocompleteRole) {
    res = _.omit(res, config.COMMUNICATION_SECURE_FIELDS)
  }

  return res
}

/**
 * Get member skills with user id
 * @param {BigInt} userId prisma BigInt userId
 */
async function getMemberSkills (userId) {
  const skillList = await prisma.memberSkill.findMany({
    where: {
      userId: userId
    },
    include: prismaHelper.skillsIncludeParams
  })
  // convert to response format
  return prismaHelper.buildMemberSkills(skillList)
}

/**
 * Get member profile data.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member profile data
 */
async function getMember (currentUser, handle, query) {
  // validate and parse query parameter
  const selectFields = helper.parseCommaSeparatedString(query.fields, MEMBER_FIELDS) || MEMBER_FIELDS

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

  // To keep original business logic, let's use findMany
  const member = await prisma.member.findUnique(prismaFilter)
  if (!member || !member.userId) {
    throw new errors.NotFoundError(`Member with handle: "${handle}" doesn't exist`)
  }
  // convert members data structure to response
  prismaHelper.convertMember(member)
  // get member skills
  if (_.includes(selectFields, 'skills')) {
    member.skills = await getMemberSkills(member.userId)
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
  const memberFields = { 'fields': 'userId,handle,handleLower,photoURL,description,skills,verified,availableForGigs' }
  const member = await getMember(currentUser, handle, memberFields)

  // Used for calculating the percentComplete
  let completeItems = 0

  // Magic number - 6 total items for profile "completeness"
  // TODO: Bump this back up to 7 once verification is implemented
  const totalItems = 6

  let response = {}
  response.userId = member.userId
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

  if (member.availableForGigs != null) {
    completeItems += 1
    data.gigAvailability = true
  }

  _.forEach(memberTraits, (item) => {
    if (item.traitId === 'education' && item.traits.data.length > 0 && data.education === false) {
      completeItems += 1
      data.education = true
    }

    if (item.traitId === 'work' && item.traits.data.length > 0 && !data.workHistory === false) {
      completeItems += 1
      data.workHistory = true
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
  } else {
    showToast.push('skills')
  }

  if (member.photoURL) {
    completeItems += 1
    data.profilePicture = true
  } else {
    showToast.push('profilePicture')
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
  // set updated fields in data
  data.updatedAt = new Date()
  data.updatedBy = operatorId

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
          userId: member.userId,
          createdBy: operatorId
        }))
      })
    }
    // clear addresses so it doesn't affect prisma.udpate
    delete data.addresses

    return tx.member.update({
      where: { userId: member.userId },
      data,
      include: { addresses: true }
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
    data: member
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

uploadPhoto.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  files: Joi.object().keys({
    photo: Joi.object().required()
  }).required()
}

module.exports = {
  getMember,
  getProfileCompleteness,
  getMemberUserIdSignature,
  updateMember,
  verifyEmail,
  uploadPhoto
}

logger.buildService(module.exports)

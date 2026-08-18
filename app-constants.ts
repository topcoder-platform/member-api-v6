/**
 * App constants
 */
const ADMIN_ROLES = ['administrator', 'admin']
const SENSITIVE_DATA_ROLES = [...ADMIN_ROLES, 'Talent Manager']
const SEARCH_BY_EMAIL_ROLES = ADMIN_ROLES.concat('tgadmin', 'copilot', 'Project Manager', 'Talent Manager')
const AUTOCOMPLETE_ROLES = ['copilot', 'administrator', 'admin', 'Connect Copilot', 'Connect Account Manager', 'Connect Admin', 'Account Executive', 'Talent Manager', 'Project Manager']

const EVENT_ORIGINATOR = 'topcoder-member-api'

const EVENT_MIME_TYPE = 'application/json'

const TOPICS = {
  EmailChanged: 'member.action.email.profile.emailchange.verification'
}

const MAMBO_GET_REWARDS_ALLOWED_FIELDS = [
  'awarded.awardedType', 'awarded.message', 'awarded.name', 'awarded.type',
  'awarded.reward.active', 'awarded.reward.attrs', 'awarded.reward.id', 'awarded.reward.imageUrl', 'awarded.reward.mimeType', 'awarded.reward.hint', 'awarded.reward.message',
  'awardedOn', 'expiryOn', 'isExpired', 'id'
]

const BOOLEAN_OPERATOR = {
  AND: 'AND',
  OR: 'OR'
}

const PHONE_REGEX = /^\+[1-9]\d{1,14}$/

module.exports = {
  ADMIN_ROLES,
  SENSITIVE_DATA_ROLES,
  SEARCH_BY_EMAIL_ROLES,
  AUTOCOMPLETE_ROLES,
  EVENT_ORIGINATOR,
  EVENT_MIME_TYPE,
  TOPICS,
  MAMBO_GET_REWARDS_ALLOWED_FIELDS,
  BOOLEAN_OPERATOR,
  PHONE_REGEX
}

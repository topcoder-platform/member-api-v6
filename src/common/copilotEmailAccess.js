const _ = require('lodash')
const helper = require('./helper')
const logger = require('./logger')
const prismaManager = require('./prisma')
const { ROLES_NAMES_MAP } = require('../../app-constants')

const resourcesPrisma = prismaManager.getResourcesClient()
const COPILOT_RESOURCE_ROLE_NAME_LOWER = 'copilot'

function hasCopilotRole (currentUser) {
  if (!currentUser || !currentUser.roles) {
    return false
  }

  return helper.checkIfExists(['copilot'], currentUser.roles)
}

function hasTalentManagerRole (currentUser) {
  if (!currentUser || !currentUser.roles) {
    return false
  }

  return helper.checkIfExists([ROLES_NAMES_MAP.TALENT_MANAGER], currentUser.roles)
}

function shouldLimitCopilotEmailAccess (currentUser) {
  if (!currentUser || currentUser.isMachine) {
    return false
  }

  if (helper.hasAdminRole(currentUser)) {
    return false
  }

  return hasCopilotRole(currentUser)
}

function normalizeUserId (userId) {
  if (userId === null || userId === undefined) {
    return null
  }

  const asNumber = Number(userId)
  if (!Number.isNaN(asNumber) && Number.isFinite(asNumber)) {
    return String(asNumber)
  }

  const asString = String(userId).trim()
  return asString.length > 0 ? asString : null
}

async function getCopilotAccessibleMemberIdSet (currentUser, memberUserIds) {
  const copilotUserId = normalizeUserId(currentUser && currentUser.userId)
  const accessibleMemberIds = new Set()

  if (!copilotUserId) {
    return accessibleMemberIds
  }

  accessibleMemberIds.add(copilotUserId)

  const targetMemberIds = _.uniq(
    (memberUserIds || [])
      .map(id => normalizeUserId(id))
      .filter(Boolean)
  )

  if (targetMemberIds.length === 0) {
    return accessibleMemberIds
  }

  try {
    const copilotResources = await resourcesPrisma.resource.findMany({
      where: {
        memberId: copilotUserId,
        resourceRole: { nameLower: COPILOT_RESOURCE_ROLE_NAME_LOWER }
      },
      select: { challengeId: true }
    })
    const challengeIds = _.uniq(
      copilotResources
        .map(resource => resource.challengeId)
        .filter(challengeId => challengeId !== null && challengeId !== undefined)
    )

    if (challengeIds.length === 0) {
      return accessibleMemberIds
    }

    const sharedResources = await resourcesPrisma.resource.findMany({
      where: {
        memberId: { in: targetMemberIds },
        challengeId: { in: challengeIds }
      },
      select: { memberId: true }
    })

    _.forEach(sharedResources, resource => {
      const memberId = normalizeUserId(resource.memberId)
      if (memberId) {
        accessibleMemberIds.add(memberId)
      }
    })
  } catch (error) {
    logger.error(
      `Unable to compute copilot email visibility for userId=${copilotUserId}: ${error.message}`
    )
  }

  return accessibleMemberIds
}

async function canCopilotAccessMemberEmail (currentUser, memberUserId) {
  if (!shouldLimitCopilotEmailAccess(currentUser)) {
    return true
  }

  const normalizedMemberUserId = normalizeUserId(memberUserId)
  if (!normalizedMemberUserId) {
    return false
  }

  const accessibleMemberIds = await getCopilotAccessibleMemberIdSet(currentUser, [normalizedMemberUserId])
  return accessibleMemberIds.has(normalizedMemberUserId)
}

async function stripUnauthorizedCopilotEmails (currentUser, members) {
  if (!shouldLimitCopilotEmailAccess(currentUser) || !Array.isArray(members) || members.length === 0) {
    return members
  }

  const membersWithEmail = members.filter(member => (
    member && member.email !== undefined && member.email !== null
  ))

  if (membersWithEmail.length === 0) {
    return members
  }

  const accessibleMemberIds = await getCopilotAccessibleMemberIdSet(
    currentUser,
    membersWithEmail.map(member => member.userId)
  )

  _.forEach(membersWithEmail, member => {
    const memberUserId = normalizeUserId(member.userId)
    if (!memberUserId || !accessibleMemberIds.has(memberUserId)) {
      delete member.email
    }
  })

  return members
}

module.exports = {
  shouldLimitCopilotEmailAccess,
  canCopilotAccessMemberEmail,
  hasTalentManagerRole,
  stripUnauthorizedCopilotEmails
}

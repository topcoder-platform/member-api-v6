// Use the package-scoped generated clients to avoid cross-package overrides in the monorepo
const {
  PrismaClient: MembersPrismaClient,
  Prisma
} = require('../../prisma/generated/client')
const { PrismaClient: SkillsPrismaClient } = require('../../prisma/generated/skills-client')
const config = require('config')

const clientOptions = {
  transactionOptions: {
    timeout: config.MEMBER_SERVICE_PRISMA_TIMEOUT,
  },
  log: [
    { level: 'query', emit: 'event' },
    { level: 'info', emit: 'event' },
    { level: 'warn', emit: 'event' },
    { level: 'error', emit: 'event' }
  ]
}

let membersClient
let skillsClient

const getMembersClient = () => {
  if (!membersClient) {
    membersClient = new MembersPrismaClient(clientOptions)
  }
  return membersClient
}

const getSkillsClient = () => {
  if (!skillsClient) {
    skillsClient = new SkillsPrismaClient(clientOptions)
  }
  return skillsClient
}

/**
 * Get finance Prisma client for querying finance schema
 * Uses raw SQL queries since finance schema is in a different namespace
 * @returns {Object} Prisma client instance
 */
const getFinanceClient = () => {
  // For now it internally use members client to query finance schema
  // using Raw SQL queries since finance schema is in a different namespace
  // If we have more usecase to query finance schema, we can create a separate finance client
  return getMembersClient()
}

module.exports = {
  Prisma,
  getClient: getMembersClient,
  getMembersClient,
  getSkillsClient,
  getFinanceClient
}

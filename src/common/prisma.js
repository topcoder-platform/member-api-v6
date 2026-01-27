// Use the package-scoped generated clients to avoid cross-package overrides in the monorepo
const {
  PrismaClient: MembersPrismaClient,
  Prisma
} = require('../../prisma/generated/client')
const { PrismaClient: FinancePrismaClient } = require('@topcoder/tc-finance-api/packages/finance-prisma-client')
const { PrismaClient: SkillsPrismaClient } = require('@topcoder/standardized-skills-api/packages/skills-prisma-client')
const { PrismaClient: ChallengesPrismaClient } = require('@topcoder/challenge-api-v6/packages/challenge-prisma-client')
const { PrismaClient: AcademyPrismaClient } = require('@topcoder/learning-paths-api/packages/academy-prisma-client')
const config = require('config')
const skillsDbUrl = process.env.SKILLS_DB_URL
const challengesDbUrl = process.env.CHALLENGES_DB_URL
const academyDbUrl = process.env.ACADEMY_DB_URL

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
const getMembersClient = () => {
  if (!membersClient) {
    membersClient = new MembersPrismaClient(clientOptions)
  }
  return membersClient
}

let skillsClient
const getSkillsClient = () => {
  if (!skillsClient) {
    if (!skillsDbUrl) {
      throw new Error('SKILLS_DB_URL must be set for skills Prisma client')
    }
    skillsClient = new SkillsPrismaClient({
      ...clientOptions,
      datasources: { db: { url: skillsDbUrl } }
    })
  }
  return skillsClient
}

let challengesClient
const getChallengesClient = () => {
  if (!challengesClient) {
    if (!challengesDbUrl) {
      throw new Error('CHALLENGES_DB_URL must be set for challenges Prisma client')
    }
    challengesClient = new ChallengesPrismaClient({
      ...clientOptions,
      datasources: { db: { url: challengesDbUrl } }
    })
  }
  return challengesClient
}

let academyClient
const getAcademyClient = () => {
  if (!academyClient) {
    if (!academyDbUrl) {
      throw new Error('ACADEMY_DB_URL must be set for academy Prisma client')
    }
    academyClient = new AcademyPrismaClient({
      ...clientOptions,
      datasources: { db: { url: academyDbUrl } }
    })
  }
  return academyClient
}

let financeClient
/**
 * Get finance Prisma client for querying finance schema
 * Creates a dedicated Prisma client instance for the finance database
 * @returns {Object} Prisma client instance
 */
const getFinanceClient = () => {
  if (!financeClient) {
    const connectionString = config.FINANCE_DATABASE_URL
    if (!connectionString) {
      throw new Error('FINANCE_DATABASE_URL is not configured. Please set FINANCE_DATABASE_URL environment variable or add it to config.')
    }
    financeClient = new FinancePrismaClient({
      ...clientOptions,
      datasources: { db: { url: connectionString } }
    })
  }
  return financeClient
}

module.exports = {
  Prisma,
  getClient: getMembersClient,
  getMembersClient,
  getSkillsClient,
  getChallengesClient,
  getAcademyClient,
  getFinanceClient
}

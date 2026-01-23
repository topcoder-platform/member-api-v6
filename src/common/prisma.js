// Use the package-scoped generated clients to avoid cross-package overrides in the monorepo
const {
  PrismaClient: MembersPrismaClient,
  Prisma
} = require('../../prisma/generated/client')
const { PrismaClient: SkillsPrismaClient } = require('@topcoder/standardized-skills-api/packages/skills-prisma-client')
const config = require('config')
const skillsDbUrl = process.env.SKILLS_DB_URL

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

module.exports = {
  Prisma,
  getClient: getMembersClient,
  getMembersClient,
  getSkillsClient
}

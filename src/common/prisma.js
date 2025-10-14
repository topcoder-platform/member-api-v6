// Use the package-scoped generated clients to avoid cross-package overrides in the monorepo
const {
  PrismaClient: MembersPrismaClient,
  Prisma
} = require('../../prisma/generated/client')
const { PrismaClient: SkillsPrismaClient } = require('../../prisma/generated/skills-client')

const clientOptions = {
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

module.exports = {
  Prisma,
  getClient: getMembersClient,
  getMembersClient,
  getSkillsClient
}

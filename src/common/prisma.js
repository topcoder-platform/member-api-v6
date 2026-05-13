const { PrismaPg } = require('@prisma/adapter-pg')
// Use the package-scoped generated clients to avoid cross-package overrides in the monorepo
const {
  PrismaClient: MembersPrismaClient,
  Prisma
} = require('../../prisma/generated/client')
const config = require('config')
const {
  PrismaClient: FinancePrismaClient
} = require('@topcoder/tc-finance-api/packages/finance-prisma-client')
const {
  PrismaClient: SkillsPrismaClient
} = require('@topcoder/standardized-skills-api/packages/skills-prisma-client')
const {
  PrismaClient: ResourcesPrismaClient
} = require('@topcoder/resource-api-v6/packages/resources-prisma-client')
const {
  PrismaClient: ChallengesPrismaClient
} = require('@topcoder/challenge-api-v6/packages/challenge-prisma-client')
const {
  PrismaClient: AcademyPrismaClient
} = require('@topcoder/learning-paths-api/packages/academy-prisma-client')
const {
  PrismaClient: EngagementsPrismaClient
} = require('@topcoder/engagements-api-v6/packages/engagements-prisma-client')

const extractSchemaFromUrl = (dbUrl) => {
  if (!dbUrl) {
    return null
  }
  try {
    const url = new URL(dbUrl)
    return url.searchParams.get('schema')
  } catch (error) {
    return null
  }
}

const skillsDbUrl = process.env.SKILLS_DB_URL
const challengesDbUrl = process.env.CHALLENGES_DB_URL || process.env.CHALLENGE_DB_URL
const academyDbUrl = process.env.ACADEMY_DB_URL
const resourcesDbUrl = process.env.RESOURCES_DB_URL
const engagementsDbUrl = process.env.ENGAGEMENTS_DB_URL
const financeDbUrl = process.env.FINANCE_DATABASE_URL
const mmDbUrl = process.env.MM_DB_URL

const clientOptions = {
  transactionOptions: {
    timeout: config.MEMBER_SERVICE_PRISMA_TIMEOUT
  },
  log: [
    { level: 'query', emit: 'event' },
    { level: 'info', emit: 'event' },
    { level: 'warn', emit: 'event' },
    { level: 'error', emit: 'event' }
  ]
}

const createPgAdapter = (dbUrl) => {
  const schema = extractSchemaFromUrl(dbUrl)
  const args = [{ connectionString: dbUrl }]

  if (schema) {
    args.push({ schema })
  }

  return new PrismaPg(...args)
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
      throw new Error(
        'CHALLENGES_DB_URL or CHALLENGE_DB_URL must be set for challenges Prisma client'
      )
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

let engagementsClient
const getEngagementsClient = () => {
  if (!engagementsClient) {
    if (!engagementsDbUrl) {
      throw new Error(
        'ENGAGEMENTS_DB_URL must be set for engagements Prisma client'
      )
    }

    engagementsClient = new EngagementsPrismaClient({
      ...clientOptions,
      adapter: createPgAdapter(engagementsDbUrl)
    })
  }
  return engagementsClient
}

let resourcesClient
const getResourcesClient = () => {
  if (!resourcesClient) {
    if (!resourcesDbUrl) {
      throw new Error(
        'RESOURCES_DB_URL must be set for resources Prisma client'
      )
    }
    resourcesClient = new ResourcesPrismaClient({
      ...clientOptions,
      datasources: { db: { url: resourcesDbUrl } }
    })
  }
  return resourcesClient
}

let financeClient
/**
 * Get finance Prisma client for querying finance schema
 * Creates a dedicated Prisma client instance for the finance database
 * @returns {Object} Prisma client instance
 */
const getFinanceClient = () => {
  if (!financeClient) {
    if (!financeDbUrl) {
      throw new Error(
        'FINANCE_DATABASE_URL is not configured. Please set FINANCE_DATABASE_URL environment variable or add it to config.'
      )
    }
    financeClient = new FinancePrismaClient({
      ...clientOptions,
      datasources: { db: { url: financeDbUrl } }
    })
  }
  return financeClient
}

let MmPool
const getMmPoolConstructor = () => {
  if (!MmPool) {
    try {
      const { Pool } = require('pg')
      if (typeof Pool !== 'function') {
        throw new Error('pg Pool constructor is unavailable')
      }
      MmPool = Pool
    } catch (error) {
      throw new Error(
        `MM database client dependencies are unavailable. Install the member-api MM database dependencies before using MM config lookups. Original error: ${error.message}`
      )
    }
  }

  return MmPool
}

const validateMmClientDependencies = () => {
  if (!mmDbUrl) {
    return
  }

  getMmPoolConstructor()
}

const createMmConfigClient = (dbUrl) => {
  const Pool = getMmPoolConstructor()
  const pool = new Pool({
    connectionString: dbUrl
  })

  return {
    marathonMatchConfig: {
      /**
       * Find one Marathon Match config row by challenge identifier.
       * Supports the subset of Prisma's findUnique contract needed by legacy
       * config lookups.
       * @param {Object} args lookup arguments
       * @param {Object} args.where unique challenge lookup
       * @param {Object} args.select selected fields
       * @returns {Promise<Object|null>} selected config row or null
       */
      async findUnique (args = {}) {
        const challengeId = args.where && args.where.challengeId
        if (!challengeId) {
          throw new Error('MM config lookup requires where.challengeId')
        }

        const select = args.select || {}
        const selectedColumns = []

        if (Object.keys(select).length === 0 || select.relativeScoringEnabled) {
          selectedColumns.push('"relativeScoringEnabled"')
        }
        if (Object.keys(select).length === 0 || select.scoreDirection) {
          selectedColumns.push('"scoreDirection"')
        }

        if (selectedColumns.length === 0) {
          throw new Error('MM config lookup requires at least one selected field')
        }

        const result = await pool.query(
          `
            SELECT ${selectedColumns.join(', ')}
            FROM "marathon_match"."marathonMatchConfig"
            WHERE "challengeId" = $1
            LIMIT 1
          `,
          [String(challengeId)]
        )

        return result.rows[0] || null
      }
    },
    async $disconnect () {
      await pool.end()
    }
  }
}

let mmClient
/**
 * Get Marathon Match config client for querying marathonMatchConfig.
 * Creates a dedicated read client instance for the Marathon Match database.
 * @returns {Object} MM config client instance
 */
const getMmClient = () => {
  if (!mmClient) {
    if (!mmDbUrl) {
      throw new Error('MM_DB_URL must be set for Marathon Match config client')
    }

    mmClient = createMmConfigClient(mmDbUrl)
  }

  return mmClient
}

validateMmClientDependencies()

module.exports = {
  Prisma,
  getClient: getMembersClient,
  getMembersClient,
  getSkillsClient,
  getChallengesClient,
  getAcademyClient,
  getEngagementsClient,
  getResourcesClient,
  getFinanceClient,
  getMmClient
}

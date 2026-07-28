/*
 * Unit tests for Prisma client manager utilities.
 */

const path = require('path')
const chai = require('chai')

const should = chai.should()
const { expect } = chai

const prismaPath = path.resolve(__dirname, '../../src/common/prisma.ts')
const prismaAdapterPath = path.resolve(__dirname, '../../src/common/prismaAdapter.ts')

/**
 * Installs a CommonJS module stub used to inspect Prisma manager configuration.
 *
 * @param {string} modulePath Absolute module path to replace.
 * @param {Object} exports Stubbed module exports.
 * @returns {void}
 * @throws This helper does not throw.
 */
function setStubModule (modulePath, exports) {
  delete require.cache[modulePath]
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports
  }
}

/**
 * Restores one CommonJS cache entry after a Prisma manager test.
 *
 * @param {string} modulePath Absolute module path to restore.
 * @param {Object|undefined} originalValue Original cache entry, if present.
 * @returns {void}
 * @throws This helper does not throw.
 */
function restoreModule (modulePath, originalValue) {
  delete require.cache[modulePath]
  if (originalValue) {
    require.cache[modulePath] = originalValue
  }
}

describe('prisma manager unit tests', () => {
  const originalChallengeDbUrl = process.env.CHALLENGE_DB_URL
  const originalChallengesDbUrl = process.env.CHALLENGES_DB_URL
  const originalDatabaseUrl = process.env.DATABASE_URL
  let originalPrismaAdapterModule

  beforeEach(() => {
    originalPrismaAdapterModule = require.cache[prismaAdapterPath]
  })

  afterEach(() => {
    if (originalChallengeDbUrl === undefined) {
      delete process.env.CHALLENGE_DB_URL
    } else {
      process.env.CHALLENGE_DB_URL = originalChallengeDbUrl
    }

    if (originalChallengesDbUrl === undefined) {
      delete process.env.CHALLENGES_DB_URL
    } else {
      process.env.CHALLENGES_DB_URL = originalChallengesDbUrl
    }

    if (originalDatabaseUrl === undefined) {
      delete process.env.DATABASE_URL
    } else {
      process.env.DATABASE_URL = originalDatabaseUrl
    }

    delete require.cache[prismaPath]
    restoreModule(prismaAdapterPath, originalPrismaAdapterModule)
  })

  it('getChallengesClient should accept CHALLENGE_DB_URL as a fallback', async () => {
    delete process.env.CHALLENGES_DB_URL
    process.env.CHALLENGE_DB_URL = 'postgresql://user:password@localhost:5432/topcoder?schema=challenges'
    delete require.cache[prismaPath]

    const prismaManager = require('../../src/common/prisma')
    const challengesClient = prismaManager.getChallengesClient()

    should.exist(challengesClient)
    await challengesClient.$disconnect()
  })

  it('retains one warmed members database connection', async () => {
    const databaseUrl = 'postgresql://user:password@localhost:5432/topcoder?schema=members'
    const adapterCalls = []
    const actualPrismaAdapter = require(prismaAdapterPath)

    /**
     * Captures members adapter arguments while returning the production adapter.
     *
     * @param {...unknown} args Arguments supplied by the Prisma manager.
     * @returns {Object} Production Prisma PostgreSQL adapter.
     * @throws Propagates production adapter configuration errors.
     */
    function createPostgresAdapterStub (...args) {
      adapterCalls.push(args)
      return actualPrismaAdapter.createPostgresAdapter(...args)
    }

    process.env.DATABASE_URL = databaseUrl
    setStubModule(prismaAdapterPath, {
      ...actualPrismaAdapter,
      createPostgresAdapter: createPostgresAdapterStub
    })
    delete require.cache[prismaPath]

    const prismaManager = require(prismaPath)
    const membersClient = prismaManager.getMembersClient()

    expect(adapterCalls).to.deep.equal([
      [databaseUrl, 'DATABASE_URL', { min: 1 }]
    ])
    await membersClient.$disconnect()
  })
})

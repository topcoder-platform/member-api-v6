/*
 * Unit tests for member Copilot and Reviewer challenge statistics.
 */

const path = require('path')
const chai = require('chai')

const should = chai.should()

const servicePath = path.resolve(__dirname, '../../src/services/SpecialRoleService.ts')
const helperPath = path.resolve(__dirname, '../../src/common/helper.ts')
const loggerPath = path.resolve(__dirname, '../../src/common/logger.ts')
const prismaPath = path.resolve(__dirname, '../../src/common/prisma.ts')

const REVIEWER_ROLE_NAMES = [
  'iterative reviewer',
  'primary reviewer',
  'reviewer',
  'failure reviewer',
  'final reviewer',
  'aggregator',
  'stress reviewer',
  'accuracy reviewer',
  'primary screener',
  'checkpoint screener',
  'checkpoint reviewer'
]

/**
 * Install one dependency stub in Node's module cache for isolated service use.
 * Tests call this before loading the service; normal use does not raise.
 * @param {String} modulePath absolute module path to replace
 * @param {Object} exports replacement module exports
 * @returns {void}
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
 * Restore dependency cache entries captured before a service test. Test
 * finally blocks call this so later suites see the original modules.
 * @param {Object} originalEntries module-path to original cache-entry map
 * @returns {void}
 */
function restoreModuleCache (originalEntries) {
  Object.entries(originalEntries).forEach(([modulePath, originalValue]) => {
    delete require.cache[modulePath]
    if (originalValue) {
      require.cache[modulePath] = originalValue
    }
  })
}

/**
 * Render nested test SQL fragments to readable text. Query-aware client stubs
 * use the output to return count, page, or metric rows; this helper does not
 * execute SQL or raise for supported fragments.
 * @param {*} value SQL fragment, joined values, or scalar interpolation
 * @returns {String} flattened SQL-like text
 */
function renderSqlValue (value) {
  if (value && value.__sqlText !== undefined) {
    return value.__sqlText
  }
  if (value && value.__joinedValues) {
    return value.__joinedValues.join(', ')
  }
  return String(value)
}

/**
 * Create the subset of Prisma's parameterized SQL helper used by the service.
 * It preserves interpolations in readable text for assertions and does not
 * connect to a database.
 * @returns {Object} `sql` tagged-template and `join` test helpers
 */
function createPrismaSqlStub () {
  return {
    sql: (strings, ...values) => ({
      __sqlText: strings.reduce((text, part, index) => (
        text + part + (index < values.length ? renderSqlValue(values[index]) : '')
      ), '')
    }),
    join: values => ({ __joinedValues: values })
  }
}

/**
 * Load SpecialRoleService with deterministic raw-query clients. Callback
 * options receive rendered SQL and may return rows or throw database errors.
 * @param {Object} options resource/challenge query callbacks and optional member
 * @returns {Object} loaded service plus a cache-restoration callback
 * @throws {Error} propagates module-loading failures to the calling test
 */
function loadSpecialRoleService (options = {}) {
  const originalEntries = {
    [servicePath]: require.cache[servicePath],
    [helperPath]: require.cache[helperPath],
    [loggerPath]: require.cache[loggerPath],
    [prismaPath]: require.cache[prismaPath]
  }
  const member = options.member || {
    userId: global.BigInt(88770025),
    handle: 'devtest1400',
    handleLower: 'devtest1400'
  }
  const sqlHelper = createPrismaSqlStub()

  setStubModule(helperPath, {
    getMemberByHandle: async () => member
  })
  setStubModule(loggerPath, {
    buildService: () => {}
  })
  setStubModule(prismaPath, {
    ChallengesPrisma: sqlHelper,
    ResourcesPrisma: sqlHelper,
    getResourcesClient: () => ({
      $queryRaw: async query => options.onResourceQuery(query.__sqlText)
    }),
    getChallengesClient: () => ({
      $queryRaw: async query => options.onChallengeQuery(query.__sqlText)
    })
  })

  delete require.cache[servicePath]
  const service = require(servicePath)
  return {
    service,
    restore: () => restoreModuleCache(originalEntries)
  }
}

describe('special role service unit tests', () => {
  afterEach(() => {
    delete require.cache[servicePath]
  })

  it('getMemberRoleStats should return resource-only distinct counts for exact role families', async () => {
    let resourceSql
    let challengeQueryCalled = false
    const { service, restore } = loadSpecialRoleService({
      onResourceQuery: async (sql) => {
        resourceSql = sql
        return [
          { role: 'copilot', challengeCount: 4 },
          { role: 'reviewer', challengeCount: 3 }
        ]
      },
      onChallengeQuery: async () => {
        challengeQueryCalled = true
        return []
      }
    })

    try {
      const result = await service.getMemberRoleStats('devtest1400')

      resourceSql.should.include('COUNT(*)::int AS "challengeCount"')
      resourceSql.should.include('GROUP BY 1, resource."challengeId"')
      resourceSql.should.include('copilot')
      REVIEWER_ROLE_NAMES.forEach(roleName => resourceSql.should.include(roleName))
      should.equal(challengeQueryCalled, false)
      result.should.deep.equal({
        copilot: { challengeCount: 4 },
        reviewer: { challengeCount: 3 }
      })
    } finally {
      restore()
    }
  })

  it('getMemberRoleChallenges should return a visible Reviewer page newest first', async () => {
    const challengeSql = []
    const { service, restore } = loadSpecialRoleService({
      onResourceQuery: async () => [],
      onChallengeQuery: async (sql) => {
        challengeSql.push(sql)
        if (sql.includes('SELECT COUNT(*)::int AS "challengeCount"')) {
          return [{ challengeCount: 2 }]
        }
        if (sql.includes('challengeType."name" AS "typeName"')) {
          return [
            {
              id: 'review-2',
              name: 'Newest Review',
              status: 'COMPLETED',
              startDate: new Date('2024-02-01T00:00:00Z'),
              endDate: new Date('2024-03-01T00:00:00Z'),
              resourceCreatedAt: new Date('2024-02-15T00:00:00Z'),
              trackId: 'design',
              trackName: 'Design',
              typeId: 'challenge',
              typeName: 'Challenge'
            },
            {
              id: 'review-1',
              name: 'Earlier Review',
              status: 'COMPLETED',
              startDate: new Date('2024-01-01T00:00:00Z'),
              endDate: new Date('2024-02-01T00:00:00Z'),
              resourceCreatedAt: new Date('2024-01-15T00:00:00Z'),
              trackId: 'dev',
              trackName: 'Development',
              typeId: 'f2f',
              typeName: 'First2Finish'
            }
          ]
        }
        throw new Error(`Unexpected query: ${sql}`)
      }
    })

    try {
      const result = await service.getMemberRoleChallenges('devtest1400', 'reviewer', {})
      const allSql = challengeSql.join('\n')

      allSql.should.include('cardinality(challenge."groups") = 0')
      allSql.should.include('challenge."taskIsTask" = FALSE')
      allSql.should.include('challenges."ChallengeUserWhitelist"')
      allSql.should.include('MAX(resource."createdAt") AS "resourceCreatedAt"')
      allSql.should.include('COALESCE(')
      allSql.should.include('challenge."endDate"')
      allSql.should.include('LIMIT 100')
      allSql.should.include('OFFSET 0')
      REVIEWER_ROLE_NAMES.forEach(roleName => allSql.should.include(roleName))
      result.should.deep.equal({
        role: 'reviewer',
        total: 2,
        page: 1,
        perPage: 100,
        totalPages: 1,
        challenges: [
          {
            id: 'review-2',
            name: 'Newest Review',
            status: 'COMPLETED',
            track: { id: 'design', name: 'Design' },
            type: { id: 'challenge', name: 'Challenge' },
            startDate: '2024-02-01T00:00:00.000Z',
            endDate: '2024-03-01T00:00:00.000Z',
            resourceCreatedAt: '2024-02-15T00:00:00.000Z'
          },
          {
            id: 'review-1',
            name: 'Earlier Review',
            status: 'COMPLETED',
            track: { id: 'dev', name: 'Development' },
            type: { id: 'f2f', name: 'First2Finish' },
            startDate: '2024-01-01T00:00:00.000Z',
            endDate: '2024-02-01T00:00:00.000Z',
            resourceCreatedAt: '2024-01-15T00:00:00.000Z'
          }
        ]
      })
    } finally {
      restore()
    }
  })

  it('getMemberRoleChallenges should paginate Copilot rows and aggregate visible terminal metrics', async () => {
    const { service, restore } = loadSpecialRoleService({
      onResourceQuery: async () => [],
      onChallengeQuery: async (sql) => {
        if (sql.includes('SELECT COUNT(*)::int AS "challengeCount"')) {
          return [{ challengeCount: 3 }]
        }
        if (sql.includes('challengeType."name" AS "typeName"')) {
          return [{
            id: 'copilot-2',
            name: 'Second Copilot Challenge',
            status: 'CANCELLED_ZERO_SUBMISSIONS',
            startDate: new Date('2024-02-01T00:00:00Z'),
            endDate: null,
            resourceCreatedAt: new Date('2024-02-02T00:00:00Z'),
            trackId: 'dev',
            trackName: 'Development',
            typeId: 'challenge',
            typeName: 'Challenge'
          }]
        }
        if (sql.includes('challengeTrack."track"::text AS "track"')) {
          return [
            {
              status: 'COMPLETED',
              track: 'DEVELOPMENT',
              trackName: 'Development',
              trackAbbreviation: 'DEV',
              challengeCount: 1
            },
            {
              status: 'CANCELLED_ZERO_SUBMISSIONS',
              track: 'DEVELOPMENT',
              trackName: 'Development',
              trackAbbreviation: 'DEV',
              challengeCount: 1
            },
            {
              status: 'ACTIVE',
              track: 'DESIGN',
              trackName: 'Design',
              trackAbbreviation: 'DES',
              challengeCount: 1
            }
          ]
        }
        throw new Error(`Unexpected query: ${sql}`)
      }
    })

    try {
      const result = await service.getMemberRoleChallenges('devtest1400', 'copilot', {
        page: 2,
        perPage: 1
      })

      result.total.should.equal(3)
      result.totalPages.should.equal(3)
      result.page.should.equal(2)
      result.perPage.should.equal(1)
      result.trackCounts.should.deep.equal({ DEVELOPMENT: 2, DESIGN: 1 })
      result.fulfillment.should.deep.equal({
        completed: 1,
        cancelled: 1,
        total: 2,
        rate: 50
      })
      result.challenges[0].should.deep.equal({
        id: 'copilot-2',
        name: 'Second Copilot Challenge',
        status: 'CANCELLED_ZERO_SUBMISSIONS',
        track: { id: 'dev', name: 'Development' },
        type: { id: 'challenge', name: 'Challenge' },
        startDate: '2024-02-01T00:00:00.000Z',
        endDate: null,
        resourceCreatedAt: '2024-02-02T00:00:00.000Z'
      })
    } finally {
      restore()
    }
  })

  it('getMemberRoleStats should omit role keys with zero resource challenges', async () => {
    const { service, restore } = loadSpecialRoleService({
      onResourceQuery: async () => [{ role: 'reviewer', challengeCount: 2 }],
      onChallengeQuery: async () => []
    })

    try {
      const result = await service.getMemberRoleStats('devtest1400')
      result.should.deep.equal({ reviewer: { challengeCount: 2 } })
    } finally {
      restore()
    }
  })

  it('getMemberRoleChallenges should explain missing co-located schemas', async () => {
    const missingRelationError = new Error('relation does not exist')
    missingRelationError.code = 'P2010'
    missingRelationError.meta = { code: '3F000' }
    const { service, restore } = loadSpecialRoleService({
      onResourceQuery: async () => [],
      onChallengeQuery: async () => {
        throw missingRelationError
      }
    })

    try {
      await service.getMemberRoleChallenges('devtest1400', 'reviewer', {})
      should.fail('Expected a service availability error')
    } catch (error) {
      error.httpStatus.should.equal(503)
      error.message.should.include('schemas to be co-located')
    } finally {
      restore()
    }
  })
})

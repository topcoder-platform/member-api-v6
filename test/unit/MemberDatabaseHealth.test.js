/*
 * Unit tests for the coalesced primary members database health check.
 */

const path = require('path')
const { expect } = require('chai')

const healthPath = path.resolve(__dirname, '../../src/common/memberDatabaseHealth.ts')
const prismaPath = path.resolve(__dirname, '../../src/common/prisma.ts')

/**
 * Installs a CommonJS module stub used to isolate the database health helper.
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
 * Restores one CommonJS cache entry after a database health test.
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

describe('member database health check', () => {
  let originalHealthModule
  let originalPrismaModule

  beforeEach(() => {
    originalHealthModule = require.cache[healthPath]
    originalPrismaModule = require.cache[prismaPath]
  })

  afterEach(() => {
    restoreModule(healthPath, originalHealthModule)
    restoreModule(prismaPath, originalPrismaModule)
  })

  it('coalesces concurrent checks and runs a fresh query afterward', async () => {
    let queryCalls = 0
    const queries = []
    let resolveQuery
    const firstQuery = new Promise(resolve => {
      resolveQuery = resolve
    })
    const client = {
      $queryRaw: (queryParts, ...values) => {
        queryCalls += 1
        queries.push({ parts: Array.from(queryParts), values })
        return queryCalls === 1 ? firstQuery : Promise.resolve([{ '?column?': 1 }])
      }
    }

    setStubModule(prismaPath, {
      getMembersClient: () => client
    })
    delete require.cache[healthPath]
    const { checkMemberDatabaseHealth } = require(healthPath)

    const firstCheck = checkMemberDatabaseHealth()
    const secondCheck = checkMemberDatabaseHealth()

    expect(secondCheck).to.equal(firstCheck)
    expect(queryCalls).to.equal(1)

    resolveQuery([{ '?column?': 1 }])
    await Promise.all([firstCheck, secondCheck])
    await checkMemberDatabaseHealth()

    expect(queryCalls).to.equal(2)
    expect(queries).to.deep.equal([
      { parts: ['SELECT 1'], values: [] },
      { parts: ['SELECT 1'], values: [] }
    ])
  })

  it('coalesces a rejected check and clears it so a later request can retry', async () => {
    const databaseError = new Error('database unavailable')
    let queryCalls = 0
    let rejectQuery
    const firstQuery = new Promise((resolve, reject) => {
      rejectQuery = reject
    })
    const client = {
      $queryRaw: () => {
        queryCalls += 1
        return queryCalls === 1
          ? firstQuery
          : Promise.resolve([{ '?column?': 1 }])
      }
    }

    setStubModule(prismaPath, {
      getMembersClient: () => client
    })
    delete require.cache[healthPath]
    const { checkMemberDatabaseHealth } = require(healthPath)

    const firstCheck = checkMemberDatabaseHealth()
    const secondCheck = checkMemberDatabaseHealth()
    let firstError
    let secondError
    const caughtChecks = Promise.all([
      firstCheck.catch(error => {
        firstError = error
      }),
      secondCheck.catch(error => {
        secondError = error
      })
    ])

    expect(secondCheck).to.equal(firstCheck)
    expect(queryCalls).to.equal(1)

    rejectQuery(databaseError)
    await caughtChecks

    expect([firstError, secondError]).to.deep.equal([databaseError, databaseError])
    await checkMemberDatabaseHealth()
    expect(queryCalls).to.equal(2)
  })
})

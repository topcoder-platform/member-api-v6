/*
 * Unit tests for the public health controller contract.
 */

const path = require('path')
const { expect } = require('chai')

const controllerPath = path.resolve(__dirname, '../../src/controllers/HealthController.ts')
const databaseHealthPath = path.resolve(__dirname, '../../src/common/memberDatabaseHealth.ts')
const configPath = require.resolve('config')

/**
 * Installs a CommonJS module stub used to isolate the health controller.
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
 * Restores one CommonJS cache entry after a health controller test.
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

describe('health controller', () => {
  let originalControllerModule
  let originalDatabaseHealthModule
  let originalConfigModule

  beforeEach(() => {
    originalControllerModule = require.cache[controllerPath]
    originalDatabaseHealthModule = require.cache[databaseHealthPath]
    originalConfigModule = require.cache[configPath]
  })

  afterEach(() => {
    restoreModule(controllerPath, originalControllerModule)
    restoreModule(databaseHealthPath, originalDatabaseHealthModule)
    restoreModule(configPath, originalConfigModule)
  })

  it('returns the established response after a successful database check', async () => {
    let databaseChecks = 0
    let responseBody

    setStubModule(databaseHealthPath, {
      checkMemberDatabaseHealth: async () => {
        databaseChecks += 1
        return 10
      }
    })
    setStubModule(configPath, { HEALTH_CHECK_TIMEOUT: 3000 })
    delete require.cache[controllerPath]
    const { checkHealth } = require(controllerPath)

    await checkHealth({}, {
      send: body => {
        responseBody = body
      }
    })

    expect(databaseChecks).to.equal(1)
    expect(responseBody).to.deep.equal({ checksRun: 1 })
  })

  it('preserves the service-unavailable contract for database errors', async () => {
    setStubModule(databaseHealthPath, {
      checkMemberDatabaseHealth: async () => {
        throw new Error('database unavailable')
      }
    })
    setStubModule(configPath, { HEALTH_CHECK_TIMEOUT: 3000 })
    delete require.cache[controllerPath]
    const { checkHealth } = require(controllerPath)

    let caughtError
    try {
      await checkHealth({}, { send: () => {} })
    } catch (error) {
      caughtError = error
    }

    expect(caughtError.httpStatus).to.equal(503)
    expect(caughtError.message).to.equal(
      'There is database operation error, database unavailable'
    )
  })

  it('preserves the service-unavailable contract for slow checks', async () => {
    setStubModule(databaseHealthPath, {
      checkMemberDatabaseHealth: async () => 3001
    })
    setStubModule(configPath, { HEALTH_CHECK_TIMEOUT: 3000 })
    delete require.cache[controllerPath]
    const { checkHealth } = require(controllerPath)

    let caughtError
    try {
      await checkHealth({}, { send: () => {} })
    } catch (error) {
      caughtError = error
    }

    expect(caughtError.httpStatus).to.equal(503)
    expect(caughtError.message).to.equal('Database operation is slow.')
  })
})

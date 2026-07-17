/*
 * Unit tests for NestJS startup ordering.
 */

const path = require('path')
const { expect } = require('chai')

const mainPath = path.resolve(__dirname, '../../main.ts')
const appPath = path.resolve(__dirname, '../../app.ts')
const appModulePath = path.resolve(__dirname, '../../src/app.module.ts')
const databaseHealthPath = path.resolve(__dirname, '../../src/common/memberDatabaseHealth.ts')
const loggerPath = path.resolve(__dirname, '../../src/common/logger.ts')
const configPath = require.resolve('config')
const nestCorePath = require.resolve('@nestjs/core')
const nestExpressPath = require.resolve('@nestjs/platform-express')

/**
 * Installs a CommonJS module stub used to isolate application bootstrap.
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
 * Restores all CommonJS cache entries replaced by a bootstrap test.
 *
 * @param {Object<string, Object|undefined>} originalModules Original entries keyed by path.
 * @returns {void}
 * @throws This helper does not throw.
 */
function restoreModules (originalModules) {
  Object.entries(originalModules).forEach(([modulePath, originalValue]) => {
    delete require.cache[modulePath]
    if (originalValue) {
      require.cache[modulePath] = originalValue
    }
  })
}

/**
 * Loads bootstrap with isolated Nest, Express, health, logging, and config stubs.
 *
 * Tests use this helper to observe startup ordering without opening a listener
 * or connecting to a database.
 *
 * @param {Object} options Test behavior options.
 * @param {Error} [options.warmError] Error raised by the database warm-up stub.
 * @returns {{bootstrap: Function, events: string[]}} Isolated bootstrap and event log.
 * @throws Propagates an unexpected failure while loading the bootstrap module.
 */
function setupMain (options = {}) {
  const events = []
  const application = {
    listen: async port => {
      events.push(`listen:${port}`)
    }
  }

  setStubModule(nestCorePath, {
    NestFactory: {
      create: async () => {
        events.push('create')
        return application
      }
    }
  })
  setStubModule(nestExpressPath, {
    ExpressAdapter: class {}
  })
  setStubModule(appPath, {})
  setStubModule(appModulePath, { AppModule: class AppModule {} })
  setStubModule(databaseHealthPath, {
    checkMemberDatabaseHealth: async () => {
      events.push('warm')
      if (options.warmError) {
        throw options.warmError
      }
    }
  })
  setStubModule(loggerPath, { info: () => {} })
  setStubModule(configPath, { PORT: 3000 })
  delete require.cache[mainPath]

  return {
    bootstrap: require(mainPath).bootstrap,
    events
  }
}

describe('application bootstrap', () => {
  let originalModules

  beforeEach(() => {
    originalModules = {
      [mainPath]: require.cache[mainPath],
      [appPath]: require.cache[appPath],
      [appModulePath]: require.cache[appModulePath],
      [databaseHealthPath]: require.cache[databaseHealthPath],
      [loggerPath]: require.cache[loggerPath],
      [configPath]: require.cache[configPath],
      [nestCorePath]: require.cache[nestCorePath],
      [nestExpressPath]: require.cache[nestExpressPath]
    }
  })

  afterEach(() => {
    restoreModules(originalModules)
  })

  it('warms the members database before opening the listener', async () => {
    const { bootstrap, events } = setupMain()

    await bootstrap()

    expect(events).to.deep.equal(['create', 'warm', 'listen:3000'])
  })

  it('does not listen when the database warm-up fails', async () => {
    const warmError = new Error('database unavailable')
    const { bootstrap, events } = setupMain({ warmError })

    let caughtError
    try {
      await bootstrap()
    } catch (error) {
      caughtError = error
    }

    expect(caughtError).to.equal(warmError)
    expect(events).to.deep.equal(['create', 'warm'])
  })
})

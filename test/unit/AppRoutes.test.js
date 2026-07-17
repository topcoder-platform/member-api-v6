/*
 * Unit tests for route middleware behavior.
 */

const path = require('path')
const chai = require('chai')

chai.should()

const appRoutesPath = path.resolve(__dirname, '../../app-routes.ts')
const helperPath = path.resolve(__dirname, '../../src/common/helper.ts')
const loggerPath = path.resolve(__dirname, '../../src/common/logger.ts')
const routesPath = path.resolve(__dirname, '../../src/routes.ts')
const configPath = require.resolve('config')
const coreLibraryPath = require.resolve('tc-core-library-js')
const statisticsControllerPath = path.resolve(__dirname, '../../src/controllers/StatisticsController.ts')

function setStubModule (modulePath, exports) {
  delete require.cache[modulePath]
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports
  }
}

function restoreModuleCache (originalEntries) {
  Object.entries(originalEntries).forEach(([modulePath, originalValue]) => {
    delete require.cache[modulePath]
    if (originalValue) {
      require.cache[modulePath] = originalValue
    }
  })
}

function runHandlers (handlers, req) {
  const res = {
    send: () => {}
  }
  let index = -1

  return new Promise((resolve, reject) => {
    const next = (error) => {
      if (error) {
        reject(error)
        return
      }

      index += 1
      if (index >= handlers.length) {
        resolve()
        return
      }

      try {
        handlers[index](req, res, next)
      } catch (handlerError) {
        reject(handlerError)
      }
    }

    next()
  })
}

describe('app routes unit tests', () => {
  afterEach(() => {
    delete require.cache[appRoutesPath]
  })

  it('public routes should skip optional JWT authentication when no authorization header is present', async () => {
    const originalEntries = {
      [appRoutesPath]: require.cache[appRoutesPath],
      [helperPath]: require.cache[helperPath],
      [loggerPath]: require.cache[loggerPath],
      [routesPath]: require.cache[routesPath],
      [configPath]: require.cache[configPath],
      [coreLibraryPath]: require.cache[coreLibraryPath],
      [statisticsControllerPath]: require.cache[statisticsControllerPath]
    }
    let authenticatorCalls = 0
    let controllerCalls = 0
    const registeredRoutes = {}
    const app = {
      get: (routePath, handlers) => {
        registeredRoutes[routePath] = handlers
      },
      use: () => {}
    }

    setStubModule(helperPath, {
      autoWrapExpress: value => value
    })
    setStubModule(loggerPath, {
      debug: () => {}
    })
    setStubModule(routesPath, {
      '/members/stats/distribution': {
        get: {
          controller: 'StatisticsController',
          method: 'getDistribution'
        }
      }
    })
    setStubModule(configPath, {
      API_VERSION: 'v6',
      AUTH_SECRET: 'secret',
      VALID_ISSUERS: '["issuer"]'
    })
    setStubModule(coreLibraryPath, {
      middleware: {
        jwtAuthenticator: () => () => {
          authenticatorCalls += 1
          throw new Error('authenticator should not run without an authorization header')
        }
      }
    })
    setStubModule(statisticsControllerPath, {
      getDistribution: (req, res, next) => {
        controllerCalls += 1
        next()
      }
    })

    try {
      const registerRoutes = require(appRoutesPath)
      registerRoutes(app)

      await runHandlers(registeredRoutes['/v6/members/stats/distribution'], {
        headers: {},
        query: {}
      })

      authenticatorCalls.should.equal(0)
      controllerCalls.should.equal(1)
    } finally {
      restoreModuleCache(originalEntries)
    }
  })
})

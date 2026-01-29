const config = require('config')
const errors = require('./errors')
const { PrismaClient: IdentityExternalPrismaClient } = require('@topcoder/tc-identity-service/packages/identity-prisma-client')

let identityClient

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

function getIdentityClient () {
  if (!config.IDENTITY_DB_URL) {
    throw new errors.BadRequestError('IDENTITY_DB_URL is not configured')
  }

  if (!identityClient) {
    identityClient = new IdentityExternalPrismaClient({
      ...clientOptions,
      datasources: { db: { url: config.IDENTITY_DB_URL } }
    })
  }

  return identityClient
}

module.exports = {
  getIdentityClient
}

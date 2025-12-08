const config = require('config')
const errors = require('./errors')
const { PrismaClient: IdentityPrismaClient } = require('../../prisma/generated/identity-client')

let identityClient

function getIdentityClient () {
  if (!config.IDENTITY_DB_URL) {
    throw new errors.BadRequestError('IDENTITY_DB_URL is not configured')
  }

  if (!identityClient) {
    identityClient = new IdentityPrismaClient({
      datasources: {
        identitydb: {
          url: config.IDENTITY_DB_URL
        }
      }
    })
  }

  return identityClient
}

module.exports = {
  getIdentityClient
}

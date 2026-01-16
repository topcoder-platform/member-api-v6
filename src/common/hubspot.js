const axios = require('axios')
const config = require('config')
const errors = require('./errors')
const logger = require('./logger')

function getHubSpotConfig () {
  const apiKey = config.HUBSPOT_API_KEY
  if (!apiKey) {
    throw new errors.BadRequestError('HubSpot API key is not configured')
  }

  return {
    apiKey,
    baseUrl: config.HUBSPOT_BASE_URL || 'https://api.hubapi.com'
  }
}

async function deleteContactByEmail (email) {
  if (!email) {
    logger.warn('HubSpot deletion skipped because email is missing')
    return
  }

  const { apiKey, baseUrl } = getHubSpotConfig()
  const encodedEmail = encodeURIComponent(email)
  let contactId

  try {
    const response = await axios.get(`${baseUrl}/crm/v3/objects/contacts/${encodedEmail}`, {
      params: {
        idProperty: 'email'
      },
      headers: {Authorization: `Bearer ${apiKey}`}
    })
    contactId = response.id
  } catch (err) {
    if (err.response && err.response.status === 404) {
      logger.info(`HubSpot contact not found for ${email}`)
      return
    }
    logger.error(`Failed to fetch HubSpot contact for ${email}: ${err.message}`)
    throw err
  }

  if (!contactId) {
    logger.info(`HubSpot contact id missing for ${email}`)
    return
  }

  try {
    await axios.delete(`${baseUrl}/crm/v3/objects/contacts/${contactId}`, {
      headers: {Authorization: `Bearer ${apiKey}`}
    })
  } catch (err) {
    if (err.response && err.response.status === 404) {
      logger.info(`HubSpot contact not found for deletion: ${contactId}`)
      return
    }
    logger.error(`Failed to delete HubSpot contact ${contactId} for ${email}: ${err.message}`)
    throw err
  }
}

module.exports = {
  deleteContactByEmail
}

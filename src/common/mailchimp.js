const axios = require('axios')
const config = require('config')
const crypto = require('crypto')
const errors = require('./errors')
const logger = require('./logger')

function getMailChimpConfig () {
  const apiKey = config.MAILCHIMP && config.MAILCHIMP.API_KEY
  if (!apiKey) {
    throw new errors.BadRequestError('MailChimp API key is not configured')
  }
  const serverPrefix = (config.MAILCHIMP && config.MAILCHIMP.SERVER_PREFIX) || apiKey.split('-')[1]
  if (!serverPrefix) {
    throw new errors.BadRequestError('MailChimp server prefix is not configured')
  }
  const token = Buffer.from(`user:${apiKey}`).toString('base64')
  const baseUrl = `https://${serverPrefix}.api.mailchimp.com/3.0`
  const listFetchCount = (config.MAILCHIMP && config.MAILCHIMP.LIST_FETCH_COUNT) || 1000

  return {
    baseUrl,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Basic ${token}`
    },
    listFetchCount
  }
}

async function deleteSubscriber (email) {
  if (!email) {
    logger.warn('MailChimp deletion skipped because email is missing')
    return
  }
  const { baseUrl, headers, listFetchCount } = getMailChimpConfig()
  const subscriberHash = crypto.createHash('md5').update(email.toLowerCase()).digest('hex')

  let lists = []
  try {
    const response = await axios.get(`${baseUrl}/lists`, {
      params: {
        fields: 'lists.id',
        count: listFetchCount
      },
      headers
    })
    lists = response.data && response.data.lists ? response.data.lists : []
  } catch (err) {
    logger.error(`Failed to fetch MailChimp lists: ${err.message}`)
    throw err
  }

  const deletionResults = await Promise.allSettled(
    lists.map(async (list) => {
      try {
        await axios.post(`${baseUrl}/lists/${list.id}/members/${subscriberHash}/actions/delete-permanent`, null, { headers })
      } catch (err) {
        // A 404 means the subscriber was not present in that list, which is fine
        if (err.response && err.response.status === 404) {
          logger.info(`MailChimp subscriber not found in list ${list.id}`)
        } else {
          logger.error(`Failed to delete MailChimp subscriber from list ${list.id}: ${err.message}`)
          throw err
        }
      }
    })
  )

  const failedDeletion = deletionResults.find((result) => result.status === 'rejected')
  if (failedDeletion) {
    throw failedDeletion.reason
  }
}

module.exports = {
  deleteSubscriber
}

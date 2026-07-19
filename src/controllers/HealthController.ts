/**
 * Controller for health check endpoint
 */
const config = require('config')
const { checkMemberDatabaseHealth } = require('../common/memberDatabaseHealth')
const errors = require('../common/errors')

// the topcoder-healthcheck-dropin library returns checksRun count,
// here it follows that to return such count
let checksRun = 0

/**
 * Checks application readiness through the primary members database.
 *
 * Load balancer requests use this controller to run the shared lightweight
 * database probe and receive the established `{ checksRun }` response.
 *
 * @param {Object} req The Express request.
 * @param {Object} res The Express response.
 * @returns {Promise<void>} Resolves after the health response is sent.
 * @throws {ServiceUnavailableError} When the database query fails or exceeds
 * the configured health-check duration.
 */
async function checkHealth (req, res) {
  // perform a quick database access operation, if there is no error and is quick, then consider it healthy
  checksRun += 1
  let durationMS
  try {
    durationMS = await checkMemberDatabaseHealth()
  } catch (e) {
    throw new errors.ServiceUnavailableError(`There is database operation error, ${e.message}`)
  }
  if (durationMS > Number(config.HEALTH_CHECK_TIMEOUT)) {
    throw new errors.ServiceUnavailableError('Database operation is slow.')
  }
  // there is no error, and it is quick, then return checks run count
  res.send({ checksRun })
}

module.exports = {
  checkHealth
}

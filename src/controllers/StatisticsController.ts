/**
 * Controller for statistics endpoints
 */
const service = require('../services/StatisticsService')

/**
 * Get distribution statistics
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getDistribution (req, res) {
  const result = await service.getDistribution(req.query)
  res.send(result)
}

/**
 * Get member history statistics
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getHistoryStats (req, res) {
  const result = await service.getHistoryStats(req.authUser, req.params.handle, req.query)
  res.send(result)
}

/**
 * Create member history statistics
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function createHistoryStats (req, res) {
  const result = await service.createHistoryStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Partially update history stats
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function partiallyUpdateHistoryStats (req, res) {
  const result = await service.partiallyUpdateHistoryStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Get member statistics
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMemberStats (req, res) {
  const result = await service.getMemberStats(req.authUser, req.params.handle, req.query, true)
  res.send(result)
}

/**
 * Create member stats
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function createMemberStats (req, res) {
  const result = await service.createMemberStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Partially update member stats
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function partiallyUpdateMemberStats (req, res) {
  const result = await service.partiallyUpdateMemberStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Refresh memberStats and memberStatsHistory for the specified member.
 * Callable by admin users or M2M tokens with the refresh:member_stats scope.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function refreshMemberStats (req, res) {
  const result = await service.refreshMemberStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Trigger all applicable rating updates for submitters on a completed challenge.
 * Callable by admin users or M2M tokens with the rerate:member_stats scope.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function rerateChallengeSubmitterRatings (req, res) {
  const result = await service.rerateChallengeSubmitterRatings(req.authUser, req.body)
  res.send(result)
}

/**
 * Trigger a re-rating pass for the specified member from the requested challenge onward.
 * Callable by admin users or M2M tokens with the rerate:member_stats scope.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function rerateMemberStats (req, res) {
  const result = await service.rerateMemberStats(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Get member skills
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMemberSkills (req, res) {
  const result = await service.getMemberSkills(req.params.handle)
  res.send(result)
}

/**
 * Create member skills
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function createMemberSkills (req, res) {
  const result = await service.createMemberSkills(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Partially update member skills
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function partiallyUpdateMemberSkills (req, res) {
  const result = await service.partiallyUpdateMemberSkills(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Verify a set of member skills (bulk) to 'verified' level
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function verifyMemberSkills (req, res) {
  const result = await service.verifyMemberSkills(req.authUser, req.params.handle, req.body)
  res.send(result)
}

module.exports = {
  getDistribution,
  getHistoryStats,
  createHistoryStats,
  partiallyUpdateHistoryStats,
  getMemberStats,
  createMemberStats,
  partiallyUpdateMemberStats,
  refreshMemberStats,
  rerateChallengeSubmitterRatings,
  rerateMemberStats,
  getMemberSkills,
  createMemberSkills,
  partiallyUpdateMemberSkills,
  verifyMemberSkills
}

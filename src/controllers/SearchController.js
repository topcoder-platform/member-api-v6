/**
 * Controller for search endpoints
 */
const helper = require('../common/helper')
const service = require('../services/SearchService')

/**
 * Search members
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function searchMembers (req, res) {
  const result = await service.searchMembers(req.authUser, req.query)
  helper.setResHeaders(req, res, result)
  res.send(result.result)
}

/**
 * Search members
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function autocomplete (req, res) {
  const result = await service.autocomplete(req.authUser, req.query)
  helper.setResHeaders(req, res, result)
  res.send(result.result)
}

/**
 * Autocomplete members by handle prefix provided as path parameter.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function autocompleteByHandlePrefix (req, res) {
  const result = await service.autocompleteByHandlePrefix(req.authUser, req.params.term)
  res.send(result)
}

/**
 * Search members with additional parameters, like skills
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function searchMembersBySkills (req, res) {
  const result = await service.searchMembersBySkills(req.authUser, req.query)
  helper.setResHeaders(req, res, result)
  res.send(result.result)
}
module.exports = {
  searchMembers,
  searchMembersBySkills,
  autocomplete,
  autocompleteByHandlePrefix
}

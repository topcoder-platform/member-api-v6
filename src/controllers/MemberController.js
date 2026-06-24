/**
 * Controller for members endpoints
 */
const service = require('../services/MemberService')

/**
 * Get member data
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMember (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.getMember(req.authUser, req.params.handle, req.query)
  res.send(result)
}
/**
 * Get member profile completeness data, for new profile nudge (MP-70)
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getProfileCompleteness (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.getProfileCompleteness(req.authUser, req.params.handle, req.query)
  res.send(result)
}

/**
 * Get member's hashed user id as signature for various UI api integrations
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMemberUserIdSignature (req, res) {
  const result = await service.getMemberUserIdSignature(req.authUser, req.query)
  res.send(result)
}

/**
 * Replace stored challenge-point rows for one challenge.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function updateChallengePoints (req, res) {
  const result = await service.updateChallengePoints(req.authUser, req.params.challengeId, req.body)
  res.send(result)
}

/**
 * Get a specific member skill by skill ID
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMemberSkill (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.getMemberSkill(req.authUser, req.params.handle, req.params.skillid)
  res.send(result)
}

/**
 * Update member data, only passed fields are updated
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function updateMember (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.updateMember(req.authUser, req.params.handle, req.query, req.body)
  res.send(result)
}

/**
 * Update member handle
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function updateHandle (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.updateHandle(req.authUser, req.params.handle, req.query, req.body)
  res.send(result)
}

/**
 * Verify email
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function verifyEmail (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.verifyEmail(req.authUser, req.params.handle, req.query)
  res.send(result)
}

/**
 * Upload photo
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function uploadPhoto (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.uploadPhoto(req.authUser, req.params.handle, req.files)
  res.send(result)
}

/**
 * Delete member data
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function deleteMember (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.deleteMember(req.authUser, req.params.handle, req.body)
  res.send(result)
}

/**
 * Confirm member profile data
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function confirmProfileData (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.confirmProfileData(req.authUser, req.params.handle)
  res.send(result)
}

/**
 * Download member profile as PDF
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function downloadProfile (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const pdfStream = await service.downloadProfile(req.authUser, req.params.handle)
  res.setHeader('Content-Type', 'application/pdf')
  res.setHeader('Content-Disposition', `attachment; filename="topcoder-profile-${req.params.handle}.pdf"`)
  pdfStream.pipe(res)
}

/**
 * Get up to the most recent 20 SendGrid emails sent to a member in the last 30 days.
 * @param {Object} req the request
 * @param {Object} res the response
 */
async function getMemberSendgridEmails (req, res) {
  const handle = decodeURIComponent(req.params.handle)
  const result = await service.getMemberSendgridEmails(req.authUser, req.params.handle)
  res.send(result)
}

module.exports = {
  getMember,
  getProfileCompleteness,
  getMemberUserIdSignature,
  updateChallengePoints,
  getMemberSkill,
  updateMember,
  updateHandle,
  verifyEmail,
  uploadPhoto,
  deleteMember,
  confirmProfileData,
  downloadProfile,
  getMemberSendgridEmails
}

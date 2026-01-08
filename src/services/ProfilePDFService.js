/**
 * Service for generating member profile PDFs
 */
const ReactPDF = require('@react-pdf/renderer')
const { buildProfileTemplate } = require('../common/profileTemplate')

/**
 * Generate PDF stream for member profile
 * @param {Object} memberData the member profile data
 * @returns {Stream} PDF stream
 */
async function generatePDF (memberData) {
  const document = buildProfileTemplate(memberData)
  const stream = await ReactPDF.renderToStream(document)
  return stream
}

module.exports = {
  generatePDF
}
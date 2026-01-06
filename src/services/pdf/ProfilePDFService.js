/**
 * Service for generating member profile PDFs
 */
const ReactPDF = require('@react-pdf/renderer')
const { buildProfileTemplate } = require('./templates/ProfileTemplate')

/**
 * Generate PDF stream for member profile
 * @param {Object} memberData the member profile data
 * @returns {Stream} PDF stream
 */
async function generatePDF (memberData) {
  // Build the React element tree using the template
  const document = buildProfileTemplate(memberData)

  // Render to stream (memory efficient for AWS Fargate)
  const stream = await ReactPDF.renderToStream(document)

  return stream
}

module.exports = {
  generatePDF
}


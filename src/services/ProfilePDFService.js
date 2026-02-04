/**
 * Service for generating member profile PDFs
 */
const ReactPDF = require('@react-pdf/renderer')
const { Font } = require('@react-pdf/renderer')
const path = require('path')
const { buildProfileTemplate } = require('../common/profileTemplate')

// Register Arial fonts if not already registered
const registeredFamilies = Font.getRegisteredFontFamilies()
if (!registeredFamilies.includes('Arial')) {
  Font.register({
    family: 'Arial',
    fonts: [
      { src: path.join(__dirname, '../fonts/arial/ARIAL.TTF') },
      { src: path.join(__dirname, '../fonts/arial/ARIALBD.TTF'), fontWeight: 'bold' },
      { src: path.join(__dirname, '../fonts/arial/ARIALI.TTF'), fontStyle: 'italic' },
      { src: path.join(__dirname, '../fonts/arial/ARIALBI.TTF'), fontWeight: 'bold', fontStyle: 'italic' }
    ]
  })
}

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

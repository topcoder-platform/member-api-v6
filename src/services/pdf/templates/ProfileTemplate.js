/**
 * PDF Template for Member Profile
 */
const React = require('react')
const {
  Document,
  Page,
  Text,
  StyleSheet
} = require('@react-pdf/renderer')

// Define styles
const styles = StyleSheet.create({
  page: {
    padding: 30,
    fontSize: 12,
    fontFamily: 'Helvetica'
  },
  title: {
    fontSize: 24,
    marginBottom: 20,
    fontWeight: 'bold'
  }
})

/**
 * Build the PDF template for member profile
 * @param {Object} memberData the member profile data
 * @returns {Object} React element tree
 */
function buildProfileTemplate (memberData) {
  return React.createElement(
    Document,
    {},
    React.createElement(
      Page,
      { size: 'A4', style: styles.page },
      React.createElement(
        Text,
        { style: styles.title },
        `Member Profile: ${memberData.handle || 'N/A'}`
      )
    )
  )
}

module.exports = {
  buildProfileTemplate
}


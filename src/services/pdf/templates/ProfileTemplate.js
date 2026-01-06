/**
 * PDF Template for Member Profile
 * Uses React.createElement syntax (no JSX) to avoid transpilation
 */
const React = require('react')
const {
  Document,
  Page,
  Text,
  View,
  StyleSheet,
  Image
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
  },
  section: {
    marginBottom: 15
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: 'bold',
    marginBottom: 8,
    borderBottom: '1pt solid #000000',
    paddingBottom: 4
  },
  field: {
    marginBottom: 5
  },
  fieldLabel: {
    fontWeight: 'bold',
    display: 'inline'
  },
  fieldValue: {
    display: 'inline',
    marginLeft: 5
  },
  photo: {
    width: 100,
    height: 100,
    marginBottom: 15,
    objectFit: 'cover'
  },
  skillsList: {
    marginTop: 5
  },
  skillItem: {
    marginBottom: 3
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
      // Title
      React.createElement(
        Text,
        { style: styles.title },
        `Member Profile: ${memberData.handle || 'N/A'}`
      ),
      // Photo (if available)
      memberData.photoURL
        ? React.createElement(
            Image,
            {
              src: memberData.photoURL,
              style: styles.photo
            }
          )
        : null,
      // Basic Information Section
      React.createElement(
        View,
        { style: styles.section },
        React.createElement(
          Text,
          { style: styles.sectionTitle },
          'Basic Information'
        ),
        memberData.firstName || memberData.lastName
          ? React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Name: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                `${memberData.firstName || ''} ${memberData.lastName || ''}`.trim() || 'N/A'
              )
            )
          : null,
        memberData.email
          ? React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Email: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                memberData.email
              )
            )
          : null,
        memberData.country
          ? React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Country: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                memberData.country
              )
            )
          : null,
        memberData.status
          ? React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Status: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                memberData.status
              )
            )
          : null,
        memberData.verified !== undefined
          ? React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Verified: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                memberData.verified ? 'Yes' : 'No'
              )
            )
          : null
      ),
      // Description Section
      memberData.description
        ? React.createElement(
            View,
            { style: styles.section },
            React.createElement(
              Text,
              { style: styles.sectionTitle },
              'Description'
            ),
            React.createElement(
              Text,
              {},
              memberData.description
            )
          )
        : null,
      // Skills Section
      memberData.skills && memberData.skills.length > 0
        ? React.createElement(
            View,
            { style: styles.section },
            React.createElement(
              Text,
              { style: styles.sectionTitle },
              'Skills'
            ),
            React.createElement(
              View,
              { style: styles.skillsList },
              ...memberData.skills.map((skill, index) =>
                React.createElement(
                  View,
                  { key: index, style: styles.skillItem },
                  React.createElement(
                    Text,
                    {},
                    skill.name || 'N/A'
                  )
                )
              )
            )
          )
        : null,
      // Tracks Section
      memberData.tracks && memberData.tracks.length > 0
        ? React.createElement(
            View,
            { style: styles.section },
            React.createElement(
              Text,
              { style: styles.sectionTitle },
              'Tracks'
            ),
            React.createElement(
              Text,
              {},
              memberData.tracks.join(', ')
            )
          )
        : null,
      // Max Rating Section
      memberData.maxRating
        ? React.createElement(
            View,
            { style: styles.section },
            React.createElement(
              Text,
              { style: styles.sectionTitle },
              'Rating'
            ),
            React.createElement(
              View,
              { style: styles.field },
              React.createElement(
                Text,
                { style: styles.fieldLabel },
                'Track: '
              ),
              React.createElement(
                Text,
                { style: styles.fieldValue },
                memberData.maxRating.track || 'N/A'
              )
            ),
            memberData.maxRating.rating
              ? React.createElement(
                  View,
                  { style: styles.field },
                  React.createElement(
                    Text,
                    { style: styles.fieldLabel },
                    'Rating: '
                  ),
                  React.createElement(
                    Text,
                    { style: styles.fieldValue },
                    String(memberData.maxRating.rating)
                  )
                )
              : null
          )
        : null,
      // Available for Gigs
      memberData.availableForGigs !== undefined && memberData.availableForGigs !== null
        ? React.createElement(
            View,
            { style: styles.section },
            React.createElement(
              Text,
              { style: styles.sectionTitle },
              'Availability'
            ),
            React.createElement(
              Text,
              {},
              memberData.availableForGigs ? 'Available for Gigs' : 'Not Available for Gigs'
            )
          )
        : null
    )
  )
}

module.exports = {
  buildProfileTemplate
}


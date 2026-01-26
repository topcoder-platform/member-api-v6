/**
 * PDF Template for Member Profile
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
    padding: 40,
    fontSize: 11,
    fontFamily: 'Helvetica',
    backgroundColor: '#FFFFFF',
    color: '#000000'
  },
  // Header styles
  header: {
    marginBottom: 20
  },
  headerTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 15
  },
  logo: {
    width: 120,
    height: 30
  },
  generatedOn: {
    fontSize: 9,
    color: '#666666',
    textAlign: 'right'
  },
  memberName: {
    fontSize: 28,
    fontWeight: 'bold',
    textAlign: 'center',
    marginBottom: 5,
    color: '#000000'
  },
  memberTitle: {
    fontSize: 14,
    textAlign: 'center',
    marginBottom: 10,
    color: '#000000'
  },
  personalInfo: {
    fontSize: 10,
    textAlign: 'center',
    marginBottom: 5,
    color: '#000000'
  },
  handleInfo: {
    fontSize: 10,
    textAlign: 'center',
    marginBottom: 15,
    color: '#000000'
  },
  statusBar: {
    backgroundColor: '#000000',
    padding: 8,
    marginBottom: 20
  },
  statusBarText: {
    color: '#FFFFFF',
    fontSize: 10,
    fontWeight: 'bold',
    textAlign: 'left'
  },
  // Section styles
  section: {
    marginBottom: 20
  },
  sectionHeader: {
    fontSize: 14,
    fontWeight: 'bold',
    color: '#00BFA5',
    marginBottom: 5
  },
  sectionUnderline: {
    height: 1,
    backgroundColor: '#E0E0E0',
    marginBottom: 10
  },
  // Biography
  biographyText: {
    fontSize: 10,
    lineHeight: 1.5,
    marginBottom: 5,
    color: '#000000'
  },
  // Skills
  skillsSubsection: {
    marginBottom: 10
  },
  skillsSubsectionTitle: {
    fontSize: 11,
    fontWeight: 'bold',
    marginBottom: 5,
    color: '#000000'
  },
  skillsList: {
    fontSize: 10,
    marginLeft: 10,
    marginBottom: 5,
    color: '#000000'
  },
  skillsLabel: {
    fontWeight: 'bold'
  },
  // Languages
  languagesText: {
    fontSize: 10,
    color: '#000000'
  },
  // Topcoder Activity
  activityItem: {
    fontSize: 10,
    marginBottom: 5,
    color: '#000000'
  },
  activityLabel: {
    fontWeight: 'bold'
  },
  // Education/Experience items
  itemTitle: {
    fontSize: 11,
    fontWeight: 'bold',
    marginBottom: 2,
    color: '#000000'
  },
  itemSubtitle: {
    fontSize: 10,
    marginBottom: 2,
    color: '#000000'
  },
  itemDate: {
    fontSize: 10,
    textAlign: 'right',
    color: '#000000'
  },
  itemRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 5
  },
  itemDescription: {
    fontSize: 10,
    marginTop: 5,
    marginBottom: 5,
    lineHeight: 1.4,
    color: '#000000'
  },
  itemSkills: {
    fontSize: 10,
    marginTop: 5,
    fontStyle: 'italic',
    color: '#000000'
  },
  bulletPoint: {
    fontSize: 10,
    marginLeft: 15,
    marginBottom: 3,
    lineHeight: 1.4,
    color: '#000000'
  },
  // Certifications
  certificationItem: {
    fontSize: 10,
    marginBottom: 3,
    color: '#000000'
  },
  certificationLabel: {
    fontWeight: 'bold'
  }
})

/**
 * Create a section header with underline
 */
function createSectionHeader (title) {
  return React.createElement(
    View,
    { style: styles.section },
    React.createElement(
      Text,
      { style: styles.sectionHeader },
      title
    ),
    React.createElement(
      View,
      { style: styles.sectionUnderline }
    )
  )
}

/**
 * Create skills subsection
 */
function createSkillsSubsection (title, verified, notVerified) {
  const items = []
  
  if (verified.length > 0) {
    items.push(
      React.createElement(
        Text,
        { key: 'verified-label', style: styles.skillsList },
        React.createElement(Text, { style: styles.skillsLabel }, '• Verified Skills: '),
        verified.join(', ')
      )
    )
  }
  
  if (notVerified.length > 0) {
    items.push(
      React.createElement(
        Text,
        { key: 'not-verified-label', style: styles.skillsList },
        React.createElement(Text, { style: styles.skillsLabel }, '• Not Verified Skills: '),
        notVerified.join(', ')
      )
    )
  }
  
  if (items.length === 0) {
    return null
  }
  
  return React.createElement(
    View,
    { style: styles.skillsSubsection },
    React.createElement(
      Text,
      { style: styles.skillsSubsectionTitle },
      title
    ),
    ...items
  )
}

/**
 * Build the PDF template for member profile
 * @param {Object} pdfData the aggregated PDF data
 * @returns {Object} React element tree
 */
function buildProfileTemplate (pdfData) {
  const { member, workExperience, education, languages, basicInfo, skills, topcoderActivity, certifications, courses } = pdfData
  
  const children = []
  
  // Header Section
  children.push(
    React.createElement(
      View,
      { key: 'header', style: styles.header },
      React.createElement(
        View,
        { style: styles.headerTop },
        // Logo placeholder - will need actual logo file
        React.createElement(
          Text,
          { style: { fontSize: 12, fontWeight: 'bold' } },
          'topcoder'
        ),
        React.createElement(
          Text,
          { style: styles.generatedOn },
          `Generated on ${member.generatedOn}`
        )
      ),
      React.createElement(
        Text,
        { style: styles.memberName },
        `${member.firstName || ''} ${member.lastName || ''}`.trim() || member.handle
      ),
      basicInfo?.shortBio ? React.createElement(
        Text,
        { style: styles.memberTitle },
        basicInfo.shortBio
      ) : null,
      (member.addresses && member.addresses.length > 0) || basicInfo?.currentLocation || member.email ? React.createElement(
        Text,
        { style: styles.personalInfo },
        [
          basicInfo?.currentLocation || (member.addresses && member.addresses.length > 0 ? `${member.addresses[0].city || ''}, ${member.addresses[0].stateCode || ''} ${member.addresses[0].country || ''}`.trim() : ''),
          member.email
        ].filter(Boolean).join(' | ')
      ) : null,
      React.createElement(
        Text,
        { style: styles.handleInfo },
        `Topcoder Handle: ${member.handle}${member.createdAt ? ` | Member Since ${new Date(member.createdAt).getFullYear()}` : ''}`
      ),
      member.statusBarText ? React.createElement(
        View,
        { style: styles.statusBar },
        React.createElement(
          Text,
          { style: styles.statusBarText },
          member.statusBarText
        )
      ) : null
    )
  )
  
  // Biography Section
  const biography = member.description || basicInfo?.shortBio
  if (biography) {
    children.push(
      createSectionHeader('BIOGRAPHY'),
      React.createElement(
        Text,
        { key: 'biography-text', style: styles.biographyText },
        biography
      )
    )
  }
  
  // Technical Skills Section
  const hasSkills = skills.principal.verified.length > 0 || skills.principal.notVerified.length > 0 ||
                    skills.additional.verified.length > 0 || skills.additional.notVerified.length > 0
  if (hasSkills) {
    children.push(createSectionHeader('TECHNICAL SKILLS'))
    
    const principalSubsection = createSkillsSubsection(
      'Principal Skills:',
      skills.principal.verified,
      skills.principal.notVerified
    )
    if (principalSubsection) {
      children.push(principalSubsection)
    }
    
    const additionalSubsection = createSkillsSubsection(
      'Additional Skills:',
      skills.additional.verified,
      skills.additional.notVerified
    )
    if (additionalSubsection) {
      children.push(additionalSubsection)
    }
  }
  
  // Languages Section
  if (languages && languages.length > 0) {
    children.push(
      createSectionHeader('LANGUAGES'),
      React.createElement(
        Text,
        { key: 'languages-text', style: styles.languagesText },
        languages.join(', ')
      )
    )
  }
  
  // Topcoder Activity Section
  if (topcoderActivity.specialRole || topcoderActivity.achievements) {
    children.push(createSectionHeader('TOPCODER ACTIVITY'))
    
    if (topcoderActivity.specialRole) {
      children.push(
        React.createElement(
          Text,
          { key: 'special-role', style: styles.activityItem },
          React.createElement(Text, { style: styles.activityLabel }, topcoderActivity.specialRole)
        )
      )
    }
    
    if (topcoderActivity.achievements) {
      children.push(
        React.createElement(
          Text,
          { key: 'achievements', style: styles.activityItem },
          topcoderActivity.achievements
        )
      )
    }
  }
  
  // Education Section
  if (education && education.length > 0) {
    children.push(createSectionHeader('EDUCATION'))
    
    education.forEach((edu, index) => {
      children.push(
        React.createElement(
          View,
          { key: `edu-${index}`, style: { marginBottom: 10 } },
          React.createElement(
            View,
            { style: styles.itemRow },
            React.createElement(
              Text,
              { style: styles.itemTitle },
              edu.degree
            ),
            edu.endYear ? React.createElement(
              Text,
              { style: styles.itemDate },
              edu.endYear
            ) : null
          ),
          React.createElement(
            Text,
            { style: styles.itemSubtitle },
            edu.college
          )
        )
      )
    })
  }
  
  // Certifications & Courses Section
  if ((certifications && certifications.length > 0) || (courses && courses.length > 0)) {
    children.push(createSectionHeader('CERTIFICATIONS & COURSES'))
    
    if (certifications && certifications.length > 0) {
      children.push(
        React.createElement(
          Text,
          { key: 'certifications-label', style: styles.certificationItem },
          React.createElement(Text, { style: styles.certificationLabel }, 'Certifications: ')
        )
      )
      certifications.forEach((cert, index) => {
        children.push(
          React.createElement(
            Text,
            { key: `cert-${index}`, style: styles.certificationItem },
            cert
          )
        )
      })
    }
    
    if (courses && courses.length > 0) {
      children.push(
        React.createElement(
          Text,
          { key: 'courses-label', style: [styles.certificationItem, { marginTop: 5 }] },
          React.createElement(Text, { style: styles.certificationLabel }, 'Courses: ')
        )
      )
      courses.forEach((course, index) => {
        children.push(
          React.createElement(
            Text,
            { key: `course-${index}`, style: styles.certificationItem },
            course
          )
        )
      })
    }
  }
  
  // Experience Section
  if (workExperience && workExperience.length > 0) {
    children.push(createSectionHeader('EXPERIENCE'))
    
    workExperience.forEach((work, index) => {
      const dateRange = [work.startDate, work.endDate].filter(Boolean).join(' - ')
      children.push(
        React.createElement(
          View,
          { key: `work-${index}`, style: { marginBottom: 15 } },
          React.createElement(
            View,
            { style: styles.itemRow },
            React.createElement(
              Text,
              { style: styles.itemTitle },
              work.position
            ),
            dateRange ? React.createElement(
              Text,
              { style: styles.itemDate },
              dateRange
            ) : null
          ),
          React.createElement(
            Text,
            { style: styles.itemSubtitle },
            work.company
          ),
          work.description ? React.createElement(
            Text,
            { style: styles.itemDescription },
            work.description
          ) : null,
          work.skills && work.skills.length > 0 ? React.createElement(
            Text,
            { style: styles.itemSkills },
            `Skills: ${work.skills.join(', ')}`
          ) : null
        )
      )
    })
  }
  
  return React.createElement(
    Document,
    {},
    React.createElement(
      Page,
      { size: 'A4', style: styles.page },
      ...children
    )
  )
}

module.exports = {
  buildProfileTemplate
}

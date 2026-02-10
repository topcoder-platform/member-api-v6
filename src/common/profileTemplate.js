/**
 * PDF Template for Member Profile
 */
const React = require('react')
const path = require('path')
const {
  Document,
  Page,
  Text,
  View,
  StyleSheet,
  Image
} = require('@react-pdf/renderer')
const { Html } = require('react-pdf-html')
const { htmlToText } = require('./htmlUtils')

// Define styles
const styles = StyleSheet.create({
  page: {
    padding: 40,
    fontSize: 11,
    fontFamily: 'Arial',
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
    marginBottom: 15,
    borderBottomWidth: 1,
    borderBottomColor: '#AAAAAA',
    paddingBottom: 10
  },
  logo: {
    width: 120,
    height: 30,
    objectFit: 'contain'
  },
  generatedOn: {
    fontSize: 9,
    color: '#666666',
    textAlign: 'left',
    fontStyle: 'italic'
  },
  memberName: {
    fontSize: 28,
    fontWeight: 700,
    textAlign: 'center',
    marginBottom: 5,
    color: '#000000',
    textTransform: 'uppercase'
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
    marginTop: 10,
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
    marginBottom: 10
  },
  statusBarSeparator: {
    height: 1,
    backgroundColor: '#AAAAAA',
    marginTop: 10,
    marginBottom: 10
  },
  statusBarText: {
    color: '#FFFFFF',
    fontSize: 10,
    fontWeight: 700,
    textAlign: 'center'
  },
  // Section styles
  section: {
    marginBottom: 10
  },
  sectionHeader: {
    fontSize: 14,
    fontWeight: 'bold',
    color: '#227681',
    marginBottom: 5
  },
  sectionUnderline: {
    height: 1,
    backgroundColor: '#AAAAAA',
    marginBottom: 10
  },
  // Biography
  biographyText: {
    fontSize: 11,
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
    color: '#000000',
    lineHeight: 1.6
  },
  skillsLabel: {
    fontWeight: 'bold'
  },
  // Languages
  languagesText: {
    fontSize: 10,
    marginBottom: 10,
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
    fontStyle: 'italic',
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
    marginBottom: 2,
    lineHeight: 1.4,
    color: '#000000'
  },
  itemSkills: {
    fontSize: 10,
    marginTop: 4,
    marginBottom: 6,
    lineHeight: 1.4,
    color: '#000000'
  },
  bulletPoint: {
    fontSize: 10,
    marginLeft: 15,
    marginBottom: 3,
    lineHeight: 1.4,
    color: '#000000'
  },
  // Work description HTML: tighten paragraph spacing so typed (p) and pasted (br) look consistent
  descriptionListStylesheet: {
    p: { margin: 0, marginBottom: 2 },
    ul: { paddingLeft: 15, marginTop: 3, marginBottom: 3 },
    ol: { paddingLeft: 15, marginTop: 3, marginBottom: 3 },
    li: { marginBottom: 2 }
  },
  // Certifications & Courses
  certificationItem: {
    fontSize: 10,
    marginBottom: 8,
    lineHeight: 1.5,
    color: '#000000'
  },
  courseItem: {
    fontSize: 10,
    marginTop: 10,
    marginBottom: 8,
    lineHeight: 1.5,
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
    { style: { marginBottom: 0 } },
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
        React.createElement(
          Image,
          {
            src: path.join(__dirname, '../images/topcoder-logo.png'),
            style: styles.logo
          }
        ),
        React.createElement(
          View,
          { style: { alignItems: 'flex-start' } },
          React.createElement(
            Text,
            { style: styles.generatedOn },
            'Generated on'
          ),
          React.createElement(
            Text,
            { style: styles.generatedOn },
            member.generatedOn
          )
        )
      ),
      React.createElement(
        Text,
        { style: styles.memberName },
        `${member.firstName || ''} ${member.lastName || ''}`.trim() || member.handle
      ),
      basicInfo && basicInfo.shortBio ? React.createElement(
        Text,
        { style: styles.memberTitle },
        basicInfo.shortBio
      ) : null,
      (member.addresses && member.addresses.length > 0) || (basicInfo && basicInfo.currentLocation) || member.email ? React.createElement(
        Text,
        { style: styles.personalInfo },
        [
          (basicInfo && basicInfo.currentLocation) || (() => {
            if (member.addresses && member.addresses.length > 0) {
              const city = member.addresses[0].city || ''
              const stateCode = member.addresses[0].stateCode || ''
              const country = member.country || ''
              const parts = [city, stateCode, country].filter(Boolean)
              return parts.length > 0 ? parts.join(', ') : ''
            }
            return member.country || ''
          })(),
          member.timezone ? `Timezone: ${member.timezone}` : null,
          member.email
        ].filter(Boolean).join(' | ')
      ) : null,
      React.createElement(
        Text,
        { style: styles.handleInfo },
        `Topcoder Handle: ${member.handle}${member.createdAt ? ` | Member Since ${new Date(member.createdAt).getFullYear()}` : ''}`
      ),
      member.statusBarText
        ? React.createElement(
            View,
            { style: styles.statusBar },
            React.createElement(
              Text,
              { style: styles.statusBarText },
              member.statusBarText
            )
          )
        : React.createElement(View, { style: styles.statusBarSeparator })
    )
  )

  // Biography Section
  const biography = member.description || (basicInfo && basicInfo.shortBio)
  if (biography) {
    children.push(
      React.createElement(
        View,
        { key: 'biography-section', style: styles.section },
        createSectionHeader('BIOGRAPHY'),
        React.createElement(
          Text,
          { key: 'biography-text', style: styles.biographyText },
          biography
        )
      )
    )
  }

  // Technical Skills Section
  const hasSkills = skills.principal.verified.length > 0 || skills.principal.notVerified.length > 0 ||
                    skills.additional.verified.length > 0 || skills.additional.notVerified.length > 0
  if (hasSkills) {
    const skillsContent = [
      createSectionHeader('TECHNICAL SKILLS')
    ]

    const principalSubsection = createSkillsSubsection(
      'Principal Skills:',
      skills.principal.verified,
      skills.principal.notVerified
    )
    if (principalSubsection) {
      skillsContent.push(principalSubsection)
    }

    const additionalSubsection = createSkillsSubsection(
      'Additional Skills:',
      skills.additional.verified,
      skills.additional.notVerified
    )
    if (additionalSubsection) {
      skillsContent.push(additionalSubsection)
    }

    children.push(
      React.createElement(
        View,
        { key: 'skills-section', style: styles.section },
        ...skillsContent
      )
    )
  }

  // Languages Section
  if (languages && languages.length > 0) {
    children.push(
      React.createElement(
        View,
        { key: 'languages-section', style: styles.section },
        createSectionHeader('LANGUAGES'),
        React.createElement(
          Text,
          { key: 'languages-text', style: styles.languagesText },
          languages.join(', ')
        )
      )
    )
  }

  // Experience Section
  if (workExperience && workExperience.length > 0) {
    const experienceContent = [createSectionHeader('EXPERIENCE')]

    workExperience.forEach((work, index) => {
      const endPart = work.endDate || (work.startDate ? 'PRESENT' : null)
      const dateRange = [work.startDate, endPart].filter(Boolean).join(' - ')
      experienceContent.push(
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
            Html,
            { style: styles.itemDescription, stylesheet: styles.descriptionListStylesheet },
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

    children.push(
      React.createElement(
        View,
        { key: 'experience-section', style: styles.section },
        ...experienceContent
      )
    )
  }

  // Topcoder Activity Section
  if (topcoderActivity.specialRole || topcoderActivity.achievements) {
    const activityContent = [createSectionHeader('TOPCODER ACTIVITY')]

    if (topcoderActivity.specialRole) {
      activityContent.push(
        React.createElement(
          Text,
          { key: 'special-role', style: styles.activityItem },
          React.createElement(Text, { style: styles.activityLabel }, topcoderActivity.specialRole)
        )
      )
    }

    if (topcoderActivity.achievements) {
      const achievements = topcoderActivity.achievements
      const plainText = typeof achievements === 'string' && htmlToText(achievements)
      activityContent.push(
        React.createElement(
          Text,
          { key: 'achievements', style: styles.activityItem },
          plainText
        )
      )
    }

    children.push(
      React.createElement(
        View,
        { key: 'activity-section', style: styles.section },
        ...activityContent
      )
    )
  }

  // Education Section
  if (education && education.length > 0) {
    const educationContent = [createSectionHeader('EDUCATION')]

    education.forEach((edu, index) => {
      educationContent.push(
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

    children.push(
      React.createElement(
        View,
        { key: 'education-section', style: styles.section },
        ...educationContent
      )
    )
  }

  // Certifications & Courses Section
  if ((certifications && certifications.length > 0) || (courses && courses.length > 0)) {
    const certContent = [createSectionHeader('CERTIFICATIONS & COURSES')]

    if (certifications && certifications.length > 0) {
      const certificationsText = certifications.join(', ')
      certContent.push(
        React.createElement(
          Text,
          { key: 'certifications', style: styles.certificationItem },
          React.createElement(Text, { style: styles.certificationLabel }, 'Certifications: '),
          certificationsText
        )
      )
    }

    if (courses && courses.length > 0) {
      const coursesText = courses.join(', ')
      certContent.push(
        React.createElement(
          Text,
          { key: 'courses', style: styles.courseItem },
          React.createElement(Text, { style: styles.certificationLabel }, 'Courses: '),
          coursesText
        )
      )
    }

    children.push(
      React.createElement(
        View,
        { key: 'certifications-section', style: styles.section },
        ...certContent
      )
    )
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

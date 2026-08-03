/*
 * Unit tests for downloaded-profile member stats aggregation.
 */

require('../../app-bootstrap')
const chai = require('chai')

const {
  buildProfileActivityStats,
  buildProfileActivityStatsFromRequests
} = require('../../src/common/profileStats')
const { buildProfileTemplate } = require('../../src/common/profileTemplate')

const should = chai.should()

/**
 * Flatten text children from a React element tree for template assertions.
 * @param {*} node React element, child array, or primitive value
 * @returns {string} concatenated visible text
 */
function getTemplateText (node) {
  if (node === null || node === undefined || typeof node === 'boolean') {
    return ''
  }
  if (Array.isArray(node)) {
    return node.map(getTemplateText).join('')
  }
  if (typeof node === 'string' || typeof node === 'number') {
    return String(node)
  }
  return getTemplateText(node.props && node.props.children)
}

describe('profile stats helper unit tests', () => {
  it('should match Profiles track totals for the PM-5793 member data', () => {
    const stats = {
      DEVELOP: {
        subTracks: [
          { name: 'Task', challenges: 178, wins: 178, submissions: { submissions: 178 } },
          { name: 'BUG_HUNT', challenges: 5, wins: 0, submissions: { submissions: 2 } },
          { name: 'TEST_SUITES', challenges: 6, wins: 2, submissions: { submissions: 4 } },
          { name: 'First2Finish', challenges: 17, wins: 7, submissions: { submissions: 7 } },
          { name: 'CODE', challenges: 29, wins: 7, submissions: { submissions: 21 } },
          { name: 'Challenge', challenges: 22, wins: 8, submissions: { submissions: 22 } }
        ]
      },
      DESIGN: {
        subTracks: [
          { name: 'Challenge', challenges: 6, wins: null }
        ]
      },
      QA: {
        subTracks: [
          { name: 'Challenge', challenges: 5, wins: 2, submissions: { submissions: 5 } },
          { name: 'Task', challenges: 4, wins: 4, submissions: { submissions: 4 } }
        ]
      },
      DATA_SCIENCE: {
        SRM: { challenges: null, wins: null, rank: { rating: 0 } },
        Task: { challenges: 1, wins: 1, submissions: { submissions: 1 } }
      }
    }
    const history = {
      DEVELOP: {
        subTracks: [
          {
            name: 'Task',
            history: Array.from({ length: 178 }, (_, index) => ({
              challengeId: `task-${index}`,
              challengeName: `Task ${index}`,
              placement: 1,
              ratingDate: index
            }))
          },
          {
            name: 'Challenge',
            history: Array.from({ length: 23 }, (_, index) => ({
              challengeId: `challenge-${index}`,
              challengeName: `Challenge ${index}`,
              placement: index < 8 ? 1 : index < 22 ? 2 : undefined,
              ratingDate: index
            }))
          }
        ]
      },
      DESIGN: {
        subTracks: [{
          name: 'Challenge',
          history: [
            { challengeId: 'design-1', challengeName: 'Design 1', placement: 50, ratingDate: 1 },
            { challengeId: 'design-2', challengeName: 'Design 2', placement: 1, ratingDate: 2 }
          ]
        }]
      },
      QA: {
        subTracks: [
          {
            name: 'Challenge',
            history: Array.from({ length: 5 }, (_, index) => ({
              challengeId: `qa-challenge-${index}`,
              challengeName: `QA Challenge ${index}`,
              placement: index < 2 ? 1 : 2,
              ratingDate: index
            }))
          },
          {
            name: 'Task',
            history: Array.from({ length: 4 }, (_, index) => ({
              challengeId: `qa-task-${index}`,
              challengeName: `QA Task ${index}`,
              placement: 1,
              ratingDate: index
            }))
          }
        ]
      }
    }

    buildProfileActivityStats(stats, history).should.deep.equal([
      { trackName: 'Development', challenges: 247, wins: 200, submissions: 229 },
      { trackName: 'Design', challenges: 6, wins: 1, submissions: 6 },
      { trackName: 'Testing', challenges: 20, wins: 8, submissions: 15 }
    ])
  })

  it('should keep active SRM stats separate from Data Science activity', () => {
    const inactiveStats = buildProfileActivityStats({
      DATA_SCIENCE: {
        Challenge: { challenges: 5, wins: 2, rank: { rating: 1500 } },
        MARATHON_MATCH: { challenges: 8, wins: 1, rank: { rating: 1800 } },
        SRM: { challenges: 0, wins: 0, rank: { rating: 0 } }
      }
    }, {})

    inactiveStats.should.deep.equal([
      { trackName: 'Data Science', rating: 1800, challenges: 13, wins: 3, submissions: 13 }
    ])
    inactiveStats.map(stat => stat.trackName).should.not.include('Competitive Programming')

    const activeStats = buildProfileActivityStats({
      DATA_SCIENCE: {
        Challenge: { challenges: 5, wins: 2, rank: { rating: 1500 } },
        MARATHON_MATCH: { challenges: 8, wins: 1, rank: { rating: 1800 } },
        SRM: { challenges: 170, wins: 1, rank: { rating: 2741 } }
      }
    }, {})

    activeStats.should.deep.equal([
      { trackName: 'Data Science', rating: 1800, challenges: 13, wins: 3, submissions: 13 },
      { trackName: 'Competitive Programming', rating: 2741, wins: 1, competitions: 170 }
    ])
    should.equal(activeStats[1].rating, 2741)
  })

  it('should include the strongest native Data Science rating and combined activity', () => {
    buildProfileActivityStats({
      DATA_SCIENCE: {
        Challenge: {
          challenges: 2,
          wins: 2,
          submissions: { submissions: 1 },
          rank: { rating: 1499, percentile: 10 }
        },
        MARATHON_MATCH: {
          challenges: 1,
          wins: 2,
          rank: { rating: 763, percentile: 20 }
        }
      }
    }, {
      DATA_SCIENCE: {
        Challenge: {
          history: [
            { challengeId: 1, placement: 1 },
            { challengeId: 2, placement: 2 }
          ]
        },
        MARATHON_MATCH: {
          history: [
            { challengeId: 3, placement: 1 },
            { challengeId: 4, placement: 3 }
          ]
        }
      }
    }).should.deep.equal([
      { trackName: 'Data Science', rating: 1499, challenges: 3, wins: 2, submissions: 4 }
    ])
  })

  it('should include history-aware custom ratings after Competitive Programming', () => {
    buildProfileActivityStats({
      DATA_SCIENCE: {
        'Java MySQL': {
          challenges: 3,
          wins: 1,
          rank: { rating: 1422, overallPercentile: 12 }
        },
        Python: {
          challenges: 2,
          wins: 2,
          rank: { rating: 1500, overallPercentile: 20 }
        },
        NO_RATING: {
          challenges: 2,
          wins: 1,
          rank: {}
        },
        SRM: { challenges: 1, wins: 0, rank: { rating: 900 } }
      }
    }, {
      DATA_SCIENCE: {
        'Java MySQL': {
          history: [
            { challengeId: 1, placement: 2 },
            { challengeId: 2, placement: 1 },
            { challengeId: 3, placement: 3 },
            { challengeId: 4, placement: 2 }
          ]
        }
      }
    }).should.deep.equal([
      { trackName: 'Competitive Programming', rating: 900, wins: 0, competitions: 1 },
      { trackName: 'Python', rating: 1500, wins: 2, submissions: 2, challenges: 2 },
      { trackName: 'Java MySQL', rating: 1422, wins: 1, submissions: 4, challenges: 3 }
    ])
  })

  it('should use Profiles tie breakers for AI Engineering rating aliases', () => {
    buildProfileActivityStats({
      DATA_SCIENCE: {
        AI: {
          challenges: 1,
          wins: 1,
          rank: { rating: 1200, percentile: 10 }
        },
        AI_ENGINEERING: {
          challenges: 3,
          wins: 2,
          rank: { rating: 1200, percentile: 20 }
        }
      }
    }, {}).should.deep.equal([
      { trackName: 'Development', challenges: 3, wins: 2, submissions: 3 }
    ])
  })

  it('should include and de-duplicate rated AI Engineering activity under Development', () => {
    const sharedHistory = [
      { challengeId: 'shared-1', challengeName: 'Shared 1', placement: 1, ratingDate: 1 },
      { challengeId: 'shared-2', challengeName: 'Shared 2', placement: 1, ratingDate: 2 }
    ]
    const aiHistory = [
      { challengeId: 'ai-1', challengeName: 'AI 1', placement: 1, ratingDate: 3 },
      { challengeId: 'ai-2', challengeName: 'AI 2', placement: 1, ratingDate: 4 },
      { challengeId: 'ai-3', challengeName: 'AI 3', placement: 1, ratingDate: 5 },
      { challengeId: 'ai-4', challengeName: 'AI 4', placement: 1, ratingDate: 6 },
      ...sharedHistory
    ]

    buildProfileActivityStats({
      DEVELOP: {
        subTracks: [{
          name: 'Challenge',
          challenges: 2,
          wins: 2,
          submissions: { submissions: 2 }
        }]
      },
      DATA_SCIENCE: {
        'AI Engineering': {
          challenges: 6,
          wins: 6,
          submissions: { submissions: 6 },
          rank: { rating: 1200 }
        }
      }
    }, {
      DEVELOP: {
        subTracks: [{ name: 'Challenge', history: sharedHistory }]
      },
      DATA_SCIENCE: {
        'AI Engineering': { history: aiHistory }
      }
    }).should.deep.equal([
      { trackName: 'Development', challenges: 6, wins: 6, submissions: 6 }
    ])
  })

  it('should resolve activity requests with the application Bluebird Promise', async () => {
    const stats = {
      DEVELOP: {
        subTracks: [
          { name: 'Task', challenges: 2, wins: 1, submissions: { submissions: 2 } }
        ]
      }
    }

    const result = await buildProfileActivityStatsFromRequests(
      (async () => [stats])(),
      (async () => [{}])()
    )

    result.should.deep.equal([
      { trackName: 'Development', challenges: 2, wins: 1, submissions: 2 }
    ])
  })

  it('should retain aggregate activity when the optional history request fails', async () => {
    const historyError = new Error('history unavailable')
    let reportedError

    const result = await buildProfileActivityStatsFromRequests(
      (async () => [{
        DESIGN: {
          subTracks: [
            { name: 'Challenge', challenges: 3, wins: 1 }
          ]
        }
      }])(),
      (async () => { throw historyError })(),
      error => { reportedError = error }
    )

    result.should.deep.equal([
      { trackName: 'Design', challenges: 3, wins: 1, submissions: 3 }
    ])
    should.equal(reportedError, historyError)
  })

  it('should render Data Science ratings in the downloaded-profile activity row', () => {
    const template = buildProfileTemplate({
      member: {
        generatedOn: 'August 3, 2026',
        handle: 'rated-member'
      },
      workExperience: [],
      education: [],
      languages: [],
      skills: { principal: { verified: [], notVerified: [] } },
      skillsByCategory: [],
      topcoderActivity: {
        statsByTrack: [
          { trackName: 'Data Science', rating: 1499, wins: 1, submissions: 2, challenges: 2 }
        ]
      },
      certifications: [],
      courses: []
    })

    getTemplateText(template).should.include(
      'Data Science: 1499 rating, 1 win, 2 submissions, 2 challenges'
    )
  })
})

/*
 * Unit tests for downloaded-profile member stats aggregation.
 */

const chai = require('chai')

const { buildProfileActivityStats } = require('../../src/common/profileStats')

const should = chai.should()

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

  it('should create Competitive Programming only from active SRM stats', () => {
    buildProfileActivityStats({
      DATA_SCIENCE: {
        Challenge: { challenges: 5, wins: 2, rank: { rating: 1500 } },
        MARATHON_MATCH: { challenges: 8, wins: 1, rank: { rating: 1800 } },
        SRM: { challenges: 0, wins: 0, rank: { rating: 0 } }
      }
    }, {}).should.deep.equal([])

    const activeStats = buildProfileActivityStats({
      DATA_SCIENCE: {
        Challenge: { challenges: 5, wins: 2, rank: { rating: 1500 } },
        MARATHON_MATCH: { challenges: 8, wins: 1, rank: { rating: 1800 } },
        SRM: { challenges: 170, wins: 1, rank: { rating: 2741 } }
      }
    }, {})

    activeStats.should.deep.equal([
      { trackName: 'Competitive Programming', rating: 2741, wins: 1, competitions: 170 }
    ])
    should.equal(activeStats[0].rating, 2741)
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
})

/*
 * Unit tests for configured rating path bulk rerate helpers.
 */

const chai = require('chai')
const fs = require('fs')
const os = require('os')
const path = require('path')

const rerateRatingPath = require('../../src/scripts/rerateRatingPath')

chai.should()

describe('rerateRatingPath unit tests', () => {
  it('should parse rating path rerate options', () => {
    const options = rerateRatingPath.parseArgs([
      '--rating-name',
      'AI',
      '--concurrency',
      '5',
      '--limit',
      '10',
      '--user-id',
      '123',
      '--user-ids',
      '456,789,123',
      '--dry-run',
      '--processed-user-ids-path',
      '/tmp/processed.json'
    ])

    options.ratingName.should.equal('AI')
    options.concurrency.should.equal(5)
    options.limit.should.equal(10)
    options.userIds.should.deep.equal(['123', '456', '789'])
    options.dryRun.should.equal(true)
    options.processedUserIdsPath.should.equal('/tmp/processed.json')
  })

  it('should discover distinct rating path members in path order', async () => {
    const pathHistory = [
      { challengeId: 'challenge-1', source: 'development', eventDate: new Date('2024-01-01T00:00:00Z') },
      { challengeId: 'challenge-2', source: 'marathon_match', eventDate: new Date('2024-02-01T00:00:00Z') }
    ]
    const participantsByChallengeId = {
      'challenge-1': [{ userId: '1001' }, { userId: '1002' }],
      'challenge-2': [{ memberId: '1003' }, { memberId: '1001' }]
    }

    const result = await rerateRatingPath.discoverRatingPathMembers({}, null, pathHistory, {
      fetchParticipants: async (reviewDbClient, mmDbClient, historyEntry) => ({
        participantRows: participantsByChallengeId[historyEntry.challengeId]
      }),
      resolveParticipantId: (row) => global.BigInt(row.userId || row.memberId)
    })

    result.challengesScanned.should.equal(2)
    result.participantRowsScanned.should.equal(4)
    result.members.map(member => String(member.userId)).should.deep.equal(['1001', '1002', '1003'])
    result.members[0].firstChallengeId.should.equal('challenge-1')
    result.members[2].firstChallengeId.should.equal('challenge-2')
  })

  it('should support dry-run discovery without rerating members', async () => {
    let rerateCalls = 0

    const summary = await rerateRatingPath.run({
      ratingName: 'AI',
      concurrency: 2,
      limit: null,
      userIds: [],
      dryRun: true
    }, {
      config: {
        RATING_PATHS: [
          { name: 'AI', track: 'DATA_SCIENCE', tags: ['AI'] }
        ]
      },
      membersClient: {},
      challengeClient: {},
      reviewDbClient: {},
      mmDbClient: null,
      disconnect: false,
      fetchRatingPathHistory: async () => [
        { challengeId: 'challenge-1', source: 'development', eventDate: new Date('2024-01-01T00:00:00Z') }
      ],
      fetchParticipants: async () => ({
        participantRows: [{ userId: '1001' }, { userId: '1002' }]
      }),
      resolveParticipantId: row => global.BigInt(row.userId),
      rerateMmTrack: async () => {
        rerateCalls += 1
      }
    })

    summary.dryRun.should.equal(true)
    summary.usersDiscovered.should.equal(2)
    summary.usersProcessed.should.equal(0)
    summary.ratingsUpdated.should.equal(0)
    rerateCalls.should.equal(0)
  })

  it('should rerate each discovered member from the start of the configured path', async () => {
    const rerated = []
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'rerate-rating-path-'))
    const processedUserIdsPath = path.join(tempDir, 'processed.json')

    try {
      const summary = await rerateRatingPath.run({
        ratingName: 'AI',
        concurrency: 2,
        limit: null,
        userIds: [],
        dryRun: false,
        processedUserIdsPath
      }, {
        config: {
          RATING_PATHS: [
            { name: 'AI', track: 'DATA_SCIENCE', tags: ['AI'] }
          ]
        },
        membersClient: {},
        challengeClient: {},
        reviewDbClient: {},
        mmDbClient: null,
        disconnect: false,
        fetchRatingPathHistory: async () => [
          { challengeId: 'challenge-1', source: 'development', eventDate: new Date('2024-01-01T00:00:00Z') }
        ],
        fetchParticipants: async () => ({
          participantRows: [{ userId: '1001' }, { userId: '1002' }]
        }),
        resolveParticipantId: row => global.BigInt(row.userId),
        rerateMmTrack: async (membersClient, challengeClient, mmDbClient, reviewDbClient, userId, fromChallengeId, options) => {
          rerated.push({
            userId: String(userId),
            fromChallengeId,
            ratingPathName: options.ratingPath.name
          })
          return {
            challengesProcessed: 1,
            ratingPathChallengesProcessed: 1,
            ratingsUpdated: 1
          }
        }
      })

      summary.usersProcessed.should.equal(2)
      summary.usersFailed.should.equal(0)
      summary.ratingsUpdated.should.equal(2)
      rerated.should.deep.equal([
        { userId: '1001', fromChallengeId: null, ratingPathName: 'AI' },
        { userId: '1002', fromChallengeId: null, ratingPathName: 'AI' }
      ])
      JSON.parse(fs.readFileSync(processedUserIdsPath, 'utf8')).should.deep.equal(['1001', '1002'])
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true })
    }
  })
})

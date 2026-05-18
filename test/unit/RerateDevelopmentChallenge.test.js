/*
 * Unit tests for Development Challenge bulk rerate helpers.
 */

const chai = require('chai')
const fs = require('fs')
const os = require('os')
const path = require('path')

const rerateDevelopmentChallenge = require('../../src/scripts/rerateDevelopmentChallenge')
const { clearChallengeDimensionLookupCache } = require('../../src/common/statsDimensionHelper')

chai.should()

/**
 * Build a minimal challenge Prisma client test double for dimension lookups and
 * challenge history queries.
 * @param {Function} onFindMany handler for challenge.findMany calls
 * @returns {Object} challenge client test double
 */
function createChallengeClient (onFindMany) {
  return {
    $queryRaw: async (strings) => {
      const sql = Array.isArray(strings) ? strings.join('') : String(strings)
      if (sql.includes('"ChallengeTrack"')) {
        return [
          { id: 'track-dev-id', name: 'Development', abbreviation: 'DEV', legacyId: 1 },
          { id: 'track-ds-id', name: 'Data Science', abbreviation: 'DS', legacyId: 2 }
        ]
      }
      if (sql.includes('"ChallengeType"')) {
        return [
          { id: 'type-ch-id', name: 'Challenge', abbreviation: 'CH', legacyId: 3, isTask: false },
          { id: 'type-code-id', name: 'Code', abbreviation: 'CODE', legacyId: 4, isTask: false },
          { id: 'type-mm-id', name: 'Marathon Match', abbreviation: 'MM', legacyId: 5, isTask: false }
        ]
      }
      return []
    },
    challenge: {
      findMany: onFindMany
    }
  }
}

describe('rerateDevelopmentChallenge unit tests', () => {
  beforeEach(() => {
    clearChallengeDimensionLookupCache()
  })

  it('should parse Development Challenge rerate options', () => {
    const options = rerateDevelopmentChallenge.parseArgs([
      '--',
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
      '/tmp/dev-processed.json'
    ])

    options.concurrency.should.equal(5)
    options.limit.should.equal(10)
    options.userIds.should.deep.equal(['123', '456', '789'])
    options.dryRun.should.equal(true)
    options.processedUserIdsPath.should.equal('/tmp/dev-processed.json')
  })

  it('should load completed Development Challenge and CODE history', async () => {
    let capturedWhere
    const challengeClient = createChallengeClient(async (args) => {
      capturedWhere = args.where
      return [
        {
          id: 'dev-challenge',
          legacyId: 2002,
          legacyRecord: { legacySystemId: 22002 },
          status: 'COMPLETED',
          trackId: 'track-dev-id',
          typeId: 'type-ch-id',
          endDate: new Date('2024-02-01T00:00:00.000Z'),
          metadata: []
        },
        {
          id: 'dev-code',
          legacyId: 1001,
          legacyRecord: { legacySystemId: 11001 },
          status: 'COMPLETED',
          trackId: 'track-dev-id',
          typeId: 'type-code-id',
          endDate: new Date('2024-01-01T00:00:00.000Z'),
          metadata: []
        },
        {
          id: 'unrated-dev',
          status: 'COMPLETED',
          trackId: 'track-dev-id',
          typeId: 'type-ch-id',
          endDate: new Date('2024-03-01T00:00:00.000Z'),
          metadata: [{ name: 'unrated', value: 'true' }]
        }
      ]
    })

    const result = await rerateDevelopmentChallenge.fetchDevelopmentChallengeHistory(challengeClient)

    capturedWhere.should.deep.equal({
      trackId: 'track-dev-id',
      typeId: {
        in: ['type-ch-id', 'type-code-id']
      },
      status: 'COMPLETED'
    })
    result.trackId.should.equal('track-dev-id')
    result.typeIds.should.deep.equal(['type-ch-id', 'type-code-id'])
    result.history.map(row => row.challengeId).should.deep.equal(['dev-code', 'dev-challenge'])
    result.history.map(row => row.reviewChallengeIds).should.deep.equal([
      ['dev-code', '1001', '11001'],
      ['dev-challenge', '2002', '22002']
    ])
  })

  it('should discover distinct Development Challenge members in path order', async () => {
    const developmentHistory = [
      { challengeId: 'dev-1', reviewChallengeIds: ['dev-1', '101'], eventDate: new Date('2024-01-01T00:00:00Z') },
      { challengeId: 'dev-2', reviewChallengeIds: ['dev-2', '102'], eventDate: new Date('2024-02-01T00:00:00Z') }
    ]
    const participantsByChallengeId = {
      'dev-1': [{ userId: '1001' }, { userId: '1002' }],
      'dev-2': [{ userId: '1003' }, { userId: '1001' }]
    }
    const discoveredChallengeAliases = []

    const result = await rerateDevelopmentChallenge.discoverDevelopmentChallengeMembers({}, developmentHistory, {
      fetchParticipants: async (reviewDbClient, historyEntry) => {
        discoveredChallengeAliases.push(historyEntry.reviewChallengeIds)
        return participantsByChallengeId[historyEntry.challengeId]
      },
      resolveParticipantId: row => global.BigInt(row.userId)
    })

    result.challengesScanned.should.equal(2)
    result.participantRowsScanned.should.equal(4)
    result.members.map(member => String(member.userId)).should.deep.equal(['1001', '1002', '1003'])
    discoveredChallengeAliases.should.deep.equal([['dev-1', '101'], ['dev-2', '102']])
    result.members[0].firstChallengeId.should.equal('dev-1')
    result.members[2].firstChallengeId.should.equal('dev-2')
  })

  it('should rerate each existing Development Challenge member from the beginning', async () => {
    const rerated = []
    const rankRecalculationCalls = []
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'rerate-development-challenge-'))
    const processedUserIdsPath = path.join(tempDir, 'processed.json')

    try {
      const summary = await rerateDevelopmentChallenge.run({
        concurrency: 2,
        limit: null,
        userIds: [],
        dryRun: false,
        processedUserIdsPath
      }, {
        membersClient: {
          member: {
            findMany: async ({ where }) => where.userId.in
              .filter(userId => String(userId) !== '1003')
              .map(userId => ({ userId }))
          }
        },
        challengeClient: {},
        reviewDbClient: {},
        disconnect: false,
        fetchDevelopmentChallengeHistory: async () => ({
          trackId: 'track-dev-id',
          ratingTrackId: 'track-dev-id',
          ratingTypeId: 'type-ch-id',
          typeIds: ['type-ch-id', 'type-code-id'],
          history: [
            { challengeId: 'dev-1', eventDate: new Date('2024-01-01T00:00:00Z') },
            { challengeId: 'dev-2', eventDate: new Date('2024-02-01T00:00:00Z') }
          ]
        }),
        fetchParticipants: async (reviewDbClient, historyEntry) => (
          historyEntry.challengeId === 'dev-1'
            ? [{ userId: '1001' }, { userId: '1002' }]
            : [{ userId: '1003' }, { userId: '1001' }]
        ),
        resolveParticipantId: row => global.BigInt(row.userId),
        rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, fromChallengeId, options) => {
          rerated.push({
            userId: String(userId),
            fromChallengeId,
            options
          })
          return {
            challengesProcessed: 3,
            ratingsUpdated: 3
          }
        },
        recalculateRatingRanks: async (membersClient, dimensionIds, options) => {
          rankRecalculationCalls.push({ dimensionIds, options })
          return 2
        }
      })

      summary.usersDiscovered.should.equal(3)
      summary.usersProcessable.should.equal(2)
      summary.usersSkippedMissing.should.equal(1)
      summary.usersProcessed.should.equal(2)
      summary.usersFailed.should.equal(0)
      summary.challengesProcessed.should.equal(6)
      summary.ratingsUpdated.should.equal(6)
      summary.rankRowsUpdated.should.equal(2)
      rerated.sort((left, right) => left.userId.localeCompare(right.userId)).should.deep.equal([
        { userId: '1001', fromChallengeId: null, options: { recalculateRanks: false, skipLegacyReviewIds: true } },
        { userId: '1002', fromChallengeId: null, options: { recalculateRanks: false, skipLegacyReviewIds: true } }
      ])
      rankRecalculationCalls.should.deep.equal([
        {
          dimensionIds: {
            trackId: 'track-dev-id',
            typeId: 'type-ch-id'
          },
          options: {
            updatedBy: 'rerate-member-stats'
          }
        }
      ])
      JSON.parse(fs.readFileSync(processedUserIdsPath, 'utf8')).sort().should.deep.equal(['1001', '1002'])
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true })
    }
  })
})

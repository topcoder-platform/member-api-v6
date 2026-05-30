/*
 * Unit tests for native Marathon Match bulk rerate helpers.
 */

const chai = require('chai')
const fs = require('fs')
const os = require('os')
const path = require('path')

const rerateMarathonMatches = require('../../src/scripts/rerateMarathonMatches')
const { clearChallengeDimensionLookupCache } = require('../../src/common/statsDimensionHelper')

const should = chai.should()

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
          { id: 'type-mm-id', name: 'Marathon Match', abbreviation: 'MM', legacyId: 3, isTask: false }
        ]
      }
      return []
    },
    challenge: {
      findMany: onFindMany
    }
  }
}

describe('rerateMarathonMatches unit tests', () => {
  beforeEach(() => {
    clearChallengeDimensionLookupCache()
  })

  it('should parse Marathon Match rerate options', () => {
    const options = rerateMarathonMatches.parseArgs([
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
      '/tmp/mm-processed.json'
    ])

    options.concurrency.should.equal(5)
    options.limit.should.equal(10)
    options.userIds.should.deep.equal(['123', '456', '789'])
    options.dryRun.should.equal(true)
    options.processedUserIdsPath.should.equal('/tmp/mm-processed.json')
  })

  it('should load Marathon Match history by type id without filtering by track', async () => {
    let capturedWhere
    const challengeClient = createChallengeClient(async (args) => {
      capturedWhere = args.where
      return [
        {
          id: 'ds-mm',
          legacyId: 2002,
          legacyRecord: { legacySystemId: 22002 },
          status: 'COMPLETED',
          trackId: 'track-ds-id',
          typeId: 'type-mm-id',
          endDate: new Date('2024-02-01T00:00:00.000Z'),
          metadata: []
        },
        {
          id: 'dev-mm',
          legacyId: 1001,
          legacyRecord: { legacySystemId: 11001 },
          status: 'COMPLETED',
          trackId: 'track-dev-id',
          typeId: 'type-mm-id',
          endDate: new Date('2024-01-01T00:00:00.000Z'),
          metadata: []
        },
        {
          id: 'unrated-mm',
          status: 'COMPLETED',
          trackId: 'track-ds-id',
          typeId: 'type-mm-id',
          endDate: new Date('2024-03-01T00:00:00.000Z'),
          metadata: [{ name: 'unrated', value: 'true' }]
        },
        {
          id: 'is-rated-false-mm',
          status: 'COMPLETED',
          trackId: 'track-ds-id',
          typeId: 'type-mm-id',
          endDate: new Date('2024-04-01T00:00:00.000Z'),
          metadata: [{ name: 'isRated', value: 'false' }]
        }
      ]
    })

    const result = await rerateMarathonMatches.fetchMarathonMatchHistory(challengeClient)

    capturedWhere.should.deep.equal({
      typeId: 'type-mm-id',
      status: 'COMPLETED'
    })
    result.trackId.should.equal('track-ds-id')
    result.typeId.should.equal('type-mm-id')
    result.history.map(row => row.challengeId).should.deep.equal(['dev-mm', 'ds-mm'])
    result.history.map(row => row.reviewChallengeIds).should.deep.equal([
      ['dev-mm', '1001', '11001'],
      ['ds-mm', '2002', '22002']
    ])
  })

  it('should load persisted Marathon Match history challenge ids from memberStatsHistory', async () => {
    const eventDate = new Date('2024-04-01T00:00:00.000Z')
    const challengeClient = createChallengeClient(async () => [])
    const membersClient = {
      $queryRaw: async () => [
        { challengeId: 'persisted-mm-2', eventDate: new Date('2024-05-01T00:00:00.000Z') },
        { challengeId: 'persisted-mm-1', eventDate }
      ]
    }

    const result = await rerateMarathonMatches.fetchPersistedMarathonMatchHistory(membersClient, challengeClient)

    result.trackId.should.equal('track-ds-id')
    result.typeId.should.equal('type-mm-id')
    result.history.map(row => row.challengeId).should.deep.equal(['persisted-mm-1', 'persisted-mm-2'])
    result.history[0].reviewChallengeIds.should.deep.equal(['persisted-mm-1'])
  })

  it('should merge Challenge API and persisted Marathon Match history aliases', () => {
    const result = rerateMarathonMatches.mergeMarathonHistories([
      {
        challengeId: 'canonical-mm',
        reviewChallengeIds: ['canonical-mm', '12345'],
        eventDate: new Date('2024-05-01T00:00:00.000Z')
      }
    ], [
      {
        challengeId: '12345',
        reviewChallengeIds: ['12345'],
        eventDate: new Date('2024-04-01T00:00:00.000Z')
      }
    ])

    result.should.have.length(1)
    result[0].challengeId.should.equal('canonical-mm')
    result[0].reviewChallengeIds.should.deep.equal(['canonical-mm', '12345'])
    result[0].eventDate.toISOString().should.equal('2024-04-01T00:00:00.000Z')
  })

  it('should discover distinct Marathon Match members in path order', async () => {
    const marathonHistory = [
      { challengeId: 'mm-1', reviewChallengeIds: ['mm-1', '101'], eventDate: new Date('2024-01-01T00:00:00Z') },
      { challengeId: 'mm-2', reviewChallengeIds: ['mm-2', '102'], eventDate: new Date('2024-02-01T00:00:00Z') }
    ]
    const participantsByChallengeId = {
      'mm-1': [{ memberId: '1001' }, { memberId: '1002' }],
      'mm-2': [{ memberId: '1003' }, { memberId: '1001' }]
    }
    const discoveredChallengeAliases = []

    const result = await rerateMarathonMatches.discoverMarathonMatchMembers({}, marathonHistory, {
      fetchParticipants: async (reviewDbClient, historyEntry) => {
        discoveredChallengeAliases.push(historyEntry.reviewChallengeIds)
        return {
          participantRows: participantsByChallengeId[historyEntry.challengeId]
        }
      },
      resolveParticipantId: row => global.BigInt(row.memberId)
    })

    result.challengesScanned.should.equal(2)
    result.participantRowsScanned.should.equal(4)
    result.members.map(member => String(member.userId)).should.deep.equal(['1001', '1002', '1003'])
    discoveredChallengeAliases.should.deep.equal([['mm-1', '101'], ['mm-2', '102']])
    result.members[0].firstChallengeId.should.equal('mm-1')
    result.members[2].firstChallengeId.should.equal('mm-2')
  })

  it('should rerate each existing Marathon Match member from the beginning', async () => {
    const rerated = []
    const rankRecalculationCalls = []
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'rerate-marathon-matches-'))
    const processedUserIdsPath = path.join(tempDir, 'processed.json')

    try {
      const summary = await rerateMarathonMatches.run({
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
        fetchMarathonMatchHistory: async () => ({
          trackId: 'track-ds-id',
          typeId: 'type-mm-id',
          history: [
            { challengeId: 'mm-1', eventDate: new Date('2024-01-01T00:00:00Z') },
            { challengeId: 'mm-2', eventDate: new Date('2024-02-01T00:00:00Z') }
          ]
        }),
        fetchParticipants: async (reviewDbClient, historyEntry) => ({
          participantRows: historyEntry.challengeId === 'mm-1'
            ? [{ memberId: '1001' }, { memberId: '1002' }]
            : [{ memberId: '1003' }, { memberId: '1001' }]
        }),
        resolveParticipantId: row => global.BigInt(row.memberId),
        rerateMmTrack: async (membersClient, challengeClient, mmDbClient, reviewDbClient, userId, fromChallengeId, options) => {
          should.equal(mmDbClient, null)
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
        {
          userId: '1001',
          fromChallengeId: null,
          options: {
            recalculateRanks: false
          }
        },
        {
          userId: '1002',
          fromChallengeId: null,
          options: {
            recalculateRanks: false
          }
        }
      ])
      rankRecalculationCalls.should.deep.equal([
        {
          dimensionIds: {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
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

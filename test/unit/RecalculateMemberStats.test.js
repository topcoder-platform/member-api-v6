/*
 * Unit tests for recalculateMemberStats helper behavior.
 */

const chai = require('chai')
const fs = require('fs')
const os = require('os')
const path = require('path')

const recalculateMemberStats = require('../../src/scripts/recalculateMemberStats')
chai.should()

describe('recalculateMemberStats unit tests', () => {
  it('should collapse legacy design subtracks into Challenge and First2Finish', async () => {
    const fakeChallengesClient = {
      $queryRaw (strings) {
        const query = strings.join('')
        if (query.includes('"ChallengeTrack"')) {
          return [
            { id: 'track-dev-id', name: 'Development', abbreviation: 'DEV', legacyId: null },
            { id: 'track-design-id', name: 'Design', abbreviation: 'DES', legacyId: null },
            { id: 'track-ds-id', name: 'Data Science', abbreviation: 'DS', legacyId: null }
          ]
        }

        if (query.includes('"ChallengeType"')) {
          return [
            { id: 'type-ch-id', name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
            { id: 'type-f2f-id', name: 'First2Finish', abbreviation: 'F2F', legacyId: null, isTask: false }
          ]
        }

        throw new Error(`Unexpected query: ${query}`)
      }
    }

    await recalculateMemberStats.initializeLegacyLookupCache(fakeChallengesClient)

    recalculateMemberStats.resolveLegacyDesignTypeId('WEB_DESIGNS', 17).should.equal('type-ch-id')
    recalculateMemberStats.resolveLegacyDesignTypeId('STUDIO_OTHER', 34).should.equal('type-ch-id')
    recalculateMemberStats.resolveLegacyDesignTypeId('DESIGN_FIRST_2_FINISH', 40).should.equal('type-f2f-id')
    recalculateMemberStats.resolveLegacyDesignTypeId(null, 40).should.equal('type-f2f-id')
  })

  it('should parse the concurrency option', () => {
    const options = recalculateMemberStats.parseArgs(['--concurrency', '8', '--skip-history'])

    options.concurrency.should.equal(8)
    options.skipHistory.should.equal(true)
  })

  it('should parse skip-rerate without disabling legacy rating backfill', () => {
    const options = recalculateMemberStats.parseArgs(['--skip-rerate'])

    options.skipRerate.should.equal(true)
    options.skipRatings.should.equal(false)
  })

  it('should parse the processed user IDs path option', () => {
    const options = recalculateMemberStats.parseArgs(['--processed-user-ids-path', '/tmp/processed-users.json'])

    options.processedUserIdsPath.should.equal('/tmp/processed-users.json')
  })

  it('should preserve input order while respecting concurrency limits', async () => {
    let inFlight = 0
    let maxInFlight = 0

    const results = await recalculateMemberStats.mapWithConcurrency([1, 2, 3, 4], 2, async (value) => {
      inFlight += 1
      maxInFlight = Math.max(maxInFlight, inFlight)

      await new Promise((resolve) => setTimeout(resolve, value === 1 ? 20 : 5))

      inFlight -= 1
      return value * 10
    })

    results.should.deep.equal([10, 20, 30, 40])
    maxInFlight.should.equal(2)
  })

  it('should write processed user IDs as a unique JSON array of strings', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'recalculate-member-stats-'))
    const outputPath = path.join(tempDir, 'processed-user-ids.json')
    const writer = recalculateMemberStats.buildProcessedUserIdsWriter(outputPath)

    try {
      await writer.end()
      await writer.appendUserIds([123, global.BigInt(456), '123'])
      await writer.appendUserIds(['789', global.BigInt(456)])

      const writtenUserIds = JSON.parse(fs.readFileSync(outputPath, 'utf8'))
      writtenUserIds.should.deep.equal(['123', '456', '789'])
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true })
    }
  })

  it('should count only first-place ChallengeWinner rows as wins', async () => {
    const capturedQueries = []
    const challengesClient = {
      $queryRawUnsafe: async (sql, ...params) => {
        capturedQueries.push({ sql, params })
        return [{
          userId: '456',
          trackId: 'track-design',
          typeId: 'type-challenge',
          challenges: 3,
          wins: 1,
          mostRecentEventDate: '2024-03-05T00:00:00.000Z',
          mostRecentSubmission: '2024-03-05T10:00:00.000Z'
        }]
      }
    }

    const result = await recalculateMemberStats.aggregateChallengeWinnerStatsForUser(
      challengesClient,
      global.BigInt(456),
      {}
    )

    capturedQueries.should.have.length(1)
    capturedQueries[0].sql.should.include(
      `COUNT(DISTINCT CASE WHEN cw."type" = 'PLACEMENT' AND cw."placement" = 1 THEN c.id END)::int AS "wins"`
    )
    capturedQueries[0].params.should.deep.equal([global.BigInt(456)])
    result.should.have.length(1)
    chai.expect(result[0]).to.include({
      trackId: 'track-design',
      typeId: 'type-challenge',
      challenges: 3,
      wins: 1
    })
    result[0].userId.should.equal(global.BigInt(456))
    result[0].mostRecentEventDate.toISOString().should.equal('2024-03-05T00:00:00.000Z')
    result[0].mostRecentSubmission.toISOString().should.equal('2024-03-05T10:00:00.000Z')
  })

  it('should bulk update and insert history rows during legacy backfill', async () => {
    const fakeChallengesClient = {
      $queryRaw (strings) {
        const query = strings.join('')
        if (query.includes('"ChallengeTrack"')) {
          return [
            { id: 'track-dev-id', name: 'Development', abbreviation: 'DEV', legacyId: null },
            { id: 'track-design-id', name: 'Design', abbreviation: 'DES', legacyId: null },
            { id: 'track-ds-id', name: 'Data Science', abbreviation: 'DS', legacyId: null }
          ]
        }

        if (query.includes('"ChallengeType"')) {
          return [
            { id: 'type-ch-id', name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
            { id: 'type-f2f-id', name: 'First2Finish', abbreviation: 'F2F', legacyId: null, isTask: false }
          ]
        }

        throw new Error(`Unexpected query: ${query}`)
      }
    }

    await recalculateMemberStats.initializeLegacyLookupCache(fakeChallengesClient)

    const transactionCalls = []
    let rawHistoryQueryIndex = 0
    const membersClient = {
      $queryRaw (strings) {
        const query = strings.join('')
        if (query.includes('FROM "members"."memberHistoryStats"')) {
          return [{ id: 1 }]
        }

        throw new Error(`Unexpected query: ${query}`)
      },
      $queryRawUnsafe: async () => {
        rawHistoryQueryIndex += 1

        if (rawHistoryQueryIndex === 1) {
          return [{
            challengeId: 'challenge-dev',
            ratingDate: '2024-01-02T00:00:00.000Z',
            newRating: 1600,
            subTrack: 'Challenge',
            subTrackId: null
          }]
        }

        if (rawHistoryQueryIndex === 2) {
          return [{
            challengeId: 'challenge-ds',
            date: '2024-02-03T00:00:00.000Z',
            rating: 1700,
            subTrack: 'Challenge',
            subTrackId: null
          }]
        }

        throw new Error(`Unexpected raw query index: ${rawHistoryQueryIndex}`)
      },
      memberStatsHistory: {
        findMany: async () => [{
          id: 99,
          trackId: 'track-dev-id',
          typeId: 'type-ch-id',
          challengeId: 'challenge-dev'
        }]
      },
      $executeRawUnsafe: (sql, ...params) => ({ action: 'executeRawUnsafe', sql, params }),
      $transaction: async (queries) => {
        transactionCalls.push(queries)
      }
    }

    const result = await recalculateMemberStats.backfillHistoryFromLegacy(
      membersClient,
      global.BigInt(123),
      { refreshMostRecent: false }
    )

    result.should.deep.equal({
      upserted: 2,
      refreshed: 0
    })
    transactionCalls.should.have.length(1)
    const rawQueries = transactionCalls[0].filter((query) => query.action === 'executeRawUnsafe')
    rawQueries.should.have.length(2)
    rawQueries[0].sql.should.include('UPDATE "members"."memberStatsHistory"')
    rawQueries[1].sql.should.include('INSERT INTO "members"."memberStatsHistory"')
  })

  it('should supplement history rows from completed review and winner challenges', async () => {
    const transactionCalls = []
    const membersClient = {
      memberStatsHistory: {
        findMany: async () => []
      },
      $executeRawUnsafe: (sql, ...params) => ({ action: 'executeRawUnsafe', sql, params }),
      $transaction: async (queries) => {
        transactionCalls.push(queries)
      }
    }

    const challengesClient = {
      challenge: {
        findMany: async () => [{
          id: 'challenge-review',
          trackId: 'track-design-id',
          typeId: 'type-ch-id',
          endDate: '2024-03-02T00:00:00.000Z',
          status: 'COMPLETED'
        }]
      },
      $queryRawUnsafe: async () => [{
        challengeId: 'challenge-winner',
        createdAt: '2024-03-03T00:00:00.000Z',
        canonicalChallengeId: 'challenge-winner',
        trackId: 'track-design-id',
        typeId: 'type-ch-id',
        status: 'COMPLETED',
        endDate: '2024-03-04T00:00:00.000Z'
      }]
    }

    const reviewDbClient = {
      query: async (sql) => {
        if (sql.includes('pg_catalog.pg_class')) {
          return { rows: [{ schemaName: 'reviews' }] }
        }

        return {
          rows: [{
            challengeId: 'challenge-review',
            userId: '123',
            placement: 2,
            createdAt: '2024-03-01T12:00:00.000Z'
          }]
        }
      }
    }

    const result = await recalculateMemberStats.backfillHistoryFromCompletedChallenges(
      membersClient,
      challengesClient,
      reviewDbClient,
      global.BigInt(123),
      { refreshMostRecent: false }
    )

    result.should.deep.equal({
      upserted: 2,
      refreshed: 0
    })
    transactionCalls.should.have.length(1)
    const rawQueries = transactionCalls[0].filter((query) => query.action === 'executeRawUnsafe')
    rawQueries.should.have.length(1)
    rawQueries[0].sql.should.include('INSERT INTO "members"."memberStatsHistory"')
  })

  it('should not delete legacy-backed rows during replacement cleanup', async () => {
    const transactionCalls = []
    const membersClient = {
      memberStats: {
        findMany: async () => [{
          id: 11,
          userId: 123,
          trackId: 'track-ds',
          typeId: 'type-challenge',
          rating: null,
          avgRank: null,
          avgNumSubmissions: null,
          bestRank: null,
          globalRank: null,
          countryRank: null,
          schoolRank: null,
          volatility: null,
          maxRating: null,
          minRating: null,
          topFiveFinishes: null,
          topTenFinishes: null
        }],
        deleteMany: (args) => ({ action: 'deleteMany', args })
      },
      $executeRawUnsafe: (sql, ...params) => ({ action: 'executeRawUnsafe', sql, params }),
      $queryRawUnsafe: async () => [{ id: 11 }],
      $transaction: async (queries) => {
        transactionCalls.push(queries)
      }
    }

    await recalculateMemberStats.writeStatsToDatabase(membersClient, [{
      userId: 123,
      trackId: 'track-ds',
      typeId: 'type-srm',
      challenges: 1,
      wins: 0,
      mostRecentEventDate: null,
      mostRecentSubmission: null,
      rating: null,
      avgRank: null,
      avgNumSubmissions: null,
      bestRank: null,
      globalRank: null,
      countryRank: null,
      schoolRank: null,
      volatility: null,
      maxRating: null,
      minRating: null,
      topFiveFinishes: null,
      topTenFinishes: null,
      isPrivate: false
    }], new Set(['123']))

    transactionCalls.should.have.length(1)
    transactionCalls[0].filter((query) => query.action === 'deleteMany').should.have.length(0)
    const rawQueries = transactionCalls[0].filter((query) => query.action === 'executeRawUnsafe')
    rawQueries.should.have.length(1)
    rawQueries[0].sql.should.include('INSERT INTO "members"."memberStats"')
  })

  it('should delete stale unified-only rows during replacement cleanup', async () => {
    const transactionCalls = []
    const membersClient = {
      memberStats: {
        findMany: async () => [{
          id: 21,
          userId: 456,
          trackId: 'track-design',
          typeId: 'type-old',
          rating: null,
          avgRank: null,
          avgNumSubmissions: null,
          bestRank: null,
          globalRank: null,
          countryRank: null,
          schoolRank: null,
          volatility: null,
          maxRating: null,
          minRating: null,
          topFiveFinishes: null,
          topTenFinishes: null
        }],
        deleteMany: (args) => ({ action: 'deleteMany', args })
      },
      $executeRawUnsafe: (sql, ...params) => ({ action: 'executeRawUnsafe', sql, params }),
      $queryRawUnsafe: async () => [],
      $transaction: async (queries) => {
        transactionCalls.push(queries)
      }
    }

    await recalculateMemberStats.writeStatsToDatabase(membersClient, [{
      userId: 456,
      trackId: 'track-design',
      typeId: 'type-f2f',
      challenges: 4,
      wins: 1,
      mostRecentEventDate: null,
      mostRecentSubmission: null,
      rating: null,
      avgRank: null,
      avgNumSubmissions: null,
      bestRank: null,
      globalRank: null,
      countryRank: null,
      schoolRank: null,
      volatility: null,
      maxRating: null,
      minRating: null,
      topFiveFinishes: null,
      topTenFinishes: null,
      isPrivate: false
    }], new Set(['456']))

    transactionCalls.should.have.length(1)
    const deleteQueries = transactionCalls[0].filter((query) => query.action === 'deleteMany')
    deleteQueries.should.have.length(1)
    deleteQueries[0].args.should.deep.equal({
      where: {
        id: {
          in: [21]
        }
      }
    })
    const rawQueries = transactionCalls[0].filter((query) => query.action === 'executeRawUnsafe')
    rawQueries.should.have.length(1)
    rawQueries[0].sql.should.include('INSERT INTO "members"."memberStats"')
  })

  it('should only supplement legacy aggregates with newer review challenge results', () => {
    const reviewRows = [
      {
        challengeId: 'challenge-old',
        userId: '123',
        placement: 2,
        createdAt: '2024-01-18T10:00:00.000Z'
      },
      {
        challengeId: 'challenge-new',
        userId: '123',
        placement: 1,
        createdAt: '2024-02-18T10:00:00.000Z'
      },
      {
        challengeId: 'challenge-f2f',
        userId: '123',
        placement: 1,
        createdAt: '2024-02-20T10:00:00.000Z'
      }
    ]
    const challengeMetadataById = new Map([
      ['challenge-old', {
        id: 'challenge-old',
        trackId: 'track-dev',
        typeId: 'type-challenge',
        endDate: '2024-01-20T00:00:00.000Z'
      }],
      ['challenge-new', {
        id: 'challenge-new',
        trackId: 'track-dev',
        typeId: 'type-challenge',
        endDate: '2024-02-19T00:00:00.000Z'
      }],
      ['challenge-f2f', {
        id: 'challenge-f2f',
        trackId: 'track-design',
        typeId: 'type-f2f',
        endDate: '2024-02-21T00:00:00.000Z'
      }]
    ])
    const existingRows = [{
      userId: global.BigInt(123),
      trackId: 'track-dev',
      typeId: 'type-challenge',
      challenges: 10,
      wins: 3,
      mostRecentEventDate: new Date('2024-02-01T00:00:00.000Z'),
      mostRecentSubmission: new Date('2024-02-02T00:00:00.000Z')
    }]

    const results = recalculateMemberStats.buildAggregatedStatsFromReviewResults(
      reviewRows,
      challengeMetadataById,
      {},
      existingRows
    )

    results.should.have.length(2)
    const challengeRow = results.find((row) => row.trackId === 'track-dev' && row.typeId === 'type-challenge')
    const f2fRow = results.find((row) => row.trackId === 'track-design' && row.typeId === 'type-f2f')

    chai.expect(challengeRow).to.include({
      trackId: 'track-dev',
      typeId: 'type-challenge',
      challenges: 1,
      wins: 1
    })
    challengeRow.mostRecentEventDate.toISOString().should.equal('2024-02-19T00:00:00.000Z')
    challengeRow.mostRecentSubmission.toISOString().should.equal('2024-02-18T10:00:00.000Z')

    chai.expect(f2fRow).to.include({
      trackId: 'track-design',
      typeId: 'type-f2f',
      challenges: 1,
      wins: 1
    })
  })

  it('should aggregate review challenge results when no legacy baseline exists', () => {
    const reviewRows = [
      {
        challengeId: 'challenge-1',
        userId: '456',
        placement: 2,
        createdAt: '2024-03-01T10:00:00.000Z'
      },
      {
        challengeId: 'challenge-2',
        userId: '456',
        placement: 1,
        createdAt: '2024-03-03T10:00:00.000Z'
      },
      {
        challengeId: 'challenge-3',
        userId: '456',
        placement: 1,
        createdAt: '2024-03-04T10:00:00.000Z'
      }
    ]
    const challengeMetadataById = new Map([
      ['challenge-1', {
        id: 'challenge-1',
        trackId: 'track-dev',
        typeId: 'type-challenge',
        endDate: '2024-03-02T00:00:00.000Z'
      }],
      ['challenge-2', {
        id: 'challenge-2',
        trackId: 'track-dev',
        typeId: 'type-challenge',
        endDate: '2024-03-05T00:00:00.000Z'
      }],
      ['challenge-3', {
        id: 'challenge-3',
        trackId: 'track-design',
        typeId: 'type-f2f',
        endDate: '2024-03-06T00:00:00.000Z'
      }]
    ])

    const results = recalculateMemberStats.buildAggregatedStatsFromReviewResults(
      reviewRows,
      challengeMetadataById,
      {
        trackId: 'track-dev',
        typeId: 'type-challenge'
      }
    )

    results.should.have.length(1)
    chai.expect(results[0]).to.include({
      trackId: 'track-dev',
      typeId: 'type-challenge',
      challenges: 2,
      wins: 1
    })
    results[0].mostRecentEventDate.toISOString().should.equal('2024-03-05T00:00:00.000Z')
    results[0].mostRecentSubmission.toISOString().should.equal('2024-03-03T10:00:00.000Z')
  })
})

/*
 * Unit tests for migrated Marathon Match history dedupe helpers.
 */

const chai = require('chai')

const dedupeMarathonMatchHistory = require('../../src/scripts/dedupeMarathonMatchHistory')

chai.should()

describe('dedupeMarathonMatchHistory unit tests', () => {
  it('should parse dry-run options passed through pnpm', () => {
    const options = dedupeMarathonMatchHistory.parseArgs([
      '--',
      '--dry-run',
      '--user-id',
      '40562752',
      '--limit=25'
    ])

    options.dryRun.should.equal(true)
    options.userId.should.equal('40562752')
    options.limit.should.equal(25)
  })

  it('should return dry-run summary without deleting rows', async () => {
    let connected = false
    let disconnected = false
    const membersClient = {
      $connect: async () => {
        connected = true
      },
      $disconnect: async () => {
        disconnected = true
      },
      $queryRaw: async () => [
        {
          legacyHistoryId: 876081,
          userId: 40562752,
          matchNumber: '145',
          legacyChallengeId: '19628',
          legacyChallengeName: 'MM 145',
          legacyPlacement: 2,
          legacyNewRating: 2925,
          canonicalHistoryId: 876110,
          canonicalChallengeId: '7289e32b-a4d8-4349-9060-192a512368f8',
          canonicalChallengeName: 'Marathon Match 145',
          canonicalPlacement: 1,
          canonicalNewRating: 2591
        }
      ]
    }

    const summary = await dedupeMarathonMatchHistory.run({
      dryRun: true,
      userId: '40562752',
      limit: null
    }, {
      membersClient
    })

    connected.should.equal(true)
    disconnected.should.equal(true)
    summary.should.deep.equal({
      dryRun: true,
      duplicateRows: 1,
      deletedRows: 0,
      usersRefreshed: 0
    })
  })

  it('should delete duplicate legacy rows and refresh affected users', async () => {
    const rawQueries = []
    const membersClient = {
      $transaction: async (callback) => {
        const tx = {
          $executeRaw: async (strings) => {
            rawQueries.push(strings.join(''))
            return rawQueries.length === 1 ? 2 : 1
          }
        }
        return callback(tx)
      }
    }

    const summary = await dedupeMarathonMatchHistory.deleteDuplicateHistoryRows(membersClient, [
      {
        legacyHistoryId: 876081,
        userId: 40562752
      },
      {
        legacyHistoryId: 876083,
        userId: 40562752
      }
    ])

    summary.should.deep.equal({
      deletedRows: 2,
      usersRefreshed: 1
    })
    rawQueries[0].should.include('DELETE FROM "members"."memberStatsHistory"')
    rawQueries[1].should.include('"rankedHistory"')
    rawQueries[1].should.include('"mostRecent"')
  })
})

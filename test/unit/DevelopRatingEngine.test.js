/*
 * Unit tests for the development rating rerate engine.
 */

require('../../app-bootstrap')
const chai = require('chai')

const {
  rerateDevTrack
} = require('../../src/ratings/developRatingEngine')
const {
  DEFAULT_VOLATILITY,
  getRatingColor,
  runQubitsRating
} = require('../../src/ratings/qubitsAlgorithm')

const should = chai.should()
const DEVELOP_TRACK_ID = 'track-develop-id'
const DATA_SCIENCE_TRACK_ID = 'track-data-science-id'
const CHALLENGE_TYPE_ID = 'type-challenge-id'
const MARATHON_MATCH_TYPE_ID = 'type-marathon-match-id'

function isBigIntValue (value) {
  return Object.prototype.toString.call(value) === '[object BigInt]'
}

function toBigInt (value) {
  return isBigIntValue(value) ? value : global.BigInt(String(value))
}

function cloneRow (row) {
  const clone = {}

  Object.keys(row).forEach((key) => {
    const value = row[key]
    clone[key] = value instanceof Date ? new Date(value.getTime()) : value
  })

  return clone
}

function selectRow (row, select) {
  if (!select) {
    return cloneRow(row)
  }

  const selected = {}
  Object.keys(select).forEach((field) => {
    if (select[field]) {
      selected[field] = row[field]
    }
  })

  return selected
}

function compareValues (left, right) {
  if (left instanceof Date || right instanceof Date) {
    return new Date(left).getTime() - new Date(right).getTime()
  }

  if (isBigIntValue(left) || isBigIntValue(right)) {
    const normalizedLeft = isBigIntValue(left) ? left : toBigInt(left)
    const normalizedRight = isBigIntValue(right) ? right : toBigInt(right)

    if (normalizedLeft === normalizedRight) {
      return 0
    }

    return normalizedLeft > normalizedRight ? 1 : -1
  }

  if (typeof left === 'string' && typeof right === 'string') {
    return left.localeCompare(right)
  }

  if (left === right) {
    return 0
  }

  return left > right ? 1 : -1
}

function matchesFilter (value, filter) {
  if (filter && typeof filter === 'object' && !Array.isArray(filter)) {
    if (Array.isArray(filter.in)) {
      return filter.in.some((candidate) => matchesFilter(value, candidate))
    }

    return false
  }

  return compareValues(value, filter) === 0
}

function matchesWhere (row, where) {
  if (!where) {
    return true
  }

  return Object.keys(where).every((field) => matchesFilter(row[field], where[field]))
}

function sortRows (rows, orderBy) {
  if (!orderBy || orderBy.length === 0) {
    return rows.slice()
  }

  return rows.slice().sort((left, right) => {
    for (const clause of orderBy) {
      const field = Object.keys(clause)[0]
      const direction = clause[field]
      const comparison = compareValues(left[field], right[field])

      if (comparison !== 0) {
        return direction === 'desc' ? -comparison : comparison
      }
    }

    return 0
  })
}

function createMembersClient (seed) {
  const state = {
    historyRows: seed.historyRows.map(cloneRow),
    statsRows: seed.statsRows.map(cloneRow),
    maxRatingRows: seed.maxRatingRows.map(cloneRow)
  }

  let nextHistoryId = state.historyRows.reduce((maxId, row) => {
    const numericId = Number(row.id)
    return Number.isFinite(numericId) && numericId > maxId ? numericId : maxId
  }, 0) + 1

  const memberStatsHistory = {
    async findMany (args = {}) {
      return sortRows(
        state.historyRows.filter((row) => matchesWhere(row, args.where)),
        args.orderBy
      ).map((row) => selectRow(row, args.select))
    },
    async findFirst (args = {}) {
      const rows = sortRows(
        state.historyRows.filter((row) => matchesWhere(row, args.where)),
        args.orderBy
      )
      const match = rows[args.skip || 0]
      return match ? selectRow(match, args.select) : null
    },
    async updateMany (args = {}) {
      let count = 0

      state.historyRows.forEach((row) => {
        if (!matchesWhere(row, args.where)) {
          return
        }

        Object.assign(row, cloneRow(args.data))
        count += 1
      })

      return { count }
    },
    async update (args = {}) {
      const row = state.historyRows.find((item) => matchesWhere(item, args.where))
      if (!row) {
        throw new Error('History row not found for update')
      }

      Object.assign(row, cloneRow(args.data))
      return cloneRow(row)
    },
    async create (args = {}) {
      const newRow = {
        id: toBigInt(nextHistoryId),
        ...cloneRow(args.data)
      }

      nextHistoryId += 1
      state.historyRows.push(newRow)
      return cloneRow(newRow)
    }
  }

  const memberStats = {
    async findFirst (args = {}) {
      const row = state.statsRows.find((item) => matchesWhere(item, args.where))
      return row ? selectRow(row, args.select) : null
    },
    async upsert (args = {}) {
      const where = args.where && args.where.userId_trackId_typeId
      const row = state.statsRows.find((item) => matchesWhere(item, where))

      if (row) {
        Object.assign(row, cloneRow(args.update))
        return cloneRow(row)
      }

      const createdRow = cloneRow(args.create)
      state.statsRows.push(createdRow)
      return cloneRow(createdRow)
    }
  }

  const memberMaxRating = {
    async findFirst (args = {}) {
      const row = state.maxRatingRows.find((item) => matchesWhere(item, args.where))
      return row ? selectRow(row, args.select) : null
    },
    async upsert (args = {}) {
      const row = state.maxRatingRows.find((item) => matchesWhere(item, args.where))

      if (row) {
        Object.assign(row, cloneRow(args.update))
        return cloneRow(row)
      }

      const createdRow = cloneRow(args.create)
      state.maxRatingRows.push(createdRow)
      return cloneRow(createdRow)
    }
  }

  const client = {
    memberStatsHistory,
    memberStats,
    memberMaxRating,
    async $transaction (transactionWork) {
      return transactionWork({
        memberStatsHistory,
        memberStats,
        memberMaxRating
      })
    }
  }

  return { client, state }
}

function createReviewDbClient (rows) {
  const resultRows = rows.map(cloneRow)

  return {
    async query (sql, params) {
      if (sql.includes('pg_catalog.pg_class')) {
        return {
          rows: [{ schemaName: 'public' }]
        }
      }

      if (sql.includes('WHERE "userId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.userId) === String(params[0]))
            .sort((left, right) => compareValues(left.createdAt, right.createdAt))
        }
      }

      if (sql.includes('WHERE "challengeId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.challengeId) === String(params[0]) && row.rated === true)
            .sort((left, right) => {
              const placementComparison = compareValues(left.placement, right.placement)
              if (placementComparison !== 0) {
                return placementComparison
              }

              const scoreComparison = compareValues(right.finalScore, left.finalScore)
              if (scoreComparison !== 0) {
                return scoreComparison
              }

              return compareValues(left.createdAt, right.createdAt)
            })
        }
      }

      throw new Error(`Unexpected review query: ${sql}`)
    }
  }
}

function createChallengeClient (metadataById) {
  return {
    async $queryRaw (strings) {
      const sql = Array.isArray(strings) ? strings.join('') : String(strings)

      if (sql.includes('FROM "ChallengeTrack"')) {
        return [
          { id: DEVELOP_TRACK_ID, name: 'Development', abbreviation: 'DEV', legacyId: null },
          { id: DATA_SCIENCE_TRACK_ID, name: 'Data Science', abbreviation: 'DS', legacyId: null }
        ]
      }

      if (sql.includes('FROM "ChallengeType"')) {
        return [
          { id: CHALLENGE_TYPE_ID, name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
          { id: MARATHON_MATCH_TYPE_ID, name: 'Marathon Match', abbreviation: 'MM', legacyId: null, isTask: false }
        ]
      }

      throw new Error(`Unexpected challenge lookup query: ${sql}`)
    },
    challenge: {
      async findMany (args) {
        return args.where.id.in
          .map((challengeId) => metadataById[String(challengeId)])
          .filter(Boolean)
          .map(cloneRow)
      }
    }
  }
}

function createParticipant (coderId, rating, volatility, numRatings, score) {
  return {
    coderId: String(coderId),
    rating,
    volatility,
    numRatings,
    score
  }
}

function findHistoryRow (historyRows, userId, challengeId) {
  return historyRows.find((row) => String(row.userId) === String(userId) && String(row.challengeId) === String(challengeId))
}

describe('develop rating engine unit tests', () => {
  it('rerateDevTrack should seed rerates from prior history instead of current snapshots', async () => {
    const targetUserId = toBigInt(1001)
    const opponentUserId = toBigInt(2002)
    const challengeOneId = 'challenge-1'
    const challengeTwoId = 'challenge-2'
    const opponentSeedChallengeId = 'opponent-seed'

    const buildMembersSeed = (opponentSnapshotRating, opponentSnapshotVolatility) => ({
      historyRows: [
        {
          id: toBigInt(1),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeOneId,
          mostRecent: false,
          oldRating: null,
          newRating: 1500,
          eventDate: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          id: toBigInt(2),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeTwoId,
          mostRecent: true,
          oldRating: 1500,
          newRating: 2100,
          eventDate: new Date('2024-02-01T00:00:00.000Z')
        },
        {
          id: toBigInt(3),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: opponentSeedChallengeId,
          mostRecent: false,
          oldRating: null,
          newRating: 1200,
          eventDate: new Date('2023-12-01T00:00:00.000Z')
        },
        {
          id: toBigInt(4),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeTwoId,
          mostRecent: true,
          oldRating: 1200,
          newRating: 1300,
          eventDate: new Date('2024-02-01T00:00:00.000Z')
        }
      ],
      statsRows: [
        {
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          rating: 2100,
          volatility: 260
        },
        {
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          rating: opponentSnapshotRating,
          volatility: opponentSnapshotVolatility
        }
      ],
      maxRatingRows: [
        {
          userId: targetUserId,
          rating: 2100,
          track: 'DEVELOP',
          subTrack: 'Challenge',
          ratingColor: getRatingColor(2100)
        }
      ]
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: challengeOneId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-01-01T09:00:00.000Z')
      },
      {
        challengeId: challengeTwoId,
        userId: targetUserId,
        finalScore: 50,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-02-01T10:00:00.000Z')
      },
      {
        challengeId: challengeTwoId,
        userId: opponentUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-02-01T09:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeOneId]: {
        id: challengeOneId,
        endDate: new Date('2024-01-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      },
      [challengeTwoId]: {
        id: challengeTwoId,
        endDate: new Date('2024-02-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      }
    })

    const highSnapshot = createMembersClient(buildMembersSeed(2500, 120))
    const lowSnapshot = createMembersClient(buildMembersSeed(900, 900))

    const highSnapshotResult = await rerateDevTrack(
      highSnapshot.client,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeTwoId
    )

    const lowSnapshotResult = await rerateDevTrack(
      lowSnapshot.client,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeTwoId
    )

    should.equal(highSnapshotResult.challengesProcessed, 1)
    should.equal(highSnapshotResult.ratingsUpdated, 1)
    should.equal(lowSnapshotResult.challengesProcessed, 1)
    should.equal(lowSnapshotResult.ratingsUpdated, 1)

    const highSnapshotHistoryRow = findHistoryRow(highSnapshot.state.historyRows, targetUserId, challengeTwoId)
    const lowSnapshotHistoryRow = findHistoryRow(lowSnapshot.state.historyRows, targetUserId, challengeTwoId)

    should.equal(highSnapshotHistoryRow.oldRating, 1500)
    should.equal(lowSnapshotHistoryRow.oldRating, 1500)
    should.equal(highSnapshotHistoryRow.newRating, lowSnapshotHistoryRow.newRating)
    should.equal(
      highSnapshot.state.statsRows.find((row) => String(row.userId) === String(targetUserId)).rating,
      lowSnapshot.state.statsRows.find((row) => String(row.userId) === String(targetUserId)).rating
    )
  })

  it('rerateDevTrack should preserve a higher Marathon Match memberMaxRating', async () => {
    const targetUserId = toBigInt(3003)
    const opponentUserId = toBigInt(4004)
    const challengeOneId = 'peak-1'
    const challengeTwoId = 'peak-2'
    const challengeThreeId = 'peak-3'
    const opponentSeedChallengeId = 'peak-seed'
    const marathonPeakChallengeId = 'mm-peak'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(11),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeOneId,
          mostRecent: false,
          oldRating: null,
          newRating: 1800,
          eventDate: new Date('2024-03-01T00:00:00.000Z')
        },
        {
          id: toBigInt(12),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeTwoId,
          mostRecent: false,
          oldRating: 1800,
          newRating: 2200,
          eventDate: new Date('2024-04-01T00:00:00.000Z')
        },
        {
          id: toBigInt(13),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeThreeId,
          mostRecent: true,
          oldRating: 2200,
          newRating: 2100,
          eventDate: new Date('2024-05-01T00:00:00.000Z')
        },
        {
          id: toBigInt(14),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: opponentSeedChallengeId,
          mostRecent: false,
          oldRating: null,
          newRating: 1200,
          eventDate: new Date('2024-02-01T00:00:00.000Z')
        },
        {
          id: toBigInt(15),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeTwoId,
          mostRecent: false,
          oldRating: 1200,
          newRating: 1300,
          eventDate: new Date('2024-04-01T00:00:00.000Z')
        },
        {
          id: toBigInt(16),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeThreeId,
          mostRecent: true,
          oldRating: 1300,
          newRating: 1400,
          eventDate: new Date('2024-05-01T00:00:00.000Z')
        },
        {
          id: toBigInt(17),
          userId: targetUserId,
          trackId: DATA_SCIENCE_TRACK_ID,
          typeId: MARATHON_MATCH_TYPE_ID,
          challengeId: marathonPeakChallengeId,
          mostRecent: true,
          oldRating: 2100,
          newRating: 2200,
          eventDate: new Date('2024-02-15T00:00:00.000Z')
        }
      ],
      statsRows: [
        {
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          rating: 2100,
          volatility: 210
        }
      ],
      maxRatingRows: [
        {
          userId: targetUserId,
          rating: 2200,
          track: 'DATA_SCIENCE',
          subTrack: 'MARATHON_MATCH',
          ratingColor: getRatingColor(2200)
        }
      ]
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: challengeOneId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-03-01T09:00:00.000Z')
      },
      {
        challengeId: challengeTwoId,
        userId: targetUserId,
        finalScore: 40,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-04-01T10:00:00.000Z')
      },
      {
        challengeId: challengeTwoId,
        userId: opponentUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-04-01T09:00:00.000Z')
      },
      {
        challengeId: challengeThreeId,
        userId: targetUserId,
        finalScore: 30,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-05-01T10:00:00.000Z')
      },
      {
        challengeId: challengeThreeId,
        userId: opponentUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-05-01T09:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeOneId]: {
        id: challengeOneId,
        endDate: new Date('2024-03-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      },
      [challengeTwoId]: {
        id: challengeTwoId,
        endDate: new Date('2024-04-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      },
      [challengeThreeId]: {
        id: challengeThreeId,
        endDate: new Date('2024-05-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      }
    })

    const challengeTwoParticipants = [
      createParticipant(targetUserId, 1800, DEFAULT_VOLATILITY, 1, 40),
      createParticipant(opponentUserId, 1200, DEFAULT_VOLATILITY, 1, 100)
    ]
    runQubitsRating(challengeTwoParticipants)
    const reratedTargetAfterChallengeTwo = challengeTwoParticipants.find((participant) => participant.coderId === String(targetUserId))

    const challengeThreeParticipants = [
      createParticipant(
        targetUserId,
        reratedTargetAfterChallengeTwo.rating,
        reratedTargetAfterChallengeTwo.volatility,
        reratedTargetAfterChallengeTwo.numRatings,
        30
      ),
      createParticipant(opponentUserId, 1300, DEFAULT_VOLATILITY, 2, 100)
    ]
    runQubitsRating(challengeThreeParticipants)
    const reratedTargetAfterChallengeThree = challengeThreeParticipants.find((participant) => participant.coderId === String(targetUserId))
    const expectedPeakRating = Math.max(1800, reratedTargetAfterChallengeTwo.rating, reratedTargetAfterChallengeThree.rating)

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeTwoId
    )

    should.equal(result.challengesProcessed, 2)
    should.equal(result.ratingsUpdated, 2)

    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))
    should.equal(expectedPeakRating < 2200, true)
    should.equal(maxRatingRow.rating, 2200)
    should.equal(maxRatingRow.track, 'DATA_SCIENCE')
    should.equal(maxRatingRow.subTrack, 'MARATHON_MATCH')
    should.equal(maxRatingRow.ratingColor, getRatingColor(2200))
  })
})

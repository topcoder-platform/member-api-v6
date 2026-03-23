/*
 * Unit tests for the Marathon Match rating rerate engine.
 */

require('../../app-bootstrap')
const chai = require('chai')

const {
  rerateMmTrack
} = require('../../src/ratings/mmRatingEngine')
const {
  getRatingColor,
  runQubitsRating
} = require('../../src/ratings/qubitsAlgorithm')

const should = chai.should()

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

function createMmReviewDbClient (rows) {
  const resultRows = rows.map(cloneRow)

  return {
    async query (sql, params) {
      if (sql.includes('WHERE s."memberId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.memberId) === String(params[0]))
            .sort((left, right) => {
              const reviewedDateComparison = compareValues(left.reviewedDate, right.reviewedDate)
              if (reviewedDateComparison !== 0) {
                return reviewedDateComparison
              }

              const createdAtComparison = compareValues(left.createdAt, right.createdAt)
              if (createdAtComparison !== 0) {
                return createdAtComparison
              }

              return compareValues(left.submissionId, right.submissionId)
            })
        }
      }

      if (sql.includes('WHERE s."challengeId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.challengeId) === String(params[0]))
            .sort((left, right) => {
              const createdComparison = compareValues(left.submissionCreatedAt, right.submissionCreatedAt)
              if (createdComparison !== 0) {
                return createdComparison
              }

              return compareValues(left.submissionId, right.submissionId)
            })
        }
      }

      throw new Error(`Unexpected review query: ${sql}`)
    }
  }
}

function createChallengeClient (metadataById) {
  return {
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

function createMmDbClient (configByChallengeId) {
  return {
    marathonMatchConfig: {
      async findUnique (args = {}) {
        const config = configByChallengeId[String(args.where.challengeId)]
        if (!config) {
          return null
        }

        return selectRow(config, args.select)
      }
    }
  }
}

function findHistoryRow (historyRows, userId, challengeId) {
  return historyRows.find((row) => String(row.userId) === String(userId) && String(row.challengeId) === String(challengeId))
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

function normalizeExpectedMmScore (score, scoringConfig) {
  if (
    scoringConfig.relativeScoringEnabled === false &&
    scoringConfig.scoreDirection === 'MINIMIZE'
  ) {
    return -score
  }

  return score
}

function buildExpectedTargetState (targetUserId, opponentUserId, targetScore, opponentScore, scoringConfig) {
  const participants = [
    createParticipant(
      targetUserId,
      0,
      0,
      0,
      normalizeExpectedMmScore(targetScore, scoringConfig)
    ),
    createParticipant(
      opponentUserId,
      0,
      0,
      0,
      normalizeExpectedMmScore(opponentScore, scoringConfig)
    )
  ]

  runQubitsRating(participants)
  return participants.find((participant) => participant.coderId === String(targetUserId))
}

describe('marathon match rating engine unit tests', () => {
  const targetUserId = toBigInt(5005)
  const opponentUserId = toBigInt(6006)
  const challengeId = 'mm-challenge-1'
  const challengeMetadata = {
    [challengeId]: {
      id: challengeId,
      endDate: new Date('2024-06-01T00:00:00.000Z'),
      track: { name: 'DATA_SCIENCE' },
      type: { name: 'MARATHON_MATCH' },
      metadata: []
    }
  }

  const baseReviewRows = [
    {
      submissionId: 'submission-target',
      memberId: targetUserId,
      challengeId,
      aggregateScore: 20,
      reviewedDate: new Date('2024-06-01T10:00:00.000Z'),
      createdAt: new Date('2024-06-01T10:00:00.000Z'),
      submissionCreatedAt: new Date('2024-06-01T09:00:00.000Z')
    },
    {
      submissionId: 'submission-opponent',
      memberId: opponentUserId,
      challengeId,
      aggregateScore: 10,
      reviewedDate: new Date('2024-06-01T10:05:00.000Z'),
      createdAt: new Date('2024-06-01T10:05:00.000Z'),
      submissionCreatedAt: new Date('2024-06-01T09:05:00.000Z')
    }
  ]

  ;[
    {
      name: 'should keep MAXIMIZE scores when relative scoring is enabled',
      scoringConfig: {
        relativeScoringEnabled: true,
        scoreDirection: 'MAXIMIZE'
      }
    },
    {
      name: 'should keep MINIMIZE relative scores in higher-is-better order',
      scoringConfig: {
        relativeScoringEnabled: true,
        scoreDirection: 'MINIMIZE'
      }
    },
    {
      name: 'should keep MAXIMIZE scores when relative scoring is disabled',
      scoringConfig: {
        relativeScoringEnabled: false,
        scoreDirection: 'MAXIMIZE'
      }
    },
    {
      name: 'should invert raw MINIMIZE scores when relative scoring is disabled',
      scoringConfig: {
        relativeScoringEnabled: false,
        scoreDirection: 'MINIMIZE'
      }
    }
  ].forEach(({ name, scoringConfig }) => {
    it(`rerateMmTrack ${name}`, async () => {
      const { client: membersClient, state } = createMembersClient({
        historyRows: [],
        statsRows: [],
        maxRatingRows: []
      })

      const reviewDbClient = createMmReviewDbClient(baseReviewRows)
      const challengeClient = createChallengeClient(challengeMetadata)
      const mmDbClient = createMmDbClient({
        [challengeId]: scoringConfig
      })

      const expectedTargetState = buildExpectedTargetState(
        targetUserId,
        opponentUserId,
        20,
        10,
        scoringConfig
      )

      const result = await rerateMmTrack(
        membersClient,
        challengeClient,
        mmDbClient,
        reviewDbClient,
        targetUserId,
        challengeId
      )

      should.equal(result.challengesProcessed, 1)
      should.equal(result.ratingsUpdated, 1)

      const statsRow = state.statsRows.find((row) =>
        String(row.userId) === String(targetUserId) &&
        row.trackId === 'DATA_SCIENCE' &&
        row.typeId === 'MARATHON_MATCH'
      )
      const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

      should.equal(statsRow.rating, expectedTargetState.rating)
      should.equal(statsRow.volatility, expectedTargetState.volatility)
      should.equal(historyRow.oldRating, null)
      should.equal(historyRow.newRating, expectedTargetState.rating)
    })
  })

  it('rerateMmTrack should preserve a higher Develop memberMaxRating', async () => {
    const developPeakChallengeId = 'develop-peak'
    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(1),
          userId: targetUserId,
          trackId: 'DEVELOP',
          typeId: 'Challenge',
          challengeId: developPeakChallengeId,
          mostRecent: true,
          oldRating: 2300,
          newRating: 2500,
          eventDate: new Date('2024-05-01T00:00:00.000Z')
        }
      ],
      statsRows: [
        {
          userId: targetUserId,
          trackId: 'DEVELOP',
          typeId: 'Challenge',
          rating: 2500,
          volatility: 220
        }
      ],
      maxRatingRows: [
        {
          userId: targetUserId,
          rating: 2500,
          track: 'DEVELOP',
          subTrack: 'Challenge',
          ratingColor: getRatingColor(2500)
        }
      ]
    })

    const reviewDbClient = createMmReviewDbClient(baseReviewRows)
    const challengeClient = createChallengeClient(challengeMetadata)
    const mmDbClient = createMmDbClient({
      [challengeId]: {
        relativeScoringEnabled: true,
        scoreDirection: 'MAXIMIZE'
      }
    })

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      mmDbClient,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))
    should.equal(maxRatingRow.rating, 2500)
    should.equal(maxRatingRow.track, 'DEVELOP')
    should.equal(maxRatingRow.subTrack, 'Challenge')
    should.equal(maxRatingRow.ratingColor, getRatingColor(2500))
  })
})

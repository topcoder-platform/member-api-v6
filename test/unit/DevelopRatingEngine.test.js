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
const QA_TRACK_ID = 'track-qa-id'
const CHALLENGE_TYPE_ID = 'type-challenge-id'
const CODE_TYPE_ID = 'type-code-id'
const BUG_HUNT_TYPE_ID = 'type-bug-hunt-id'
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

    if (Array.isArray(filter.notIn)) {
      return !filter.notIn.some((candidate) => matchesFilter(value, candidate))
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
    maxRatingRows: seed.maxRatingRows.map(cloneRow),
    rankRecalculationCalls: []
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
    },
    async deleteMany (args = {}) {
      let count = 0

      for (let index = state.historyRows.length - 1; index >= 0; index -= 1) {
        if (!matchesWhere(state.historyRows[index], args.where)) {
          continue
        }

        state.historyRows.splice(index, 1)
        count += 1
      }

      return { count }
    }
  }

  const memberStats = {
    async findMany (args = {}) {
      return state.statsRows
        .filter((item) => matchesWhere(item, args.where))
        .map((row) => selectRow(row, args.select))
    },
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

  async function executeRawUnsafe (sql, trackId, typeId, isPrivate) {
    state.rankRecalculationCalls.push({ sql, trackId, typeId, isPrivate })
    const scopedRows = state.statsRows
      .filter((row) =>
        row.trackId === trackId &&
        row.typeId === typeId &&
        (row.isPrivate === true) === isPrivate
      )
      .sort((left, right) => Number(right.rating) - Number(left.rating))

    let previousRating
    let previousRank = 0
    scopedRows.forEach((row, index) => {
      const rating = Number(row.rating)
      const rank = rating === previousRating ? previousRank : index + 1
      row.globalRank = Number.isFinite(rating) ? rank : null
      row.countryRank = null
      previousRating = rating
      previousRank = rank
    })
    return scopedRows.length
  }

  const client = {
    memberStatsHistory,
    memberStats,
    memberMaxRating,
    $executeRawUnsafe: executeRawUnsafe,
    async $transaction (transactionWork) {
      return transactionWork({
        memberStatsHistory,
        memberStats,
        memberMaxRating,
        $executeRawUnsafe: executeRawUnsafe
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

      if (sql.includes('WHERE "challengeId" IN')) {
        const challengeIds = new Set(params.map((challengeId) => String(challengeId)))
        return {
          rows: resultRows
            .filter((row) => challengeIds.has(String(row.challengeId)))
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

      if (sql.includes('WHERE "challengeId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.challengeId) === String(params[0]))
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

function createChallengeClient (metadataById, winnerRows = []) {
  const challengeWinnerRows = winnerRows.map(cloneRow)

  return {
    async $queryRaw (strings) {
      const sql = Array.isArray(strings) ? strings.join('') : String(strings)

      if (sql.includes('FROM "ChallengeTrack"')) {
        return [
          { id: DEVELOP_TRACK_ID, name: 'Development', abbreviation: 'DEV', legacyId: null },
          { id: DATA_SCIENCE_TRACK_ID, name: 'Data Science', abbreviation: 'DS', legacyId: null },
          { id: QA_TRACK_ID, name: 'Quality Assurance', abbreviation: 'QA', legacyId: null }
        ]
      }

      if (sql.includes('FROM "ChallengeType"')) {
        return [
          { id: CHALLENGE_TYPE_ID, name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
          { id: CODE_TYPE_ID, name: 'Code', abbreviation: 'CODE', legacyId: null, isTask: false },
          { id: BUG_HUNT_TYPE_ID, name: 'BUG_HUNT', abbreviation: 'LBGH', legacyId: 120, isTask: false },
          { id: MARATHON_MATCH_TYPE_ID, name: 'Marathon Match', abbreviation: 'MM', legacyId: null, isTask: false }
        ]
      }

      throw new Error(`Unexpected challenge lookup query: ${sql}`)
    },
    challenge: {
      async findMany (args) {
        const idCandidates = new Set()
        const legacyIdCandidates = new Set()
        const whereClauses = args.where && args.where.OR ? args.where.OR : [args.where]

        whereClauses.forEach((where) => {
          if (where && where.id && Array.isArray(where.id.in)) {
            where.id.in.forEach((challengeId) => idCandidates.add(String(challengeId)))
          }
          if (where && where.legacyId && Array.isArray(where.legacyId.in)) {
            where.legacyId.in.forEach((challengeId) => legacyIdCandidates.add(String(challengeId)))
          }
          if (where && where.legacyRecord && where.legacyRecord.is && where.legacyRecord.is.legacySystemId && Array.isArray(where.legacyRecord.is.legacySystemId.in)) {
            where.legacyRecord.is.legacySystemId.in.forEach((challengeId) => legacyIdCandidates.add(String(challengeId)))
          }
        })

        return Object.keys(metadataById)
          .map((challengeId) => metadataById[challengeId])
          .filter((challenge) => (
            idCandidates.has(String(challenge.id)) ||
            legacyIdCandidates.has(String(challenge.legacyId)) ||
            (challenge.legacyRecord && legacyIdCandidates.has(String(challenge.legacyRecord.legacySystemId)))
          ))
          .filter(Boolean)
          .map(cloneRow)
      }
    },
    ChallengeWinner: {
      async findMany (args = {}) {
        return challengeWinnerRows
          .filter((row) => matchesWhere(row, args.where))
          .map((row) => selectRow(row, args.select))
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
  it('rerateDevTrack should rate ChallengeWinner placements when challengeResult is missing', async () => {
    const targetUserId = toBigInt(9011)
    const opponentUserId = toBigInt(9022)
    const challengeId = 'winner-only-dev'
    const winnerCreatedAt = new Date('2026-01-15T11:00:00.000Z')

    const members = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createReviewDbClient([])
    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        status: 'COMPLETED',
        endDate: new Date('2026-01-15T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' },
        metadata: []
      }
    }, [
      {
        challengeId,
        userId: Number(targetUserId),
        type: 'PLACEMENT',
        placement: 1,
        createdAt: winnerCreatedAt
      },
      {
        challengeId,
        userId: Number(opponentUserId),
        type: 'PLACEMENT',
        placement: 2,
        createdAt: winnerCreatedAt
      }
    ])

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, -1),
      createParticipant(opponentUserId, 0, 0, 0, -2)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      members.client,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    result.challengesProcessed.should.equal(1)
    result.ratingsUpdated.should.equal(1)

    const statsRow = members.state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(members.state.historyRows, targetUserId, challengeId)
    const maxRatingRow = members.state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))

    should.exist(statsRow)
    should.exist(historyRow)
    should.exist(maxRatingRow)
    statsRow.rating.should.equal(expectedTarget.rating)
    statsRow.volatility.should.equal(expectedTarget.volatility)
    statsRow.challenges.should.equal(1)
    should.equal(historyRow.oldRating, null)
    historyRow.newRating.should.equal(expectedTarget.rating)
    maxRatingRow.rating.should.equal(expectedTarget.rating)
    maxRatingRow.track.should.equal('DEVELOP')
    maxRatingRow.subTrack.should.equal('Challenge')
  })

  it('rerateDevTrack should support Data Science Challenge rating dimensions', async () => {
    const targetUserId = toBigInt(1001)
    const opponentUserId = toBigInt(2002)
    const challengeId = 'ds-challenge-1'
    const eventDate = new Date('2026-06-02T05:30:04.536Z')
    const members = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createReviewDbClient([
      {
        challengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2026-06-02T04:49:42.752Z')
      },
      {
        challengeId,
        userId: opponentUserId,
        finalScore: 88.89,
        placement: 2,
        rated: true,
        createdAt: new Date('2026-06-02T04:46:08.538Z')
      }
    ])
    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: eventDate,
        track: { name: 'Data Science' },
        type: { name: 'Challenge' }
      }
    })
    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 88.89)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      members.client,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId,
      {
        targetTrackName: 'DATA_SCIENCE',
        targetTypeName: 'Challenge',
        challengeTrackNames: ['DATA_SCIENCE'],
        challengeTypeNames: ['Challenge']
      }
    )

    result.challengesProcessed.should.equal(1)
    result.ratingsUpdated.should.equal(1)

    const statsRow = members.state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    should.exist(statsRow)
    statsRow.rating.should.equal(expectedTarget.rating)
    statsRow.volatility.should.equal(expectedTarget.volatility)
    statsRow.challenges.should.equal(1)
    statsRow.mostRecentEventDate.should.deep.equal(eventDate)

    const historyRow = findHistoryRow(members.state.historyRows, targetUserId, challengeId)
    should.exist(historyRow)
    historyRow.trackId.should.equal(DATA_SCIENCE_TRACK_ID)
    historyRow.typeId.should.equal(CHALLENGE_TYPE_ID)
    historyRow.newRating.should.equal(expectedTarget.rating)
    historyRow.mostRecent.should.equal(true)

    const maxRatingRow = members.state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))
    should.exist(maxRatingRow)
    maxRatingRow.rating.should.equal(expectedTarget.rating)
    maxRatingRow.track.should.equal('DATA_SCIENCE')
    maxRatingRow.subTrack.should.equal('Challenge')
    maxRatingRow.ratingColor.should.equal(getRatingColor(expectedTarget.rating))

    members.state.rankRecalculationCalls.should.have.length(1)
    members.state.rankRecalculationCalls[0].trackId.should.equal(DATA_SCIENCE_TRACK_ID)
    members.state.rankRecalculationCalls[0].typeId.should.equal(CHALLENGE_TYPE_ID)
  })

  it('rerateDevTrack should include QA ChallengeWinner placements in QA rerates', async () => {
    const targetUserId = toBigInt(89770374)
    const opponentUserId = toBigInt(100000039)
    const challengeId = 'qa-rating-jun-2'
    const eventDate = new Date('2026-06-02T06:36:07.735Z')
    const members = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createReviewDbClient([])
    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        status: 'COMPLETED',
        endDate: eventDate,
        track: { name: 'Quality Assurance' },
        type: { name: 'Challenge' },
        metadata: []
      }
    }, [
      {
        challengeId,
        userId: Number(targetUserId),
        type: 'PLACEMENT',
        placement: 1,
        createdAt: new Date('2026-06-02T06:37:00.000Z')
      },
      {
        challengeId,
        userId: Number(opponentUserId),
        type: 'PLACEMENT',
        placement: 2,
        createdAt: new Date('2026-06-02T06:37:00.000Z')
      }
    ])
    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, -1),
      createParticipant(opponentUserId, 0, 0, 0, -2)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      members.client,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId,
      {
        targetTrackName: 'QA',
        targetTypeName: 'Challenge',
        challengeTrackNames: ['QUALITY_ASSURANCE'],
        challengeTypeNames: ['Challenge']
      }
    )

    result.challengesProcessed.should.equal(1)
    result.ratingsUpdated.should.equal(1)

    const statsRow = members.state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === QA_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    should.exist(statsRow)
    statsRow.rating.should.equal(expectedTarget.rating)
    statsRow.volatility.should.equal(expectedTarget.volatility)
    statsRow.challenges.should.equal(1)
    statsRow.mostRecentEventDate.should.deep.equal(eventDate)

    const historyRow = findHistoryRow(members.state.historyRows, targetUserId, challengeId)
    should.exist(historyRow)
    historyRow.trackId.should.equal(QA_TRACK_ID)
    historyRow.typeId.should.equal(CHALLENGE_TYPE_ID)
    historyRow.newRating.should.equal(expectedTarget.rating)
    historyRow.mostRecent.should.equal(true)

    const maxRatingRow = members.state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))
    should.exist(maxRatingRow)
    maxRatingRow.rating.should.equal(expectedTarget.rating)
    maxRatingRow.track.should.equal('QA')
    maxRatingRow.subTrack.should.equal('Challenge')
    maxRatingRow.ratingColor.should.equal(getRatingColor(expectedTarget.rating))
  })

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

  it('rerateDevTrack should seed rerates from historical volatility checkpoints', async () => {
    const targetUserId = toBigInt(1011)
    const opponentUserId = toBigInt(2022)
    const challengeOneId = 'volatility-seed-1'
    const challengeTwoId = 'volatility-seed-2'
    const opponentSeedChallengeId = 'opponent-volatility-seed'

    const targetSeedRating = 1500
    const targetSeedVolatility = 220
    const opponentSeedRating = 1600
    const opponentSeedVolatility = 780

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(101),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: challengeOneId,
          mostRecent: false,
          oldRating: null,
          newRating: targetSeedRating,
          oldVolatility: null,
          newVolatility: targetSeedVolatility,
          eventDate: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          id: toBigInt(102),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: opponentSeedChallengeId,
          mostRecent: false,
          oldRating: null,
          newRating: opponentSeedRating,
          oldVolatility: null,
          newVolatility: opponentSeedVolatility,
          eventDate: new Date('2024-01-01T00:00:00.000Z')
        }
      ],
      statsRows: [],
      maxRatingRows: []
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

    const expectedParticipants = [
      createParticipant(targetUserId, targetSeedRating, targetSeedVolatility, 1, 50),
      createParticipant(opponentUserId, opponentSeedRating, opponentSeedVolatility, 1, 100)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeTwoId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeTwoId)
    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(statsRow.volatility, expectedTarget.volatility)
    should.equal(maxRatingRow.rating, expectedTarget.rating)
    should.equal(maxRatingRow.track, 'DEVELOP')
    should.equal(maxRatingRow.subTrack, 'Challenge')
    should.equal(maxRatingRow.ratingColor, getRatingColor(expectedTarget.rating))
    should.equal(statsRow.globalRank, 1)
    state.rankRecalculationCalls.should.have.length(1)
    state.rankRecalculationCalls[0].trackId.should.equal(DEVELOP_TRACK_ID)
    state.rankRecalculationCalls[0].typeId.should.equal(CHALLENGE_TYPE_ID)
    should.equal(historyRow.oldRating, targetSeedRating)
    should.equal(historyRow.oldVolatility, targetSeedVolatility)
    should.equal(historyRow.newRating, expectedTarget.rating)
    should.equal(historyRow.newVolatility, expectedTarget.volatility)
  })

  it('rerateDevTrack should allow bulk rerates to skip rank recalculation', async () => {
    const targetUserId = toBigInt(3011)
    const opponentUserId = toBigInt(3022)
    const challengeId = 'bulk-rank-skip'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-03-01T09:00:00.000Z')
      },
      {
        challengeId,
        userId: opponentUserId,
        finalScore: 50,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-03-01T10:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-03-01T00:00:00.000Z'),
        track: { name: 'DEVELOPMENT' },
        type: { name: 'Challenge' }
      }
    })

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      null,
      { recalculateRanks: false }
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)
    state.rankRecalculationCalls.should.have.length(0)
    should.exist(findHistoryRow(state.historyRows, targetUserId, challengeId))
    should.exist(state.statsRows.find((row) => String(row.userId) === String(targetUserId)))
  })

  it('rerateDevTrack should skip null rating history rows when seeding partial rerates', async () => {
    const targetUserId = toBigInt(2031)
    const opponentUserId = toBigInt(2032)
    const seedChallengeId = 'rated-seed-before-null'
    const nullHistoryChallengeId = 'null-history-before-rerate'
    const rerateChallengeId = 'rerate-after-null-history'
    const opponentSeedChallengeId = 'opponent-rated-seed'
    const targetSeedRating = 1500
    const targetSeedVolatility = 240
    const opponentSeedRating = 1400
    const opponentSeedVolatility = 260

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(201),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: seedChallengeId,
          mostRecent: false,
          oldRating: null,
          newRating: targetSeedRating,
          oldVolatility: null,
          newVolatility: targetSeedVolatility,
          eventDate: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          id: toBigInt(202),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: nullHistoryChallengeId,
          mostRecent: false,
          oldRating: null,
          newRating: null,
          oldVolatility: null,
          newVolatility: null,
          eventDate: new Date('2024-02-01T00:00:00.000Z')
        },
        {
          id: toBigInt(203),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: opponentSeedChallengeId,
          mostRecent: true,
          oldRating: null,
          newRating: opponentSeedRating,
          oldVolatility: null,
          newVolatility: opponentSeedVolatility,
          eventDate: new Date('2024-02-01T00:00:00.000Z')
        }
      ],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: seedChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-01-01T09:00:00.000Z')
      },
      {
        challengeId: nullHistoryChallengeId,
        userId: targetUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-02-01T09:00:00.000Z')
      },
      {
        challengeId: rerateChallengeId,
        userId: targetUserId,
        finalScore: 90,
        placement: 2,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-03-01T10:00:00.000Z')
      },
      {
        challengeId: rerateChallengeId,
        userId: opponentUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-03-01T09:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [seedChallengeId]: {
        id: seedChallengeId,
        endDate: new Date('2024-01-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [nullHistoryChallengeId]: {
        id: nullHistoryChallengeId,
        endDate: new Date('2024-02-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [rerateChallengeId]: {
        id: rerateChallengeId,
        endDate: new Date('2024-03-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, targetSeedRating, targetSeedVolatility, 1, 90),
      createParticipant(opponentUserId, opponentSeedRating, opponentSeedVolatility, 1, 100)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      rerateChallengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, rerateChallengeId)
    should.equal(historyRow.oldRating, targetSeedRating)
    should.equal(historyRow.oldVolatility, targetSeedVolatility)
    should.equal(historyRow.newRating, expectedTarget.rating)
    should.equal(historyRow.newVolatility, expectedTarget.volatility)
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
        },
        {
          userId: targetUserId,
          trackId: DATA_SCIENCE_TRACK_ID,
          typeId: MARATHON_MATCH_TYPE_ID,
          rating: 2200,
          volatility: 180
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

  it('rerateDevTrack should treat null rated flags as rated unless the challenge is explicitly unrated', async () => {
    const targetUserId = toBigInt(7007)
    const opponentUserId = toBigInt(8008)
    const challengeId = 'null-rated-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: null,
        passedReview: true,
        createdAt: new Date('2024-06-01T09:00:00.000Z')
      },
      {
        challengeId,
        userId: opponentUserId,
        finalScore: 50,
        placement: 2,
        rated: null,
        passedReview: true,
        createdAt: new Date('2024-06-01T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        isRated: null,
        rated: null,
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 50)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(statsRow.volatility, expectedTarget.volatility)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should include Development CODE challenges in the Challenge rating stream', async () => {
    const targetUserId = toBigInt(7101)
    const opponentUserId = toBigInt(7102)
    const canonicalChallengeId = 'code-canonical-dev'
    const legacyChallengeId = 710123

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: String(legacyChallengeId),
        userId: targetUserId,
        finalScore: 95,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-06-15T09:00:00.000Z')
      },
      {
        challengeId: String(legacyChallengeId),
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-06-15T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [canonicalChallengeId]: {
        id: canonicalChallengeId,
        legacyId: legacyChallengeId,
        legacyRecord: { legacySystemId: legacyChallengeId },
        endDate: new Date('2024-06-15T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'CODE' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 95),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      legacyChallengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, canonicalChallengeId)
    const legacyHistoryRow = findHistoryRow(state.historyRows, targetUserId, legacyChallengeId)

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(statsRow.volatility, expectedTarget.volatility)
    should.equal(historyRow.newRating, expectedTarget.rating)
    should.equal(legacyHistoryRow, undefined)
  })

  it('rerateDevTrack should use passed review rows even when challengeResult.rated is false', async () => {
    const targetUserId = toBigInt(9009)
    const opponentUserId = toBigInt(9010)
    const challengeId = 'false-rated-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: false,
        passedReview: true,
        createdAt: new Date('2024-07-01T09:00:00.000Z')
      },
      {
        challengeId,
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: false,
        passedReview: true,
        createdAt: new Date('2024-07-01T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-07-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should include failed review rows when final scores are present', async () => {
    const targetUserId = toBigInt(9011)
    const opponentUserId = toBigInt(9012)
    const challengeId = 'failed-review-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId,
        userId: targetUserId,
        finalScore: 60,
        placement: null,
        rated: true,
        passedReview: false,
        createdAt: new Date('2024-08-01T09:05:00.000Z')
      },
      {
        challengeId,
        userId: opponentUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-08-01T09:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-08-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 60),
      createParticipant(opponentUserId, 0, 0, 0, 100)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(statsRow.volatility, expectedTarget.volatility)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should skip invalid zero-score review rows', async () => {
    const targetUserId = toBigInt(9021)
    const opponentUserId = toBigInt(9022)
    const invalidChallengeId = 'invalid-zero-score-dev'
    const ratedChallengeId = 'valid-score-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: invalidChallengeId,
        userId: targetUserId,
        finalScore: 0,
        placement: 0,
        rated: false,
        passedReview: false,
        validSubmission: false,
        createdAt: new Date('2024-08-15T09:00:00.000Z')
      },
      {
        challengeId: ratedChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-09-01T09:00:00.000Z')
      },
      {
        challengeId: ratedChallengeId,
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-09-01T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [invalidChallengeId]: {
        id: invalidChallengeId,
        endDate: new Date('2024-08-15T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [ratedChallengeId]: {
        id: ratedChallengeId,
        endDate: new Date('2024-09-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      null
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)
    should.equal(findHistoryRow(state.historyRows, targetUserId, invalidChallengeId), undefined)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, ratedChallengeId)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should preserve legacy source ratings when requested by bulk stats regeneration', async () => {
    const targetUserId = toBigInt(9031)
    const opponentUserId = toBigInt(9032)
    const legacyAliasChallengeId = 903100
    const legacyAliasCanonicalChallengeId = 'legacy-alias-canonical-dev'
    const legacyUuidChallengeId = 'legacy-uuid-dev'
    const canonicalChallengeId = 'canonical-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: String(legacyAliasChallengeId),
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        oldRating: 900,
        newRating: 950,
        createdAt: new Date('2024-05-01T09:00:00.000Z')
      },
      {
        challengeId: legacyUuidChallengeId,
        userId: targetUserId,
        finalScore: 97,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        oldRating: 950,
        newRating: 970,
        createdAt: new Date('2024-05-02T09:05:00.000Z')
      },
      {
        challengeId: canonicalChallengeId,
        userId: targetUserId,
        finalScore: 95,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-06-01T09:00:00.000Z')
      },
      {
        challengeId: canonicalChallengeId,
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-06-01T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [legacyAliasCanonicalChallengeId]: {
        id: legacyAliasCanonicalChallengeId,
        legacyId: legacyAliasChallengeId,
        legacyRecord: { legacySystemId: legacyAliasChallengeId },
        endDate: new Date('2024-05-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [legacyUuidChallengeId]: {
        id: legacyUuidChallengeId,
        legacyId: 903101,
        legacyRecord: { legacySystemId: 903101 },
        endDate: new Date('2024-05-02T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [canonicalChallengeId]: {
        id: canonicalChallengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 970, DEFAULT_VOLATILITY, 3, 95),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      null,
      {
        useLegacySourceRatings: true
      }
    )

    should.equal(result.challengesProcessed, 3)
    should.equal(result.ratingsUpdated, 3)

    const legacyAliasHistoryRow = findHistoryRow(state.historyRows, targetUserId, legacyAliasCanonicalChallengeId)
    should.equal(legacyAliasHistoryRow.oldRating, 900)
    should.equal(legacyAliasHistoryRow.newRating, 950)

    const legacyUuidHistoryRow = findHistoryRow(state.historyRows, targetUserId, legacyUuidChallengeId)
    should.equal(legacyUuidHistoryRow.oldRating, 950)
    should.equal(legacyUuidHistoryRow.newRating, 970)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, canonicalChallengeId)
    should.equal(historyRow.oldRating, 970)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should skip non-completed Development Challenge rows', async () => {
    const targetUserId = toBigInt(9031)
    const opponentUserId = toBigInt(9032)
    const cancelledChallengeId = 'cancelled-dev'
    const draftChallengeId = 'draft-dev'
    const completedChallengeId = 'completed-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(301),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: cancelledChallengeId,
          mostRecent: false,
          oldRating: 900,
          newRating: 850,
          oldVolatility: 300,
          newVolatility: 280,
          eventDate: new Date('2024-08-01T00:00:00.000Z')
        },
        {
          id: toBigInt(302),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: draftChallengeId,
          mostRecent: true,
          oldRating: 850,
          newRating: 830,
          oldVolatility: 280,
          newVolatility: 260,
          eventDate: new Date('2024-08-15T00:00:00.000Z')
        }
      ],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: cancelledChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-08-01T09:00:00.000Z')
      },
      {
        challengeId: draftChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-08-15T09:00:00.000Z')
      },
      {
        challengeId: completedChallengeId,
        userId: targetUserId,
        finalScore: 95,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-09-01T09:00:00.000Z')
      },
      {
        challengeId: completedChallengeId,
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-09-01T09:05:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [cancelledChallengeId]: {
        id: cancelledChallengeId,
        endDate: new Date('2024-08-01T00:00:00.000Z'),
        status: 'CANCELLED_FAILED_REVIEW',
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [draftChallengeId]: {
        id: draftChallengeId,
        endDate: new Date('2024-08-15T00:00:00.000Z'),
        status: 'DRAFT',
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [completedChallengeId]: {
        id: completedChallengeId,
        endDate: new Date('2024-09-01T00:00:00.000Z'),
        status: 'COMPLETED',
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 95),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      null
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)
    should.equal(findHistoryRow(state.historyRows, targetUserId, cancelledChallengeId), undefined)
    should.equal(findHistoryRow(state.historyRows, targetUserId, draftChallengeId), undefined)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, completedChallengeId)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateDevTrack should include failed review opponents so later challenge ratings still move', async () => {
    const targetUserId = toBigInt(9013)
    const opponentUserId = toBigInt(9014)
    const seedChallengeId = 'seed-dev'
    const rerateChallengeId = 'rerate-dev'

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(21),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: seedChallengeId,
          mostRecent: true,
          oldRating: null,
          newRating: 1500,
          eventDate: new Date('2024-03-01T00:00:00.000Z')
        },
        {
          id: toBigInt(22),
          userId: opponentUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          challengeId: seedChallengeId,
          mostRecent: true,
          oldRating: null,
          newRating: 1400,
          eventDate: new Date('2024-03-01T00:00:00.000Z')
        }
      ],
      statsRows: [
        {
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
          rating: 1500,
          volatility: DEFAULT_VOLATILITY
        }
      ],
      maxRatingRows: []
    })

    const reviewDbClient = createReviewDbClient([
      {
        challengeId: seedChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-03-01T09:00:00.000Z')
      },
      {
        challengeId: rerateChallengeId,
        userId: targetUserId,
        finalScore: 90,
        placement: 1,
        rated: true,
        passedReview: true,
        createdAt: new Date('2024-03-16T09:05:00.000Z')
      },
      {
        challengeId: rerateChallengeId,
        userId: opponentUserId,
        finalScore: 100,
        placement: null,
        rated: true,
        passedReview: false,
        createdAt: new Date('2024-03-16T09:00:00.000Z')
      }
    ])

    const challengeClient = createChallengeClient({
      [seedChallengeId]: {
        id: seedChallengeId,
        endDate: new Date('2024-03-01T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      },
      [rerateChallengeId]: {
        id: rerateChallengeId,
        endDate: new Date('2024-03-16T00:00:00.000Z'),
        metadata: [],
        track: { name: 'Development' },
        type: { name: 'Challenge' }
      }
    })

    const result = await rerateDevTrack(
      membersClient,
      challengeClient,
      reviewDbClient,
      targetUserId,
      rerateChallengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === CHALLENGE_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, rerateChallengeId)

    should.equal(historyRow.oldRating, 1500)
    should.equal(historyRow.newRating === historyRow.oldRating, false)
    should.equal(historyRow.newRating < historyRow.oldRating, true)
    should.equal(statsRow.rating, historyRow.newRating)
  })
})

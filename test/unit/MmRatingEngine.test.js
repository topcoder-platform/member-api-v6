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
const { clearChallengeDimensionLookupCache } = require('../../src/common/statsDimensionHelper')
const { normalizeRatingPathConfig } = require('../../src/ratings/ratingPathConfig')

const should = chai.should()
const DEVELOP_TRACK_ID = 'track-develop-id'
const DATA_SCIENCE_TRACK_ID = 'track-data-science-id'
const CHALLENGE_TYPE_ID = 'type-challenge-id'
const MARATHON_MATCH_TYPE_ID = 'type-marathon-match-id'
const AI_RATING_TYPE_ID = 'rating-path-ai'
const JAVA_MYSQL_RATING_TYPE_ID = 'rating-path-java-mysql'

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

function createMmReviewDbClient (rows) {
  const resultRows = rows.map(cloneRow)

  /**
   * Match the production MM reviewSummation readiness guard.
   * Rows without test progress metadata are legacy/imported rows and remain
   * eligible; rows with progress metadata need a successful reviewed checkpoint.
   * @param {Object} row stub review summation row
   * @returns {boolean} true when the row is ready for MM rating replay
   */
  function isMmReviewRowReady (row) {
    const testProgressDetails = row && row.metadata && row.metadata.testProgressDetails
    if (!testProgressDetails) {
      return true
    }

    const reviewedDate = row.reviewedDate ? new Date(row.reviewedDate) : null
    return !!(
      reviewedDate &&
      !Number.isNaN(reviewedDate.getTime()) &&
      testProgressDetails.status === 'SUCCESS'
    )
  }

  const normalizeReviewRow = (row) => {
    const normalized = cloneRow(row)
    if ((normalized.challengeId === null || normalized.challengeId === undefined) && normalized.legacyChallengeId !== null && normalized.legacyChallengeId !== undefined) {
      normalized.challengeId = String(normalized.legacyChallengeId)
    }
    return normalized
  }

  return {
    async query (sql, params) {
      if (sql.includes('pg_catalog.pg_class')) {
        return {
          rows: [{ schemaName: 'reviews' }]
        }
      }

      if (sql.includes('WHERE s."memberId" = $1')) {
        return {
          rows: resultRows
            .filter(isMmReviewRowReady)
            .filter((row) => String(row.memberId) === String(params[0]))
            .map(normalizeReviewRow)
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

      if (sql.includes('WHERE s."challengeId" = $1') || sql.includes('s."challengeId" = ANY($1::text[])')) {
        const challengeIds = new Set((Array.isArray(params[0]) ? params[0] : [params[0]]).map((value) => String(value)))
        return {
          rows: resultRows
            .filter(isMmReviewRowReady)
            .filter((row) =>
              challengeIds.has(String(row.challengeId)) ||
              challengeIds.has(String(row.legacyChallengeId))
            )
            .map(normalizeReviewRow)
            .sort((left, right) => {
              const createdComparison = compareValues(left.submissionCreatedAt, right.submissionCreatedAt)
              if (createdComparison !== 0) {
                return createdComparison
              }

              return compareValues(left.submissionId, right.submissionId)
            })
        }
      }

      if (sql.includes('WHERE "challengeId" = $1')) {
        return {
          rows: resultRows
            .filter((row) => String(row.challengeId) === String(params[0]) && row.userId !== undefined)
            .sort((left, right) => {
              const placementComparison = compareValues(left.placement, right.placement)
              if (placementComparison !== 0) {
                return placementComparison
              }

              const scoreComparison = compareValues(left.finalScore, right.finalScore)
              if (scoreComparison !== 0) {
                return -scoreComparison
              }

              return compareValues(left.createdAt, right.createdAt)
            })
        }
      }

      throw new Error(`Unexpected review query: ${sql}`)
    }
  }
}

/**
 * Check whether stub challenge metadata contains any requested tag.
 * @param {Object} challenge challenge metadata in the test fixture
 * @param {Array<string>} tags requested rating path tags
 * @returns {boolean} true when any requested tag is present
 */
function challengeHasSomeTag (challenge, tags) {
  if (!Array.isArray(challenge.tags) || !Array.isArray(tags)) {
    return false
  }

  const requestedTags = tags.map((tag) => String(tag))
  return challenge.tags.some((tag) => requestedTags.includes(String(tag)))
}

/**
 * Check whether stub challenge metadata contains a requested skill id.
 * @param {Object} challenge challenge metadata in the test fixture
 * @param {string} skillId requested skill id
 * @returns {boolean} true when the challenge has the skill
 */
function challengeHasSkill (challenge, skillId) {
  if (!Array.isArray(challenge.skills)) {
    return false
  }

  return challenge.skills.some((skill) => String(skill.skillId) === String(skillId))
}

/**
 * Evaluate the subset of Prisma challenge where clauses used by rating path tests.
 * @param {Object} challenge challenge metadata in the test fixture
 * @param {Object} where Prisma-style where clause
 * @returns {boolean} true when the challenge satisfies the where clause
 */
function matchesChallengeWhere (challenge, where) {
  if (!where) {
    return true
  }

  if (Array.isArray(where.OR)) {
    return where.OR.some((condition) => matchesChallengeWhere(challenge, condition))
  }

  if (Array.isArray(where.AND)) {
    return where.AND.every((condition) => matchesChallengeWhere(challenge, condition))
  }

  if (where.id && !matchesFilter(challenge.id, where.id)) {
    return false
  }

  if (where.legacyId && !matchesFilter(challenge.legacyId, where.legacyId)) {
    return false
  }

  if (where.legacyRecord && where.legacyRecord.is) {
    const legacySystemId = challenge.legacyRecord && challenge.legacyRecord.legacySystemId
    if (!matchesChallengeWhere({ legacySystemId }, where.legacyRecord.is)) {
      return false
    }
  }

  if (where.legacySystemId && !matchesFilter(challenge.legacySystemId, where.legacySystemId)) {
    return false
  }

  if (where.tags && Array.isArray(where.tags.hasSome) && !challengeHasSomeTag(challenge, where.tags.hasSome)) {
    return false
  }

  if (where.skills && where.skills.some && !challengeHasSkill(challenge, where.skills.some.skillId)) {
    return false
  }

  return true
}

function createChallengeClient (metadataById) {
  const typeRows = [
    { id: CHALLENGE_TYPE_ID, name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
    { id: MARATHON_MATCH_TYPE_ID, name: 'Marathon Match', abbreviation: 'MM', legacyId: null, isTask: false }
  ]

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
        return typeRows.map(cloneRow)
      }

      throw new Error(`Unexpected challenge lookup query: ${sql}`)
    },
    async $queryRawUnsafe (sql, ...params) {
      const normalizedName = params[0]
      if (sql.includes('SELECT "id"') && sql.includes('FROM "ChallengeType"')) {
        return typeRows
          .filter((row) =>
            String(row.name).toUpperCase() === normalizedName ||
            String(row.abbreviation).toUpperCase() === normalizedName
          )
          .map(row => ({ id: row.id }))
      }

      if (sql.includes('INSERT INTO "ChallengeType"')) {
        const row = {
          id: params[0],
          name: params[1],
          abbreviation: params[3],
          legacyId: null,
          isTask: false
        }
        const existingIndex = typeRows.findIndex(existing => existing.id === row.id)
        if (existingIndex >= 0) {
          typeRows[existingIndex] = row
        } else {
          typeRows.push(row)
        }
        return [{ id: row.id }]
      }

      throw new Error(`Unexpected challenge raw query: ${sql}`)
    },
    challenge: {
      async findMany (args) {
        if (args.where && Array.isArray(args.where.OR)) {
          return Object.values(metadataById)
            .filter((challenge) => matchesChallengeWhere(challenge, args.where))
            .map(cloneRow)
        }

        if (args.where && args.where.id && Array.isArray(args.where.id.in)) {
          return args.where.id.in
            .map((challengeId) => metadataById[String(challengeId)])
            .filter(Boolean)
            .map(cloneRow)
        }

        if (args.where && args.where.tags && Array.isArray(args.where.tags.hasSome)) {
          return Object.values(metadataById)
            .filter((challenge) => matchesChallengeWhere(challenge, args.where))
            .map(cloneRow)
        }

        if (args.where && (Array.isArray(args.where.AND) || args.where.skills)) {
          return Object.values(metadataById)
            .filter((challenge) => matchesChallengeWhere(challenge, args.where))
            .map(cloneRow)
        }

        return Object.values(metadataById).map(cloneRow)
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
  beforeEach(() => {
    clearChallengeDimensionLookupCache()
  })

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
      placement: 1,
      aggregateScore: 20,
      reviewedDate: new Date('2024-06-01T10:00:00.000Z'),
      createdAt: new Date('2024-06-01T10:00:00.000Z'),
      submissionCreatedAt: new Date('2024-06-01T09:00:00.000Z')
    },
    {
      submissionId: 'submission-opponent',
      memberId: opponentUserId,
      challengeId,
      placement: 2,
      aggregateScore: 10,
      reviewedDate: new Date('2024-06-01T10:05:00.000Z'),
      createdAt: new Date('2024-06-01T10:05:00.000Z'),
      submissionCreatedAt: new Date('2024-06-01T09:05:00.000Z')
    }
  ]

  it('rerateMmTrack should replay MM review summation scores without an MM database client', async () => {
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const reviewDbClient = createMmReviewDbClient(baseReviewRows)
    const challengeClient = createChallengeClient(challengeMetadata)
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
      null,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === MARATHON_MATCH_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)
    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(statsRow.challenges, 1)
    should.equal(statsRow.mostRecentEventDate.getTime(), challengeMetadata[challengeId].endDate.getTime())
    should.equal(statsRow.mostRecentSubmission.getTime(), baseReviewRows[0].createdAt.getTime())
    should.equal(maxRatingRow.rating, expectedTargetState.rating)
    should.equal(maxRatingRow.track, 'DATA_SCIENCE')
    should.equal(maxRatingRow.subTrack, 'MARATHON_MATCH')
    should.equal(maxRatingRow.ratingColor, getRatingColor(expectedTargetState.rating))
    should.equal(statsRow.globalRank, 1)
    state.rankRecalculationCalls.should.have.length(1)
    state.rankRecalculationCalls[0].trackId.should.equal(DATA_SCIENCE_TRACK_ID)
    state.rankRecalculationCalls[0].typeId.should.equal(MARATHON_MATCH_TYPE_ID)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
  })

  it('rerateMmTrack should prefer final MM standings placement over raw aggregate score order', async () => {
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const conflictingScoreRows = [
      {
        submissionId: 'submission-target',
        memberId: targetUserId,
        challengeId,
        placement: 1,
        aggregateScore: 10,
        reviewedDate: new Date('2024-06-01T10:00:00.000Z'),
        createdAt: new Date('2024-06-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-06-01T09:00:00.000Z')
      },
      {
        submissionId: 'submission-opponent',
        memberId: opponentUserId,
        challengeId,
        placement: 2,
        aggregateScore: 20,
        reviewedDate: new Date('2024-06-01T10:05:00.000Z'),
        createdAt: new Date('2024-06-01T10:05:00.000Z'),
        submissionCreatedAt: new Date('2024-06-01T09:05:00.000Z')
      }
    ]
    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, -1),
      createParticipant(opponentUserId, 0, 0, 0, -2)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTargetState = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateMmTrack(
      membersClient,
      createChallengeClient(challengeMetadata),
      null,
      createMmReviewDbClient(conflictingScoreRows),
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === MARATHON_MATCH_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
  })

  it('rerateMmTrack should skip MM challenges with challenge metadata isRated false', async () => {
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        metadata: [{ name: 'isRated', value: 'false' }]
      }
    })

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
      createMmReviewDbClient(baseReviewRows),
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 0)
    should.equal(result.ratingsUpdated, 0)
    state.statsRows.should.have.length(0)
    state.historyRows.should.have.length(0)
    state.maxRatingRows.should.have.length(0)
  })

  it('rerateMmTrack should wait for MM system test progress before rating', async () => {
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const inProgressRows = baseReviewRows.map((row) => ({
      ...cloneRow(row),
      placement: null,
      aggregateScore: 0,
      reviewedDate: null,
      metadata: {
        testProgressDetails: {
          status: 'RUNNING',
          progress: 0.5
        }
      }
    }))

    const result = await rerateMmTrack(
      membersClient,
      createChallengeClient(challengeMetadata),
      null,
      createMmReviewDbClient(inProgressRows),
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 0)
    should.equal(result.ratingsUpdated, 0)
    state.statsRows.should.have.length(0)
    state.historyRows.should.have.length(0)
    state.maxRatingRows.should.have.length(0)
  })

  it('rerateMmTrack should still replay legacy MM summations without progress metadata', async () => {
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const legacyRows = baseReviewRows.map((row) => ({
      ...cloneRow(row),
      reviewedDate: null
    }))
    const expectedTargetState = buildExpectedTargetState(
      targetUserId,
      opponentUserId,
      20,
      10,
      scoringConfig
    )

    const result = await rerateMmTrack(
      membersClient,
      createChallengeClient(challengeMetadata),
      null,
      createMmReviewDbClient(legacyRows),
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
  })

  it('rerateMmTrack should rate MM summations when challenge metadata is rated despite stale row metadata', async () => {
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const challengeClient = createChallengeClient({
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        metadata: [{ name: 'isRated', value: 'true' }]
      }
    })
    const staleMetadataRows = baseReviewRows.map((row) => ({
      ...cloneRow(row),
      rated: false
    }))
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
      null,
      createMmReviewDbClient(staleMetadataRows),
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === MARATHON_MATCH_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
  })

  it('rerateMmTrack should match review submissions by legacyChallengeId while storing canonical history', async () => {
    const canonicalChallengeId = 'mm-canonical-challenge-id'
    const legacyChallengeId = 30012345
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })

    const legacyReviewRows = baseReviewRows.map((row) => ({
      ...cloneRow(row),
      challengeId: null,
      legacyChallengeId: toBigInt(legacyChallengeId)
    }))
    const challengeClient = createChallengeClient({
      [canonicalChallengeId]: {
        id: canonicalChallengeId,
        legacyId: legacyChallengeId,
        legacyRecord: { legacySystemId: legacyChallengeId },
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        metadata: []
      }
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
      null,
      createMmReviewDbClient(legacyReviewRows),
      targetUserId,
      canonicalChallengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const canonicalHistoryRow = findHistoryRow(state.historyRows, targetUserId, canonicalChallengeId)
    const legacyHistoryRow = findHistoryRow(state.historyRows, targetUserId, legacyChallengeId)

    should.equal(canonicalHistoryRow.newRating, expectedTargetState.rating)
    should.equal(canonicalHistoryRow.newVolatility, expectedTargetState.volatility)
    should.equal(canonicalHistoryRow.placement, 1)
    should.equal(legacyHistoryRow, undefined)
  })

  it('rerateMmTrack should seed rerates from historical volatility checkpoints', async () => {
    const challengeOneId = 'mm-volatility-seed-1'
    const challengeTwoId = 'mm-volatility-seed-2'
    const opponentSeedChallengeId = 'mm-opponent-volatility-seed'
    const targetSeedRating = 1500
    const targetSeedVolatility = 230
    const opponentSeedRating = 1600
    const opponentSeedVolatility = 790

    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(1001),
          userId: targetUserId,
          trackId: DATA_SCIENCE_TRACK_ID,
          typeId: MARATHON_MATCH_TYPE_ID,
          challengeId: challengeOneId,
          mostRecent: false,
          oldRating: null,
          newRating: targetSeedRating,
          oldVolatility: null,
          newVolatility: targetSeedVolatility,
          eventDate: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          id: toBigInt(1002),
          userId: opponentUserId,
          trackId: DATA_SCIENCE_TRACK_ID,
          typeId: MARATHON_MATCH_TYPE_ID,
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

    const reviewRows = [
      {
        submissionId: 'submission-mm-vol-target-seed',
        memberId: targetUserId,
        challengeId: challengeOneId,
        aggregateScore: 100,
        reviewedDate: new Date('2024-01-01T10:00:00.000Z'),
        createdAt: new Date('2024-01-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-01-01T09:00:00.000Z')
      },
      {
        submissionId: 'submission-mm-vol-target',
        memberId: targetUserId,
        challengeId: challengeTwoId,
        aggregateScore: 50,
        reviewedDate: new Date('2024-02-01T10:00:00.000Z'),
        createdAt: new Date('2024-02-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-02-01T09:00:00.000Z')
      },
      {
        submissionId: 'submission-mm-vol-opponent',
        memberId: opponentUserId,
        challengeId: challengeTwoId,
        aggregateScore: 100,
        reviewedDate: new Date('2024-02-01T10:05:00.000Z'),
        createdAt: new Date('2024-02-01T10:05:00.000Z'),
        submissionCreatedAt: new Date('2024-02-01T09:05:00.000Z')
      }
    ]

    const challengeClient = createChallengeClient({
      [challengeOneId]: {
        id: challengeOneId,
        endDate: new Date('2024-01-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        metadata: []
      },
      [challengeTwoId]: {
        id: challengeTwoId,
        endDate: new Date('2024-02-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        metadata: []
      }
    })

    const expectedParticipants = [
      createParticipant(targetUserId, targetSeedRating, targetSeedVolatility, 1, 50),
      createParticipant(opponentUserId, opponentSeedRating, opponentSeedVolatility, 1, 100)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
      createMmReviewDbClient(reviewRows),
      targetUserId,
      challengeTwoId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === MARATHON_MATCH_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeTwoId)

    should.equal(statsRow.rating, expectedTarget.rating)
    should.equal(statsRow.volatility, expectedTarget.volatility)
    should.equal(statsRow.challenges, 2)
    should.equal(statsRow.mostRecentEventDate.getTime(), new Date('2024-02-01T00:00:00.000Z').getTime())
    should.equal(statsRow.mostRecentSubmission.getTime(), new Date('2024-02-01T10:00:00.000Z').getTime())
    should.equal(historyRow.oldRating, targetSeedRating)
    should.equal(historyRow.oldVolatility, targetSeedVolatility)
    should.equal(historyRow.newRating, expectedTarget.rating)
    should.equal(historyRow.newVolatility, expectedTarget.volatility)
    should.equal(historyRow.placement, 2)
  })

  it('rerateMmTrack should treat Development-track Marathon Match challenges as Data Science MM ratings', async () => {
    const developmentTrackMmMetadata = {
      [challengeId]: {
        id: challengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Marathon Match' },
        metadata: []
      }
    }
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createMmReviewDbClient(baseReviewRows)
    const challengeClient = createChallengeClient(developmentTrackMmMetadata)

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
      null,
      reviewDbClient,
      targetUserId,
      challengeId
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)
    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DATA_SCIENCE_TRACK_ID &&
      row.typeId === MARATHON_MATCH_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
  })

  it('rerateMmTrack should replay tagged Development Challenge and MM events under the configured destination track', async () => {
    const priorChallengeId = 'ai-prior-development-challenge'
    const targetChallengeId = 'ai-target-challenge'
    const nonAiChallengeId = 'non-ai-challenge'
    const unratedAiChallengeId = 'unrated-ai-mm-challenge'
    const ratingPath = normalizeRatingPathConfig({
      name: 'AI',
      track: 'DEVELOPMENT',
      tags: ['AI', 'AI Exponential League']
    })
    const pathMetadata = {
      [priorChallengeId]: {
        id: priorChallengeId,
        endDate: new Date('2024-05-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: ['AI'],
        metadata: []
      },
      [targetChallengeId]: {
        id: targetChallengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        tags: ['AI Exponential League'],
        metadata: []
      },
      [nonAiChallengeId]: {
        id: nonAiChallengeId,
        endDate: new Date('2024-07-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        tags: ['Other'],
        metadata: []
      },
      [unratedAiChallengeId]: {
        id: unratedAiChallengeId,
        endDate: new Date('2024-08-01T00:00:00.000Z'),
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        tags: ['AI'],
        metadata: [{ name: 'isRated', value: 'false' }]
      }
    }
    const reviewRows = [
      {
        challengeId: priorChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-05-01T10:00:00.000Z')
      },
      {
        challengeId: priorChallengeId,
        userId: opponentUserId,
        finalScore: 50,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-05-01T10:05:00.000Z')
      },
      {
        submissionId: 'submission-target-ai',
        memberId: targetUserId,
        challengeId: targetChallengeId,
        aggregateScore: 20,
        reviewedDate: new Date('2024-06-01T10:00:00.000Z'),
        createdAt: new Date('2024-06-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-06-01T09:00:00.000Z')
      },
      {
        submissionId: 'submission-opponent-ai',
        memberId: opponentUserId,
        challengeId: targetChallengeId,
        aggregateScore: 10,
        reviewedDate: new Date('2024-06-01T10:05:00.000Z'),
        createdAt: new Date('2024-06-01T10:05:00.000Z'),
        submissionCreatedAt: new Date('2024-06-01T09:05:00.000Z')
      },
      {
        submissionId: 'submission-target-non-ai',
        memberId: targetUserId,
        challengeId: nonAiChallengeId,
        aggregateScore: 100,
        reviewedDate: new Date('2024-07-01T10:00:00.000Z'),
        createdAt: new Date('2024-07-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-07-01T09:00:00.000Z')
      },
      {
        submissionId: 'submission-target-unrated-ai',
        memberId: targetUserId,
        challengeId: unratedAiChallengeId,
        aggregateScore: 100,
        reviewedDate: new Date('2024-08-01T10:00:00.000Z'),
        createdAt: new Date('2024-08-01T10:00:00.000Z'),
        submissionCreatedAt: new Date('2024-08-01T09:00:00.000Z')
      }
    ]
    const priorParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 50)
    ]
    runQubitsRating(priorParticipants)
    const seededTarget = priorParticipants.find((participant) => participant.coderId === String(targetUserId))
    const seededOpponent = priorParticipants.find((participant) => participant.coderId === String(opponentUserId))
    const targetParticipants = [
      createParticipant(
        targetUserId,
        seededTarget.rating,
        seededTarget.volatility,
        seededTarget.numRatings,
        20
      ),
      createParticipant(
        opponentUserId,
        seededOpponent.rating,
        seededOpponent.volatility,
        seededOpponent.numRatings,
        10
      )
    ]
    runQubitsRating(targetParticipants)
    const expectedTargetState = targetParticipants.find((participant) => participant.coderId === String(targetUserId))

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createMmReviewDbClient(reviewRows)
    const challengeClient = createChallengeClient(pathMetadata)

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
      reviewDbClient,
      targetUserId,
      targetChallengeId,
      {
        ratingPath
      }
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingPathChallengesProcessed, 2)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === AI_RATING_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, targetChallengeId)
    const nonAiHistoryRow = findHistoryRow(state.historyRows, targetUserId, nonAiChallengeId)
    const unratedAiHistoryRow = findHistoryRow(state.historyRows, targetUserId, unratedAiChallengeId)
    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(statsRow.challenges, 2)
    should.equal(statsRow.mostRecentEventDate.getTime(), pathMetadata[targetChallengeId].endDate.getTime())
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
    should.equal(findHistoryRow(state.historyRows, targetUserId, priorChallengeId), undefined)
    should.equal(nonAiHistoryRow, undefined)
    should.equal(unratedAiHistoryRow, undefined)
    should.equal(maxRatingRow.track, 'DEVELOP')
    should.equal(maxRatingRow.subTrack, 'AI')
  })

  it('rerateMmTrack should skip invalid zero-score Development Challenge rows for configured rating paths', async () => {
    const targetUserId = toBigInt(7201)
    const opponentUserId = toBigInt(7202)
    const invalidChallengeId = 'ai-invalid-zero-dev'
    const validChallengeId = 'ai-valid-score-dev'
    const ratingPath = normalizeRatingPathConfig({
      name: 'AI',
      track: 'DEVELOPMENT',
      tags: ['AI']
    })
    const pathMetadata = {
      [invalidChallengeId]: {
        id: invalidChallengeId,
        endDate: new Date('2024-05-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: ['AI'],
        metadata: []
      },
      [validChallengeId]: {
        id: validChallengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: ['AI'],
        metadata: []
      }
    }
    const reviewRows = [
      {
        challengeId: invalidChallengeId,
        userId: targetUserId,
        finalScore: 0,
        placement: 0,
        rated: false,
        passedReview: false,
        validSubmission: false,
        createdAt: new Date('2024-05-01T09:00:00.000Z')
      },
      {
        challengeId: validChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-06-01T09:00:00.000Z')
      },
      {
        challengeId: validChallengeId,
        userId: opponentUserId,
        finalScore: 80,
        placement: 2,
        rated: true,
        passedReview: true,
        validSubmission: true,
        createdAt: new Date('2024-06-01T09:05:00.000Z')
      }
    ]
    const expectedParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 80)
    ]
    runQubitsRating(expectedParticipants)
    const expectedTarget = expectedParticipants.find((participant) => participant.coderId === String(targetUserId))

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createMmReviewDbClient(reviewRows)
    const challengeClient = createChallengeClient(pathMetadata)

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
      reviewDbClient,
      targetUserId,
      null,
      {
        ratingPath
      }
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingPathChallengesProcessed, 1)
    should.equal(result.ratingsUpdated, 1)
    should.equal(findHistoryRow(state.historyRows, targetUserId, invalidChallengeId), undefined)

    const historyRow = findHistoryRow(state.historyRows, targetUserId, validChallengeId)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTarget.rating)
  })

  it('rerateMmTrack should replay only challenges with every configured rating path skill', async () => {
    const javaSkillId = 'java-skill-id'
    const mysqlSkillId = 'mysql-skill-id'
    const priorChallengeId = 'java-mysql-prior-challenge'
    const targetChallengeId = 'java-mysql-target-challenge'
    const javaOnlyChallengeId = 'java-only-challenge'
    const ratingPath = normalizeRatingPathConfig({
      name: 'Java MySQL',
      track: 'DEVELOPMENT',
      skillIds: [javaSkillId, mysqlSkillId]
    })
    const pathMetadata = {
      [priorChallengeId]: {
        id: priorChallengeId,
        endDate: new Date('2024-05-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [
          { skillId: javaSkillId },
          { skillId: mysqlSkillId }
        ],
        metadata: []
      },
      [targetChallengeId]: {
        id: targetChallengeId,
        endDate: new Date('2024-06-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [
          { skillId: javaSkillId },
          { skillId: mysqlSkillId }
        ],
        metadata: []
      },
      [javaOnlyChallengeId]: {
        id: javaOnlyChallengeId,
        endDate: new Date('2024-07-01T00:00:00.000Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [
          { skillId: javaSkillId }
        ],
        metadata: []
      }
    }
    const reviewRows = [
      {
        challengeId: priorChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-05-01T10:00:00.000Z')
      },
      {
        challengeId: priorChallengeId,
        userId: opponentUserId,
        finalScore: 50,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-05-01T10:05:00.000Z')
      },
      {
        challengeId: targetChallengeId,
        userId: targetUserId,
        finalScore: 80,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-06-01T10:00:00.000Z')
      },
      {
        challengeId: targetChallengeId,
        userId: opponentUserId,
        finalScore: 70,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-06-01T10:05:00.000Z')
      },
      {
        challengeId: javaOnlyChallengeId,
        userId: targetUserId,
        finalScore: 100,
        placement: 1,
        rated: true,
        createdAt: new Date('2024-07-01T10:00:00.000Z')
      },
      {
        challengeId: javaOnlyChallengeId,
        userId: opponentUserId,
        finalScore: 30,
        placement: 2,
        rated: true,
        createdAt: new Date('2024-07-01T10:05:00.000Z')
      }
    ]
    const priorParticipants = [
      createParticipant(targetUserId, 0, 0, 0, 100),
      createParticipant(opponentUserId, 0, 0, 0, 50)
    ]
    runQubitsRating(priorParticipants)
    const seededTarget = priorParticipants.find((participant) => participant.coderId === String(targetUserId))
    const seededOpponent = priorParticipants.find((participant) => participant.coderId === String(opponentUserId))
    const targetParticipants = [
      createParticipant(
        targetUserId,
        seededTarget.rating,
        seededTarget.volatility,
        seededTarget.numRatings,
        80
      ),
      createParticipant(
        opponentUserId,
        seededOpponent.rating,
        seededOpponent.volatility,
        seededOpponent.numRatings,
        70
      )
    ]
    runQubitsRating(targetParticipants)
    const expectedTargetState = targetParticipants.find((participant) => participant.coderId === String(targetUserId))

    const { client: membersClient, state } = createMembersClient({
      historyRows: [],
      statsRows: [],
      maxRatingRows: []
    })
    const reviewDbClient = createMmReviewDbClient(reviewRows)
    const challengeClient = createChallengeClient(pathMetadata)

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
      reviewDbClient,
      targetUserId,
      targetChallengeId,
      {
        ratingPath
      }
    )

    should.equal(result.challengesProcessed, 1)
    should.equal(result.ratingPathChallengesProcessed, 2)
    should.equal(result.ratingsUpdated, 1)

    const statsRow = state.statsRows.find((row) =>
      String(row.userId) === String(targetUserId) &&
      row.trackId === DEVELOP_TRACK_ID &&
      row.typeId === JAVA_MYSQL_RATING_TYPE_ID
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, targetChallengeId)
    const javaOnlyHistoryRow = findHistoryRow(state.historyRows, targetUserId, javaOnlyChallengeId)

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(statsRow.challenges, 2)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(historyRow.placement, 1)
    should.equal(findHistoryRow(state.historyRows, targetUserId, priorChallengeId), undefined)
    should.equal(javaOnlyHistoryRow, undefined)
  })

  it('rerateMmTrack should preserve a higher Develop memberMaxRating', async () => {
    const developPeakChallengeId = 'develop-peak'
    const { client: membersClient, state } = createMembersClient({
      historyRows: [
        {
          id: toBigInt(1),
          userId: targetUserId,
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
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
          trackId: DEVELOP_TRACK_ID,
          typeId: CHALLENGE_TYPE_ID,
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

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      null,
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

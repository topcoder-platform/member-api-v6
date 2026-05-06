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
const { normalizeRatingPathConfig } = require('../../src/ratings/ratingPathConfig')

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

function createMmReviewDbClient (rows) {
  const resultRows = rows.map(cloneRow)

  return {
    async query (sql, params) {
      if (sql.includes('pg_catalog.pg_class')) {
        return {
          rows: [{ schemaName: 'public' }]
        }
      }

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

  if (Array.isArray(where.AND)) {
    return where.AND.every((condition) => matchesChallengeWhere(challenge, condition))
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
        row.trackId === DATA_SCIENCE_TRACK_ID &&
        row.typeId === MARATHON_MATCH_TYPE_ID
      )
      const historyRow = findHistoryRow(state.historyRows, targetUserId, challengeId)

      should.equal(statsRow.rating, expectedTargetState.rating)
      should.equal(statsRow.volatility, expectedTargetState.volatility)
      should.equal(historyRow.oldRating, null)
      should.equal(historyRow.newRating, expectedTargetState.rating)
    })
  })

  it('rerateMmTrack should replay tagged Development Challenge and MM events under the configured destination track', async () => {
    const priorChallengeId = 'ai-prior-development-challenge'
    const targetChallengeId = 'ai-target-challenge'
    const nonAiChallengeId = 'non-ai-challenge'
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
      }
    ]
    const scoringConfig = {
      relativeScoringEnabled: true,
      scoreDirection: 'MAXIMIZE'
    }
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
    const mmDbClient = createMmDbClient({
      [priorChallengeId]: scoringConfig,
      [targetChallengeId]: scoringConfig,
      [nonAiChallengeId]: scoringConfig
    })

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      mmDbClient,
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
      row.typeId === 'AI'
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, targetChallengeId)
    const nonAiHistoryRow = findHistoryRow(state.historyRows, targetUserId, nonAiChallengeId)
    const maxRatingRow = state.maxRatingRows.find((row) => String(row.userId) === String(targetUserId))

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(statsRow.challenges, 2)
    should.equal(statsRow.mostRecentEventDate.getTime(), pathMetadata[targetChallengeId].endDate.getTime())
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTargetState.rating)
    should.equal(findHistoryRow(state.historyRows, targetUserId, priorChallengeId), undefined)
    should.equal(nonAiHistoryRow, undefined)
    should.equal(maxRatingRow.track, 'DEVELOP')
    should.equal(maxRatingRow.subTrack, 'AI')
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
    const mmDbClient = createMmDbClient({})

    const result = await rerateMmTrack(
      membersClient,
      challengeClient,
      mmDbClient,
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
      row.typeId === 'Java MySQL'
    )
    const historyRow = findHistoryRow(state.historyRows, targetUserId, targetChallengeId)
    const javaOnlyHistoryRow = findHistoryRow(state.historyRows, targetUserId, javaOnlyChallengeId)

    should.equal(statsRow.rating, expectedTargetState.rating)
    should.equal(statsRow.volatility, expectedTargetState.volatility)
    should.equal(statsRow.challenges, 2)
    should.equal(historyRow.oldRating, null)
    should.equal(historyRow.newRating, expectedTargetState.rating)
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

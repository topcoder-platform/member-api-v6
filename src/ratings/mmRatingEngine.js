/**
 * Re-rate unified member stats for rated DATA_SCIENCE / MARATHON_MATCH results.
 *
 * The engine reads final reviewSummation rows from review-api, resolves challenge
 * metadata from challenge-api, applies the Qubits math one challenge at a
 * time, and persists the target member's rating state into memberStats,
 * memberStatsHistory, and memberMaxRating.
 */

'use strict'

const errors = require('../common/errors')
const {
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup
} = require('../common/statsDimensionHelper')
const { runQubitsRating, getRatingColor, DEFAULT_VOLATILITY } = require('./qubitsAlgorithm')

const TRACK_NAME = 'DATA_SCIENCE'
const TYPE_NAME = 'MARATHON_MATCH'
const CHALLENGE_TRACK_NAME = 'DATA_SCIENCE'
const CHALLENGE_TYPE_NAME = 'MARATHON_MATCH'
const RERATE_ACTOR = 'rerate-mm-stats'
const SCORE_DIRECTION_MINIMIZE = 'MINIMIZE'

/**
 * Determine whether the supplied value is a BigInt.
 * @param {*} value value to inspect
 * @returns {boolean} true when the value is a BigInt
 */
function isBigIntValue (value) {
  return Object.prototype.toString.call(value) === '[object BigInt]'
}

/**
 * Normalize a user identifier into BigInt form for member table lookups.
 * @param {string|number|BigInt} value member identifier
 * @returns {BigInt} normalized member identifier
 * @throws {Error} if BigInt is unavailable in the runtime
 */
function toBigIntUserId (value) {
  if (isBigIntValue(value)) {
    return value
  }

  if (typeof global.BigInt !== 'function') {
    throw new Error('BigInt is not supported in this runtime')
  }

  return global.BigInt(String(value).trim())
}

/**
 * Normalize a date-like value with a fallback.
 * @param {*} value primary date value
 * @param {*} fallbackValue fallback date value
 * @returns {Date|null} normalized Date or null when parsing fails
 */
function normalizeDate (value, fallbackValue) {
  const date = value ? new Date(value) : new Date(fallbackValue)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date
}

/**
 * Create the default unrated participant state for the Qubits engine.
 * @returns {Object} empty rating state
 */
function createDefaultState () {
  return {
    rating: 0,
    volatility: 0,
    numRatings: 0
  }
}

/**
 * Clone a rating state into the normalized shape expected by the Qubits engine.
 * @param {Object} state source state
 * @returns {Object} normalized rating state
 */
function cloneState (state) {
  if (!state) {
    return createDefaultState()
  }

  return {
    rating: Number.isFinite(Number(state.rating)) ? Number(state.rating) : 0,
    volatility: Number.isFinite(Number(state.volatility)) ? Number(state.volatility) : 0,
    numRatings: Number.isFinite(Number(state.numRatings)) ? Math.max(0, Number(state.numRatings)) : 0
  }
}

/**
 * Build the cache key used for participant state maps.
 * @param {string|number|BigInt} userId participant identifier
 * @returns {string} normalized cache key
 */
function buildUserStateKey (userId) {
  return String(userId)
}

/**
 * Resolve the unified track/type UUIDs used for DATA_SCIENCE / MARATHON_MATCH rows.
 * @param {Object} challengeClient prisma challenge client
 * @returns {Promise<{trackId: string, typeId: string}>} resolved unified ids
 */
async function resolveUnifiedDimensionIds (challengeClient) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const trackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAME)
  const typeId = resolveTypeIdFromLookup(dimensionLookup, TYPE_NAME)

  if (!trackId || !typeId) {
    throw new Error(`Unable to resolve unified dimension ids for ${TRACK_NAME}/${TYPE_NAME}`)
  }

  return {
    trackId,
    typeId
  }
}

/**
 * Build a rerate seed state from the latest authoritative history row before a challenge.
 * Unified memberStatsHistory does not checkpoint volatility, so rated history seeds fall
 * back to the default Qubits volatility until the rerate pass advances them.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {number} seedIndex index of the last history row before the target challenge
 * @returns {Object} seeded rating state
 */
function createHistorySeedState (historyRows, seedIndex) {
  if (!Array.isArray(historyRows) || seedIndex < 0 || !historyRows[seedIndex]) {
    return createDefaultState()
  }

  return {
    rating: Number.isFinite(Number(historyRows[seedIndex].newRating))
      ? Number(historyRows[seedIndex].newRating)
      : 0,
    volatility: DEFAULT_VOLATILITY,
    numRatings: seedIndex + 1
  }
}

/**
 * Find the history row index for a challenge within one participant timeline.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {string} challengeId challenge identifier
 * @returns {number} index of the matching row or -1 when absent
 */
function findHistoryIndexForChallenge (historyRows, challengeId) {
  const normalizedChallengeId = String(challengeId)

  for (let index = historyRows.length - 1; index >= 0; index -= 1) {
    if (String(historyRows[index].challengeId) === normalizedChallengeId) {
      return index
    }
  }

  return -1
}

/**
 * Find the last history row that is safely before the supplied challenge boundary.
 * When the current challenge already has a history row, the immediately preceding row is
 * authoritative. Otherwise the unified history table only exposes eventDate ordering, so
 * same-day rows without the current challenge checkpoint are excluded conservatively.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {Object} challengeEntry rerate challenge metadata
 * @returns {number} index of the latest pre-challenge history row or -1
 */
function findHistorySeedIndexForChallenge (historyRows, challengeEntry) {
  if (!Array.isArray(historyRows) || historyRows.length === 0) {
    return -1
  }

  const currentChallengeIndex = findHistoryIndexForChallenge(historyRows, challengeEntry.challengeId)
  if (currentChallengeIndex >= 0) {
    return currentChallengeIndex - 1
  }

  const challengeEventDate = challengeEntry && challengeEntry.eventDate
  if (!challengeEventDate) {
    return -1
  }

  const challengeTimestamp = challengeEventDate.getTime()
  let seedIndex = -1

  for (let index = 0; index < historyRows.length; index += 1) {
    if (historyRows[index].eventDate.getTime() < challengeTimestamp) {
      seedIndex = index
      continue
    }

    break
  }

  return seedIndex
}

/**
 * Compute the highest historical rating reached through the supplied history index.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {number} endIndex inclusive history index bound
 * @returns {number} maximum rating through that bound
 */
function computeMaxRatingFromHistory (historyRows, endIndex) {
  if (!Array.isArray(historyRows) || endIndex < 0) {
    return 0
  }

  let maxRating = 0

  for (let index = 0; index <= endIndex; index += 1) {
    const rating = Number(historyRows[index].newRating)
    if (Number.isFinite(rating) && rating > maxRating) {
      maxRating = rating
    }
  }

  return maxRating
}

/**
 * Normalize challenge track/type names into comparison-friendly enum-like values.
 * @param {*} value challenge dimension value
 * @returns {string} normalized uppercase identifier with underscores
 */
function normalizeChallengeDimension (value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, '_')
}

/**
 * Parse a loosely-typed boolean-like value.
 * @param {*} value candidate value
 * @returns {boolean|undefined} parsed boolean or undefined when indeterminate
 */
function parseBooleanLike (value) {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (normalized === 'true') {
      return true
    }
    if (normalized === 'false') {
      return false
    }
  }

  return undefined
}

/**
 * Resolve whether a challenge should be treated as rated.
 * @param {Object} challenge challenge metadata record
 * @returns {boolean} true when the challenge is rated or rating status is unspecified
 */
function isChallengeRated (challenge) {
  if (!challenge) {
    return false
  }

  const directRated = parseBooleanLike(challenge.isRated)
  if (directRated !== undefined) {
    return directRated
  }

  const legacyRated = parseBooleanLike(challenge.rated)
  if (legacyRated !== undefined) {
    return legacyRated
  }

  if (!Array.isArray(challenge.metadata)) {
    return true
  }

  for (const entry of challenge.metadata) {
    const name = normalizeChallengeDimension(entry && entry.name)
    const value = parseBooleanLike(entry && entry.value)

    if (value === undefined) {
      continue
    }

    if (name === 'UNRATED') {
      return !value
    }

    if (name === 'RATED' || name === 'ISRATED' || name === 'IS_RATED') {
      return value
    }
  }

  return true
}

/**
 * Normalize one Marathon Match score for Qubits ordering.
 * Relative-scoring aggregates are already emitted as higher-is-better, while
 * non-relative MINIMIZE challenges need inversion to match standings order.
 * @param {Object} row participant result row
 * @param {Object} scoringConfig relative scoring configuration for the challenge
 * @returns {number} normalized Qubits score
 */
function normalizeScore (row, scoringConfig) {
  const aggregateScore = Number(row.aggregateScore)
  if (!Number.isFinite(aggregateScore)) {
    return 0
  }

  if (
    scoringConfig &&
    scoringConfig.relativeScoringEnabled === false &&
    scoringConfig.scoreDirection === SCORE_DIRECTION_MINIMIZE
  ) {
    return -aggregateScore
  }

  return aggregateScore
}

/**
 * Fetch all final Marathon Match system results for the target member.
 * The reviewSummation table is submission-scoped, so the latest final summation
 * per submission is selected before replay history is built.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {BigInt} userId target member identifier
 * @returns {Promise<Array<Object>>} ordered result rows for the member
 */
async function fetchMmResultsForUser (reviewDbClient, userId) {
  const result = await reviewDbClient.query(
    `
      WITH "latestSubmissionSummation" AS (
        SELECT
          rs."submissionId",
          s."memberId",
          s."challengeId",
          rs."aggregateScore",
          rs."reviewedDate",
          rs."createdAt",
          CASE
            WHEN jsonb_typeof(rs."metadata") = 'object' AND rs."metadata" ? 'rated'
              THEN rs."metadata"->>'rated'
            WHEN jsonb_typeof(rs."metadata") = 'object' AND rs."metadata" ? 'isRated'
              THEN rs."metadata"->>'isRated'
            ELSE NULL
          END AS "rated",
          ROW_NUMBER() OVER (
            PARTITION BY rs."submissionId"
            ORDER BY
              COALESCE(rs."reviewedDate", rs."createdAt") DESC NULLS LAST,
              rs."createdAt" DESC,
              rs."aggregateScore" DESC
          ) AS "summationRank"
        FROM "reviewSummation" rs
        INNER JOIN "submission" s
          ON s."id" = rs."submissionId"
        WHERE s."memberId" = $1
          AND rs."isFinal" = true
      )
      SELECT
        "submissionId",
        "memberId",
        "challengeId",
        "aggregateScore",
        "reviewedDate",
        "createdAt",
        "rated"
      FROM "latestSubmissionSummation"
      WHERE "summationRank" = 1
      ORDER BY COALESCE("reviewedDate", "createdAt") ASC, "submissionId" ASC
    `,
    [String(userId)]
  )

  return result.rows
}

/**
 * Fetch all challenge participants using the latest scored submission per member.
 * Relative scoring config is loaded from the Marathon Match database for score normalization.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Object} mmDbClient prisma Marathon Match client
 * @param {string} challengeId challenge identifier
 * @returns {Promise<Object>} participant rows and scoring config for the challenge
 */
async function fetchMmParticipantsForChallenge (reviewDbClient, mmDbClient, challengeId) {
  const result = await reviewDbClient.query(
    `
      WITH "latestSubmissionSummation" AS (
        SELECT
          rs."submissionId",
          s."memberId",
          s."challengeId",
          s."createdAt" AS "submissionCreatedAt",
          rs."aggregateScore",
          rs."reviewedDate",
          rs."createdAt",
          ROW_NUMBER() OVER (
            PARTITION BY rs."submissionId"
            ORDER BY
              COALESCE(rs."reviewedDate", rs."createdAt") DESC NULLS LAST,
              rs."createdAt" DESC,
              rs."aggregateScore" DESC
          ) AS "summationRank"
        FROM "reviewSummation" rs
        INNER JOIN "submission" s
          ON s."id" = rs."submissionId"
        WHERE s."challengeId" = $1
          AND s."memberId" IS NOT NULL
          AND rs."isFinal" = true
      ),
      "latestMemberSubmission" AS (
        SELECT
          "submissionId",
          "memberId",
          "challengeId",
          "aggregateScore",
          "reviewedDate",
          "createdAt",
          "submissionCreatedAt",
          ROW_NUMBER() OVER (
            PARTITION BY "memberId"
            ORDER BY "submissionCreatedAt" DESC, "submissionId" DESC
          ) AS "memberRank"
        FROM "latestSubmissionSummation"
        WHERE "summationRank" = 1
      )
      SELECT
        "submissionId",
        "memberId",
        "challengeId",
        "aggregateScore",
        "reviewedDate",
        "createdAt",
        "submissionCreatedAt"
      FROM "latestMemberSubmission"
      WHERE "memberRank" = 1
      ORDER BY "submissionCreatedAt" ASC, "submissionId" ASC
    `,
    [String(challengeId)]
  )

  const config = await mmDbClient.marathonMatchConfig.findUnique({
    where: {
      challengeId: String(challengeId)
    },
    select: {
      relativeScoringEnabled: true,
      scoreDirection: true
    }
  })

  return {
    participantRows: result.rows,
    scoringConfig: {
      relativeScoringEnabled: config ? config.relativeScoringEnabled !== false : true,
      scoreDirection: config ? config.scoreDirection : 'MAXIMIZE'
    }
  }
}

/**
 * Load challenge metadata required for MM rerating from challenge-api.
 * Rated metadata is loaded defensively from challenge metadata key/value rows
 * when no top-level rated flag is available.
 * @param {Object} challengeClient prisma challenge client
 * @param {Array<string>} challengeIds challenge identifiers
 * @returns {Promise<Map<string, Object>>} challenge metadata keyed by challenge id
 */
async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  if (!challengeIds || challengeIds.length === 0) {
    return new Map()
  }

  const challenges = await challengeClient.challenge.findMany({
    where: {
      id: {
        in: challengeIds
      }
    },
    select: {
      id: true,
      endDate: true,
      track: {
        select: {
          name: true
        }
      },
      type: {
        select: {
          name: true
        }
      },
      metadata: {
        where: {
          name: {
            in: ['rated', 'isRated', 'unrated']
          }
        },
        select: {
          name: true,
          value: true
        }
      }
    }
  })

  return new Map(challenges.map((challenge) => [challenge.id, challenge]))
}

/**
 * Build the ordered rerate history for the target member.
 * Multiple submission-level rows for one challenge are collapsed to the latest
 * final system result so the rating pass replays each challenge only once.
 * @param {Array<Object>} mmResultRows submission-level MM result rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by id
 * @returns {Array<Object>} ordered challenge history entries for rerating
 */
function buildTargetHistory (mmResultRows, challengeMetadataById) {
  const historyByChallengeId = new Map()

  mmResultRows.forEach((row) => {
    const rowRated = parseBooleanLike(row.rated)
    if (rowRated === false) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.endDate) {
      return
    }

    if (!challenge.track || !challenge.type) {
      return
    }

    if (!isChallengeRated(challenge)) {
      return
    }

    if (normalizeChallengeDimension(challenge.track.name) !== CHALLENGE_TRACK_NAME ||
      normalizeChallengeDimension(challenge.type.name) !== CHALLENGE_TYPE_NAME) {
      return
    }

    const scoreDate = normalizeDate(row.reviewedDate, row.createdAt)
    const eventDate = normalizeDate(challenge.endDate, scoreDate)
    if (!eventDate || !scoreDate) {
      return
    }

    historyByChallengeId.set(String(row.challengeId), {
      challengeId: String(row.challengeId),
      createdAt: scoreDate,
      endDate: normalizeDate(challenge.endDate, scoreDate),
      eventDate
    })
  })

  const history = Array.from(historyByChallengeId.values())

  history.sort((left, right) => {
    const leftEventDate = left.eventDate ? left.eventDate.getTime() : 0
    const rightEventDate = right.eventDate ? right.eventDate.getTime() : 0
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    const leftCreatedAt = left.createdAt ? left.createdAt.getTime() : 0
    const rightCreatedAt = right.createdAt ? right.createdAt.getTime() : 0
    if (leftCreatedAt !== rightCreatedAt) {
      return leftCreatedAt - rightCreatedAt
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return history
}

/**
 * Cache the unified history timeline for participants needed during the rerate span.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds participant identifiers
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 * @returns {Promise<void>} resolves when the requested history rows are cached
 */
async function loadParticipantHistoryCache (membersClient, participantIds, historyByUserId, dimensionIds) {
  const idsToLoad = participantIds.filter((participantId) => !historyByUserId.has(buildUserStateKey(participantId)))
  if (idsToLoad.length === 0) {
    return
  }

  const historyRows = await membersClient.memberStatsHistory.findMany({
    where: {
      userId: {
        in: idsToLoad
      },
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ userId: 'asc' }, { eventDate: 'asc' }, { id: 'asc' }],
    select: {
      id: true,
      userId: true,
      challengeId: true,
      newRating: true,
      eventDate: true
    }
  })

  idsToLoad.forEach((participantId) => {
    historyByUserId.set(buildUserStateKey(participantId), [])
  })

  historyRows.forEach((row) => {
    const stateKey = buildUserStateKey(row.userId)
    const cachedRows = historyByUserId.get(stateKey)
    const eventDate = normalizeDate(row.eventDate, row.eventDate)

    if (!cachedRows || !eventDate) {
      return
    }

    cachedRows.push({
      id: row.id,
      challengeId: String(row.challengeId),
      newRating: row.newRating,
      eventDate
    })
  })
}

/**
 * Build per-challenge participant seed states from authoritative history rows.
 * The target user's in-memory state is taken from the rerate replay so later challenges
 * in the rerate span see prior rerated values, while every other participant is loaded
 * fresh from history as of the current challenge boundary.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds challenge participant identifiers
 * @param {BigInt} targetUserId target member id
 * @param {Object} targetState current rerated target state
 * @param {Object} challengeEntry rerate challenge metadata
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 * @returns {Promise<Map<string, Object>>} seed state keyed by participant id
 */
async function loadParticipantStates (membersClient, participantIds, targetUserId, targetState, challengeEntry, historyByUserId, dimensionIds) {
  const targetStateKey = buildUserStateKey(targetUserId)
  const stateByUserId = new Map()
  const nonTargetParticipantIds = participantIds.filter((participantId) => buildUserStateKey(participantId) !== targetStateKey)

  if (nonTargetParticipantIds.length > 0) {
    await loadParticipantHistoryCache(membersClient, nonTargetParticipantIds, historyByUserId, dimensionIds)
  }

  participantIds.forEach((participantId) => {
    const stateKey = buildUserStateKey(participantId)
    if (stateKey === targetStateKey) {
      stateByUserId.set(stateKey, cloneState(targetState))
      return
    }

    const historyRows = historyByUserId.get(stateKey) || []
    const seedIndex = findHistorySeedIndexForChallenge(historyRows, challengeEntry)
    stateByUserId.set(stateKey, createHistorySeedState(historyRows, seedIndex))
  })

  return stateByUserId
}

/**
 * Insert or update the member's history checkpoint for one rerated challenge.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {string} challengeId challenge identifier
 * @param {number|null} oldRating rating before the challenge
 * @param {number} newRating rating after the challenge
 * @param {Date} eventDate challenge event date
 * @returns {Promise<void>} resolves when the history row is written
 */
async function upsertHistoryRow (tx, userId, challengeId, oldRating, newRating, eventDate, dimensionIds) {
  const existingHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      challengeId
    },
    select: {
      id: true
    }
  })

  if (existingHistory) {
    await tx.memberStatsHistory.update({
      where: {
        id: existingHistory.id
      },
      data: {
        oldRating,
        newRating,
        eventDate,
        updatedBy: RERATE_ACTOR
      }
    })
    return
  }

  await tx.memberStatsHistory.create({
    data: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      challengeId,
      mostRecent: false,
      oldRating,
      newRating,
      eventDate,
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    }
  })
}

/**
 * Decide whether a rerated peak should replace the stored global max rating row.
 * @param {Object|null} existingMaxRating current memberMaxRating row
 * @param {number} rating rerated peak rating for this track
 * @returns {boolean} true when the rerated peak should replace the stored row
 */
function shouldReplaceGlobalMaxRating (existingMaxRating, rating) {
  if (!existingMaxRating) {
    return true
  }

  const storedRating = Number(existingMaxRating.rating)
  return !Number.isFinite(storedRating) || rating > storedRating
}

/**
 * Persist the member's global max rating row when the rerated MM peak exceeds it.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {number} rating recomputed peak rating
 * @returns {Promise<void>} resolves when max rating is stored
 */
async function updateMaxRating (tx, userId, rating) {
  const existingMaxRating = await tx.memberMaxRating.findFirst({
    where: {
      userId
    },
    select: {
      rating: true
    }
  })

  if (!shouldReplaceGlobalMaxRating(existingMaxRating, rating)) {
    return
  }

  await tx.memberMaxRating.upsert({
    where: {
      userId
    },
    create: {
      userId,
      rating,
      track: TRACK_NAME,
      subTrack: TYPE_NAME,
      ratingColor: getRatingColor(rating),
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    },
    update: {
      rating,
      track: TRACK_NAME,
      subTrack: TYPE_NAME,
      ratingColor: getRatingColor(rating),
      updatedBy: RERATE_ACTOR
    }
  })
}

/**
 * Mark the newest MM history row as mostRecent and align its rating snapshot.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @returns {Promise<void>} resolves when history flags are refreshed
 */
async function refreshMostRecentHistoryFlag (tx, userId, dimensionIds) {
  await tx.memberStatsHistory.updateMany({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      mostRecent: true
    },
    data: {
      mostRecent: false,
      updatedBy: RERATE_ACTOR
    }
  })

  const latestHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    select: {
      id: true
    }
  })

  if (!latestHistory) {
    return
  }

  const currentStats = await tx.memberStats.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    select: {
      rating: true
    }
  })

  const previousHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    skip: 1,
    select: {
      newRating: true
    }
  })

  const latestUpdate = {
    mostRecent: true,
    oldRating: previousHistory ? previousHistory.newRating : null,
    updatedBy: RERATE_ACTOR
  }

  if (currentStats && currentStats.rating !== null) {
    latestUpdate.newRating = currentStats.rating
  }

  await tx.memberStatsHistory.update({
    where: {
      id: latestHistory.id
    },
    data: latestUpdate
  })
}

/**
 * Re-rate one member's Marathon Match timeline beginning at the requested challenge.
 * Each challenge is replayed forward with the latest final MM system score per participant.
 * @param {Object} membersClient prisma members client
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} mmDbClient prisma Marathon Match client
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|number|BigInt} userId target member identifier
 * @param {string|number} fromChallengeId starting challenge identifier
 * @returns {Promise<Object>} rerate summary counts
 * @throws {Error} when required review or MM database connections are missing
 * @throws {errors.BadRequestError} when the start challenge is not a rated MM event for the member
 */
async function rerateMmTrack (membersClient, challengeClient, mmDbClient, reviewDbClient, userId, fromChallengeId) {
  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate marathon match stats')
  }

  if (!mmDbClient) {
    throw new Error('MM_DB_URL must be configured to rerate marathon match stats')
  }

  const normalizedUserId = toBigIntUserId(userId)
  const mmResultRows = await fetchMmResultsForUser(reviewDbClient, normalizedUserId)
  if (mmResultRows.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengeClient,
    Array.from(new Set(mmResultRows.map((row) => String(row.challengeId))))
  )

  const targetHistory = buildTargetHistory(mmResultRows, challengeMetadataById)
  if (targetHistory.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  const dimensionIds = await resolveUnifiedDimensionIds(challengeClient)

  let startIndex = 0
  if (fromChallengeId) {
    startIndex = targetHistory.findIndex((entry) => entry.challengeId === String(fromChallengeId))
    if (startIndex < 0) {
      throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${TYPE_NAME} event for this member`)
    }
  }

  const participantHistoryByUserId = new Map()
  let targetState = createDefaultState()
  let recomputedMaxRating = 0

  if (startIndex > 0) {
    await loadParticipantHistoryCache(membersClient, [normalizedUserId], participantHistoryByUserId, dimensionIds)

    const targetHistoryRows = participantHistoryByUserId.get(buildUserStateKey(normalizedUserId)) || []
    let startSeedIndex = findHistoryIndexForChallenge(targetHistoryRows, targetHistory[startIndex - 1].challengeId)

    if (startSeedIndex < 0) {
      startSeedIndex = findHistorySeedIndexForChallenge(targetHistoryRows, targetHistory[startIndex])
    }

    targetState = createHistorySeedState(targetHistoryRows, startSeedIndex)
    recomputedMaxRating = computeMaxRatingFromHistory(targetHistoryRows, startSeedIndex)
  }

  let challengesProcessed = 0
  let ratingsUpdated = 0

  for (let index = startIndex; index < targetHistory.length; index += 1) {
    const historyEntry = targetHistory[index]
    const { participantRows, scoringConfig } = await fetchMmParticipantsForChallenge(
      reviewDbClient,
      mmDbClient,
      historyEntry.challengeId
    )
    if (participantRows.length === 0) {
      continue
    }

    const participantIds = participantRows.map((row) => toBigIntUserId(row.memberId))
    const stateByUserId = await loadParticipantStates(
      membersClient,
      participantIds,
      normalizedUserId,
      targetState,
      historyEntry,
      participantHistoryByUserId,
      dimensionIds
    )

    const targetStateBeforeRun = cloneState(targetState)
    const participants = participantRows.map((row) => {
      const participantUserId = toBigIntUserId(row.memberId)
      const participantState = stateByUserId.get(buildUserStateKey(participantUserId)) || createDefaultState()

      return {
        coderId: String(participantUserId),
        rating: participantState.rating,
        volatility: participantState.volatility,
        numRatings: participantState.numRatings,
        score: normalizeScore(row, scoringConfig)
      }
    })

    runQubitsRating(participants)

    const updatedTarget = participants.find((participant) => participant.coderId === String(normalizedUserId))
    if (!updatedTarget) {
      continue
    }

    targetState = cloneState(updatedTarget)
    challengesProcessed += 1
    ratingsUpdated += 1
    recomputedMaxRating = Math.max(recomputedMaxRating, updatedTarget.rating)

    await membersClient.$transaction(async (tx) => {
      await tx.memberStats.upsert({
        where: {
          userId_trackId_typeId: {
            userId: normalizedUserId,
            trackId: dimensionIds.trackId,
            typeId: dimensionIds.typeId
          }
        },
        create: {
          userId: normalizedUserId,
          trackId: dimensionIds.trackId,
          typeId: dimensionIds.typeId,
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          createdBy: RERATE_ACTOR,
          updatedBy: RERATE_ACTOR
        },
        update: {
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          updatedBy: RERATE_ACTOR
        }
      })

      await upsertHistoryRow(
        tx,
        normalizedUserId,
        historyEntry.challengeId,
        targetStateBeforeRun.numRatings > 0 ? targetStateBeforeRun.rating : null,
        updatedTarget.rating,
        historyEntry.eventDate,
        dimensionIds
      )
    })
  }

  if (ratingsUpdated > 0) {
    await membersClient.$transaction(async (tx) => {
      await refreshMostRecentHistoryFlag(tx, normalizedUserId, dimensionIds)
      await updateMaxRating(tx, normalizedUserId, recomputedMaxRating)
    })
  }

  return {
    challengesProcessed,
    ratingsUpdated
  }
}

module.exports = {
  rerateMmTrack
}

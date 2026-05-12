/**
 * Re-rate unified member stats for rated DATA_SCIENCE / MARATHON_MATCH results.
 *
 * The engine reads final reviewSummation rows from review-api for Marathon
 * Match rerates. Configured rating paths also include tagged or skilled
 * Development / Challenge rows from challengeResult. In both cases it resolves challenge
 * metadata from challenge-api, applies the Qubits math one challenge at a time,
 * and persists the target member's rating state and placement metadata into
 * memberStats, memberStatsHistory, and memberMaxRating.
 */

'use strict'

const errors = require('../common/errors')
const {
  resolveChallengeResultRelation,
  resolveReviewDbRelation
} = require('../common/reviewDbHelper')
const {
  clearChallengeDimensionLookupCache,
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup
} = require('../common/statsDimensionHelper')
const { runQubitsRating, getRatingColor, DEFAULT_VOLATILITY } = require('./qubitsAlgorithm')
const {
  RATING_METADATA_SELECT,
  isChallengeRated
} = require('./challengeRatingStatus')
const {
  challengeMatchesRatingPath,
  normalizeRatingPathName
} = require('./ratingPathConfig')

const TRACK_NAME = 'DATA_SCIENCE'
const TYPE_NAME = 'MARATHON_MATCH'
const CHALLENGE_TYPE_NAME = 'MARATHON_MATCH'
const DEVELOPMENT_CHALLENGE_TRACK_NAME = 'DEVELOPMENT'
const DEVELOPMENT_CHALLENGE_TYPE_NAME = 'Challenge'
const RERATE_ACTOR = 'rerate-mm-stats'
const SCORE_DIRECTION_MINIMIZE = 'MINIMIZE'
const RATING_PATH_SOURCE_DEVELOPMENT = 'DEVELOPMENT_CHALLENGE'
const RATING_PATH_SOURCE_MARATHON_MATCH = 'MARATHON_MATCH'
const DEFAULT_MM_SCORING_CONFIG = Object.freeze({
  relativeScoringEnabled: true,
  scoreDirection: 'MAXIMIZE'
})

/**
 * Build the default Marathon Match scoring configuration used for historical
 * reviewSummation replay. Historical data is sourced from review-api only, and
 * aggregateScore is treated as the already-normalized higher-is-better result.
 * @returns {Object} scoring configuration for MM participant normalization
 */
function getDefaultMmScoringConfig () {
  return {
    relativeScoringEnabled: DEFAULT_MM_SCORING_CONFIG.relativeScoringEnabled,
    scoreDirection: DEFAULT_MM_SCORING_CONFIG.scoreDirection
  }
}

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
 * Add a non-empty challenge identifier candidate to the supplied set.
 * @param {Set<string>} candidates target candidate set
 * @param {*} value challenge identifier value
 * @returns {void}
 */
function addChallengeIdCandidate (candidates, value) {
  if (value === null || value === undefined) {
    return
  }

  const normalized = String(value).trim()
  if (normalized) {
    candidates.add(normalized)
  }
}

/**
 * Build all review-database challenge identifiers known for a challenge entry.
 * Challenge-api exposes canonical UUIDs while migrated review rows may still
 * store legacy numeric ids, so replay queries need to try each stable alias.
 * @param {*} challengeRef challenge id, challenge metadata row, or history entry
 * @returns {Array<string>} unique challenge id candidates
 */
function buildChallengeIdCandidates (challengeRef) {
  const candidates = new Set()

  if (Array.isArray(challengeRef)) {
    challengeRef.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    return Array.from(candidates)
  }

  if (challengeRef && typeof challengeRef === 'object' && !(challengeRef instanceof Date)) {
    addChallengeIdCandidate(candidates, challengeRef.challengeId)
    addChallengeIdCandidate(candidates, challengeRef.id)
    addChallengeIdCandidate(candidates, challengeRef.legacyId)

    if (Array.isArray(challengeRef.reviewChallengeIds)) {
      challengeRef.reviewChallengeIds.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (Array.isArray(challengeRef.challengeIds)) {
      challengeRef.challengeIds.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (Array.isArray(challengeRef.challengeIdCandidates)) {
      challengeRef.challengeIdCandidates.forEach((candidate) => addChallengeIdCandidate(candidates, candidate))
    }
    if (challengeRef.legacyRecord) {
      addChallengeIdCandidate(candidates, challengeRef.legacyRecord.legacySystemId)
    }

    return Array.from(candidates)
  }

  addChallengeIdCandidate(candidates, challengeRef)
  return Array.from(candidates)
}

/**
 * Merge challenge metadata aliases with an optional source row id.
 * @param {Object} challenge challenge-api metadata row
 * @param {*} [sourceChallengeId] challenge id observed in review data
 * @returns {Array<string>} unique review challenge id candidates
 */
function buildReviewChallengeIds (challenge, sourceChallengeId) {
  const candidates = new Set()
  addChallengeIdCandidate(candidates, sourceChallengeId)
  buildChallengeIdCandidates(challenge).forEach((candidate) => candidates.add(candidate))
  return Array.from(candidates)
}

/**
 * Test whether a history entry can be addressed by the supplied challenge id.
 * @param {Object} historyEntry rerate history entry
 * @param {*} challengeId requested challenge id
 * @returns {boolean} true when the entry has the challenge id as an alias
 */
function historyEntryMatchesChallengeId (historyEntry, challengeId) {
  const normalizedChallengeId = String(challengeId).trim()
  return buildChallengeIdCandidates(historyEntry).includes(normalizedChallengeId)
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
 * Build the deterministic ChallengeType id for a configured rating path.
 * The unified stats tables enforce ChallengeType foreign keys, so custom rating
 * names need stable dimension rows before memberStats can reference them.
 * @param {Object} ratingPath normalized rating path config
 * @returns {string} deterministic custom ChallengeType id
 */
function buildRatingPathTypeId (ratingPath) {
  const slug = normalizeRatingPathName(ratingPath && ratingPath.name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')

  return `rating-path-${slug || 'custom'}`
}

/**
 * Find an existing ChallengeType row matching a configured rating path name.
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} ratingPath normalized rating path config
 * @returns {Promise<string|undefined>} ChallengeType id when found
 */
async function findRatingPathTypeId (challengeClient, ratingPath) {
  const normalizedName = normalizeRatingPathName(ratingPath && ratingPath.name)
  if (!normalizedName || typeof challengeClient.$queryRawUnsafe !== 'function') {
    return undefined
  }

  const rows = await challengeClient.$queryRawUnsafe(
    `
      SELECT "id"
      FROM "ChallengeType"
      WHERE UPPER("name") = $1
         OR UPPER("abbreviation") = $1
      ORDER BY CASE
        WHEN UPPER("name") = $1 THEN 0
        ELSE 1
      END
      LIMIT 1
    `,
    normalizedName
  )

  return rows && rows[0] ? String(rows[0].id) : undefined
}

/**
 * Create or reuse the ChallengeType row needed by a configured rating path.
 * The insert is deterministic and idempotent so concurrent rerate runs converge
 * on the same custom type id.
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} ratingPath normalized rating path config
 * @returns {Promise<string>} ChallengeType id for the rating path
 * @throws {Error} when the challenge client cannot provision custom dimensions
 */
async function ensureRatingPathTypeId (challengeClient, ratingPath) {
  const existingTypeId = await findRatingPathTypeId(challengeClient, ratingPath)
  if (existingTypeId) {
    return existingTypeId
  }

  if (typeof challengeClient.$queryRawUnsafe !== 'function') {
    throw new Error(`Unable to provision ChallengeType for rating path '${ratingPath.name}'`)
  }

  const typeId = buildRatingPathTypeId(ratingPath)
  const rows = await challengeClient.$queryRawUnsafe(
    `
      INSERT INTO "ChallengeType" (
        "id",
        "name",
        "description",
        "isActive",
        "isTask",
        "abbreviation",
        "createdAt",
        "createdBy",
        "updatedAt",
        "updatedBy"
      )
      VALUES ($1, $2, $3, true, false, $4, NOW(), $5, NOW(), $5)
      ON CONFLICT ("id") DO UPDATE SET
        "name" = EXCLUDED."name",
        "description" = EXCLUDED."description",
        "abbreviation" = EXCLUDED."abbreviation",
        "updatedAt" = NOW(),
        "updatedBy" = EXCLUDED."updatedBy"
      RETURNING "id"
    `,
    typeId,
    ratingPath.name,
    `Configured rating path: ${ratingPath.name}`,
    ratingPath.name,
    RERATE_ACTOR
  )

  clearChallengeDimensionLookupCache()
  return rows && rows[0] ? String(rows[0].id) : typeId
}

/**
 * Resolve the unified track/type identifiers used for MM rating rows.
 * Configured rating paths use the configured destination track and a custom
 * ChallengeType row named after the rating path.
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} ratingPath normalized rating path config
 * @returns {Promise<{trackId: string, typeId: string, trackName: string, typeName: string}>} resolved unified ids
 */
async function resolveUnifiedDimensionIds (challengeClient, ratingPath) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const trackName = ratingPath ? ratingPath.trackName : TRACK_NAME
  const trackId = resolveTrackIdFromLookup(dimensionLookup, trackName)
  const typeId = ratingPath
    ? (resolveTypeIdFromLookup(dimensionLookup, ratingPath.name) || await ensureRatingPathTypeId(challengeClient, ratingPath))
    : resolveTypeIdFromLookup(dimensionLookup, TYPE_NAME)

  if (!trackId || !typeId) {
    throw new Error(`Unable to resolve unified dimension ids for ${trackName}/${ratingPath ? ratingPath.name : TYPE_NAME}`)
  }

  return {
    trackId,
    typeId,
    trackName,
    typeName: ratingPath ? ratingPath.name : TYPE_NAME
  }
}

/**
 * Build a rerate seed state from the latest authoritative history row before a challenge.
 * Historical volatility is used when available. Older history rows that predate
 * volatility checkpoints fall back to the default Qubits volatility.
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
    volatility: Number.isFinite(Number(historyRows[seedIndex].newVolatility))
      ? Number(historyRows[seedIndex].newVolatility)
      : DEFAULT_VOLATILITY,
    numRatings: seedIndex + 1
  }
}

/**
 * Find the history row index for a challenge within one participant timeline.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {string|Object} challengeId challenge identifier or history entry with aliases
 * @returns {number} index of the matching row or -1 when absent
 */
function findHistoryIndexForChallenge (historyRows, challengeId) {
  const challengeIds = new Set(buildChallengeIdCandidates(challengeId))

  for (let index = historyRows.length - 1; index >= 0; index -= 1) {
    if (challengeIds.has(String(historyRows[index].challengeId))) {
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

  const currentChallengeIndex = findHistoryIndexForChallenge(historyRows, challengeEntry)
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
 * Resolve whether challenge metadata belongs to the MM scoring source.
 * Marathon Match is identified by type because some imported challenge rows
 * carry the Development track while still belonging to the MM rating stream.
 * @param {Object} challenge challenge metadata record
 * @returns {boolean} true when the challenge uses the MM rating source
 */
function isMarathonMatchChallenge (challenge) {
  return !!(
    challenge &&
    challenge.type &&
    normalizeChallengeDimension(challenge.type.name) === CHALLENGE_TYPE_NAME
  )
}

/**
 * Resolve whether challenge metadata belongs to the Development Challenge source.
 * @param {Object} challenge challenge metadata record
 * @returns {boolean} true when the challenge uses the Development Challenge rating source
 */
function isDevelopmentChallenge (challenge) {
  return !!(
    challenge &&
    challenge.track &&
    challenge.type &&
    normalizeChallengeDimension(challenge.track.name) === DEVELOPMENT_CHALLENGE_TRACK_NAME &&
    normalizeChallengeDimension(challenge.type.name) === normalizeChallengeDimension(DEVELOPMENT_CHALLENGE_TYPE_NAME)
  )
}

/**
 * Resolve the supported rating source for a tagged challenge path event.
 * @param {Object} challenge challenge metadata record
 * @returns {string|null} rating path source name or null when unsupported
 */
function resolveRatingPathSource (challenge) {
  if (isDevelopmentChallenge(challenge)) {
    return RATING_PATH_SOURCE_DEVELOPMENT
  }

  if (isMarathonMatchChallenge(challenge)) {
    return RATING_PATH_SOURCE_MARATHON_MATCH
  }

  return null
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
 * Normalize one Development Challenge score for Qubits ordering.
 * @param {Object} row challengeResult participant row
 * @returns {number} normalized Qubits score
 */
function normalizeDevelopmentScore (row) {
  const finalScore = Number(row.finalScore)
  if (Number.isFinite(finalScore)) {
    return finalScore
  }

  const placement = Number(row.placement)
  if (Number.isFinite(placement)) {
    return -placement
  }

  return 0
}

/**
 * Resolve whether a Development Challenge participant should count toward rerating.
 * @param {Object} row challengeResult participant row
 * @returns {boolean} true when the participant has a usable score or placement
 */
function isDevelopmentParticipantEligibleForRating (row) {
  const finalScore = Number(row && row.finalScore)
  if (Number.isFinite(finalScore)) {
    return true
  }

  const placement = Number(row && row.placement)
  return Number.isInteger(placement) && placement > 0
}

/**
 * Normalize a placement value so only positive integer ranks are persisted.
 * @param {*} value raw source placement value
 * @returns {number|undefined} positive placement or undefined when unavailable
 */
function toOptionalPlacement (value) {
  const placement = Number(value)
  return Number.isInteger(placement) && placement > 0 ? placement : undefined
}

/**
 * Compute deterministic competition placements from participant scores.
 * @param {Array<Object>} participants Qubits participants containing coderId and score
 * @returns {Map<string, number>} placement keyed by coder id
 */
function computePlacementByCoderId (participants) {
  const placementByCoderId = new Map()
  const sortedParticipants = (participants || []).slice().sort((left, right) => {
    const leftScore = Number.isFinite(Number(left.score)) ? Number(left.score) : 0
    const rightScore = Number.isFinite(Number(right.score)) ? Number(right.score) : 0
    if (leftScore !== rightScore) {
      return rightScore - leftScore
    }

    return String(left.coderId).localeCompare(String(right.coderId))
  })

  let currentPlacement = 0
  let previousScore
  sortedParticipants.forEach((participant, index) => {
    const score = Number(participant.score)
    if (index === 0 || score !== previousScore) {
      currentPlacement = index + 1
      previousScore = score
    }

    placementByCoderId.set(String(participant.coderId), currentPlacement)
  })

  return placementByCoderId
}

/**
 * Build optional history metadata for the target participant.
 * Source-provided placements are preferred; score ordering is the fallback for
 * historical MM submissions that do not expose a stored placement.
 * @param {Array<Object>} participants rated participant states for one challenge
 * @param {string} targetUserKey normalized target user id
 * @param {Object} sourceRow source participant row for the target user
 * @returns {Object} persisted memberStatsHistory metadata
 */
function buildHistoryResultFields (participants, targetUserKey, sourceRow) {
  const sourcePlacement = toOptionalPlacement(sourceRow && sourceRow.placement)
  if (sourcePlacement) {
    return {
      placement: sourcePlacement
    }
  }

  const computedPlacement = computePlacementByCoderId(participants).get(String(targetUserKey))
  return omitUndefinedFields({
    placement: computedPlacement
  })
}

/**
 * Fetch all final Marathon Match system results for the target member.
 * The reviewSummation table is submission-scoped, so the latest final summation
 * per submission is selected before replay history is built. Some imported MM
 * summations have a null isFinal flag; those are treated as final unless the
 * row is explicitly marked false.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {BigInt} userId target member identifier
 * @returns {Promise<Array<Object>>} ordered result rows for the member
 */
async function fetchMmResultsForUser (reviewDbClient, userId) {
  const reviewSummationRelation = await resolveReviewDbRelation(reviewDbClient, 'reviewSummation')
  const submissionRelation = await resolveReviewDbRelation(reviewDbClient, 'submission')
  const result = await reviewDbClient.query(
    `
      WITH "latestSubmissionSummation" AS (
        SELECT
          rs."submissionId",
          s."memberId",
          COALESCE(s."challengeId", s."legacyChallengeId"::text) AS "challengeId",
          s."legacyChallengeId",
          s."placement",
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
        FROM ${reviewSummationRelation} rs
        INNER JOIN ${submissionRelation} s
          ON s."id" = rs."submissionId"
        WHERE s."memberId" = $1
          AND (s."challengeId" IS NOT NULL OR s."legacyChallengeId" IS NOT NULL)
          AND rs."isFinal" IS NOT FALSE
      )
      SELECT
        "submissionId",
        "memberId",
        "challengeId",
        "legacyChallengeId",
        "placement",
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
 * Historical Marathon Match data is read from review-api only; reviewSummation
 * aggregateScore is treated as already normalized for higher-is-better ordering.
 * Both submission.challengeId and submission.legacyChallengeId are considered.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|Object} challengeId challenge identifier or history entry with legacy aliases
 * @returns {Promise<Object>} participant rows and scoring config for the challenge
 */
async function fetchMmParticipantsForChallenge (reviewDbClient, challengeId) {
  const challengeIds = buildChallengeIdCandidates(challengeId)
  if (challengeIds.length === 0) {
    return {
      participantRows: [],
      scoringConfig: getDefaultMmScoringConfig()
    }
  }

  const reviewSummationRelation = await resolveReviewDbRelation(reviewDbClient, 'reviewSummation')
  const submissionRelation = await resolveReviewDbRelation(reviewDbClient, 'submission')
  const result = await reviewDbClient.query(
    `
      WITH "latestSubmissionSummation" AS (
        SELECT
          rs."submissionId",
          s."memberId",
          COALESCE(s."challengeId", s."legacyChallengeId"::text) AS "challengeId",
          s."legacyChallengeId",
          s."placement",
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
        FROM ${reviewSummationRelation} rs
        INNER JOIN ${submissionRelation} s
          ON s."id" = rs."submissionId"
        WHERE (
            s."challengeId" = ANY($1::text[])
            OR s."legacyChallengeId"::text = ANY($1::text[])
          )
          AND s."memberId" IS NOT NULL
          AND rs."isFinal" IS NOT FALSE
      ),
      "latestMemberSubmission" AS (
        SELECT
          "submissionId",
          "memberId",
          "challengeId",
          "legacyChallengeId",
          "placement",
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
        "legacyChallengeId",
        "placement",
        "aggregateScore",
        "reviewedDate",
        "createdAt",
        "submissionCreatedAt"
      FROM "latestMemberSubmission"
      WHERE "memberRank" = 1
      ORDER BY "submissionCreatedAt" ASC, "submissionId" ASC
    `,
    [challengeIds]
  )

  return {
    participantRows: result.rows,
    scoringConfig: getDefaultMmScoringConfig()
  }
}

/**
 * Fetch all scored Development Challenge participants from challengeResult.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string} challengeId challenge identifier
 * @returns {Promise<Array<Object>>} eligible participant rows
 */
async function fetchDevelopmentParticipantsForChallenge (reviewDbClient, challengeId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "passedReview", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "challengeId" = $1
      ORDER BY "placement" ASC, "finalScore" DESC, "createdAt" ASC
    `,
    [String(challengeId)]
  )

  return result.rows.filter((row) => isDevelopmentParticipantEligibleForRating(row))
}

/**
 * Load challenge metadata required for MM rerating from challenge-api.
 * Rated metadata is loaded defensively from challenge metadata key/value rows
 * when no top-level rated flag is available. The returned map is keyed by both
 * canonical UUID id and legacy numeric aliases so migrated review rows resolve.
 * @param {Object} challengeClient prisma challenge client
 * @param {Array<string>} challengeIds challenge identifiers
 * @returns {Promise<Map<string, Object>>} challenge metadata keyed by UUID and legacy id
 */
async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  const normalizedChallengeIds = buildChallengeIdCandidates(challengeIds)
  if (normalizedChallengeIds.length === 0) {
    return new Map()
  }

  const numericChallengeIds = normalizedChallengeIds
    .filter((challengeId) => /^\d+$/.test(challengeId))
    .map((challengeId) => Number(challengeId))
    .filter(Number.isSafeInteger)

  const whereClauses = [{
    id: {
      in: normalizedChallengeIds
    }
  }]

  if (numericChallengeIds.length > 0) {
    whereClauses.push({
      legacyId: {
        in: numericChallengeIds
      }
    })
    whereClauses.push({
      legacyRecord: {
        is: {
          legacySystemId: {
            in: numericChallengeIds
          }
        }
      }
    })
  }

  const challenges = await challengeClient.challenge.findMany({
    where: whereClauses.length === 1 ? whereClauses[0] : { OR: whereClauses },
    select: {
      id: true,
      legacyId: true,
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
      tags: true,
      metadata: RATING_METADATA_SELECT,
      legacyRecord: {
        select: {
          legacySystemId: true
        }
      }
    }
  })

  const metadataByChallengeId = new Map()
  challenges.forEach((challenge) => {
    buildReviewChallengeIds(challenge).forEach((candidate) => {
      metadataByChallengeId.set(candidate, challenge)
    })
  })

  return metadataByChallengeId
}

/**
 * Build the ordered rerate history for the target member.
 * Multiple submission-level rows for one challenge are collapsed to the latest
 * final system result so the rating pass replays each challenge only once.
 * Challenge-level rating metadata decides whether the challenge is rated.
 * @param {Array<Object>} mmResultRows submission-level MM result rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by id
 * @returns {Array<Object>} ordered challenge history entries for rerating
 */
function buildTargetHistory (mmResultRows, challengeMetadataById, ratingPath) {
  const historyByChallengeId = new Map()

  mmResultRows.forEach((row) => {
    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.endDate) {
      return
    }

    if (!isChallengeRated(challenge)) {
      return
    }

    if (!isMarathonMatchChallenge(challenge)) {
      return
    }

    if (ratingPath && !challengeMatchesRatingPath(challenge, ratingPath)) {
      return
    }

    const scoreDate = normalizeDate(row.reviewedDate, row.createdAt)
    const eventDate = normalizeDate(challenge.endDate, scoreDate)
    if (!eventDate || !scoreDate) {
      return
    }

    const canonicalChallengeId = String(challenge.id)
    historyByChallengeId.set(canonicalChallengeId, {
      challengeId: canonicalChallengeId,
      reviewChallengeIds: buildReviewChallengeIds(challenge, row.challengeId),
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
      newVolatility: true,
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
      newVolatility: row.newVolatility,
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
 * Remove undefined fields before passing dynamic stats data to Prisma writes.
 * @param {Object} data write payload
 * @returns {Object} shallow copy without undefined values
 */
function omitUndefinedFields (data) {
  const result = {}
  Object.keys(data || {}).forEach((key) => {
    if (data[key] !== undefined) {
      result[key] = data[key]
    }
  })
  return result
}

/**
 * Insert or update the current rating stats row for one rating dimension.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {Object} updatedTarget Qubits participant state after the challenge
 * @param {Object} dimensionIds unified stats track/type identifiers
 * @param {Object} statsFields optional aggregate fields for custom rating paths
 * @returns {Promise<void>} resolves when the stats row is written
 */
async function upsertRatingStatsRow (tx, userId, updatedTarget, dimensionIds, statsFields = {}) {
  await tx.memberStats.upsert({
    where: {
      userId_trackId_typeId: {
        userId,
        trackId: dimensionIds.trackId,
        typeId: dimensionIds.typeId
      }
    },
    create: omitUndefinedFields({
      userId,
      trackId: dimensionIds.trackId,
      typeId: dimensionIds.typeId,
      rating: updatedTarget.rating,
      volatility: updatedTarget.volatility,
      ...statsFields,
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    }),
    update: omitUndefinedFields({
      rating: updatedTarget.rating,
      volatility: updatedTarget.volatility,
      ...statsFields,
      updatedBy: RERATE_ACTOR
    })
  })
}

/**
 * Insert or update the member's history checkpoint for one rerated challenge.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {string} challengeId challenge identifier
 * @param {Object} previousState target state before the challenge
 * @param {Object} updatedState target state after the challenge
 * @param {Date} eventDate challenge event date
 * @param {Object} historyFields optional placement and percentile history metadata
 * @returns {Promise<void>} resolves when the history row is written
 */
async function upsertHistoryRow (tx, userId, challengeId, previousState, updatedState, eventDate, dimensionIds, historyFields = {}) {
  const hasPreviousRating = previousState && previousState.numRatings > 0
  const oldRating = hasPreviousRating ? previousState.rating : null
  const oldVolatility = hasPreviousRating ? previousState.volatility : null
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
  const historyData = omitUndefinedFields({
    oldRating,
    newRating: updatedState.rating,
    placement: historyFields.placement,
    percentile: historyFields.percentile,
    oldVolatility,
    newVolatility: updatedState.volatility,
    eventDate,
    updatedBy: RERATE_ACTOR
  })

  if (existingHistory) {
    await tx.memberStatsHistory.update({
      where: {
        id: existingHistory.id
      },
      data: historyData
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
      ...historyData,
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
async function updateMaxRating (tx, userId, rating, dimensionIds) {
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
      track: dimensionIds.trackName,
      subTrack: dimensionIds.typeName,
      ratingColor: getRatingColor(rating),
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    },
    update: {
      rating,
      track: dimensionIds.trackName,
      subTrack: dimensionIds.typeName,
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
      rating: true,
      volatility: true
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
      newRating: true,
      newVolatility: true
    }
  })

  const latestUpdate = {
    mostRecent: true,
    oldRating: previousHistory ? previousHistory.newRating : null,
    oldVolatility: previousHistory ? previousHistory.newVolatility : null,
    updatedBy: RERATE_ACTOR
  }

  if (currentStats && currentStats.rating !== null) {
    latestUpdate.newRating = currentStats.rating
  }
  if (currentStats && currentStats.volatility !== null) {
    latestUpdate.newVolatility = currentStats.volatility
  }

  await tx.memberStatsHistory.update({
    where: {
      id: latestHistory.id
    },
    data: latestUpdate
  })
}

/**
 * Build the Challenge API where clause for a configured rating path.
 * Tags are queried as any-of filters. Skill ids are queried as an all-of
 * combination by requiring a ChallengeSkill row for each configured skill id.
 * @param {Object} ratingPath normalized rating path config
 * @returns {Object} Prisma where clause for candidate challenges
 */
function buildRatingPathChallengeWhere (ratingPath) {
  const conditions = []

  if (ratingPath && Array.isArray(ratingPath.tags) && ratingPath.tags.length > 0) {
    conditions.push({
      tags: {
        hasSome: ratingPath.tags
      }
    })
  }

  if (ratingPath && Array.isArray(ratingPath.skillIds) && ratingPath.skillIds.length > 0) {
    ratingPath.skillIds.forEach((skillId) => {
      conditions.push({
        skills: {
          some: {
            skillId
          }
        }
      })
    })
  }

  if (conditions.length === 1) {
    return conditions[0]
  }

  return {
    AND: conditions
  }
}

/**
 * Load completed, rated challenges that belong to a configured rating path.
 * The Challenge API stores tags on the challenge row and skill links in
 * ChallengeSkill rows, while rated state is kept in metadata for some historical
 * imports. Supported sources are Development / Challenge and DATA_SCIENCE /
 * MARATHON_MATCH. Entries include canonical and legacy challenge id aliases for
 * review-api lookups.
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} ratingPath normalized rating path config
 * @returns {Promise<Array<Object>>} ordered challenge entries for the rating path
 */
async function fetchRatingPathHistory (challengeClient, ratingPath) {
  const challenges = await challengeClient.challenge.findMany({
    where: buildRatingPathChallengeWhere(ratingPath),
    select: {
      id: true,
      legacyId: true,
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
      tags: true,
      skills: {
        select: {
          skillId: true
        }
      },
      metadata: RATING_METADATA_SELECT,
      legacyRecord: {
        select: {
          legacySystemId: true
        }
      }
    }
  })

  const history = []
  challenges.forEach((challenge) => {
    if (!challenge || !challenge.endDate) {
      return
    }

    if (!isChallengeRated(challenge)) {
      return
    }

    if (!challengeMatchesRatingPath(challenge, ratingPath)) {
      return
    }

    const source = resolveRatingPathSource(challenge)
    if (!source) {
      return
    }

    const eventDate = normalizeDate(challenge.endDate, challenge.endDate)
    if (!eventDate) {
      return
    }

    history.push({
      challengeId: String(challenge.id),
      reviewChallengeIds: buildReviewChallengeIds(challenge),
      eventDate,
      source
    })
  })

  history.sort((left, right) => {
    const leftEventDate = left.eventDate ? left.eventDate.getTime() : 0
    const rightEventDate = right.eventDate ? right.eventDate.getTime() : 0
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return history
}

/**
 * Fetch participants for one tagged rating path event from its source table.
 * The second argument is retained for older callers that passed an MM client;
 * it is ignored because historical MM replay no longer reads marathon-match-api.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Object|null} maybeMmDbClientOrHistoryEntry ignored MM client or rating path challenge entry
 * @param {Object} [maybeHistoryEntry] rating path challenge entry
 * @returns {Promise<Object>} participant rows and optional MM scoring config
 */
async function fetchRatingPathParticipantsForChallenge (reviewDbClient, maybeMmDbClientOrHistoryEntry, maybeHistoryEntry) {
  const historyEntry = maybeHistoryEntry || maybeMmDbClientOrHistoryEntry

  if (historyEntry.source === RATING_PATH_SOURCE_DEVELOPMENT) {
    return {
      participantRows: await fetchDevelopmentParticipantsForChallenge(reviewDbClient, historyEntry.challengeId),
      scoringConfig: null
    }
  }

  if (historyEntry.source === RATING_PATH_SOURCE_MARATHON_MATCH) {
    return fetchMmParticipantsForChallenge(reviewDbClient, historyEntry)
  }

  return {
    participantRows: [],
    scoringConfig: null
  }
}

/**
 * Resolve a rating path participant's member id.
 * @param {Object} row participant source row
 * @param {string} source rating path source
 * @returns {BigInt} participant member id
 */
function resolveRatingPathParticipantId (row, source) {
  return toBigIntUserId(source === RATING_PATH_SOURCE_MARATHON_MATCH ? row.memberId : row.userId)
}

/**
 * Normalize a rating path participant's score based on its source.
 * @param {Object} row participant source row
 * @param {string} source rating path source
 * @param {Object} scoringConfig MM scoring config when source is Marathon Match
 * @returns {number} normalized Qubits score
 */
function normalizeRatingPathScore (row, source, scoringConfig) {
  if (source === RATING_PATH_SOURCE_DEVELOPMENT) {
    return normalizeDevelopmentScore(row)
  }

  return normalizeScore(row, scoringConfig)
}

/**
 * Re-rate one target member on a configured rating path.
 * The path is replayed from its first matching challenge in memory so opponent
 * states are correct even before their custom rating histories have been stored.
 * Only the requested target member's stats/history rows are persisted. Tag
 * predicates match any configured tag; skill predicates require every configured
 * skill id to be present on the challenge.
 * @param {Object} membersClient prisma members client
 * @param {Object} challengeClient prisma challenge client
 * @param {Object|null} mmDbClient ignored legacy Marathon Match client parameter
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|number|BigInt} userId target member identifier
 * @param {string|number} fromChallengeId starting challenge identifier
 * @param {Object} ratingPath normalized rating path config
 * @returns {Promise<Object>} rerate summary counts
 * @throws {errors.BadRequestError} when the start challenge is not in the member's path
 */
async function rerateMmRatingPath (membersClient, challengeClient, mmDbClient, reviewDbClient, userId, fromChallengeId, ratingPath) {
  const normalizedUserId = toBigIntUserId(userId)
  const targetUserKey = buildUserStateKey(normalizedUserId)
  const pathHistory = await fetchRatingPathHistory(challengeClient, ratingPath)
  if (pathHistory.length === 0) {
    return {
      challengesProcessed: 0,
      ratingPathChallengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  let startIndex = 0
  if (fromChallengeId) {
    startIndex = pathHistory.findIndex((entry) => historyEntryMatchesChallengeId(entry, fromChallengeId))
    if (startIndex < 0) {
      throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${ratingPath.name} event`)
    }
  }

  const dimensionIds = await resolveUnifiedDimensionIds(challengeClient, ratingPath)
  const stateByUserId = new Map()
  let targetChallengeCount = 0
  let targetMostRecentEventDate
  let recomputedMaxRating = 0
  let challengesProcessed = 0
  let ratingPathChallengesProcessed = 0
  let ratingsUpdated = 0

  for (let index = 0; index < pathHistory.length; index += 1) {
    const historyEntry = pathHistory[index]
    const { participantRows, scoringConfig } = await fetchRatingPathParticipantsForChallenge(
      reviewDbClient,
      mmDbClient,
      historyEntry
    )

    if (participantRows.length === 0) {
      if (fromChallengeId && index === startIndex) {
        throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${ratingPath.name} event for this member`)
      }
      continue
    }

    ratingPathChallengesProcessed += 1

    const targetStateBeforeRun = cloneState(stateByUserId.get(targetUserKey))
    const participantRowsByUserId = new Map()
    const participants = participantRows.map((row) => {
      const participantUserId = resolveRatingPathParticipantId(row, historyEntry.source)
      const participantState = stateByUserId.get(buildUserStateKey(participantUserId)) || createDefaultState()
      participantRowsByUserId.set(buildUserStateKey(participantUserId), row)

      return {
        coderId: String(participantUserId),
        rating: participantState.rating,
        volatility: participantState.volatility,
        numRatings: participantState.numRatings,
        score: normalizeRatingPathScore(row, historyEntry.source, scoringConfig)
      }
    })

    runQubitsRating(participants)

    participants.forEach((participant) => {
      stateByUserId.set(buildUserStateKey(participant.coderId), cloneState(participant))
    })

    const updatedTarget = participants.find((participant) => participant.coderId === String(normalizedUserId))
    if (!updatedTarget) {
      if (fromChallengeId && index === startIndex) {
        throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${ratingPath.name} event for this member`)
      }
      continue
    }

    targetChallengeCount += 1
    targetMostRecentEventDate = historyEntry.eventDate
    recomputedMaxRating = Math.max(recomputedMaxRating, updatedTarget.rating)
    const targetHistoryFields = buildHistoryResultFields(
      participants,
      targetUserKey,
      participantRowsByUserId.get(targetUserKey)
    )

    if (index < startIndex) {
      continue
    }

    challengesProcessed += 1
    ratingsUpdated += 1

    await membersClient.$transaction(async (tx) => {
      await upsertRatingStatsRow(
        tx,
        normalizedUserId,
        updatedTarget,
        dimensionIds,
        {
          challenges: targetChallengeCount,
          mostRecentEventDate: targetMostRecentEventDate
        }
      )

      await upsertHistoryRow(
        tx,
        normalizedUserId,
        historyEntry.challengeId,
        targetStateBeforeRun,
        updatedTarget,
        historyEntry.eventDate,
        dimensionIds,
        targetHistoryFields
      )
    })
  }

  if (ratingsUpdated > 0) {
    await membersClient.$transaction(async (tx) => {
      await refreshMostRecentHistoryFlag(tx, normalizedUserId, dimensionIds)
      await updateMaxRating(tx, normalizedUserId, recomputedMaxRating, dimensionIds)
    })
  }

  return {
    challengesProcessed,
    ratingPathChallengesProcessed,
    ratingsUpdated
  }
}

/**
 * Re-rate one member's Marathon Match timeline beginning at the requested challenge.
 * Each challenge is replayed forward with the latest final MM system score per participant.
 * @param {Object} membersClient prisma members client
 * @param {Object} challengeClient prisma challenge client
 * @param {Object|null} mmDbClient ignored legacy Marathon Match client parameter
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|number|BigInt} userId target member identifier
 * @param {string|number} fromChallengeId starting challenge identifier
 * @param {Object} options optional rerate controls
 * @param {Object} options.ratingPath normalized rating path config
 * @returns {Promise<Object>} rerate summary counts
 * @throws {Error} when the required review database connection is missing
 * @throws {errors.BadRequestError} when the start challenge is not a rated MM event for the member
 */
async function rerateMmTrack (membersClient, challengeClient, mmDbClient, reviewDbClient, userId, fromChallengeId, options = {}) {
  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate marathon match stats')
  }

  const ratingPath = options.ratingPath || null
  if (ratingPath) {
    return rerateMmRatingPath(
      membersClient,
      challengeClient,
      mmDbClient,
      reviewDbClient,
      userId,
      fromChallengeId,
      ratingPath
    )
  }

  const normalizedUserId = toBigIntUserId(userId)
  const targetUserKey = buildUserStateKey(normalizedUserId)
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
    startIndex = targetHistory.findIndex((entry) => historyEntryMatchesChallengeId(entry, fromChallengeId))
    if (startIndex < 0) {
      throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_NAME}/${TYPE_NAME} event for this member`)
    }
  }

  const participantHistoryByUserId = new Map()
  let targetState = createDefaultState()
  let recomputedMaxRating = 0

  if (startIndex > 0) {
    await loadParticipantHistoryCache(membersClient, [normalizedUserId], participantHistoryByUserId, dimensionIds)

    const targetHistoryRows = participantHistoryByUserId.get(targetUserKey) || []
    let startSeedIndex = findHistoryIndexForChallenge(targetHistoryRows, targetHistory[startIndex - 1])

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
      historyEntry
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
    const participantRowsByUserId = new Map()
    const participants = participantRows.map((row) => {
      const participantUserId = toBigIntUserId(row.memberId)
      const participantState = stateByUserId.get(buildUserStateKey(participantUserId)) || createDefaultState()
      participantRowsByUserId.set(buildUserStateKey(participantUserId), row)

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
    const targetHistoryFields = buildHistoryResultFields(
      participants,
      targetUserKey,
      participantRowsByUserId.get(targetUserKey)
    )

    await membersClient.$transaction(async (tx) => {
      await upsertRatingStatsRow(tx, normalizedUserId, updatedTarget, dimensionIds)

      await upsertHistoryRow(
        tx,
        normalizedUserId,
        historyEntry.challengeId,
        targetStateBeforeRun,
        updatedTarget,
        historyEntry.eventDate,
        dimensionIds,
        targetHistoryFields
      )
    })
  }

  if (ratingsUpdated > 0) {
    await membersClient.$transaction(async (tx) => {
      await refreshMostRecentHistoryFlag(tx, normalizedUserId, dimensionIds)
      await updateMaxRating(tx, normalizedUserId, recomputedMaxRating, dimensionIds)
    })
  }

  return {
    challengesProcessed,
    ratingsUpdated
  }
}

module.exports = {
  fetchRatingPathHistory,
  fetchRatingPathParticipantsForChallenge,
  resolveRatingPathParticipantId,
  rerateMmTrack
}

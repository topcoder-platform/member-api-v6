/**
 * Provides public profile statistics for a member's Copilot and Reviewer
 * challenge assignments. Summary and detail queries join the resources and
 * challenges schemas so visibility filtering and sorting remain database-bound
 * for members with thousands of role assignments.
 */

const _ = require('lodash')
const Joi = require('joi')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prismaManager = require('../common/prisma')

const { ChallengesPrisma } = prismaManager

const COPILOT_ROLE = 'copilot'
const REVIEWER_ROLE = 'reviewer'
const SPECIAL_ROLES = [COPILOT_ROLE, REVIEWER_ROLE]
const FULFILLMENT_EXCLUDED_STATUSES = new Set([
  'CANCELLED_CLIENT_REQUEST'
])

const REVIEWER_ROLE_NAMES_LOWER = [
  'iterative reviewer',
  'primary reviewer',
  'reviewer',
  'failure reviewer',
  'final reviewer',
  'aggregator',
  'stress reviewer',
  'accuracy reviewer',
  'primary screener',
  'checkpoint screener',
  'checkpoint reviewer'
]

const ROLE_NAMES_LOWER = {
  [COPILOT_ROLE]: [COPILOT_ROLE],
  [REVIEWER_ROLE]: REVIEWER_ROLE_NAMES_LOWER
}
const TRACK_KEY_BY_NORMALIZED_NAME = {
  DEVELOPMENT: 'DEVELOPMENT',
  DEVELOP: 'DEVELOPMENT',
  DEV: 'DEVELOPMENT',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE',
  DATA_SCIENCE_TRACK: 'DATA_SCIENCE',
  DS: 'DATA_SCIENCE',
  QUALITY_ASSURANCE: 'QUALITY_ASSURANCE',
  QA: 'QUALITY_ASSURANCE'
}

/**
 * Execute a challenge-client raw query and turn a missing cross-schema relation
 * into an explicit service-availability error. Supported deployments co-locate
 * the `challenges` and `resources` schemas; split-database deployments cannot
 * safely calculate anonymous-visible summary or detail results.
 * @param {Object} query parameterized ChallengesPrisma SQL fragment
 * @returns {Promise<Array<Object>>} raw PostgreSQL result rows
 * @throws {ServiceUnavailableError} when required cross-schema tables are absent
 * @throws {Error} propagates all other challenge database query failures
 */
async function runChallengeCrossSchemaQuery (query) {
  try {
    return await prismaManager.getChallengesClient().$queryRaw(query)
  } catch (error) {
    const databaseCode = String(_.get(error, 'meta.code') || '')
    const databaseMessage = String(_.get(error, 'meta.message') || error.message || '')
    if (
      error.code === 'P2021' ||
      databaseCode === '42P01' ||
      databaseCode === '3F000' ||
      databaseMessage.includes('does not exist')
    ) {
      throw new errors.ServiceUnavailableError(
        'Special role queries require the challenges and resources schemas to be co-located.'
      )
    }
    throw error
  }
}

/**
 * Convert challenge track metadata to one of the four profile track keys. The
 * ChallengeTrack enum is preferred, with name and abbreviation as migrated-row
 * fallbacks. Copilot metric hydration uses this and it does not raise.
 * @param {Object|null} track challenge track enum, name, and abbreviation
 * @returns {String|null} normalized profile track key or null when unknown
 */
function getProfileTrackKey (track) {
  if (!track) {
    return null
  }

  const candidates = [track.track, track.name, track.abbreviation]
  for (const candidate of candidates) {
    const normalized = String(candidate || '')
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '')
    if (TRACK_KEY_BY_NORMALIZED_NAME[normalized]) {
      return TRACK_KEY_BY_NORMALIZED_NAME[normalized]
    }
  }

  return null
}

/**
 * Calculate Copilot fulfillment from terminal outcome counts. The percentage
 * is completed divided by completed plus qualifying `CANCELLED`/
 * `CANCELLED_*` statuses, rounded to two decimal places on a 0-100 scale.
 * Client-request cancellations are excluded because they are not Copilot
 * failures. It returns zero when no included terminal challenge exists and
 * does not raise.
 * @param {Number} completed number of completed public Copilot challenges
 * @param {Number} cancelled number of included cancelled public Copilot challenges
 * @returns {Object} completed, cancelled, included terminal total, and rate
 */
function buildFulfillment (completed, cancelled) {
  const total = completed + cancelled
  return {
    completed,
    cancelled,
    total,
    rate: total > 0 ? Number(((completed / total) * 100).toFixed(2)) : 0
  }
}

/**
 * Build the reusable SQL common-table expressions for one role's anonymous-
 * visible challenges. Resource assignments are de-duplicated per challenge.
 * Visibility matches challenge-api's anonymous baseline: no groups, no user
 * whitelist rows, and no tasks. This helper only constructs SQL and does not
 * execute or raise database errors.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @param {String} role `copilot` or `reviewer`
 * @returns {Object} Prisma SQL fragment defining visibleRoleChallenges
 */
function buildVisibleRoleChallengesCte (userId, role) {
  return ChallengesPrisma.sql`
    WITH "roleAssignments" AS (
      SELECT
        resource."challengeId",
        MAX(resource."createdAt") AS "resourceCreatedAt"
      FROM resources."Resource" AS resource
      INNER JOIN resources."ResourceRole" AS resourceRole
        ON resourceRole."id" = resource."roleId"
      WHERE resource."memberId" = ${String(userId)}
        AND resourceRole."nameLower" IN (${ChallengesPrisma.join(ROLE_NAMES_LOWER[role])})
      GROUP BY resource."challengeId"
    ),
    "visibleRoleChallenges" AS (
      SELECT
        challenge."id",
        challenge."name",
        challenge."status",
        challenge."trackId",
        challenge."typeId",
        challenge."startDate",
        challenge."endDate",
        challenge."createdAt",
        roleAssignment."resourceCreatedAt"
      FROM "roleAssignments" AS roleAssignment
      INNER JOIN challenges."Challenge" AS challenge
        ON challenge."id" = roleAssignment."challengeId"
      WHERE cardinality(challenge."groups") = 0
        AND challenge."taskIsTask" = FALSE
        AND NOT EXISTS (
          SELECT 1
          FROM challenges."ChallengeUserWhitelist" AS whitelist
          WHERE whitelist."challengeId" = challenge."id"
        )
    )
  `
}

/**
 * Count distinct anonymous-visible challenges for one role. The profile
 * summary uses this count so it matches the anonymous-visible detail set and
 * never discloses restricted challenge assignments.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @param {String} role `copilot` or `reviewer`
 * @returns {Promise<Number>} visible distinct challenge count
 * @throws {ServiceUnavailableError} when required cross-schema tables are absent
 * @throws {Error} propagates challenge database query failures
 */
async function countVisibleRoleChallenges (userId, role) {
  const roleChallengesCte = buildVisibleRoleChallengesCte(userId, role)
  const rows = await runChallengeCrossSchemaQuery(ChallengesPrisma.sql`
    ${roleChallengesCte}
    SELECT COUNT(*)::int AS "challengeCount"
    FROM "visibleRoleChallenges"
  `)
  return Number(_.get(rows, '[0].challengeCount')) || 0
}

/**
 * Load every anonymous-visible challenge detail. PostgreSQL
 * orders lifecycle end date, then start/creation fallback, newest first; latest
 * Resource assignment and challenge ID are deterministic tie-breakers. The
 * Resource timestamp is assignment time, not a literal review-completion date.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @param {String} role `copilot` or `reviewer`
 * @returns {Promise<Array<Object>>} newest-first visible challenge rows
 * @throws {Error} propagates cross-schema PostgreSQL query failures
 */
async function loadVisibleRoleChallenges (userId, role) {
  const roleChallengesCte = buildVisibleRoleChallengesCte(userId, role)
  return runChallengeCrossSchemaQuery(ChallengesPrisma.sql`
    ${roleChallengesCte}
    SELECT
      challenge."id",
      challenge."name",
      challenge."status"::text AS "status",
      challenge."startDate",
      challenge."endDate",
      challenge."resourceCreatedAt",
      challengeTrack."id" AS "trackId",
      challengeTrack."name" AS "trackName",
      challengeType."id" AS "typeId",
      challengeType."name" AS "typeName"
    FROM "visibleRoleChallenges" AS challenge
    INNER JOIN challenges."ChallengeTrack" AS challengeTrack
      ON challengeTrack."id" = challenge."trackId"
    INNER JOIN challenges."ChallengeType" AS challengeType
      ON challengeType."id" = challenge."typeId"
    ORDER BY
      COALESCE(
        challenge."endDate",
        challenge."startDate",
        challenge."createdAt"
      ) DESC,
      challenge."resourceCreatedAt" DESC,
      challenge."id" ASC
  `)
}

/**
 * Aggregate Copilot tracks and terminal statuses entirely in PostgreSQL over
 * the anonymous-visible challenge set. Only the clicked Copilot detail route
 * invokes this query; zero-count tracks are naturally omitted from the result.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @returns {Promise<Object>} non-zero track counts and fulfillment statistics
 * @throws {Error} propagates cross-schema PostgreSQL query failures
 */
async function loadVisibleCopilotMetrics (userId) {
  const roleChallengesCte = buildVisibleRoleChallengesCte(userId, COPILOT_ROLE)
  const rows = await runChallengeCrossSchemaQuery(ChallengesPrisma.sql`
    ${roleChallengesCte}
    SELECT
      challenge."status"::text AS "status",
      challengeTrack."track"::text AS "track",
      challengeTrack."name" AS "trackName",
      challengeTrack."abbreviation" AS "trackAbbreviation",
      COUNT(*)::int AS "challengeCount"
    FROM "visibleRoleChallenges" AS challenge
    INNER JOIN challenges."ChallengeTrack" AS challengeTrack
      ON challengeTrack."id" = challenge."trackId"
    GROUP BY
      challenge."status",
      challengeTrack."track",
      challengeTrack."name",
      challengeTrack."abbreviation"
  `)

  const trackCounts = {}
  let completed = 0
  let cancelled = 0
  _.forEach(rows, row => {
    const challengeCount = Number(row.challengeCount) || 0
    const status = String(row.status || '')
    const trackKey = getProfileTrackKey({
      track: row.track,
      name: row.trackName,
      abbreviation: row.trackAbbreviation
    })
    if (trackKey && challengeCount > 0) {
      trackCounts[trackKey] = (trackCounts[trackKey] || 0) + challengeCount
    }
    if (status === 'COMPLETED') {
      completed += challengeCount
    } else if (
      status.startsWith('CANCELLED') &&
      !FULFILLMENT_EXCLUDED_STATUSES.has(status)
    ) {
      cancelled += challengeCount
    }
  })

  return {
    trackCounts,
    fulfillment: buildFulfillment(completed, cancelled)
  }
}

/**
 * Convert a PostgreSQL or string date to the ISO-8601 form documented by the
 * role challenge API. Invalid and absent dates become null. Challenge list
 * formatting uses this helper and it does not raise.
 * @param {Date|String|Number|null} value date-like value to serialize
 * @returns {String|null} ISO-8601 date string or null
 */
function serializeDate (value) {
  if (!value) {
    return null
  }
  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? null : date.toISOString()
}

/**
 * Format a visible cross-schema query row for the public challenge-card
 * contract. This is used for every detail item and does not raise.
 * @param {Object} challenge flat visible challenge, track, and type row
 * @returns {Object} documented public role challenge list item
 */
function formatChallenge (challenge) {
  return {
    id: challenge.id,
    name: challenge.name,
    status: challenge.status,
    track: challenge.trackId
      ? { id: challenge.trackId, name: challenge.trackName }
      : null,
    type: challenge.typeId
      ? { id: challenge.typeId, name: challenge.typeName }
      : null,
    startDate: serializeDate(challenge.startDate),
    endDate: serializeDate(challenge.endDate),
    resourceCreatedAt: serializeDate(challenge.resourceCreatedAt)
  }
}

/**
 * Get public Copilot and Reviewer badge counts for a member profile. The
 * summary reuses the detail view's anonymous-visible challenge set so its
 * counts match the role details. Roles with no visible challenges are omitted,
 * and challenges with multiple reviewer assignments count once.
 * @param {String} handle member handle resolved through the members table
 * @returns {Promise<Object>} optional count-only Copilot/Reviewer summaries
 * @throws {NotFoundError} when the member handle does not exist
 * @throws {ServiceUnavailableError} when required cross-schema tables are absent
 * @throws {Error} propagates challenge database query failures
 */
async function getMemberRoleStats (handle) {
  const member = await helper.getMemberByHandle(handle)
  const [copilotCount, reviewerCount] = await Promise.all([
    countVisibleRoleChallenges(member.userId, COPILOT_ROLE),
    countVisibleRoleChallenges(member.userId, REVIEWER_ROLE)
  ])
  const counts = {
    [COPILOT_ROLE]: copilotCount,
    [REVIEWER_ROLE]: reviewerCount
  }
  const result = {}

  if (counts[COPILOT_ROLE] > 0) {
    result[COPILOT_ROLE] = { challengeCount: counts[COPILOT_ROLE] }
  }
  if (counts[REVIEWER_ROLE] > 0) {
    result[REVIEWER_ROLE] = { challengeCount: counts[REVIEWER_ROLE] }
  }
  return result
}

getMemberRoleStats.schema = {
  handle: Joi.string().trim().required()
}

/**
 * Get every newest-first anonymous-visible Copilot or Reviewer challenge for a
 * member. Copilot responses also include public-set track counts and terminal
 * fulfillment. Details never return restricted challenge IDs.
 * @param {String} handle member handle resolved through the members table
 * @param {String} role `copilot` or `reviewer`
 * @returns {Promise<Object>} complete challenge list and aggregate role metrics
 * @throws {NotFoundError} when the member handle does not exist
 * @throws {ValidationError} when role input is invalid
 * @throws {ServiceUnavailableError} when required cross-schema tables are absent
 * @throws {Error} propagates challenge database query failures
 */
async function getMemberRoleChallenges (handle, role) {
  const member = await helper.getMemberByHandle(handle)

  const [challengeRows, copilotMetrics] = await Promise.all([
    loadVisibleRoleChallenges(member.userId, role),
    role === COPILOT_ROLE
      ? loadVisibleCopilotMetrics(member.userId)
      : Promise.resolve(null)
  ])
  const result: any = {
    role,
    total: challengeRows.length,
    challenges: _.map(challengeRows, formatChallenge)
  }
  if (role === COPILOT_ROLE) {
    result.trackCounts = copilotMetrics.trackCounts
    result.fulfillment = copilotMetrics.fulfillment
  }
  return result
}

getMemberRoleChallenges.schema = {
  handle: Joi.string().trim().required(),
  role: Joi.string().trim().lowercase().valid(...SPECIAL_ROLES).required()
}

module.exports = {
  getMemberRoleStats,
  getMemberRoleChallenges
}

logger.buildService(module.exports)

/**
 * Provides public profile statistics for a member's Copilot and Reviewer
 * challenge assignments. Summary counts stay in the resources database, while
 * detail queries join the resources and challenges schemas so visibility
 * filtering, sorting, and pagination remain database-bound for members with
 * thousands of role assignments.
 */

const _ = require('lodash')
const Joi = require('joi')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prismaManager = require('../common/prisma')

const { ChallengesPrisma, ResourcesPrisma } = prismaManager

const COPILOT_ROLE = 'copilot'
const REVIEWER_ROLE = 'reviewer'
const SPECIAL_ROLES = [COPILOT_ROLE, REVIEWER_ROLE]
const DEFAULT_PER_PAGE = 100
const MAX_PER_PAGE = 100

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
const ALL_ROLE_NAMES_LOWER = [COPILOT_ROLE, ...REVIEWER_ROLE_NAMES_LOWER]

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
 * the `challenges` and `resources` schemas; split-database deployments can still
 * use the resource-only summary but cannot safely paginate visible details.
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
        'Special role challenge details require the challenges and resources schemas to be co-located.'
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
 * is completed divided by completed plus every `CANCELLED`/`CANCELLED_*`
 * status, rounded to two decimal places on a 0-100 scale. It returns zero when
 * no terminal challenge exists and does not raise.
 * @param {Number} completed number of completed public Copilot challenges
 * @param {Number} cancelled number of cancelled public Copilot challenges
 * @returns {Object} completed, cancelled, terminal total, and percentage rate
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
 * Count distinct resource challenges for both special-role families without
 * joining challenge details. The main profile summary uses this resource-only
 * query so restricted challenges still contribute to the role badge count and
 * long histories are reduced to two scalar rows in PostgreSQL.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @returns {Promise<Object>} `copilot` and `reviewer` distinct resource counts
 * @throws {Error} propagates resources database query failures
 */
async function loadRoleCounts (userId) {
  const rows = await prismaManager.getResourcesClient().$queryRaw(ResourcesPrisma.sql`
    WITH "specialRoleChallenges" AS (
      SELECT
        CASE
          WHEN resourceRole."nameLower" = ${COPILOT_ROLE} THEN ${COPILOT_ROLE}
          ELSE ${REVIEWER_ROLE}
        END AS "role",
        resource."challengeId"
      FROM resources."Resource" AS resource
      INNER JOIN resources."ResourceRole" AS resourceRole
        ON resourceRole."id" = resource."roleId"
      WHERE resource."memberId" = ${String(userId)}
        AND resourceRole."nameLower" IN (${ResourcesPrisma.join(ALL_ROLE_NAMES_LOWER)})
      GROUP BY 1, resource."challengeId"
    )
    SELECT "role", COUNT(*)::int AS "challengeCount"
    FROM "specialRoleChallenges"
    GROUP BY "role"
  `)

  const counts = {
    [COPILOT_ROLE]: 0,
    [REVIEWER_ROLE]: 0
  }
  _.forEach(rows, row => {
    if (_.includes(SPECIAL_ROLES, row.role)) {
      counts[row.role] = Number(row.challengeCount) || 0
    }
  })
  return counts
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
 * Count distinct anonymous-visible challenges for one role. Details pagination
 * uses this count, rather than the unrestricted badge count, so totals and
 * pages never disclose or reserve positions for restricted challenge IDs.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @param {String} role `copilot` or `reviewer`
 * @returns {Promise<Number>} visible distinct challenge count
 * @throws {Error} propagates resources database query failures
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
 * Load one bounded page of anonymous-visible challenge details. PostgreSQL
 * orders lifecycle end date, then start/creation fallback, newest first; latest
 * Resource assignment and challenge ID are deterministic tie-breakers. The
 * Resource timestamp is assignment time, not a literal review-completion date.
 * @param {BigInt|Number|String} userId member user ID from the members database
 * @param {String} role `copilot` or `reviewer`
 * @param {Number} offset zero-based visible challenge offset
 * @param {Number} limit maximum challenge rows, bounded to 100 by Joi
 * @returns {Promise<Array<Object>>} newest-first visible challenge rows
 * @throws {Error} propagates cross-schema PostgreSQL query failures
 */
async function loadVisibleRoleChallengesPage (userId, role, offset, limit) {
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
    LIMIT ${limit}
    OFFSET ${offset}
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
    const trackKey = getProfileTrackKey({
      track: row.track,
      name: row.trackName,
      abbreviation: row.trackAbbreviation
    })
    if (trackKey && challengeCount > 0) {
      trackCounts[trackKey] = (trackCounts[trackKey] || 0) + challengeCount
    }
    if (row.status === 'COMPLETED') {
      completed += challengeCount
    } else if (String(row.status || '').startsWith('CANCELLED')) {
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
 * contract. This is used for every paginated detail item and does not raise.
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
 * Get cheap public Copilot and Reviewer badge counts for a member profile.
 * Roles with no distinct resource challenges are omitted, and challenges with
 * multiple reviewer assignments count once. No challenge names are loaded.
 * @param {String} handle member handle resolved through the members table
 * @returns {Promise<Object>} optional count-only Copilot/Reviewer summaries
 * @throws {NotFoundError} when the member handle does not exist
 * @throws {Error} propagates resources database lookup failures
 */
async function getMemberRoleStats (handle) {
  const member = await helper.getMemberByHandle(handle)
  const counts = await loadRoleCounts(member.userId)
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
 * Get one newest-first page of a member's anonymous-visible Copilot or Reviewer
 * challenges. Copilot responses also include public-set track counts and
 * terminal fulfillment. Details never return restricted challenge IDs.
 * @param {String} handle member handle resolved through the members table
 * @param {String} role `copilot` or `reviewer`
 * @param {Object} query one-based `page` and `perPage` up to 100
 * @returns {Promise<Object>} internally consistent paginated challenge result
 * @throws {NotFoundError} when the member handle does not exist
 * @throws {ValidationError} when role or pagination input is invalid
 * @throws {Error} propagates resources database lookup failures
 */
async function getMemberRoleChallenges (handle, role, query: any = {}) {
  const member = await helper.getMemberByHandle(handle)
  const page = query.page || 1
  const perPage = query.perPage || DEFAULT_PER_PAGE
  const offset = (page - 1) * perPage

  const [total, challengeRows, copilotMetrics] = await Promise.all([
    countVisibleRoleChallenges(member.userId, role),
    loadVisibleRoleChallengesPage(member.userId, role, offset, perPage),
    role === COPILOT_ROLE
      ? loadVisibleCopilotMetrics(member.userId)
      : Promise.resolve(null)
  ])
  const result: any = {
    role,
    total,
    page,
    perPage,
    totalPages: total > 0 ? Math.ceil(total / perPage) : 0,
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
  role: Joi.string().trim().lowercase().valid(...SPECIAL_ROLES).required(),
  query: Joi.object().keys({
    page: Joi.number().integer().min(1).default(1),
    perPage: Joi.number().integer().min(1).max(MAX_PER_PAGE).default(DEFAULT_PER_PAGE)
  }).default({})
}

module.exports = {
  getMemberRoleStats,
  getMemberRoleChallenges
}

logger.buildService(module.exports)

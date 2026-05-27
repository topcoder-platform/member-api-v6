#!/usr/bin/env node
'use strict'

/**
 * Recalculate member statistics for unified reads.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL or CHALLENGE_DB_URL (challenge database)
 * - REVIEW_DB_URL (review database, required for challengeResult aggregates and rerates)
 *
 * Optional environment variables:
 * - CREATED_BY (defaults to 'stats-migration')
 * - UPDATED_BY (defaults to 'stats-migration')
 *
 * Usage examples:
 * - CSV validation:
 *   node src/scripts/recalculateMemberStats.js --csv-only --csv-path /tmp/stats.csv
 * - Write mode:
 *   node src/scripts/recalculateMemberStats.js
 * - Single user:
 *   node src/scripts/recalculateMemberStats.js --user-id 12345
 * - Specific track:
 *   node src/scripts/recalculateMemberStats.js --track-id <uuid-or-name>
 * - Bounded parallelism:
 *   node src/scripts/recalculateMemberStats.js --concurrency 8
 * - Persist processed user IDs to JSON:
 *   node src/scripts/recalculateMemberStats.js --processed-user-ids-path /tmp/recalculateMemberStats.processedUserIds.json
 * - Skip unified history seeding:
 *   node src/scripts/recalculateMemberStats.js --skip-history
 * - Skip legacy rating enrichment:
 *   node src/scripts/recalculateMemberStats.js --skip-ratings
 * - Skip only the Development rerate replay:
 *   node src/scripts/recalculateMemberStats.js --skip-rerate
 *
 * Notes:
 * - Rating and rank fields are backfilled from the legacy member stat sub-tables.
 * - Public aggregate rows start from legacy member stats tables when they exist, are
 *   supplemented with review-api challengeResult rows that extend those timelines, and
 *   fall back to review-api challengeResult or ChallengeWinner when legacy rows do not exist.
 * - Legacy design subtracks collapse to Design + Challenge, except DESIGN_FIRST_2_FINISH,
 *   which maps to Design + First2Finish.
 * - mostRecentSubmission uses review-api challengeResult timestamps whenever review rows
 *   are present for the target user and track/type.
 * - memberStatsHistory is seeded from legacy history tables and supplemented with
 *   completed review-api challengeResult and ChallengeWinner rows when newer
 *   challenges never existed in the legacy history source tables.
 * - memberStatsHistory placement and percentile values are preserved from legacy
 *   history and review-api placement sources when those fields are available.
 * - memberStatsHistory.mostRecent is recalculated from latest eventDate per (userId, trackId, typeId).
 * - memberStatsHistory.newRating on the mostRecent row is synchronized from memberStats.rating.
 * - memberMaxRating is synchronized from the highest current memberStats.rating
 *   row after each write-mode batch, and stale rows are removed when no current ratings remain.
 * - --skip-history skips the legacy history backfill pass.
 * - --skip-ratings skips the legacy rating/rank enrichment pass and the Qubits rerate backfill.
 * - --skip-rerate skips the expensive Development rerate replay while still preserving
 *   legacy rating/rank fields on the rebuilt aggregate rows. When rerates are
 *   enabled, legacy-backed review rows preserve challengeResult oldRating/newRating
 *   because those rows already carry authoritative legacy rating output.
 * - Development rank recalculation runs once per batch after concurrent member rerates
 *   complete, avoiding per-user rank update contention.
 * - --concurrency controls how many users are processed in parallel within each batch.
 * - Batch logs include timing breakdowns for preload queries, user aggregation,
 *   stats/history writes, rerates, rank recalculation, and processed-user checkpoint writes.
 * - Slow-user samples are emitted for aggregate/history/rerate phases so the
 *   worst outliers can be investigated first.
 * - Processed member user IDs are written after each completed batch so long runs can
 *   be resumed or validated incrementally.
 * - The script is idempotent and safe to run multiple times.
 * - Writes use upsert on (userId, trackId, typeId).
 * - Users missing in members.member are skipped to satisfy foreign key constraints.
 */

const fs = require('fs')
const path = require('path')

require('dotenv').config()

const { getMembersClient, getChallengesClient } = require('../common/prisma')
const helper = require('../common/helper')
const reviewDb = require('../common/reviewDb')
const { assertChallengeResultRelation, resolveChallengeResultRelation } = require('../common/reviewDbHelper')
const { recalculateRatingRanks } = require('../common/ratingRankHelper')
const { rerateDevTrack } = require('../ratings/developRatingEngine')
const {
  TYPE_NAMES,
  clearChallengeDimensionLookupCache,
  getCanonicalTypeName: resolveTypeName,
  loadChallengeDimensionLookup,
  normalizeLookupKey,
  resolveTrackIdFromLookup,
  resolveTrackNameFromLookup,
  resolveTypeIdFromLookup,
  resolveTypeNameFromLookup
} = require('../common/statsDimensionHelper')

const DEFAULT_ACTOR = process.env.UPDATED_BY || process.env.CREATED_BY || 'stats-migration'
const CREATED_BY = process.env.CREATED_BY || DEFAULT_ACTOR
const UPDATED_BY = process.env.UPDATED_BY || DEFAULT_ACTOR
const USER_BATCH_SIZE = 100
const DEFAULT_CONCURRENCY = 4
const MAX_RAW_QUERY_PARAMS = 10000
const MEMBER_STATS_BULK_UPSERT_PARAM_COUNT = 22
const HISTORY_BULK_UPDATE_PARAM_COUNT = 6
const HISTORY_BULK_INSERT_PARAM_COUNT = 11
const CHALLENGE_WINNER_HISTORY_TYPES = ['PLACEMENT', 'PASSED_REVIEW']
const CHALLENGE_WINNER_HISTORY_TYPE_SQL = CHALLENGE_WINNER_HISTORY_TYPES.map(type => `'${type}'`).join(', ')
const DEFAULT_PROCESSED_USER_IDS_PATH = 'recalculateMemberStats.processedUserIds.json'
const COMPLETED_CHALLENGE_STATUS = 'COMPLETED'
const RERATE_ACTOR = 'rerate-member-stats'
const NULL_PRESERVED_STAT_FIELDS = [
  'rating',
  'avgRank',
  'avgNumSubmissions',
  'bestRank',
  'globalRank',
  'countryRank',
  'schoolRank',
  'volatility',
  'maxRating',
  'minRating',
  'topFiveFinishes',
  'topTenFinishes'
]
const MEMBER_STATS_PRESERVE_SELECT = {
  userId: true,
  trackId: true,
  typeId: true,
  rating: true,
  avgRank: true,
  avgNumSubmissions: true,
  bestRank: true,
  globalRank: true,
  countryRank: true,
  schoolRank: true,
  volatility: true,
  maxRating: true,
  minRating: true,
  topFiveFinishes: true,
  topTenFinishes: true
}

let legacyLookupCache
const reviewChallengeMetadataCache = new Map()

function logInfo (message) {
  console.log(`[INFO] ${new Date().toISOString()} ${message}`)
}

function logWarn (message) {
  console.warn(`[WARN] ${new Date().toISOString()} ${message}`)
}

function logError (message, error) {
  if (error) {
    console.error(`[ERROR] ${new Date().toISOString()} ${message}`, error)
    return
  }
  console.error(`[ERROR] ${new Date().toISOString()} ${message}`)
}

/**
 * Capture a monotonic high-resolution timer origin.
 * @returns {bigint} timer origin in nanoseconds
 */
function startTimer () {
  return process.hrtime.bigint()
}

/**
 * Compute elapsed milliseconds since a timer origin.
 * @param {bigint} startedAt timer origin returned by startTimer
 * @returns {number} elapsed duration in milliseconds
 */
function getElapsedMilliseconds (startedAt) {
  return Number(process.hrtime.bigint() - startedAt) / 1e6
}

/**
 * Format a millisecond duration for operator-facing logs.
 * @param {number} durationMs elapsed duration in milliseconds
 * @returns {string} human-readable duration string
 */
function formatDuration (durationMs) {
  if (!Number.isFinite(durationMs) || durationMs < 0) {
    return 'n/a'
  }

  if (durationMs < 1000) {
    return `${Math.round(durationMs)}ms`
  }

  if (durationMs < 60000) {
    const precision = durationMs < 10000 ? 2 : 1
    return `${(durationMs / 1000).toFixed(precision)}s`
  }

  const totalSeconds = durationMs / 1000
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds.toFixed(0)}s`
  }

  return `${minutes}m ${seconds.toFixed(1)}s`
}

/**
 * Measure an asynchronous operation and return both its value and duration.
 * @param {Function} operation async operation to measure
 * @returns {Promise<{ result: *, durationMs: number }>} resolved result and elapsed duration
 */
async function measureAsyncStep (operation) {
  const startedAt = startTimer()
  const result = await operation()
  return {
    result,
    durationMs: getElapsedMilliseconds(startedAt)
  }
}

/**
 * Format named timing segments into a compact log string.
 * @param {Array<{ label: string, durationMs: number }>} segments timing segments to include
 * @returns {string} formatted label=value timing summary
 */
function formatTimingSegments (segments) {
  return segments
    .filter((segment) => segment && Number.isFinite(segment.durationMs))
    .map((segment) => `${segment.label}=${formatDuration(segment.durationMs)}`)
    .join(', ')
}

/**
 * Log the slowest user timings for one batch phase.
 * @param {string} label phase label for the log line
 * @param {Array<{ userId: *, durationMs: number }>} measurements per-user timing data
 * @param {Object} [options] logging options
 * @param {Function} [options.detailFormatter] optional formatter for extra measurement detail
 * @param {number} [options.limit=5] max number of slow users to include
 * @param {number} [options.minDurationMs=1000] minimum duration required to include a user
 */
function logSlowestUserTimings (label, measurements, options = {}) {
  const limit = Math.max(1, toInt(options.limit || 5))
  const minDurationMs = Number.isFinite(options.minDurationMs) ? options.minDurationMs : 1000
  const detailFormatter = typeof options.detailFormatter === 'function' ? options.detailFormatter : null
  const slowest = (measurements || [])
    .filter((measurement) => measurement && Number.isFinite(measurement.durationMs) && measurement.durationMs >= minDurationMs)
    .sort((left, right) => right.durationMs - left.durationMs)
    .slice(0, limit)

  if (slowest.length === 0) {
    return
  }

  const slowestSummary = slowest
    .map((measurement) => {
      const detail = detailFormatter ? detailFormatter(measurement) : ''
      const detailSuffix = detail ? ` (${detail})` : ''
      return `${measurement.userId.toString()}=${formatDuration(measurement.durationMs)}${detailSuffix}`
    })
    .join(', ')

  logInfo(`${label} slowest users: ${slowestSummary}`)
}

function toIsoString (value) {
  if (!value) {
    return ''
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString()
  }

  const parsed = new Date(value)
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString()
  }

  return String(value)
}

function toCsvValue (value) {
  if (value === null || value === undefined) {
    return ''
  }

  const text = String(value)
  if (text.includes('"') || text.includes(',') || text.includes('\n')) {
    return `"${text.replace(/"/g, '""')}"`
  }

  return text
}

/**
 * Convert nullable numeric inputs into integers for unified stat writes.
 * @param {*} value raw database value
 * @returns {number|null} normalized integer or null when not present
 */
function toOptionalInt (value) {
  if (value === null || value === undefined || value === '') {
    return null
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.trunc(value) : null
  }

  if (isBigIntValue(value)) {
    return Number(value)
  }

  const parsed = Number(value)
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null
}

/**
 * Convert nullable numeric inputs into floats for unified stat writes.
 * @param {*} value raw database value
 * @returns {number|null} normalized float or null when not present
 */
function toOptionalFloat (value) {
  if (value === null || value === undefined || value === '') {
    return null
  }

  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

/**
 * Normalize a challenge placement so only positive integer ranks are written.
 * @param {*} value raw placement value
 * @returns {number|null} positive placement or null when unavailable
 */
function toOptionalPlacement (value) {
  const placement = toOptionalInt(value)
  return Number.isInteger(placement) && placement > 0 ? placement : null
}

/**
 * Build the composite lookup key shared by unified stats and history rows.
 * @param {string} trackId unified track id
 * @param {string} typeId unified type id
 * @returns {string} track/type composite key
 */
function buildTrackTypeKey (trackId, typeId) {
  return `${trackId}::${typeId}`
}

/**
 * Check whether a ChallengeType id/name represents Marathon Match.
 * @param {*} typeId raw ChallengeType id or name
 * @returns {boolean} true when the type maps to Marathon Match
 */
function isMarathonMatchTypeId (typeId) {
  const typeName = resolveTypeNameFromLookup(legacyLookupCache, typeId) || typeId
  return resolveTypeName(typeName) === TYPE_NAMES.MARATHON_MATCH
}

/**
 * Normalize challenge metadata dimensions into the public stats dimensions.
 * Marathon Match stats/history belong to DATA_SCIENCE even when the source
 * challenge row was imported with a Development track.
 * @param {*} trackId raw ChallengeTrack id
 * @param {*} typeId raw ChallengeType id
 * @returns {Object} normalized trackId/typeId pair
 */
function normalizeChallengeStatsDimension (trackId, typeId) {
  const normalizedTypeId = String(typeId)
  if (legacyLookupCache && isMarathonMatchTypeId(normalizedTypeId) && legacyLookupCache.trackIds.DATA_SCIENCE) {
    return {
      trackId: legacyLookupCache.trackIds.DATA_SCIENCE,
      typeId: normalizedTypeId
    }
  }

  return {
    trackId: String(trackId),
    typeId: normalizedTypeId
  }
}

/**
 * Apply optional script track/type filters after challenge dimensions are normalized.
 * @param {Object} dimension normalized track/type pair
 * @param {Object} options script filters
 * @returns {boolean} true when the row should be kept
 */
function matchesNormalizedStatsFilters (dimension, options) {
  if (options.trackId && dimension.trackId !== options.trackId) {
    return false
  }

  if (options.typeId && dimension.typeId !== options.typeId) {
    return false
  }

  return true
}

/**
 * Determine whether a review-api challengeResult row represents a real submission.
 * Rows explicitly marked invalid, or rows from queries that expose an empty
 * submissionId, are placeholders and should not create stats/history activity.
 * Older in-memory callers that do not provide submission fields are treated as
 * unknown instead of invalid so legacy fixture data can still exercise mapping.
 * @param {Object} row raw challengeResult row
 * @returns {boolean} true when the row can be used for stats/history backfill
 */
function isUsableReviewChallengeResultRow (row) {
  if (!row || row.validSubmission === false) {
    return false
  }

  if (Object.prototype.hasOwnProperty.call(row, 'submissionId')) {
    const submissionId = row.submissionId === null || row.submissionId === undefined
      ? ''
      : String(row.submissionId).trim()
    if (!submissionId) {
      return false
    }
  }

  return true
}

/**
 * Build the composite lookup key for one user's unified stats row.
 * @param {BigInt} userId member user id
 * @param {string} trackId unified track id
 * @param {string} typeId unified type id
 * @returns {string} user/track/type composite key
 */
function buildUserTrackTypeKey (userId, trackId, typeId) {
  return `${userId.toString()}::${buildTrackTypeKey(trackId, typeId)}`
}

/**
 * Build the composite lookup key for unified history rows.
 * @param {BigInt} userId member user id
 * @param {string} trackId unified track id
 * @param {string} typeId unified type id
 * @param {string} challengeId challenge identifier
 * @returns {string} user/track/type/challenge composite key
 */
function buildHistoryKey (userId, trackId, typeId, challengeId) {
  return `${buildUserTrackTypeKey(userId, trackId, typeId)}::${challengeId}`
}

/**
 * Load and cache the challenge track/type ids needed to translate legacy rows.
 * @param {Object} challengesClient prisma challenges client
 * @returns {Object} cached challenge lookup data for this process
 * @throws {Error} if required track ids cannot be resolved from ChallengeTrack
 */
async function initializeLegacyLookupCache (challengesClient) {
  if (legacyLookupCache) {
    return legacyLookupCache
  }

  clearChallengeDimensionLookupCache()
  legacyLookupCache = await loadChallengeDimensionLookup(challengesClient)

  if (!legacyLookupCache.trackIds.DEVELOP ||
    !legacyLookupCache.trackIds.DESIGN ||
    !legacyLookupCache.trackIds.DATA_SCIENCE) {
    legacyLookupCache = null
    throw new Error('Unable to resolve required challenge track ids for legacy stats migration')
  }

  return legacyLookupCache
}

/**
 * Resolve the unified challenge type id for a legacy subtrack label.
 * @param {*} typeValue legacy subtrack name or type label
 * @param {*} fallbackValue legacy subtrack id used as a final lookup candidate
 * @returns {string|null} unified challenge type id or null when not found
 * @throws {Error} if the challenge lookup cache has not been initialized
 */
function resolveChallengeTypeId (typeValue, fallbackValue) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const candidates = [
    typeValue,
    resolveTypeName(typeValue),
    fallbackValue
  ]

  for (const candidate of candidates) {
    const resolvedId = resolveTypeIdFromLookup(legacyLookupCache, candidate)
    if (resolvedId) {
      return resolvedId
    }
  }

  return null
}

/**
 * Collapse legacy design subtracks into the unified design track/type model.
 * Most legacy design subtracks map to Design + Challenge, while the dedicated
 * first-to-finish subtype maps to Design + First2Finish.
 * @param {*} typeValue legacy design subtrack name
 * @param {*} fallbackValue legacy design subtrack id
 * @returns {string|null} unified challenge type id or null when required ids are unavailable
 * @throws {Error} if the challenge lookup cache has not been initialized
 */
function resolveLegacyDesignTypeId (typeValue, fallbackValue) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const normalizedTypeName = normalizeLookupKey(typeValue)
  const normalizedFallbackValue = normalizeLookupKey(fallbackValue)
  const isDesignFirst2Finish = normalizedTypeName === 'DESIGN_FIRST_2_FINISH' || normalizedFallbackValue === '40'

  if (isDesignFirst2Finish) {
    return legacyLookupCache.typeIds.FIRST2FINISH || resolveChallengeTypeId(TYPE_NAMES.FIRST2FINISH)
  }

  return legacyLookupCache.typeIds.CHALLENGE || resolveChallengeTypeId(TYPE_NAMES.CHALLENGE)
}

/**
 * Merge non-null stat values into the keyed legacy rating lookup.
 * Legacy rating queries order duplicate snapshots newest-first, so existing
 * non-null values win and older snapshots only fill fields missing upstream.
 * @param {Map<string, Object>} lookup keyed legacy stat lookup
 * @param {string} key unified track/type lookup key
 * @param {Object} values legacy rating/rank values for the key
 */
function mergeLegacyStatLookup (lookup, key, values) {
  const existing = lookup.get(key) || {}
  const merged = { ...existing }

  Object.keys(values).forEach((field) => {
    if (values[field] !== null && values[field] !== undefined &&
      (merged[field] === null || merged[field] === undefined)) {
      merged[field] = values[field]
    }
  })

  lookup.set(key, merged)
}

/**
 * Load the legacy memberStats parent ids that still own the legacy child tables.
 * Only public rows are included because the migration script writes public unified stats.
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @returns {Promise<Array<BigInt>>} legacy parent ids for the user
 */
async function getLegacyMemberStatsIds (membersClient, userId) {
  const normalizedUserId = normalizeBigInt(userId, 'user id')
  const lookup = await getLegacyMemberStatsIdsByUserIds(membersClient, [normalizedUserId])
  return lookup.get(normalizedUserId.toString()) || []
}

/**
 * Load legacy memberStats parent ids for multiple users in one query.
 * The resulting map is reused across the aggregate and rating enrichment passes
 * so the migration does not re-query the same parent ids for every user step.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} userIds member user ids
 * @returns {Promise<Map<string, Array<BigInt>>>} map keyed by user id string
 */
async function getLegacyMemberStatsIdsByUserIds (membersClient, userIds) {
  if (!userIds || userIds.length === 0) {
    return new Map()
  }

  const normalizedUserIds = Array.from(
    new Set(userIds.map((userId) => normalizeBigInt(userId, 'user id').toString()))
  )
  if (normalizedUserIds.length === 0) {
    return new Map()
  }

  const rows = await membersClient.$queryRawUnsafe(
    `
    SELECT ms."userId" AS "userId", ms."id" AS "id"
    FROM "members"."memberStats" ms
    WHERE ms."userId" IN (${normalizedUserIds.join(', ')})
      AND ms."isPrivate" = false
      AND (
        EXISTS (SELECT 1 FROM "members"."memberDevelopStats" mdev WHERE mdev."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberDesignStats" mdes WHERE mdes."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" mds WHERE mds."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberCopilotStats" mcs WHERE mcs."memberStatsId" = ms."id")
      )
    ORDER BY ms."userId" ASC, ms."id" ASC
    `
  )

  const legacyIdsByUserId = new Map()

  rows.forEach((row) => {
    const userKey = normalizeBigInt(row.userId, 'user id').toString()
    const ids = legacyIdsByUserId.get(userKey) || []
    ids.push(normalizeBigInt(row.id, 'legacy memberStats id'))
    legacyIdsByUserId.set(userKey, ids)
  })

  return legacyIdsByUserId
}

/**
 * Identify memberStats rows that still own legacy child-table data.
 * Replacement cleanup must never delete these rows because they remain the
 * authoritative source for reruns and rollback validation.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} memberStatsIds memberStats row ids
 * @returns {Promise<Set<string>>} stringified legacy-backed memberStats ids
 */
async function getLegacyBackedMemberStatsIdSet (membersClient, memberStatsIds) {
  if (!memberStatsIds || memberStatsIds.length === 0) {
    return new Set()
  }

  const normalizedIds = Array.from(
    new Set(memberStatsIds.map((memberStatsId) => normalizeBigInt(memberStatsId, 'memberStats id').toString()))
  )
  if (normalizedIds.length === 0) {
    return new Set()
  }

  const rows = await membersClient.$queryRawUnsafe(
    `
    SELECT ms."id" AS "id"
    FROM "members"."memberStats" ms
    WHERE ms."id" IN (${normalizedIds.join(', ')})
      AND (
        EXISTS (SELECT 1 FROM "members"."memberDevelopStats" mdev WHERE mdev."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberDesignStats" mdes WHERE mdes."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" mds WHERE mds."memberStatsId" = ms."id")
        OR EXISTS (SELECT 1 FROM "members"."memberCopilotStats" mcs WHERE mcs."memberStatsId" = ms."id")
      )
    `
  )

  return new Set(rows.map((row) => normalizeBigInt(row.id, 'memberStats id').toString()))
}

function buildAggregateRecord (userId, trackId, typeId, row) {
  return {
    userId,
    trackId,
    typeId,
    challenges: toInt(row.challenges),
    wins: toInt(row.wins),
    mostRecentEventDate: row.mostRecentEventDate ? new Date(row.mostRecentEventDate) : null,
    mostRecentSubmission: row.mostRecentSubmission ? new Date(row.mostRecentSubmission) : null,
    rating: null,
    avgRank: null,
    avgNumSubmissions: null,
    bestRank: null,
    globalRank: null,
    countryRank: null,
    schoolRank: null,
    volatility: null,
    maxRating: null,
    minRating: null,
    topFiveFinishes: null,
    topTenFinishes: null,
    isPrivate: false
  }
}

function mergeAggregateRecord (lookup, record) {
  const key = buildTrackTypeKey(record.trackId, record.typeId)
  const existing = lookup.get(key)

  if (!existing) {
    lookup.set(key, { ...record })
    return
  }

  existing.challenges = toInt(existing.challenges) + toInt(record.challenges)
  existing.wins = toInt(existing.wins) + toInt(record.wins)

  if (record.mostRecentEventDate &&
    (!existing.mostRecentEventDate || record.mostRecentEventDate > existing.mostRecentEventDate)) {
    existing.mostRecentEventDate = record.mostRecentEventDate
  }

  if (record.mostRecentSubmission &&
    (!existing.mostRecentSubmission || record.mostRecentSubmission > existing.mostRecentSubmission)) {
    existing.mostRecentSubmission = record.mostRecentSubmission
  }
}

function getAggregateLookupForUser (aggregateLookupsByUserId, userId) {
  const userKey = normalizeBigInt(userId, 'user id').toString()
  let aggregateLookup = aggregateLookupsByUserId.get(userKey)

  if (!aggregateLookup) {
    aggregateLookup = new Map()
    aggregateLookupsByUserId.set(userKey, aggregateLookup)
  }

  return aggregateLookup
}

function finalizeAggregateRowsByUserId (aggregateLookupsByUserId, options) {
  const rowsByUserId = new Map()

  aggregateLookupsByUserId.forEach((aggregateLookup, userKey) => {
    rowsByUserId.set(userKey, applyTrackTypeFilters(Array.from(aggregateLookup.values()), options))
  })

  return rowsByUserId
}

function applyTrackTypeFilters (records, options) {
  return (records || []).filter((record) => {
    if (options.trackId && record.trackId !== options.trackId) {
      return false
    }

    if (options.typeId && record.typeId !== options.typeId) {
      return false
    }

    return true
  })
}

/**
 * Parse an optional date-like value into a valid Date instance.
 * @param {*} value candidate date value
 * @returns {Date|null} normalized date or null when parsing fails
 */
function toOptionalDate (value) {
  if (!value) {
    return null
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value
  }

  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

/**
 * Resolve the latest known timestamp carried by an aggregate-style record.
 * @param {Object|null|undefined} record aggregate record with mostRecent timestamps
 * @returns {Date|null} latest timestamp or null when neither field is populated
 */
function getLatestAggregateTimestamp (record) {
  if (!record) {
    return null
  }

  const mostRecentEventDate = toOptionalDate(record.mostRecentEventDate)
  const mostRecentSubmission = toOptionalDate(record.mostRecentSubmission)

  if (mostRecentEventDate && mostRecentSubmission) {
    return mostRecentEventDate > mostRecentSubmission ? mostRecentEventDate : mostRecentSubmission
  }

  return mostRecentEventDate || mostRecentSubmission || null
}

/**
 * Normalize one review-api challengeResult row into the unified aggregate shape.
 * @param {Object} row raw challengeResult row
 * @param {Object|null|undefined} challenge challenge-api metadata for the row
 * @param {Object} options script filters
 * @returns {Object|null} normalized aggregate delta or null when the row cannot be used
 */
function buildReviewAggregateRecord (row, challenge, options) {
  if (!isUsableReviewChallengeResultRow(row)) {
    return null
  }

  if (!challenge || !challenge.trackId || !challenge.typeId) {
    return null
  }

  const dimension = normalizeChallengeStatsDimension(challenge.trackId, challenge.typeId)
  const trackId = dimension.trackId
  const typeId = dimension.typeId

  if (!matchesNormalizedStatsFilters(dimension, options)) {
    return null
  }

  const submissionDate = toOptionalDate(row.createdAt)
  const eventDate = toOptionalDate(challenge.endDate) || submissionDate

  return {
    userId: normalizeBigInt(row.userId, 'user id'),
    trackId,
    typeId,
    challenges: 1,
    wins: toInt(row.placement) === 1 ? 1 : 0,
    mostRecentEventDate: eventDate,
    mostRecentSubmission: submissionDate,
    rating: null,
    avgRank: null,
    avgNumSubmissions: null,
    bestRank: null,
    globalRank: null,
    countryRank: null,
    schoolRank: null,
    volatility: null,
    maxRating: null,
    minRating: null,
    topFiveFinishes: null,
    topTenFinishes: null,
    isPrivate: false
  }
}

/**
 * Aggregate review-api challengeResult rows into unified memberStats records.
 * When legacy rows already exist for the same track/type, only review rows that
 * extend the known timeline are counted so stale legacy aggregates can be topped up
 * without re-counting historical overlap.
 * @param {Array<Object>} reviewRows raw challengeResult rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by challengeId
 * @param {Object} options script filters
 * @param {Array<Object>} [existingRows=[]] legacy aggregate rows used as the baseline
 * @returns {Array<Object>} supplemental unified aggregate rows
 */
function buildAggregatedStatsFromReviewResults (reviewRows, challengeMetadataById, options, existingRows = []) {
  const existingByKey = new Map(
    (existingRows || []).map((row) => [buildTrackTypeKey(row.trackId, row.typeId), row])
  )
  const aggregateLookup = new Map()
  const normalizedReviewRows = reviewRows || []

  normalizedReviewRows.forEach((row) => {
    const aggregateRecord = buildReviewAggregateRecord(
      row,
      challengeMetadataById.get(String(row.challengeId)),
      options
    )
    if (!aggregateRecord) {
      return
    }

    const key = buildTrackTypeKey(aggregateRecord.trackId, aggregateRecord.typeId)
    const existingRow = existingByKey.get(key)
    const existingTimestamp = getLatestAggregateTimestamp(existingRow)
    const reviewTimestamp = getLatestAggregateTimestamp(aggregateRecord)

    if (existingTimestamp && reviewTimestamp && reviewTimestamp <= existingTimestamp) {
      return
    }

    mergeAggregateRecord(aggregateLookup, aggregateRecord)
  })

  return Array.from(aggregateLookup.values())
}

/**
 * Load review-api challengeResult rows for one member.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {BigInt} userId member identifier
 * @returns {Promise<Array<Object>>} raw challengeResult rows ordered by submission time
 */
async function fetchReviewChallengeResultsForUser (reviewDbClient, userId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "submissionId", "placement",
             "validSubmission", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "userId" = $1
        AND "validSubmission" IS DISTINCT FROM FALSE
        AND "submissionId" IS NOT NULL
      ORDER BY "createdAt" ASC, "challengeId" ASC
    `,
    [String(userId)]
  )

  return result.rows
}

/**
 * Load review-api challengeResult rows for multiple members in one query.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Array<BigInt>} userIds member identifiers
 * @returns {Promise<Map<string, Array<Object>>>} raw challengeResult rows keyed by user id
 */
async function fetchReviewChallengeResultsByUserIds (reviewDbClient, userIds) {
  if (!userIds || userIds.length === 0) {
    return new Map()
  }

  const normalizedUserIds = Array.from(
    new Set(userIds.map((userId) => normalizeBigInt(userId, 'user id').toString()))
  )
  if (normalizedUserIds.length === 0) {
    return new Map()
  }

  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "submissionId", "placement",
             "validSubmission", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "userId" = ANY($1::text[])
        AND "validSubmission" IS DISTINCT FROM FALSE
        AND "submissionId" IS NOT NULL
      ORDER BY "userId" ASC, "createdAt" ASC, "challengeId" ASC
    `,
    [normalizedUserIds]
  )

  const rowsByUserId = new Map()
  normalizedUserIds.forEach((userId) => {
    rowsByUserId.set(userId, [])
  })

  result.rows.forEach((row) => {
    const userKey = normalizeBigInt(row.userId, 'user id').toString()
    const rows = rowsByUserId.get(userKey)
    if (rows) {
      rows.push(row)
    }
  })

  return rowsByUserId
}

/**
 * Resolve challenge metadata for review aggregate rows, using a process-local cache
 * so repeated user passes do not re-query the same challenges.
 * @param {Object} challengesClient prisma challenges client
 * @param {Array<string>} challengeIds challenge ids required for aggregation
 * @returns {Promise<Map<string, Object|null>>} metadata keyed by challenge id
 */
async function fetchChallengeMetadataMap (challengesClient, challengeIds) {
  if (!challengeIds || challengeIds.length === 0) {
    return new Map()
  }

  const uniqueIds = Array.from(new Set(challengeIds.map((challengeId) => String(challengeId)).filter(Boolean)))
  const missingIds = []
  const metadataById = new Map()

  uniqueIds.forEach((challengeId) => {
    if (reviewChallengeMetadataCache.has(challengeId)) {
      metadataById.set(challengeId, reviewChallengeMetadataCache.get(challengeId))
      return
    }

    missingIds.push(challengeId)
  })

  if (missingIds.length > 0) {
    const rows = await challengesClient.challenge.findMany({
      where: {
        id: {
          in: missingIds
        }
      },
      select: {
        id: true,
        trackId: true,
        typeId: true,
        endDate: true,
        status: true
      }
    })

    const fetchedById = new Map(rows.map((row) => [String(row.id), row]))
    missingIds.forEach((challengeId) => {
      const metadata = fetchedById.get(challengeId) || null
      reviewChallengeMetadataCache.set(challengeId, metadata)
      metadataById.set(challengeId, metadata)
    })
  }

  return metadataById
}

/**
 * Resolve challenge ids that match the current track/type filters.
 * @param {Object} challengesClient prisma challenges client
 * @param {Object} options script filters
 * @returns {Promise<Array<string>|null>} filtered challenge ids, or null when unfiltered
 */
async function getFilteredChallengeIds (challengesClient, options) {
  if (!options.trackId && !options.typeId) {
    return null
  }

  const includeMarathonByType = legacyLookupCache &&
    options.trackId === legacyLookupCache.trackIds.DATA_SCIENCE &&
    legacyLookupCache.typeIds.MARATHON_MATCH

  const rows = await challengesClient.challenge.findMany({
    where: {
      ...(options.trackId && !includeMarathonByType ? { trackId: options.trackId } : {}),
      ...(includeMarathonByType
        ? {
          OR: [
            { trackId: options.trackId },
            { typeId: legacyLookupCache.typeIds.MARATHON_MATCH }
          ]
        }
        : {}),
      ...(options.typeId ? { typeId: options.typeId } : {})
    },
    select: {
      id: true
    }
  })

  return rows.map((row) => String(row.id))
}

/**
 * Load distinct numeric user ids from review-api challengeResult.
 * Track/type filtered runs first resolve matching challenge ids from challenge-api
 * so the user universe stays aligned with the requested dimensions.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Object} challengesClient prisma challenges client
 * @param {Object} options script filters
 * @returns {Promise<Array<BigInt>>} distinct member identifiers
 */
async function getReviewUserIds (reviewDbClient, challengesClient, options) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const filteredChallengeIds = await getFilteredChallengeIds(challengesClient, options)

  if (filteredChallengeIds && filteredChallengeIds.length === 0) {
    return []
  }

  const userIdStrings = new Set()

  if (!filteredChallengeIds) {
    const rows = await reviewDbClient.query(
      `
        SELECT DISTINCT "userId"
        FROM ${challengeResultRelation}
        WHERE "userId" ~ '^[0-9]+$'
          AND "validSubmission" IS DISTINCT FROM FALSE
          AND "submissionId" IS NOT NULL
        ORDER BY "userId" ASC
      `
    )

    rows.rows.forEach((row) => {
      userIdStrings.add(String(row.userId))
    })
  } else {
    const CHALLENGE_BATCH_SIZE = 5000

    for (let start = 0; start < filteredChallengeIds.length; start += CHALLENGE_BATCH_SIZE) {
      const challengeIds = filteredChallengeIds.slice(start, start + CHALLENGE_BATCH_SIZE)
      const rows = await reviewDbClient.query(
        `
          SELECT DISTINCT "userId"
          FROM ${challengeResultRelation}
          WHERE "userId" ~ '^[0-9]+$'
            AND "challengeId" = ANY($1::text[])
            AND "validSubmission" IS DISTINCT FROM FALSE
            AND "submissionId" IS NOT NULL
        `,
        [challengeIds]
      )

      rows.rows.forEach((row) => {
        userIdStrings.add(String(row.userId))
      })
    }
  }

  return Array.from(userIdStrings)
    .sort((left, right) => {
      const leftId = normalizeBigInt(left, 'user id')
      const rightId = normalizeBigInt(right, 'user id')
      if (leftId === rightId) {
        return 0
      }
      return leftId < rightId ? -1 : 1
    })
    .map((userId) => normalizeBigInt(userId, 'user id'))
}

/**
 * Aggregate unified memberStats rows from review-api challengeResult rows.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Object} challengesClient prisma challenges client
 * @param {BigInt} userId member user id
 * @param {Object} options script options
 * @param {Array<Object>} [existingRows=[]] baseline legacy rows used to suppress overlap
 * @returns {Promise<Array<Object>>} unified aggregate rows derived from review results
 */
async function aggregateReviewStatsForUser (reviewDbClient, challengesClient, userId, options, existingRows = []) {
  const reviewRows = await fetchReviewChallengeResultsForUser(reviewDbClient, userId)
  if (reviewRows.length === 0) {
    return []
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengesClient,
    reviewRows.map((row) => row.challengeId)
  )

  return buildAggregatedStatsFromReviewResults(reviewRows, challengeMetadataById, options, existingRows)
}

/**
 * Aggregate legacy memberStats rows for a batch of users in one pass.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} userIds member user ids
 * @param {Object} options script options
 * @param {Map<string, Array<BigInt>>} [legacyIdsByUserId] preloaded legacy parent ids
 * @returns {Promise<Map<string, Array<Object>>>} aggregate rows keyed by user id
 * @throws {Error} if the legacy challenge lookup cache is not initialized
 */
async function aggregateLegacyStatsByUserIds (membersClient, userIds, options, legacyIdsByUserId = null) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const resolvedLegacyIdsByUserId = legacyIdsByUserId || await getLegacyMemberStatsIdsByUserIds(membersClient, userIds)
  const allLegacyIds = Array.from(
    new Set(
      Array.from(resolvedLegacyIdsByUserId.values())
        .flat()
        .map((legacyId) => normalizeBigInt(legacyId, 'legacy memberStats id').toString())
    )
  )

  if (allLegacyIds.length === 0) {
    return new Map()
  }

  const idsSql = allLegacyIds.join(', ')
  const aggregateLookupsByUserId = new Map()

  const [developRows, designRows, srmRows, marathonRows] = await Promise.all([
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."challenges" AS "challenges",
        mdi."wins" AS "wins",
        mdi."mostRecentSubmission" AS "mostRecentSubmission",
        mdi."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDevelopStats" mds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = mds."memberStatsId"
      INNER JOIN "members"."memberDevelopStatsItem" mdi
        ON mdi."developStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, mdi."subTrackId" ASC, mdi."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."challenges" AS "challenges",
        mdi."wins" AS "wins",
        mdi."mostRecentSubmission" AS "mostRecentSubmission",
        mdi."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDesignStats" mds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = mds."memberStatsId"
      INNER JOIN "members"."memberDesignStatsItem" mdi
        ON mdi."designStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, mdi."subTrackId" ASC, mdi."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        srm."challenges" AS "challenges",
        srm."wins" AS "wins",
        srm."mostRecentSubmission" AS "mostRecentSubmission",
        srm."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = ds."memberStatsId"
      INNER JOIN "members"."memberSrmStats" srm
        ON srm."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, srm."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        marathon."challenges" AS "challenges",
        marathon."wins" AS "wins",
        marathon."mostRecentSubmission" AS "mostRecentSubmission",
        marathon."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = ds."memberStatsId"
      INNER JOIN "members"."memberMarathonStats" marathon
        ON marathon."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, marathon."id" ASC
      `
    )
  ])

  developRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy develop aggregate row for user ${row.userId} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    const userId = normalizeBigInt(row.userId, 'user id')
    mergeAggregateRecord(
      getAggregateLookupForUser(aggregateLookupsByUserId, userId),
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DEVELOP, typeId, row)
    )
  })

  designRows.forEach((row) => {
    const typeId = resolveLegacyDesignTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy design aggregate row for user ${row.userId} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    const userId = normalizeBigInt(row.userId, 'user id')
    mergeAggregateRecord(
      getAggregateLookupForUser(aggregateLookupsByUserId, userId),
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DESIGN, typeId, row)
    )
  })

  srmRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.SRM || resolveChallengeTypeId(TYPE_NAMES.SRM)
    if (!typeId) {
      logWarn(`Skipping legacy SRM aggregate row for user ${row.userId} because the SRM type id could not be resolved`)
      return
    }

    const userId = normalizeBigInt(row.userId, 'user id')
    mergeAggregateRecord(
      getAggregateLookupForUser(aggregateLookupsByUserId, userId),
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DATA_SCIENCE, typeId, row)
    )
  })

  marathonRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.MARATHON_MATCH || resolveChallengeTypeId(TYPE_NAMES.MARATHON_MATCH)
    if (!typeId) {
      logWarn(`Skipping legacy marathon aggregate row for user ${row.userId} because the marathon type id could not be resolved`)
      return
    }

    const userId = normalizeBigInt(row.userId, 'user id')
    mergeAggregateRecord(
      getAggregateLookupForUser(aggregateLookupsByUserId, userId),
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DATA_SCIENCE, typeId, row)
    )
  })

  return finalizeAggregateRowsByUserId(aggregateLookupsByUserId, options)
}

/**
 * Aggregate unified memberStats rows from the legacy public stats tables.
 * This is the parity source for cutover because it matches the legacy API output.
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @param {Object} options script options
 * @param {Array<BigInt>|null} [legacyIds=null] preloaded legacy parent ids for the user
 * @returns {Promise<Array<Object>>} per-track/type unified stats rows
 */
async function aggregateLegacyStatsForUser (membersClient, userId, options, legacyIds = null) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const resolvedLegacyIds = legacyIds || await getLegacyMemberStatsIds(membersClient, userId)
  if (resolvedLegacyIds.length === 0) {
    return []
  }

  const idsSql = resolvedLegacyIds.map((id) => id.toString()).join(', ')
  const aggregateLookup = new Map()

  const [developRows, designRows, srmRows, marathonRows] = await Promise.all([
    membersClient.$queryRawUnsafe(
      `
      SELECT
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."challenges" AS "challenges",
        mdi."wins" AS "wins",
        mdi."mostRecentSubmission" AS "mostRecentSubmission",
        mdi."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDevelopStats" mds
      INNER JOIN "members"."memberDevelopStatsItem" mdi
        ON mdi."developStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY mdi."subTrackId" ASC, mdi."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."challenges" AS "challenges",
        mdi."wins" AS "wins",
        mdi."mostRecentSubmission" AS "mostRecentSubmission",
        mdi."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDesignStats" mds
      INNER JOIN "members"."memberDesignStatsItem" mdi
        ON mdi."designStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY mdi."subTrackId" ASC, mdi."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        srm."challenges" AS "challenges",
        srm."wins" AS "wins",
        srm."mostRecentSubmission" AS "mostRecentSubmission",
        srm."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberSrmStats" srm
        ON srm."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY srm."id" ASC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        marathon."challenges" AS "challenges",
        marathon."wins" AS "wins",
        marathon."mostRecentSubmission" AS "mostRecentSubmission",
        marathon."mostRecentEventDate" AS "mostRecentEventDate"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberMarathonStats" marathon
        ON marathon."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY marathon."id" ASC
      `
    )
  ])

  developRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy develop aggregate row for user ${userId.toString()} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    mergeAggregateRecord(
      aggregateLookup,
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DEVELOP, typeId, row)
    )
  })

  designRows.forEach((row) => {
    const typeId = resolveLegacyDesignTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy design aggregate row for user ${userId.toString()} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    mergeAggregateRecord(
      aggregateLookup,
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DESIGN, typeId, row)
    )
  })

  srmRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.SRM || resolveChallengeTypeId(TYPE_NAMES.SRM)
    if (!typeId) {
      logWarn(`Skipping legacy SRM aggregate row for user ${userId.toString()} because the SRM type id could not be resolved`)
      return
    }

    mergeAggregateRecord(
      aggregateLookup,
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DATA_SCIENCE, typeId, row)
    )
  })

  marathonRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.MARATHON_MATCH || resolveChallengeTypeId(TYPE_NAMES.MARATHON_MATCH)
    if (!typeId) {
      logWarn(`Skipping legacy marathon aggregate row for user ${userId.toString()} because the marathon type id could not be resolved`)
      return
    }

    mergeAggregateRecord(
      aggregateLookup,
      buildAggregateRecord(userId, legacyLookupCache.trackIds.DATA_SCIENCE, typeId, row)
    )
  })

  return applyTrackTypeFilters(Array.from(aggregateLookup.values()), options)
}

/**
 * Load rating and rank fields from the legacy member stat sub-tables for one user.
 * The returned map is merged into the rebuilt aggregate rows before unified writes.
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @param {Array<BigInt>|null} [legacyIds=null] preloaded legacy parent ids for the user
 * @returns {Map<string, Object>} map keyed by trackId::typeId with legacy rating fields
 * @throws {Error} if the legacy challenge lookup cache is not initialized
 */
async function fetchLegacyRatingFields (membersClient, userId, legacyIds = null) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const resolvedLegacyIds = legacyIds || await getLegacyMemberStatsIds(membersClient, userId)
  if (resolvedLegacyIds.length === 0) {
    return new Map()
  }

  const idsSql = resolvedLegacyIds.map((row) => row.toString()).join(', ')
  const lookup = new Map()

  const [developRows, srmRows, marathonRows] = await Promise.all([
    membersClient.$queryRawUnsafe(
      `
      SELECT
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."rating" AS "rating",
        mdi."minRating" AS "minRating",
        mdi."maxRating" AS "maxRating",
        mdi."volatility" AS "volatility",
        mdi."overallRank" AS "overallRank",
        mdi."overallCountryRank" AS "overallCountryRank",
        mdi."overallSchoolRank" AS "overallSchoolRank"
      FROM "members"."memberDevelopStats" mds
      INNER JOIN "members"."memberDevelopStatsItem" mdi
        ON mdi."developStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY mdi."subTrackId" ASC, mdi."id" DESC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        srm."rating" AS "rating",
        srm."minimumRating" AS "minimumRating",
        srm."maximumRating" AS "maximumRating",
        srm."volatility" AS "volatility",
        srm."rank" AS "rank",
        srm."countryRank" AS "countryRank",
        srm."schoolRank" AS "schoolRank"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberSrmStats" srm
        ON srm."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY srm."id" DESC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        marathon."rating" AS "rating",
        marathon."minimumRating" AS "minimumRating",
        marathon."maximumRating" AS "maximumRating",
        marathon."volatility" AS "volatility",
        marathon."rank" AS "rank",
        marathon."countryRank" AS "countryRank",
        marathon."schoolRank" AS "schoolRank",
        marathon."avgRank" AS "avgRank",
        marathon."avgNumSubmissions" AS "avgNumSubmissions",
        marathon."bestRank" AS "bestRank",
        marathon."topFiveFinishes" AS "topFiveFinishes",
        marathon."topTenFinishes" AS "topTenFinishes"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberMarathonStats" marathon
        ON marathon."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY marathon."id" DESC
      `
    )
  ])

  developRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy develop rating row for user ${userId.toString()} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    mergeLegacyStatLookup(
      lookup,
      buildTrackTypeKey(legacyLookupCache.trackIds.DEVELOP, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: null,
        avgNumSubmissions: null,
        bestRank: null,
        globalRank: toOptionalInt(row.overallRank),
        countryRank: toOptionalInt(row.overallCountryRank),
        schoolRank: toOptionalInt(row.overallSchoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maxRating),
        minRating: toOptionalInt(row.minRating),
        topFiveFinishes: null,
        topTenFinishes: null
      }
    )
  })

  srmRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.SRM || resolveChallengeTypeId(TYPE_NAMES.SRM)
    if (!typeId) {
      logWarn(`Skipping legacy SRM rating row for user ${userId.toString()} because the SRM type id could not be resolved`)
      return
    }

    mergeLegacyStatLookup(
      lookup,
      buildTrackTypeKey(legacyLookupCache.trackIds.DATA_SCIENCE, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: null,
        avgNumSubmissions: null,
        bestRank: null,
        globalRank: toOptionalInt(row.rank),
        countryRank: toOptionalInt(row.countryRank),
        schoolRank: toOptionalInt(row.schoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maximumRating),
        minRating: toOptionalInt(row.minimumRating),
        topFiveFinishes: null,
        topTenFinishes: null
      }
    )
  })

  marathonRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.MARATHON_MATCH || resolveChallengeTypeId(TYPE_NAMES.MARATHON_MATCH)
    if (!typeId) {
      logWarn(`Skipping legacy marathon rating row for user ${userId.toString()} because the marathon type id could not be resolved`)
      return
    }

    mergeLegacyStatLookup(
      lookup,
      buildTrackTypeKey(legacyLookupCache.trackIds.DATA_SCIENCE, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: toOptionalFloat(row.avgRank),
        avgNumSubmissions: toOptionalInt(row.avgNumSubmissions),
        bestRank: toOptionalInt(row.bestRank),
        globalRank: toOptionalInt(row.rank),
        countryRank: toOptionalInt(row.countryRank),
        schoolRank: toOptionalInt(row.schoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maximumRating),
        minRating: toOptionalInt(row.minimumRating),
        topFiveFinishes: toOptionalInt(row.topFiveFinishes),
        topTenFinishes: toOptionalInt(row.topTenFinishes)
      }
    )
  })

  return lookup
}

function getLegacyFieldLookupForUser (fieldsByUserId, userId) {
  const userKey = normalizeBigInt(userId, 'user id').toString()
  let fieldLookup = fieldsByUserId.get(userKey)

  if (!fieldLookup) {
    fieldLookup = new Map()
    fieldsByUserId.set(userKey, fieldLookup)
  }

  return fieldLookup
}

/**
 * Load legacy rating/rank fields for a batch of users in one pass.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} userIds member user ids
 * @param {Map<string, Array<BigInt>>} [legacyIdsByUserId] preloaded legacy parent ids
 * @returns {Promise<Map<string, Map<string, Object>>>} rating fields keyed by user id and track/type
 * @throws {Error} if the legacy challenge lookup cache is not initialized
 */
async function fetchLegacyRatingFieldsByUserIds (membersClient, userIds, legacyIdsByUserId = null) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const resolvedLegacyIdsByUserId = legacyIdsByUserId || await getLegacyMemberStatsIdsByUserIds(membersClient, userIds)
  const allLegacyIds = Array.from(
    new Set(
      Array.from(resolvedLegacyIdsByUserId.values())
        .flat()
        .map((legacyId) => normalizeBigInt(legacyId, 'legacy memberStats id').toString())
    )
  )

  if (allLegacyIds.length === 0) {
    return new Map()
  }

  const idsSql = allLegacyIds.join(', ')
  const fieldsByUserId = new Map()

  const [developRows, srmRows, marathonRows] = await Promise.all([
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        mdi."subTrackId" AS "subTrackId",
        mdi."name" AS "name",
        mdi."rating" AS "rating",
        mdi."minRating" AS "minRating",
        mdi."maxRating" AS "maxRating",
        mdi."volatility" AS "volatility",
        mdi."overallRank" AS "overallRank",
        mdi."overallCountryRank" AS "overallCountryRank",
        mdi."overallSchoolRank" AS "overallSchoolRank"
      FROM "members"."memberDevelopStats" mds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = mds."memberStatsId"
      INNER JOIN "members"."memberDevelopStatsItem" mdi
        ON mdi."developStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, mdi."subTrackId" ASC, mdi."id" DESC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        srm."rating" AS "rating",
        srm."minimumRating" AS "minimumRating",
        srm."maximumRating" AS "maximumRating",
        srm."volatility" AS "volatility",
        srm."rank" AS "rank",
        srm."countryRank" AS "countryRank",
        srm."schoolRank" AS "schoolRank"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = ds."memberStatsId"
      INNER JOIN "members"."memberSrmStats" srm
        ON srm."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, srm."id" DESC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        ms."userId" AS "userId",
        marathon."rating" AS "rating",
        marathon."minimumRating" AS "minimumRating",
        marathon."maximumRating" AS "maximumRating",
        marathon."volatility" AS "volatility",
        marathon."rank" AS "rank",
        marathon."countryRank" AS "countryRank",
        marathon."schoolRank" AS "schoolRank",
        marathon."avgRank" AS "avgRank",
        marathon."avgNumSubmissions" AS "avgNumSubmissions",
        marathon."bestRank" AS "bestRank",
        marathon."topFiveFinishes" AS "topFiveFinishes",
        marathon."topTenFinishes" AS "topTenFinishes"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberStats" ms
        ON ms."id" = ds."memberStatsId"
      INNER JOIN "members"."memberMarathonStats" marathon
        ON marathon."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY ms."userId" ASC, marathon."id" DESC
      `
    )
  ])

  developRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.name, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy develop rating row for user ${row.userId} and subTrack ${row.name || row.subTrackId}`)
      return
    }

    mergeLegacyStatLookup(
      getLegacyFieldLookupForUser(fieldsByUserId, row.userId),
      buildTrackTypeKey(legacyLookupCache.trackIds.DEVELOP, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: null,
        avgNumSubmissions: null,
        bestRank: null,
        globalRank: toOptionalInt(row.overallRank),
        countryRank: toOptionalInt(row.overallCountryRank),
        schoolRank: toOptionalInt(row.overallSchoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maxRating),
        minRating: toOptionalInt(row.minRating),
        topFiveFinishes: null,
        topTenFinishes: null
      }
    )
  })

  srmRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.SRM || resolveChallengeTypeId(TYPE_NAMES.SRM)
    if (!typeId) {
      logWarn(`Skipping legacy SRM rating row for user ${row.userId} because the SRM type id could not be resolved`)
      return
    }

    mergeLegacyStatLookup(
      getLegacyFieldLookupForUser(fieldsByUserId, row.userId),
      buildTrackTypeKey(legacyLookupCache.trackIds.DATA_SCIENCE, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: null,
        avgNumSubmissions: null,
        bestRank: null,
        globalRank: toOptionalInt(row.rank),
        countryRank: toOptionalInt(row.countryRank),
        schoolRank: toOptionalInt(row.schoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maximumRating),
        minRating: toOptionalInt(row.minimumRating),
        topFiveFinishes: null,
        topTenFinishes: null
      }
    )
  })

  marathonRows.forEach((row) => {
    const typeId = legacyLookupCache.typeIds.MARATHON_MATCH || resolveChallengeTypeId(TYPE_NAMES.MARATHON_MATCH)
    if (!typeId) {
      logWarn(`Skipping legacy marathon rating row for user ${row.userId} because the marathon type id could not be resolved`)
      return
    }

    mergeLegacyStatLookup(
      getLegacyFieldLookupForUser(fieldsByUserId, row.userId),
      buildTrackTypeKey(legacyLookupCache.trackIds.DATA_SCIENCE, typeId),
      {
        rating: toOptionalInt(row.rating),
        avgRank: toOptionalFloat(row.avgRank),
        avgNumSubmissions: toOptionalInt(row.avgNumSubmissions),
        bestRank: toOptionalInt(row.bestRank),
        globalRank: toOptionalInt(row.rank),
        countryRank: toOptionalInt(row.countryRank),
        schoolRank: toOptionalInt(row.schoolRank),
        volatility: toOptionalInt(row.volatility),
        maxRating: toOptionalInt(row.maximumRating),
        minRating: toOptionalInt(row.minimumRating),
        topFiveFinishes: toOptionalInt(row.topFiveFinishes),
        topTenFinishes: toOptionalInt(row.topTenFinishes)
      }
    )
  })

  return fieldsByUserId
}

/**
 * Keep the latest event payload for duplicate unified history keys.
 * @param {Map<string, Object>} lookup keyed unified history lookup
 * @param {Object} row candidate history row
 */
function mergeHistoryRow (lookup, row) {
  const key = buildTrackTypeKey(row.trackId, row.typeId) + `::${row.challengeId}`
  const existing = lookup.get(key)

  if (!existing) {
    lookup.set(key, row)
    return
  }

  if (existing.eventDate < row.eventDate) {
    lookup.set(key, row)
  }
}

function isCompletedChallengeStatus (value) {
  return String(value || '').trim().toUpperCase() === COMPLETED_CHALLENGE_STATUS
}

/**
 * Persist normalized unified history rows for one user.
 * Existing rows are matched by (userId, trackId, typeId, challengeId).
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @param {Array<Object>} historyRows normalized rows to update or insert
 * @param {Object} [options] history write options
 * @param {boolean} [options.refreshMostRecent=true] whether to recompute mostRecent after writes
 * @returns {Promise<Object>} summary with upserted row count and refreshed mostRecent row count
 */
async function upsertHistoryRows (membersClient, userId, historyRows, options = {}) {
  if (!historyRows || historyRows.length === 0) {
    return {
      upserted: 0,
      refreshed: options.refreshMostRecent === false
        ? 0
        : await refreshHistoryMostRecentFlagsForUsers(membersClient, [userId])
    }
  }

  const existingRows = await membersClient.memberStatsHistory.findMany({
    where: { userId },
    select: {
      id: true,
      trackId: true,
      typeId: true,
      challengeId: true,
      placement: true,
      percentile: true
    }
  })

  const existingByKey = new Map(
    existingRows.map((row) => [
      buildHistoryKey(userId, row.trackId, row.typeId, row.challengeId),
      row
    ])
  )

  const historyRowsToUpdate = []
  const historyRowsToInsert = []
  historyRows.forEach((row) => {
    const existingRow = existingByKey.get(buildHistoryKey(userId, row.trackId, row.typeId, row.challengeId))

    if (existingRow) {
      historyRowsToUpdate.push({
        id: existingRow.id,
        eventDate: row.eventDate,
        newRating: row.newRating,
        placement: row.placement === undefined
          ? (existingRow.placement === undefined ? null : existingRow.placement)
          : row.placement,
        percentile: row.percentile === undefined
          ? (existingRow.percentile === undefined ? null : existingRow.percentile)
          : row.percentile
      })
      return
    }

    historyRowsToInsert.push({
      userId,
      trackId: row.trackId,
      typeId: row.typeId,
      challengeId: row.challengeId,
      eventDate: row.eventDate,
      newRating: row.newRating,
      placement: row.placement === undefined ? null : row.placement,
      percentile: row.percentile === undefined ? null : row.percentile
    })
  })

  const queries = []
  if (historyRowsToUpdate.length > 0) {
    chunkRecordsForParameterizedQuery(historyRowsToUpdate, HISTORY_BULK_UPDATE_PARAM_COUNT)
      .forEach((historyRowsChunk) => {
        const { sql, params } = buildMemberStatsHistoryBulkUpdateQuery(historyRowsChunk)
        queries.push(membersClient.$executeRawUnsafe(sql, ...params))
      })
  }
  if (historyRowsToInsert.length > 0) {
    chunkRecordsForParameterizedQuery(historyRowsToInsert, HISTORY_BULK_INSERT_PARAM_COUNT)
      .forEach((historyRowsChunk) => {
        const { sql, params } = buildMemberStatsHistoryBulkInsertQuery(historyRowsChunk)
        queries.push(membersClient.$executeRawUnsafe(sql, ...params))
      })
  }

  await membersClient.$transaction(queries)

  let refreshed = 0
  if (options.refreshMostRecent !== false) {
    refreshed = await refreshHistoryMostRecentFlagsForUsers(membersClient, [userId])
  }

  return {
    upserted: historyRows.length,
    refreshed
  }
}

/**
 * Seed unified memberStatsHistory from the legacy history tables for one member.
 * This function is used by the migration script after unified memberStats writes complete.
 * Reruns only refresh the fields that are authoritative in the legacy history source rows.
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @param {Object} [options] history backfill options
 * @param {boolean} [options.refreshMostRecent=true] whether to recompute mostRecent after writes
 * @returns {Object} summary with upserted row count and refreshed mostRecent row count
 * @throws {Error} if the legacy challenge lookup cache is not initialized
 */
async function backfillHistoryFromLegacy (membersClient, userId, options = {}) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const parentRows = await membersClient.$queryRaw`
    SELECT "id"
    FROM "members"."memberHistoryStats"
    WHERE "userId" = ${userId}
    ORDER BY "id" ASC
  `

  if (!parentRows || parentRows.length === 0) {
    return upsertHistoryRows(membersClient, userId, [], options)
  }

  const historyStatsIds = parentRows.map((row) => normalizeBigInt(row.id, 'history stats id').toString())
  const idsSql = historyStatsIds.join(', ')

  const [developRows, dataScienceRows] = await Promise.all([
    membersClient.$queryRawUnsafe(
      `
      SELECT
        "challengeId",
        "ratingDate",
        "newRating",
        "subTrack",
        "subTrackId"
      FROM "members"."memberDevelopHistoryStats"
      WHERE "historyStatsId" IN (${idsSql})
      ORDER BY "subTrackId" ASC, "ratingDate" DESC, "id" DESC
      `
    ),
    membersClient.$queryRawUnsafe(
      `
      SELECT
        "challengeId",
        "date",
        "rating",
        "placement",
        "percentile",
        "subTrack",
        "subTrackId"
      FROM "members"."memberDataScienceHistoryStats"
      WHERE "historyStatsId" IN (${idsSql})
      ORDER BY "subTrack" ASC, "date" DESC, "id" DESC
      `
    )
  ])

  const legacyHistoryLookup = new Map()

  developRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.subTrack, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy develop history row for user ${userId.toString()} and subTrack ${row.subTrack || row.subTrackId}`)
      return
    }

    const eventDate = row.ratingDate ? new Date(row.ratingDate) : null
    if (!eventDate || Number.isNaN(eventDate.getTime())) {
      return
    }

    mergeHistoryRow(legacyHistoryLookup, {
      userId,
      trackId: legacyLookupCache.trackIds.DEVELOP,
      typeId,
      challengeId: String(row.challengeId),
      eventDate,
      newRating: toOptionalInt(row.newRating)
    })
  })

  dataScienceRows.forEach((row) => {
    const typeId = resolveChallengeTypeId(row.subTrack, row.subTrackId)
    if (!typeId) {
      logWarn(`Skipping legacy data science history row for user ${userId.toString()} and subTrack ${row.subTrack || row.subTrackId}`)
      return
    }

    const eventDate = row.date ? new Date(row.date) : null
    if (!eventDate || Number.isNaN(eventDate.getTime())) {
      return
    }

    mergeHistoryRow(legacyHistoryLookup, {
      userId,
      trackId: legacyLookupCache.trackIds.DATA_SCIENCE,
      typeId,
      challengeId: String(row.challengeId),
      eventDate,
      newRating: toOptionalInt(row.rating),
      placement: toOptionalPlacement(row.placement),
      percentile: toOptionalFloat(row.percentile)
    })
  })

  return upsertHistoryRows(membersClient, userId, Array.from(legacyHistoryLookup.values()), options)
}

/**
 * Load completed winner rows for one member so history can be seeded
 * when a completed challenge never appeared in the legacy history tables.
 * PLACEMENT and PASSED_REVIEW rows both represent visible challenge
 * participation; wins are still counted separately from PLACEMENT rows only.
 * @param {Object} challengesClient prisma challenges client
 * @param {BigInt} userId member user id
 * @param {Object} [options] optional track/type filters
 * @returns {Promise<Array<Object>>} winner rows with embedded challenge metadata
 */
async function fetchChallengeWinnerRowsForUser (challengesClient, userId, options = {}) {
  const { whereSql, params } = buildFilterQuery(options, userId)
  const rows = await challengesClient.$queryRawUnsafe(
    `
    SELECT
      cw."challengeId" AS "challengeId",
      cw."createdAt" AS "createdAt",
      cw."placement" AS "placement",
      cw."type" AS "winnerType",
      c.id AS "canonicalChallengeId",
      c."trackId" AS "trackId",
      c."typeId" AS "typeId",
      c.status AS status,
      c."endDate" AS "endDate"
    FROM "ChallengeWinner" cw
    INNER JOIN "Challenge" c ON c.id = cw."challengeId"
    WHERE ${whereSql}
      AND cw."type" IN (${CHALLENGE_WINNER_HISTORY_TYPE_SQL})
      AND c.status::text = '${COMPLETED_CHALLENGE_STATUS}'
    ORDER BY cw."createdAt" ASC, cw."challengeId" ASC
    `,
    ...params
  )

  return rows.map((row) => ({
    challengeId: String(row.challengeId),
    createdAt: row.createdAt ? new Date(row.createdAt) : null,
    placement: row.winnerType === 'PLACEMENT' ? toOptionalPlacement(row.placement) : null,
    challenge: {
      id: String(row.canonicalChallengeId || row.challengeId),
      trackId: row.trackId ? String(row.trackId) : null,
      typeId: row.typeId ? String(row.typeId) : null,
      status: row.status,
      endDate: row.endDate ? new Date(row.endDate) : null
    }
  }))
}

/**
 * Normalize completed review-api and ChallengeWinner rows into unified history rows.
 * review-api rows are used when available, while placement winners fill gaps for
 * completed challenges that never reached challengeResult.
 * @param {BigInt} userId member user id
 * @param {Array<Object>} reviewRows raw review-api challengeResult rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by challenge id
 * @param {Array<Object>} winnerRows completed placement winner rows
 * @param {Object} [options] optional track/type filters
 * @returns {Array<Object>} normalized unified history rows
 */
function buildSupplementalHistoryRowsFromCompletedChallenges (
  userId,
  reviewRows,
  challengeMetadataById,
  winnerRows,
  options = {}
) {
  const historyLookup = new Map()

  ;(reviewRows || []).forEach((row) => {
    if (!isUsableReviewChallengeResultRow(row)) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.id || !challenge.trackId || !challenge.typeId || !isCompletedChallengeStatus(challenge.status)) {
      return
    }

    const dimension = normalizeChallengeStatsDimension(challenge.trackId, challenge.typeId)
    const trackId = dimension.trackId
    const typeId = dimension.typeId
    if (!matchesNormalizedStatsFilters(dimension, options)) {
      return
    }

    const eventDate = toOptionalDate(challenge.endDate) || toOptionalDate(row.createdAt)
    if (!eventDate) {
      return
    }

    mergeHistoryRow(historyLookup, {
      userId,
      trackId,
      typeId,
      challengeId: String(challenge.id),
      eventDate,
      newRating: null,
      placement: toOptionalPlacement(row.placement)
    })
  })

  ;(winnerRows || []).forEach((row) => {
    const challenge = row.challenge || null
    if (!challenge || !challenge.id || !challenge.trackId || !challenge.typeId || !isCompletedChallengeStatus(challenge.status)) {
      return
    }

    const dimension = normalizeChallengeStatsDimension(challenge.trackId, challenge.typeId)
    const trackId = dimension.trackId
    const typeId = dimension.typeId
    if (!matchesNormalizedStatsFilters(dimension, options)) {
      return
    }

    const eventDate = toOptionalDate(challenge.endDate) || toOptionalDate(row.createdAt)
    if (!eventDate) {
      return
    }

    mergeHistoryRow(historyLookup, {
      userId,
      trackId,
      typeId,
      challengeId: String(challenge.id),
      eventDate,
      newRating: null,
      placement: toOptionalPlacement(row.placement)
    })
  })

  return Array.from(historyLookup.values())
}

/**
 * Seed unified memberStatsHistory from completed non-legacy challenge sources.
 * This supplements legacy history so newer completed challenges still surface
 * when they were never written to legacy history tables.
 * @param {Object} membersClient prisma members client
 * @param {Object} challengesClient prisma challenges client
 * @param {Object|null} reviewDbClient raw pg review database client
 * @param {BigInt} userId member user id
 * @param {Object} [options] history backfill options
 * @param {boolean} [options.refreshMostRecent=true] whether to recompute mostRecent after writes
 * @param {string|null} [options.trackId] optional track filter
 * @param {string|null} [options.typeId] optional type filter
 * @returns {Promise<Object>} summary with upserted row count and refreshed mostRecent row count
 */
async function backfillHistoryFromCompletedChallenges (
  membersClient,
  challengesClient,
  reviewDbClient,
  userId,
  options = {}
) {
  const [reviewRows, winnerRows] = await Promise.all([
    reviewDbClient ? fetchReviewChallengeResultsForUser(reviewDbClient, userId) : Promise.resolve([]),
    challengesClient ? fetchChallengeWinnerRowsForUser(challengesClient, userId, options) : Promise.resolve([])
  ])

  const challengeMetadataById = reviewRows.length > 0
    ? await fetchChallengeMetadataMap(
      challengesClient,
      reviewRows.map((row) => row.challengeId)
    )
    : new Map()

  const historyRows = buildSupplementalHistoryRowsFromCompletedChallenges(
    userId,
    reviewRows,
    challengeMetadataById,
    winnerRows,
    options
  )

  return upsertHistoryRows(membersClient, userId, historyRows, options)
}

/**
 * Run async work over a collection with a fixed concurrency cap.
 * Results preserve input order so batch output remains deterministic.
 * @param {Array<*>} items input items
 * @param {number} concurrency maximum number of in-flight tasks
 * @param {Function} iteratee async mapper invoked with (item, index)
 * @returns {Promise<Array<*>>} resolved results in input order
 */
async function mapWithConcurrency (items, concurrency, iteratee) {
  if (!items || items.length === 0) {
    return []
  }

  const workerCount = Math.min(Math.max(1, toInt(concurrency)), items.length)
  const results = new Array(items.length)
  let nextIndex = 0

  async function worker () {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex
      nextIndex += 1
      results[currentIndex] = await iteratee(items[currentIndex], currentIndex)
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()))
  return results
}

/**
 * Preserve existing non-null rating and rank values when a rerun does not provide them.
 * @param {Object} record incoming unified stat record
 * @param {Object|null} existingRow existing unified stat row
 * @returns {Object} write payload safe for create/update
 */
function buildMemberStatsWriteData (record, existingRow) {
  const writeData = {
    userId: record.userId,
    trackId: record.trackId,
    typeId: record.typeId,
    challenges: record.challenges,
    wins: record.wins,
    mostRecentEventDate: record.mostRecentEventDate,
    mostRecentSubmission: record.mostRecentSubmission,
    rating: record.rating,
    avgRank: record.avgRank,
    avgNumSubmissions: record.avgNumSubmissions,
    bestRank: record.bestRank,
    globalRank: record.globalRank,
    countryRank: record.countryRank,
    schoolRank: record.schoolRank,
    volatility: record.volatility,
    maxRating: record.maxRating,
    minRating: record.minRating,
    topFiveFinishes: record.topFiveFinishes,
    topTenFinishes: record.topTenFinishes,
    isPrivate: record.isPrivate
  }

  if (!existingRow) {
    return writeData
  }

  NULL_PRESERVED_STAT_FIELDS.forEach((field) => {
    if ((writeData[field] === null || writeData[field] === undefined) &&
      existingRow[field] !== null &&
      existingRow[field] !== undefined) {
      writeData[field] = existingRow[field]
    }
  })

  return writeData
}

function toComparableTimestamp (value) {
  if (!value) {
    return 0
  }

  const timestamp = new Date(value).getTime()
  return Number.isFinite(timestamp) ? timestamp : 0
}

/**
 * Resolve the memberMaxRating row implied by current memberStats rows.
 * The stored row should represent the highest current rating across tracks/types,
 * not a historical peak value from older data imports or rerates.
 * @param {Array<Object>} statsRows current public memberStats rows for one user
 * @returns {Object|null} normalized memberMaxRating write payload, or null when no current rating exists
 */
function buildCurrentMemberMaxRatingData (statsRows) {
  if (!legacyLookupCache) {
    throw new Error('Challenge dimension lookup has not been initialized')
  }

  if (!statsRows || statsRows.length === 0) {
    return null
  }

  let selectedRow = null
  let selectedKey = null

  statsRows.forEach((row) => {
    const rating = toOptionalInt(row && row.rating)
    if (rating === null) {
      return
    }

    const candidateKey = buildTrackTypeKey(row.trackId, row.typeId)
    const candidate = {
      userId: normalizeBigInt(row.userId, 'user id'),
      trackId: String(row.trackId),
      typeId: String(row.typeId),
      rating,
      mostRecentEventDate: toComparableTimestamp(row.mostRecentEventDate)
    }

    if (!selectedRow ||
      candidate.rating > selectedRow.rating ||
      (candidate.rating === selectedRow.rating &&
        candidate.mostRecentEventDate > selectedRow.mostRecentEventDate) ||
      (candidate.rating === selectedRow.rating &&
        candidate.mostRecentEventDate === selectedRow.mostRecentEventDate &&
        candidateKey < selectedKey)) {
      selectedRow = candidate
      selectedKey = candidateKey
    }
  })

  if (!selectedRow) {
    return null
  }

  const track = resolveTrackNameFromLookup(legacyLookupCache, selectedRow.trackId) || selectedRow.trackId
  const subTrack = resolveTypeNameFromLookup(legacyLookupCache, selectedRow.typeId) || selectedRow.typeId

  return {
    userId: selectedRow.userId,
    rating: selectedRow.rating,
    track,
    subTrack,
    ratingColor: helper.getRatingColor(selectedRow.rating)
  }
}

/**
 * Synchronize memberMaxRating rows from the highest current memberStats.rating
 * value for the specified users. Users with no current rating rows have stale
 * memberMaxRating rows deleted.
 * @param {Object} membersClient prisma members client
 * @param {Array<*>} userIds member user ids to synchronize
 * @returns {Promise<{ upserted: number, deleted: number }>} write counts
 */
async function syncCurrentMemberMaxRatingsForUsers (membersClient, userIds) {
  if (!userIds || userIds.length === 0) {
    return { upserted: 0, deleted: 0 }
  }

  const normalizedUserIds = Array.from(
    new Set(userIds.map((userId) => normalizeBigInt(userId, 'user id').toString()))
  ).map((userId) => normalizeBigInt(userId, 'user id'))

  if (normalizedUserIds.length === 0) {
    return { upserted: 0, deleted: 0 }
  }

  const [statsRows, existingRows] = await Promise.all([
    membersClient.memberStats.findMany({
      where: {
        userId: {
          in: normalizedUserIds
        },
        rating: {
          not: null
        },
        isPrivate: false
      },
      select: {
        userId: true,
        trackId: true,
        typeId: true,
        rating: true,
        mostRecentEventDate: true
      }
    }),
    membersClient.memberMaxRating.findMany({
      where: {
        userId: {
          in: normalizedUserIds
        }
      },
      select: {
        id: true,
        userId: true,
        rating: true,
        track: true,
        subTrack: true,
        ratingColor: true
      }
    })
  ])

  const statsRowsByUserId = new Map()
  statsRows.forEach((row) => {
    const userKey = normalizeBigInt(row.userId, 'user id').toString()
    const rows = statsRowsByUserId.get(userKey) || []
    rows.push(row)
    statsRowsByUserId.set(userKey, rows)
  })

  const desiredRowsByUserId = new Map()
  normalizedUserIds.forEach((userId) => {
    const userKey = userId.toString()
    const desiredRow = buildCurrentMemberMaxRatingData(statsRowsByUserId.get(userKey) || [])
    if (desiredRow) {
      desiredRowsByUserId.set(userKey, desiredRow)
    }
  })

  const existingRowsByUserId = new Map(
    existingRows.map((row) => [normalizeBigInt(row.userId, 'user id').toString(), row])
  )

  const queries = []
  const deleteIds = existingRows
    .filter((row) => !desiredRowsByUserId.has(normalizeBigInt(row.userId, 'user id').toString()))
    .map((row) => row.id)

  if (deleteIds.length > 0) {
    queries.push(membersClient.memberMaxRating.deleteMany({
      where: {
        id: {
          in: deleteIds
        }
      }
    }))
  }

  let upserted = 0
  desiredRowsByUserId.forEach((desiredRow, userKey) => {
    const existingRow = existingRowsByUserId.get(userKey) || null
    if (existingRow &&
      toOptionalInt(existingRow.rating) === desiredRow.rating &&
      existingRow.track === desiredRow.track &&
      existingRow.subTrack === desiredRow.subTrack &&
      existingRow.ratingColor === desiredRow.ratingColor) {
      return
    }

    queries.push(membersClient.memberMaxRating.upsert({
      where: {
        userId: desiredRow.userId
      },
      create: {
        userId: desiredRow.userId,
        rating: desiredRow.rating,
        track: desiredRow.track,
        subTrack: desiredRow.subTrack,
        ratingColor: desiredRow.ratingColor,
        createdBy: CREATED_BY,
        updatedBy: UPDATED_BY
      },
      update: {
        rating: desiredRow.rating,
        track: desiredRow.track,
        subTrack: desiredRow.subTrack,
        ratingColor: desiredRow.ratingColor,
        updatedBy: UPDATED_BY
      }
    }))
    upserted += 1
  })

  if (queries.length > 0) {
    await membersClient.$transaction(queries)
  }

  return {
    upserted,
    deleted: deleteIds.length
  }
}

/**
 * Split raw-query write records so each generated query stays below the bind
 * parameter limit used by spread-based Prisma raw calls.
 * @param {Array<*>} records write records to split
 * @param {number} parametersPerRecord number of bound parameters each record emits
 * @returns {Array<Array<*>>} record chunks sized for safe raw-query execution
 * @throws {Error} if parametersPerRecord is not a positive integer
 */
function chunkRecordsForParameterizedQuery (records, parametersPerRecord) {
  if (!records || records.length === 0) {
    return []
  }

  if (!Number.isInteger(parametersPerRecord) || parametersPerRecord <= 0) {
    throw new Error('parametersPerRecord must be a positive integer')
  }

  const chunkSize = Math.max(1, Math.floor(MAX_RAW_QUERY_PARAMS / parametersPerRecord))
  const chunks = []

  for (let start = 0; start < records.length; start += chunkSize) {
    chunks.push(records.slice(start, start + chunkSize))
  }

  return chunks
}

/**
 * Build parameter placeholders and flattened values for a SQL VALUES list.
 * @param {Array<Array<*>>} rows row-major parameter values
 * @returns {{ valuesSql: string, params: Array<*> }} VALUES SQL fragment and flattened params
 */
function buildParameterizedValues (rows) {
  if (!rows || rows.length === 0) {
    return {
      valuesSql: '',
      params: []
    }
  }

  const columnCount = rows[0].length
  return {
    valuesSql: rows
      .map((row, rowIndex) => {
        const placeholders = []

        for (let columnIndex = 0; columnIndex < row.length; columnIndex += 1) {
          placeholders.push(`$${(rowIndex * columnCount) + columnIndex + 1}`)
        }

        return `(${placeholders.join(', ')})`
      })
      .join(', '),
    params: rows.flat()
  }
}

/**
 * Build the bulk upsert SQL needed to write one batch of unified memberStats rows.
 * @param {Array<Object>} statsRecords normalized memberStats write payloads
 * @returns {{ sql: string, params: Array<*> }} bulk upsert SQL and bound params
 */
function buildMemberStatsBulkUpsertQuery (statsRecords) {
  const { valuesSql, params } = buildParameterizedValues(statsRecords.map((record) => ([
    record.userId,
    record.trackId,
    record.typeId,
    record.challenges,
    record.wins,
    record.mostRecentSubmission,
    record.mostRecentEventDate,
    record.rating,
    record.avgRank,
    record.avgNumSubmissions,
    record.bestRank,
    record.globalRank,
    record.countryRank,
    record.schoolRank,
    record.volatility,
    record.maxRating,
    record.minRating,
    record.topFiveFinishes,
    record.topTenFinishes,
    record.isPrivate,
    CREATED_BY,
    UPDATED_BY
  ])))

  return {
    sql: `
      INSERT INTO "members"."memberStats" (
        "userId",
        "trackId",
        "typeId",
        "challenges",
        "wins",
        "mostRecentSubmission",
        "mostRecentEventDate",
        "rating",
        "avgRank",
        "avgNumSubmissions",
        "bestRank",
        "globalRank",
        "countryRank",
        "schoolRank",
        "volatility",
        "maxRating",
        "minRating",
        "topFiveFinishes",
        "topTenFinishes",
        "isPrivate",
        "createdBy",
        "updatedBy"
      )
      VALUES ${valuesSql}
      ON CONFLICT ("userId", "trackId", "typeId") DO UPDATE SET
        "challenges" = EXCLUDED."challenges",
        "wins" = EXCLUDED."wins",
        "mostRecentSubmission" = EXCLUDED."mostRecentSubmission",
        "mostRecentEventDate" = EXCLUDED."mostRecentEventDate",
        "rating" = EXCLUDED."rating",
        "avgRank" = EXCLUDED."avgRank",
        "avgNumSubmissions" = EXCLUDED."avgNumSubmissions",
        "bestRank" = EXCLUDED."bestRank",
        "globalRank" = EXCLUDED."globalRank",
        "countryRank" = EXCLUDED."countryRank",
        "schoolRank" = EXCLUDED."schoolRank",
        "volatility" = EXCLUDED."volatility",
        "maxRating" = EXCLUDED."maxRating",
        "minRating" = EXCLUDED."minRating",
        "topFiveFinishes" = EXCLUDED."topFiveFinishes",
        "topTenFinishes" = EXCLUDED."topTenFinishes",
        "isPrivate" = EXCLUDED."isPrivate",
        "updatedBy" = EXCLUDED."updatedBy",
        "updatedAt" = CURRENT_TIMESTAMP
    `,
    params
  }
}

/**
 * Build the bulk update SQL needed to refresh existing memberStatsHistory rows by id.
 * @param {Array<Object>} historyRows normalized existing history rows keyed by id
 * @returns {{ sql: string, params: Array<*> }} bulk update SQL and bound params
 */
function buildMemberStatsHistoryBulkUpdateQuery (historyRows) {
  const { valuesSql, params } = buildParameterizedValues(historyRows.map((row) => ([
    row.id,
    row.eventDate,
    row.newRating,
    row.placement,
    row.percentile,
    UPDATED_BY
  ])))

  return {
    sql: `
      UPDATE "members"."memberStatsHistory" msh
      SET
        "eventDate" = CAST(data."eventDate" AS TIMESTAMP),
        "newRating" = CAST(data."newRating" AS INTEGER),
        "placement" = CAST(data."placement" AS INTEGER),
        "percentile" = CAST(data."percentile" AS DOUBLE PRECISION),
        "updatedBy" = data."updatedBy",
        "updatedAt" = CURRENT_TIMESTAMP
      FROM (
        VALUES ${valuesSql}
      ) AS data ("id", "eventDate", "newRating", "placement", "percentile", "updatedBy")
      WHERE msh."id" = CAST(data."id" AS BIGINT)
    `,
    params
  }
}

/**
 * Build the bulk insert SQL needed to create new memberStatsHistory rows.
 * @param {Array<Object>} historyRows normalized history rows to insert
 * @returns {{ sql: string, params: Array<*> }} bulk insert SQL and bound params
 */
function buildMemberStatsHistoryBulkInsertQuery (historyRows) {
  const { valuesSql, params } = buildParameterizedValues(historyRows.map((row) => ([
    row.userId,
    row.trackId,
    row.typeId,
    row.challengeId,
    false,
    row.eventDate,
    row.newRating,
    row.placement,
    row.percentile,
    CREATED_BY,
    UPDATED_BY
  ])))

  return {
    sql: `
      INSERT INTO "members"."memberStatsHistory" (
        "userId",
        "trackId",
        "typeId",
        "challengeId",
        "mostRecent",
        "eventDate",
        "newRating",
        "placement",
        "percentile",
        "createdBy",
        "updatedBy"
      )
      SELECT
        CAST(data."userId" AS BIGINT),
        data."trackId",
        data."typeId",
        data."challengeId",
        data."mostRecent",
        CAST(data."eventDate" AS TIMESTAMP),
        CAST(data."newRating" AS INTEGER),
        CAST(data."placement" AS INTEGER),
        CAST(data."percentile" AS DOUBLE PRECISION),
        data."createdBy",
        data."updatedBy"
      FROM (
        VALUES ${valuesSql}
      ) AS data (
        "userId",
        "trackId",
        "typeId",
        "challengeId",
        "mostRecent",
        "eventDate",
        "newRating",
        "placement",
        "percentile",
        "createdBy",
        "updatedBy"
      )
    `,
    params
  }
}

function buildCsvWriter (csvPath) {
  if (!csvPath) {
    return {
      writeLine (values) {
        process.stdout.write(`${values.map(toCsvValue).join(',')}\n`)
      },
      async end () {}
    }
  }

  const outputPath = path.resolve(csvPath)
  const stream = fs.createWriteStream(outputPath, { encoding: 'utf8' })

  return {
    writeLine (values) {
      stream.write(`${values.map(toCsvValue).join(',')}\n`)
    },
    async end () {
      await new Promise((resolve, reject) => {
        stream.once('finish', resolve)
        stream.once('error', reject)
        stream.end()
      })
    }
  }
}

/**
 * Persist one JSON value by writing to a temporary file and renaming it into place.
 * This keeps the destination file parseable even if the process is interrupted between batches.
 * @param {string} outputPath destination JSON file path
 * @param {*} value JSON-serializable value to persist
 * @returns {Promise<void>} resolves once the write is committed
 */
async function writeJsonFileAtomically (outputPath, value) {
  const resolvedOutputPath = path.resolve(outputPath)
  const temporaryPath = `${resolvedOutputPath}.tmp`

  await fs.promises.mkdir(path.dirname(resolvedOutputPath), { recursive: true })
  await fs.promises.writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, 'utf8')
  await fs.promises.rename(temporaryPath, resolvedOutputPath)
}

/**
 * Build a writer that records processed member user IDs as a JSON array of strings.
 * The file is rewritten after each batch so partial runs leave behind a usable checkpoint.
 * @param {string} [processedUserIdsPath] destination JSON path
 * @returns {Object} writer with appendUserIds and end methods plus the resolved output path
 */
function buildProcessedUserIdsWriter (processedUserIdsPath) {
  const outputPath = path.resolve(processedUserIdsPath || DEFAULT_PROCESSED_USER_IDS_PATH)
  const processedUserIds = []
  const seenUserIds = new Set()

  return {
    outputPath,
    async appendUserIds (userIds) {
      let changed = false

      for (const userId of userIds || []) {
        const normalizedUserId = normalizeBigInt(userId, 'user id').toString()
        if (seenUserIds.has(normalizedUserId)) {
          continue
        }

        seenUserIds.add(normalizedUserId)
        processedUserIds.push(normalizedUserId)
        changed = true
      }

      if (!changed) {
        return
      }

      await writeJsonFileAtomically(outputPath, processedUserIds)
    },
    async end () {
      await writeJsonFileAtomically(outputPath, processedUserIds)
    }
  }
}

function parseArgs (argv) {
  const options = {
    csvOnly: false,
    csvPath: null,
    processedUserIdsPath: DEFAULT_PROCESSED_USER_IDS_PATH,
    userIds: [],
    trackId: null,
    typeId: null,
    limit: null,
    concurrency: DEFAULT_CONCURRENCY,
    skipHistory: false,
    skipRatings: false,
    skipRerate: false,
    help: false
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]

    if (arg === '--') {
      continue
    }

    if (arg === '--csv-only' || arg === '--csv') {
      options.csvOnly = true
      const next = argv[index + 1]
      if (next && !next.startsWith('--')) {
        options.csvPath = next
        index += 1
      }
      continue
    }

    if (arg === '--csv-path') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--csv-path requires a value')
      }
      options.csvOnly = true
      options.csvPath = next
      index += 1
      continue
    }

    if (arg === '--processed-user-ids-path') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--processed-user-ids-path requires a value')
      }
      options.processedUserIdsPath = next
      index += 1
      continue
    }

    if (arg === '--user-id') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--user-id requires a value')
      }
      options.userIds.push(next)
      index += 1
      continue
    }

    if (arg === '--user-ids') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--user-ids requires a comma-separated list')
      }
      const ids = next.split(',').map((item) => item.trim()).filter(Boolean)
      options.userIds.push(...ids)
      index += 1
      continue
    }

    if (arg === '--track-id') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--track-id requires a value')
      }
      options.trackId = next
      index += 1
      continue
    }

    if (arg === '--type-id') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--type-id requires a value')
      }
      options.typeId = next
      index += 1
      continue
    }

    if (arg === '--limit') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--limit requires a value')
      }
      const parsedLimit = Number.parseInt(next, 10)
      if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
        throw new Error('--limit must be a positive integer')
      }
      options.limit = parsedLimit
      index += 1
      continue
    }

    if (arg === '--concurrency') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--concurrency requires a value')
      }
      const parsedConcurrency = Number.parseInt(next, 10)
      if (!Number.isFinite(parsedConcurrency) || parsedConcurrency <= 0) {
        throw new Error('--concurrency must be a positive integer')
      }
      options.concurrency = parsedConcurrency
      index += 1
      continue
    }

    if (arg === '--skip-history') {
      options.skipHistory = true
      continue
    }

    if (arg === '--skip-ratings') {
      options.skipRatings = true
      continue
    }

    if (arg === '--skip-rerate') {
      options.skipRerate = true
      continue
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true
      continue
    }

    throw new Error(`Unknown option: ${arg}`)
  }

  return options
}

function printUsage () {
  console.log(`
Usage:
  node src/scripts/recalculateMemberStats.js [options]

Options:
  --csv-only, --csv         Output CSV report and skip DB writes.
  --csv-path <path>         Write CSV to file (defaults to stdout).
  --processed-user-ids-path <path>
                            Write processed user IDs to JSON (default: ${DEFAULT_PROCESSED_USER_IDS_PATH}).
  --user-id <id>            Process a single user (repeatable).
  --user-ids <id,id>        Comma-separated user IDs.
  --track-id <id>           Filter by track ID or canonical track name.
  --type-id <id>            Filter by type ID, abbreviation, or canonical type name.
  --limit <n>               Limit number of users processed.
  --concurrency <n>         Process up to n users in parallel within each batch (default: ${DEFAULT_CONCURRENCY}).
  --skip-history            Skip seeding memberStatsHistory from legacy history tables.
  --skip-ratings            Skip legacy rating enrichment and development rerating (aggregate refresh still reads review-api challengeResult).
  --skip-rerate             Skip the Development rerate replay while still backfilling legacy rating/rank fields.
  --help, -h                Show this help.
`)
}

function isBigIntValue (value) {
  return Object.prototype.toString.call(value) === '[object BigInt]'
}

function normalizeBigInt (value, label) {
  if (isBigIntValue(value)) {
    return value
  }

  if (typeof global.BigInt !== 'function') {
    throw new Error('BigInt is not supported in this runtime')
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0 || !Number.isInteger(value)) {
      throw new Error(`Invalid ${label}: ${value}`)
    }
    return global.BigInt(value)
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!/^\d+$/.test(trimmed)) {
      throw new Error(`Invalid ${label}: ${value}`)
    }
    return global.BigInt(trimmed)
  }

  throw new Error(`Invalid ${label}: ${value}`)
}

function toInt (value) {
  if (value === null || value === undefined) {
    return 0
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.trunc(value) : 0
  }

  if (isBigIntValue(value)) {
    return Number(value)
  }

  const parsed = Number(value)
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0
}

function isMissingMemberStatsUserFkError (error) {
  return Boolean(
    error &&
    error.code === 'P2003' &&
    error.meta &&
    error.meta.constraint === 'memberStats_userId_fkey'
  )
}

/**
 * Build the unified memberStats upsert operation for one track/type aggregate row.
 * Existing rating fields can be supplied so reruns preserve prior non-null values when
 * the current pass does not rebuild them.
 * @param {Object} membersClient prisma members client
 * @param {Object} record incoming unified stat record
 * @param {Object|null} [existingRow=null] existing unified stat row for null preservation
 * @returns {Object} prisma upsert operation suitable for batching in a transaction
 */
function buildMemberStatsUpsertQuery (membersClient, record, existingRow = null) {
  const writeData = buildMemberStatsWriteData(record, existingRow)

  return membersClient.memberStats.upsert({
    where: {
      userId_trackId_typeId: {
        userId: record.userId,
        trackId: record.trackId,
        typeId: record.typeId
      }
    },
    create: {
      ...writeData,
      createdBy: CREATED_BY,
      updatedBy: UPDATED_BY
    },
    update: {
      ...writeData,
      updatedBy: UPDATED_BY
    }
  })
}

function buildFilterQuery (options, includeUserFilter) {
  const clauses = [
    'c."trackId" IS NOT NULL',
    'c."typeId" IS NOT NULL'
  ]
  const params = []

  function addEqualsClause (column, value) {
    params.push(value)
    clauses.push(`${column} = $${params.length}`)
  }

  if (includeUserFilter !== null && includeUserFilter !== undefined) {
    addEqualsClause('cw."userId"', includeUserFilter)
  }

  if (options.trackId) {
    if (legacyLookupCache &&
      options.trackId === legacyLookupCache.trackIds.DATA_SCIENCE &&
      legacyLookupCache.typeIds.MARATHON_MATCH) {
      params.push(options.trackId)
      const trackPlaceholder = `$${params.length}`
      params.push(legacyLookupCache.typeIds.MARATHON_MATCH)
      const marathonTypePlaceholder = `$${params.length}`
      clauses.push(`(c."trackId" = ${trackPlaceholder} OR c."typeId" = ${marathonTypePlaceholder})`)
    } else {
      addEqualsClause('c."trackId"', options.trackId)
    }
  }

  if (options.typeId) {
    addEqualsClause('c."typeId"', options.typeId)
  }

  return {
    whereSql: clauses.join(' AND '),
    params
  }
}

async function getLegacyUserIds (membersClient, options) {
  const whereClauses = [
    'ms."isPrivate" = false',
    `(
      EXISTS (SELECT 1 FROM "members"."memberDevelopStats" mdev WHERE mdev."memberStatsId" = ms."id")
      OR EXISTS (SELECT 1 FROM "members"."memberDesignStats" mdes WHERE mdes."memberStatsId" = ms."id")
      OR EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" mds WHERE mds."memberStatsId" = ms."id")
      OR EXISTS (SELECT 1 FROM "members"."memberCopilotStats" mcs WHERE mcs."memberStatsId" = ms."id")
    )`
  ]

  if (options.trackId === legacyLookupCache.trackIds.DEVELOP) {
    whereClauses.push(
      `EXISTS (SELECT 1 FROM "members"."memberDevelopStats" mdev WHERE mdev."memberStatsId" = ms."id")`
    )
  } else if (options.trackId === legacyLookupCache.trackIds.DESIGN) {
    whereClauses.push(
      `EXISTS (SELECT 1 FROM "members"."memberDesignStats" mdes WHERE mdes."memberStatsId" = ms."id")`
    )
  } else if (options.trackId === legacyLookupCache.trackIds.DATA_SCIENCE) {
    whereClauses.push(
      `EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" mds WHERE mds."memberStatsId" = ms."id")`
    )
  }

  const rows = await membersClient.$queryRawUnsafe(
    `
    SELECT DISTINCT ms."userId" AS "userId"
    FROM "members"."memberStats" ms
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY ms."userId" ASC
    `
  )

  return rows.map((row) => normalizeBigInt(row.userId, 'user id'))
}

async function getUserIds (membersClient, challengesClient, options) {
  const explicitUserIds = Array.from(new Set(options.userIds))
  let userIds

  if (explicitUserIds.length > 0) {
    userIds = explicitUserIds.map((userId) => normalizeBigInt(userId, 'user id'))
  } else {
    const legacyUserIds = await getLegacyUserIds(membersClient, options)
    const reviewUserIds = await getReviewUserIds(reviewDb, challengesClient, options)
    const { whereSql, params } = buildFilterQuery(options, null)

    const rows = await challengesClient.$queryRawUnsafe(
      `
      SELECT DISTINCT cw."userId" AS "userId"
      FROM "ChallengeWinner" cw
      INNER JOIN "Challenge" c ON c.id = cw."challengeId"
      WHERE ${whereSql}
      ORDER BY cw."userId" ASC
      `,
      ...params
    )

    userIds = legacyUserIds
      .concat(reviewUserIds)
      .concat(rows.map((row) => normalizeBigInt(row.userId, 'user id')))
  }

  const unique = Array.from(new Set(userIds.map((userId) => userId.toString())))
    .map((userId) => normalizeBigInt(userId, 'user id'))
  unique.sort((a, b) => (a < b ? -1 : 1))

  if (options.limit && unique.length > options.limit) {
    return unique.slice(0, options.limit)
  }

  return unique
}

async function getExistingMemberUserIdSet (membersClient, userIds) {
  if (!userIds || userIds.length === 0) {
    return new Set()
  }

  const rows = await membersClient.member.findMany({
    where: {
      userId: {
        in: userIds
      }
    },
    select: {
      userId: true
    }
  })

  return new Set(rows.map((row) => normalizeBigInt(row.userId, 'user id').toString()))
}

/**
 * Aggregate unified memberStats rows from challenge winner placements.
 * Challenges count distinct completed winner-backed challenges, while wins count
 * only first-place PLACEMENT rows so second/third-place finishes do not inflate
 * the public win totals during ChallengeWinner fallback aggregation.
 * @param {Object} challengesClient prisma challenges client
 * @param {BigInt} userId member user id
 * @param {Object} options script options
 * @returns {Promise<Array<Object>>} unified aggregate rows derived from ChallengeWinner
 */
async function aggregateChallengeWinnerStatsForUser (challengesClient, userId, options) {
  const { whereSql, params } = buildFilterQuery(options, userId)

  const rows = await challengesClient.$queryRawUnsafe(
    `
    SELECT
      cw."userId" AS "userId",
      c."trackId" AS "trackId",
      c."typeId" AS "typeId",
      COUNT(DISTINCT c.id)::int AS "challenges",
      COUNT(DISTINCT CASE WHEN cw."type" = 'PLACEMENT' AND cw."placement" = 1 THEN c.id END)::int AS "wins",
      MAX(c."endDate") AS "mostRecentEventDate",
      MAX(cw."createdAt") AS "mostRecentSubmission"
    FROM "ChallengeWinner" cw
    INNER JOIN "Challenge" c ON c.id = cw."challengeId"
    WHERE ${whereSql}
    GROUP BY cw."userId", c."trackId", c."typeId"
    ORDER BY c."trackId" ASC, c."typeId" ASC
    `,
    ...params
  )

  const aggregateLookup = new Map()
  rows.forEach((row) => {
    const dimension = normalizeChallengeStatsDimension(row.trackId, row.typeId)
    if (!matchesNormalizedStatsFilters(dimension, options)) {
      return
    }

    mergeAggregateRecord(aggregateLookup, {
      userId: normalizeBigInt(row.userId, 'user id'),
      trackId: dimension.trackId,
      typeId: dimension.typeId,
      challenges: toInt(row.challenges),
      wins: toInt(row.wins),
      mostRecentEventDate: row.mostRecentEventDate ? new Date(row.mostRecentEventDate) : null,
      mostRecentSubmission: row.mostRecentSubmission ? new Date(row.mostRecentSubmission) : null,
      rating: null,
      avgRank: null,
      avgNumSubmissions: null,
      bestRank: null,
      globalRank: null,
      countryRank: null,
      schoolRank: null,
      volatility: null,
      maxRating: null,
      minRating: null,
      topFiveFinishes: null,
      topTenFinishes: null,
      isPrivate: false
    })
  })

  return Array.from(aggregateLookup.values())
}

async function aggregateStatsForUser (membersClient, challengesClient, userId, options, legacyIds = null) {
  const legacyRows = await aggregateLegacyStatsForUser(membersClient, userId, options, legacyIds)
  const reviewRows = await aggregateReviewStatsForUser(reviewDb, challengesClient, userId, options, legacyRows)

  if (legacyRows.length > 0) {
    const aggregateLookup = new Map()
    legacyRows.forEach((row) => {
      mergeAggregateRecord(aggregateLookup, row)
    })
    reviewRows.forEach((row) => {
      mergeAggregateRecord(aggregateLookup, row)
    })

    return {
      source: 'legacy',
      rows: Array.from(aggregateLookup.values())
    }
  }

  if (reviewRows.length > 0) {
    return {
      source: 'review',
      rows: reviewRows
    }
  }

  return {
    source: 'challenge-winner',
    rows: await aggregateChallengeWinnerStatsForUser(challengesClient, userId, options)
  }
}

async function aggregateStatsForUserFromPreloadedData (
  challengesClient,
  userId,
  options,
  legacyRows = [],
  reviewRows = [],
  challengeMetadataById = new Map()
) {
  const aggregatedReviewRows = reviewRows.length > 0
    ? buildAggregatedStatsFromReviewResults(reviewRows, challengeMetadataById, options, legacyRows)
    : []

  if (legacyRows.length > 0) {
    const aggregateLookup = new Map()
    legacyRows.forEach((row) => {
      mergeAggregateRecord(aggregateLookup, row)
    })
    aggregatedReviewRows.forEach((row) => {
      mergeAggregateRecord(aggregateLookup, row)
    })

    return {
      source: 'legacy',
      rows: Array.from(aggregateLookup.values())
    }
  }

  if (aggregatedReviewRows.length > 0) {
    return {
      source: 'review',
      rows: aggregatedReviewRows
    }
  }

  return {
    source: 'challenge-winner',
    rows: await aggregateChallengeWinnerStatsForUser(challengesClient, userId, options)
  }
}

async function writeStatsToDatabase (membersClient, statsRecords, replaceUsers = new Set()) {
  if (!statsRecords || statsRecords.length === 0) {
    return 0
  }

  try {
    const userIds = Array.from(new Set(statsRecords.map((record) => record.userId.toString())))
      .map((userId) => normalizeBigInt(userId, 'user id'))
    const existingRows = await membersClient.memberStats.findMany({
      where: {
        userId: {
          in: userIds
        },
        isPrivate: false
      },
      select: {
        id: true,
        ...MEMBER_STATS_PRESERVE_SELECT
      }
    })
    const legacyBackedRowIds = await getLegacyBackedMemberStatsIdSet(
      membersClient,
      existingRows.map((row) => row.id)
    )
    const existingByKey = new Map(existingRows.map((row) => [buildUserTrackTypeKey(row.userId, row.trackId, row.typeId), row]))
    const rowsByUserId = new Map()
    existingRows.forEach((row) => {
      const userKey = row.userId.toString()
      const rows = rowsByUserId.get(userKey) || []
      rows.push(row)
      rowsByUserId.set(userKey, rows)
    })
    const statsWriteRecords = statsRecords.map((record) => {
      const existingRow = existingByKey.get(buildUserTrackTypeKey(record.userId, record.trackId, record.typeId)) || null
      return buildMemberStatsWriteData(record, existingRow)
    })

    const desiredKeysByUserId = new Map()
    statsRecords.forEach((record) => {
      const userKey = record.userId.toString()
      const keys = desiredKeysByUserId.get(userKey) || new Set()
      keys.add(buildTrackTypeKey(record.trackId, record.typeId))
      desiredKeysByUserId.set(userKey, keys)
    })

    const queries = []
    const deleteIds = []

    replaceUsers.forEach((userKey) => {
      const existingUserRows = rowsByUserId.get(userKey) || []
      const desiredKeys = desiredKeysByUserId.get(userKey) || new Set()
      deleteIds.push(...existingUserRows
        .filter((row) => (
          !desiredKeys.has(buildTrackTypeKey(row.trackId, row.typeId)) &&
          !legacyBackedRowIds.has(row.id.toString())
        ))
        .map((row) => row.id)
      )
    })

    if (deleteIds.length > 0) {
      queries.push(membersClient.memberStats.deleteMany({
        where: {
          id: {
            in: deleteIds
          }
        }
      }))
    }

    chunkRecordsForParameterizedQuery(statsWriteRecords, MEMBER_STATS_BULK_UPSERT_PARAM_COUNT)
      .forEach((statsWriteChunk) => {
        const { sql, params } = buildMemberStatsBulkUpsertQuery(statsWriteChunk)
        queries.push(membersClient.$executeRawUnsafe(sql, ...params))
      })

    // Use array transactions so the bulk delete and bulk upsert stay atomic.
    await membersClient.$transaction(queries)
    return statsRecords.length
  } catch (error) {
    if (isMissingMemberStatsUserFkError(error)) {
      logWarn('memberStats batch write hit missing member FK. Retrying per-record and skipping orphan user IDs.')

      let written = 0
      let skipped = 0
      const skippedUserIds = new Set()

      for (const record of statsRecords) {
        try {
          const existingRow = await membersClient.memberStats.findUnique({
            where: {
              userId_trackId_typeId: {
                userId: record.userId,
                trackId: record.trackId,
                typeId: record.typeId
              }
            },
            select: MEMBER_STATS_PRESERVE_SELECT
          })
          await buildMemberStatsUpsertQuery(membersClient, record, existingRow)
          written += 1
        } catch (singleWriteError) {
          if (isMissingMemberStatsUserFkError(singleWriteError)) {
            skipped += 1
            skippedUserIds.add(record.userId.toString())
            continue
          }
          throw singleWriteError
        }
      }

      if (skipped > 0) {
        const skippedIdSample = Array.from(skippedUserIds).slice(0, 10)
        const suffix = skippedUserIds.size > 10 ? ', ...' : ''
        logWarn(`Skipped ${skipped} memberStats record(s) for ${skippedUserIds.size} missing user(s): ${skippedIdSample.join(', ')}${suffix}`)
      }

      return written
    }

    logError(`Transaction failed. Rolled back ${statsRecords.length} pending records.`, error)
    throw error
  }
}

/**
 * Recompute memberStatsHistory.mostRecent for the specified users.
 * The refresh clears existing flags before promoting the latest row in each
 * track/type group so reruns overwrite stale winners without tripping the
 * partial unique index on mostRecent=true.
 * @param {Object} membersClient prisma members client
 * @param {Array<*>} userIds user ids to refresh
 * @returns {Promise<number>} number of history rows refreshed
 */
async function refreshHistoryMostRecentFlagsForUsers (membersClient, userIds) {
  if (!userIds || userIds.length === 0) {
    return 0
  }

  const normalizedUserIds = Array.from(
    new Set(userIds.map((userId) => normalizeBigInt(userId, 'user id').toString()))
  )
  if (normalizedUserIds.length === 0) {
    return 0
  }

  // Keep user ids inline as validated numeric literals for deterministic matching in raw SQL.
  const whereClauses = [`msh."userId" IN (${normalizedUserIds.join(', ')})`]
  const params = [UPDATED_BY]
  const updatedByPlaceholder = `$${params.length}`

  const totalRows = await membersClient.$queryRawUnsafe(
    `
    SELECT COUNT(*)::int AS "rowCount"
    FROM "members"."memberStatsHistory" msh
    WHERE ${whereClauses.join(' AND ')}
    `
  )
  const rowCount = totalRows && totalRows[0] ? toInt(totalRows[0].rowCount) : 0
  if (rowCount === 0) {
    return 0
  }

  const clearMostRecentQuery = membersClient.$executeRawUnsafe(
    `
    UPDATE "members"."memberStatsHistory" msh
    SET
      "mostRecent" = false,
      "updatedBy" = ${updatedByPlaceholder},
      "updatedAt" = CURRENT_TIMESTAMP
    WHERE ${whereClauses.join(' AND ')}
    `,
    ...params
  )

  const refreshWinnersQuery = membersClient.$executeRawUnsafe(
    `
    WITH ranked AS (
      SELECT
        msh."id" AS "id",
        msh."userId" AS "userId",
        msh."trackId" AS "trackId",
        msh."typeId" AS "typeId",
        msh."newRating" AS "newRating",
        ROW_NUMBER() OVER (
          PARTITION BY msh."userId", msh."trackId", msh."typeId"
          ORDER BY msh."eventDate" DESC, msh."id" DESC
        ) AS "rowNum"
      FROM "members"."memberStatsHistory" msh
      WHERE ${whereClauses.join(' AND ')}
    )
    UPDATE "members"."memberStatsHistory" msh
    SET
      "mostRecent" = true,
      "oldRating" = previous."newRating",
      "newRating" = CASE
        WHEN ms."id" IS NOT NULL THEN ms."rating"
        ELSE msh."newRating"
      END,
      "updatedBy" = ${updatedByPlaceholder},
      "updatedAt" = CURRENT_TIMESTAMP
    FROM ranked
    LEFT JOIN ranked previous
      ON previous."userId" = ranked."userId"
      AND previous."trackId" = ranked."trackId"
      AND previous."typeId" = ranked."typeId"
      AND previous."rowNum" = 2
    LEFT JOIN "members"."memberStats" ms
      ON ms."userId" = ranked."userId"
      AND ms."trackId" = ranked."trackId"
      AND ms."typeId" = ranked."typeId"
    WHERE msh."id" = ranked."id"
      AND ranked."rowNum" = 1
    `,
    ...params
  )

  await membersClient.$transaction([clearMostRecentQuery, refreshWinnersQuery])

  return rowCount
}

async function main () {
  const options = parseArgs(process.argv.slice(2))
  const scriptStartedAt = startTimer()

  if (options.help) {
    printUsage()
    return
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required')
  }

  if (!process.env.CHALLENGES_DB_URL && !process.env.CHALLENGE_DB_URL) {
    throw new Error('CHALLENGES_DB_URL or CHALLENGE_DB_URL is required')
  }

  if (!reviewDb) {
    throw new Error('REVIEW_DB_URL is required because aggregate backfill and rerates read review-api challengeResult')
  }

  const membersClient = getMembersClient()
  const challengesClient = getChallengesClient()

  let csvWriter
  let processedUserIdsWriter
  let processedUsers = 0
  let writtenStats = 0
  let writtenHistory = 0
  let updatedHistoryFlags = 0
  let reratedChallenges = 0
  let reratedRatings = 0
  let recalculatedRankRows = 0
  let syncedMemberMaxRatings = 0
  let deletedMemberMaxRatings = 0

  try {
    logInfo(`Starting recalculateMemberStats in ${options.csvOnly ? 'CSV-only' : 'write'} mode with concurrency ${options.concurrency}`)

    if (!options.csvOnly && !options.skipRatings && !options.skipRerate &&
      options.userIds.length === 0 && !options.limit) {
      logWarn('Development rerates are enabled for this full run. This is the slowest phase; use --skip-rerate for the bulk backfill and rerate separately afterward if needed.')
    }

    const { durationMs: connectDurationMs } = await measureAsyncStep(async () => {
      await membersClient.$connect()
      await challengesClient.$connect()
    })

    const { durationMs: legacyLookupDurationMs } = await measureAsyncStep(async () => {
      await initializeLegacyLookupCache(challengesClient)
    })

    if (options.trackId) {
      const resolvedTrackId = resolveTrackIdFromLookup(legacyLookupCache, options.trackId)
      if (!resolvedTrackId) {
        throw new Error(`Unable to resolve track filter '${options.trackId}' to a ChallengeTrack id`)
      }
      options.trackId = resolvedTrackId
    }

    if (options.typeId) {
      const resolvedTypeId = resolveTypeIdFromLookup(legacyLookupCache, options.typeId)
      if (!resolvedTypeId) {
        throw new Error(`Unable to resolve type filter '${options.typeId}' to a ChallengeType id`)
      }
      options.typeId = resolvedTypeId
    }

    const { durationMs: reviewRelationDurationMs } = await measureAsyncStep(async () => {
      await assertChallengeResultRelation(reviewDb)
    })

    const {
      result: userIds,
      durationMs: userDiscoveryDurationMs
    } = await measureAsyncStep(async () => getUserIds(membersClient, challengesClient, options))

    logInfo(`Initialization timings: ${formatTimingSegments([
      { label: 'connect', durationMs: connectDurationMs },
      { label: 'legacyLookup', durationMs: legacyLookupDurationMs },
      { label: 'reviewRelation', durationMs: reviewRelationDurationMs },
      { label: 'discoverUsers', durationMs: userDiscoveryDurationMs }
    ])}`)

    if (userIds.length === 0) {
      logInfo('No users found for the provided filters')
      return
    }

    const totalBatches = Math.ceil(userIds.length / USER_BATCH_SIZE)

    processedUserIdsWriter = buildProcessedUserIdsWriter(options.processedUserIdsPath)
    await processedUserIdsWriter.end()
    logInfo(`Writing processed user IDs to ${processedUserIdsWriter.outputPath}`)

    if (options.csvOnly) {
      csvWriter = buildCsvWriter(options.csvPath)
      csvWriter.writeLine(['User ID', 'Track ID', 'Type ID', 'Challenges', 'Wins', 'Most Recent Event Date', 'Most Recent Submission'])
    }

    for (let batchStart = 0; batchStart < userIds.length; batchStart += USER_BATCH_SIZE) {
      const batchStartedAt = startTimer()
      const batchNumber = Math.floor(batchStart / USER_BATCH_SIZE) + 1
      const batchUserIds = userIds.slice(batchStart, batchStart + USER_BATCH_SIZE)
      const batchStatsRecords = []
      const replaceUsers = new Set()
      const {
        result: existingUserIdSet,
        durationMs: existingUsersDurationMs
      } = await measureAsyncStep(async () => getExistingMemberUserIdSet(membersClient, batchUserIds))
      const existingBatchUserIds = []
      const missingBatchUserIds = []
      let batchOutputStatsRows = 0
      let writeStatsDurationMs = 0
      let historyDurationMs = 0
      let historyRefreshDurationMs = 0
      let rerateDurationMs = 0
      let rankRecalculationDurationMs = 0
      let maxRatingSyncDurationMs = 0

      for (const userId of batchUserIds) {
        if (existingUserIdSet.has(userId.toString())) {
          existingBatchUserIds.push(userId)
        } else {
          missingBatchUserIds.push(userId)
        }
      }

      if (missingBatchUserIds.length > 0) {
        const missingIdSample = missingBatchUserIds.slice(0, 10).map((userId) => userId.toString())
        const suffix = missingBatchUserIds.length > 10 ? ', ...' : ''
        logWarn(`Skipping ${missingBatchUserIds.length} user(s) missing in members.member: ${missingIdSample.join(', ')}${suffix}`)
      }

      const {
        result: legacyIdsByUserId,
        durationMs: legacyIdsDurationMs
      } = await measureAsyncStep(async () => getLegacyMemberStatsIdsByUserIds(membersClient, existingBatchUserIds))
      const {
        result: batchLegacyStatsByUserId,
        durationMs: legacyStatsDurationMs
      } = await measureAsyncStep(async () => (
        aggregateLegacyStatsByUserIds(
          membersClient,
          existingBatchUserIds,
          options,
          legacyIdsByUserId
        )
      ))
      const {
        result: batchReviewRowsByUserId,
        durationMs: reviewRowsDurationMs
      } = await measureAsyncStep(async () => fetchReviewChallengeResultsByUserIds(reviewDb, existingBatchUserIds))
      const {
        result: batchChallengeMetadataById,
        durationMs: challengeMetadataDurationMs
      } = await measureAsyncStep(async () => (
        fetchChallengeMetadataMap(
          challengesClient,
          Array.from(
            new Set(
              Array.from(batchReviewRowsByUserId.values())
                .flat()
                .map((row) => row.challengeId)
            )
          )
        )
      ))
      const {
        result: batchLegacyRatingFieldsByUserId,
        durationMs: legacyRatingFieldsDurationMs
      } = (!options.csvOnly && !options.skipRatings)
        ? await measureAsyncStep(async () => fetchLegacyRatingFieldsByUserIds(membersClient, existingBatchUserIds, legacyIdsByUserId))
        : { result: new Map(), durationMs: 0 }

      for (let missingIndex = 0; missingIndex < missingBatchUserIds.length; missingIndex += 1) {
        processedUsers += 1
        if (processedUsers % 100 === 0 || processedUsers === userIds.length) {
          logInfo(`Processed ${processedUsers} of ${userIds.length} users`)
        }
      }

      const {
        result: batchResults,
        durationMs: aggregateUsersDurationMs
      } = await measureAsyncStep(async () => (
        mapWithConcurrency(existingBatchUserIds, options.concurrency, async (userId) => {
          const aggregateStartedAt = startTimer()
          const userKey = userId.toString()
          const legacyRows = batchLegacyStatsByUserId.get(userKey) || []
          const reviewRows = batchReviewRowsByUserId.get(userKey) || []
          const { source, rows: stats } = await aggregateStatsForUserFromPreloadedData(
            challengesClient,
            userId,
            options,
            legacyRows,
            reviewRows,
            batchChallengeMetadataById
          )

          if (!options.csvOnly && !options.skipRatings && stats.length > 0) {
            const legacyRatingFields = batchLegacyRatingFieldsByUserId.get(userKey) || new Map()
            stats.forEach((row) => {
              const legacyFields = legacyRatingFields.get(buildTrackTypeKey(row.trackId, row.typeId))
              if (legacyFields) {
                Object.assign(row, legacyFields)
              }
            })
          }

          if (stats.length === 0) {
            logWarn(`No challenge stats found for user ${userId.toString()}`)
          }

          processedUsers += 1
          if (processedUsers % 100 === 0 || processedUsers === userIds.length) {
            logInfo(`Processed ${processedUsers} of ${userIds.length} users`)
          }

          return {
            userId,
            stats,
            source,
            statsCount: stats.length,
            aggregateDurationMs: getElapsedMilliseconds(aggregateStartedAt),
            replaceUser: (source === 'legacy' || source === 'review') && !options.trackId && !options.typeId
          }
        })
      ))

      logSlowestUserTimings(
        `Batch ${batchNumber}/${totalBatches} aggregate`,
        batchResults.map((result) => ({
          userId: result.userId,
          durationMs: result.aggregateDurationMs,
          source: result.source,
          statsCount: result.statsCount
        })),
        {
          detailFormatter: (measurement) => `${measurement.statsCount} stat row(s), source=${measurement.source}`
        }
      )

      batchResults.forEach(({ userId, stats, replaceUser }) => {
        if (replaceUser) {
          replaceUsers.add(userId.toString())
        }

        batchOutputStatsRows += stats.length

        if (options.csvOnly) {
          for (const row of stats) {
            csvWriter.writeLine([
              row.userId.toString(),
              row.trackId,
              row.typeId,
              row.challenges,
              row.wins,
              toIsoString(row.mostRecentEventDate),
              toIsoString(row.mostRecentSubmission)
            ])
          }
          return
        }

        stats.forEach((stat) => {
          batchStatsRecords.push(stat)
        })
      })

      if (!options.csvOnly && batchStatsRecords.length > 0) {
        const {
          result: written,
          durationMs
        } = await measureAsyncStep(async () => writeStatsToDatabase(membersClient, batchStatsRecords, replaceUsers))
        writeStatsDurationMs = durationMs
        writtenStats += written
        logInfo(`Created/updated ${written} memberStats rows for users ${batchStart + 1}-${processedUsers}`)
      }

      let batchHistoryWrites = 0
      let batchHistoryRefreshes = 0
      let batchReratedChallenges = 0
      let batchReratedRatings = 0
      let batchRecalculatedRankRows = 0
      let batchSyncedMemberMaxRatings = 0
      let batchDeletedMemberMaxRatings = 0

      if (!options.csvOnly) {
        if (!options.skipHistory) {
          const {
            result: historyResults,
            durationMs
          } = await measureAsyncStep(async () => (
            mapWithConcurrency(existingBatchUserIds, options.concurrency, async (userId) => {
              const historyStartedAt = startTimer()
              const legacyHistoryResult = await backfillHistoryFromLegacy(membersClient, userId, { refreshMostRecent: false })
              const supplementalHistoryResult = await backfillHistoryFromCompletedChallenges(
                membersClient,
                challengesClient,
                reviewDb,
                userId,
                {
                  refreshMostRecent: false,
                  trackId: options.trackId,
                  typeId: options.typeId
                }
              )
              return {
                userId,
                upserted: legacyHistoryResult.upserted + supplementalHistoryResult.upserted,
                refreshed: legacyHistoryResult.refreshed + supplementalHistoryResult.refreshed,
                durationMs: getElapsedMilliseconds(historyStartedAt)
              }
            })
          ))
          historyDurationMs = durationMs
          batchHistoryWrites = historyResults.reduce((sum, historyResult) => sum + historyResult.upserted, 0)
          logSlowestUserTimings(
            `Batch ${batchNumber}/${totalBatches} history`,
            historyResults,
            {
              detailFormatter: (measurement) => `upserted=${measurement.upserted}`
            }
          )
          const {
            result: refreshedRows,
            durationMs: refreshDurationMs
          } = await measureAsyncStep(async () => refreshHistoryMostRecentFlagsForUsers(membersClient, existingBatchUserIds))
          batchHistoryRefreshes = refreshedRows
          historyRefreshDurationMs = refreshDurationMs

          writtenHistory += batchHistoryWrites
          updatedHistoryFlags += batchHistoryRefreshes

          if (batchHistoryWrites > 0) {
            logInfo(`Seeded/updated ${batchHistoryWrites} memberStatsHistory rows for users ${batchStart + 1}-${processedUsers}`)
          }
          if (batchHistoryRefreshes > 0) {
            logInfo(`Recomputed memberStatsHistory.mostRecent on ${batchHistoryRefreshes} row(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        } else {
          const {
            result: updatedRows,
            durationMs
          } = await measureAsyncStep(async () => refreshHistoryMostRecentFlagsForUsers(membersClient, existingBatchUserIds))
          historyRefreshDurationMs = durationMs
          batchHistoryRefreshes = updatedRows
          updatedHistoryFlags += updatedRows
          if (updatedRows > 0) {
            logInfo(`Recomputed memberStatsHistory.mostRecent on ${updatedRows} row(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        }

        if (!options.skipRatings && !options.skipRerate) {
          const {
            result: rerateResults,
            durationMs
          } = await measureAsyncStep(async () => (
            mapWithConcurrency(existingBatchUserIds, options.concurrency, async (userId) => {
              const rerateStartedAt = startTimer()
              const rerateResult = await rerateDevTrack(membersClient, challengesClient, reviewDb, userId, null, {
                useLegacySourceRatings: true,
                recalculateRanks: false
              })
              return {
                userId,
                challengesProcessed: rerateResult.challengesProcessed,
                ratingsUpdated: rerateResult.ratingsUpdated,
                durationMs: getElapsedMilliseconds(rerateStartedAt)
              }
            })
          ))
          rerateDurationMs = durationMs
          batchReratedChallenges = rerateResults.reduce((sum, rerateResult) => sum + rerateResult.challengesProcessed, 0)
          batchReratedRatings = rerateResults.reduce((sum, rerateResult) => sum + rerateResult.ratingsUpdated, 0)
          logSlowestUserTimings(
            `Batch ${batchNumber}/${totalBatches} rerate`,
            rerateResults,
            {
              detailFormatter: (measurement) => `challenges=${measurement.challengesProcessed}, ratings=${measurement.ratingsUpdated}`
            }
          )

          reratedChallenges += batchReratedChallenges
          reratedRatings += batchReratedRatings

          if (batchReratedChallenges > 0) {
            logInfo(`Re-rated ${batchReratedChallenges} development challenge(s) for users ${batchStart + 1}-${processedUsers}`)
          }

          if (batchReratedRatings > 0) {
            const {
              result: updatedRankRows,
              durationMs
            } = await measureAsyncStep(async () => recalculateRatingRanks(
              membersClient,
              {
                trackId: legacyLookupCache.trackIds.DEVELOP,
                typeId: legacyLookupCache.typeIds.CHALLENGE || resolveChallengeTypeId(TYPE_NAMES.CHALLENGE)
              },
              { updatedBy: RERATE_ACTOR }
            ))
            rankRecalculationDurationMs = durationMs
            batchRecalculatedRankRows = updatedRankRows
            recalculatedRankRows += updatedRankRows
            logInfo(`Recomputed ${updatedRankRows} development challenge rank row(s) after batch rerates`)
          }
        }

        if (!options.skipRatings) {
          const {
            result: maxRatingSyncResult,
            durationMs
          } = await measureAsyncStep(async () => syncCurrentMemberMaxRatingsForUsers(membersClient, existingBatchUserIds))
          maxRatingSyncDurationMs = durationMs
          batchSyncedMemberMaxRatings = maxRatingSyncResult.upserted
          batchDeletedMemberMaxRatings = maxRatingSyncResult.deleted
          syncedMemberMaxRatings += batchSyncedMemberMaxRatings
          deletedMemberMaxRatings += batchDeletedMemberMaxRatings

          if (batchSyncedMemberMaxRatings > 0 || batchDeletedMemberMaxRatings > 0) {
            logInfo(`Synchronized ${batchSyncedMemberMaxRatings} memberMaxRating row(s) and deleted ${batchDeletedMemberMaxRatings} stale row(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        }
      }

      const { durationMs: checkpointDurationMs } = await measureAsyncStep(async () => processedUserIdsWriter.appendUserIds(existingBatchUserIds))
      const batchTotalDurationMs = getElapsedMilliseconds(batchStartedAt)

      logInfo(`Batch ${batchNumber}/${totalBatches} summary: requested=${batchUserIds.length}, existing=${existingBatchUserIds.length}, missing=${missingBatchUserIds.length}, statRows=${batchOutputStatsRows}, historyWrites=${batchHistoryWrites}, historyMostRecentUpdates=${batchHistoryRefreshes}, reratedChallenges=${batchReratedChallenges}, reratedRatings=${batchReratedRatings}, rankRows=${batchRecalculatedRankRows}, maxRatingUpserts=${batchSyncedMemberMaxRatings}, maxRatingDeletes=${batchDeletedMemberMaxRatings}`)
      logInfo(`Batch ${batchNumber}/${totalBatches} timings: ${formatTimingSegments([
        { label: 'existingUsers', durationMs: existingUsersDurationMs },
        { label: 'legacyIds', durationMs: legacyIdsDurationMs },
        { label: 'legacyStats', durationMs: legacyStatsDurationMs },
        { label: 'reviewRows', durationMs: reviewRowsDurationMs },
        { label: 'challengeMetadata', durationMs: challengeMetadataDurationMs },
        !options.csvOnly && !options.skipRatings
          ? { label: 'legacyRatings', durationMs: legacyRatingFieldsDurationMs }
          : null,
        { label: 'aggregateUsers', durationMs: aggregateUsersDurationMs },
        !options.csvOnly ? { label: 'writeStats', durationMs: writeStatsDurationMs } : null,
        !options.csvOnly && !options.skipHistory ? { label: 'historyBackfill', durationMs: historyDurationMs } : null,
        !options.csvOnly ? { label: 'historyMostRecent', durationMs: historyRefreshDurationMs } : null,
        !options.csvOnly && !options.skipRatings && !options.skipRerate ? { label: 'rerate', durationMs: rerateDurationMs } : null,
        !options.csvOnly && !options.skipRatings && !options.skipRerate ? { label: 'rankRecalc', durationMs: rankRecalculationDurationMs } : null,
        !options.csvOnly && !options.skipRatings ? { label: 'maxRatingSync', durationMs: maxRatingSyncDurationMs } : null,
        { label: 'checkpoint', durationMs: checkpointDurationMs },
        { label: 'total', durationMs: batchTotalDurationMs }
      ])}`)
    }

    if (csvWriter) {
      await csvWriter.end()
    }
    if (processedUserIdsWriter) {
      await processedUserIdsWriter.end()
    }

    const totalRuntimeMs = getElapsedMilliseconds(scriptStartedAt)
    logInfo(`Completed processing ${processedUsers} users, created/updated ${writtenStats} stat records, seeded/updated ${writtenHistory} history records, recomputed ${updatedHistoryFlags} history mostRecent flags, rebuilt ${reratedRatings} rating update(s) across ${reratedChallenges} development challenge(s), recomputed ${recalculatedRankRows} development challenge rank row(s), synchronized ${syncedMemberMaxRatings} memberMaxRating row(s), deleted ${deletedMemberMaxRatings} stale memberMaxRating row(s), total runtime ${formatDuration(totalRuntimeMs)}`)
  } finally {
    await Promise.allSettled([
      membersClient.$disconnect(),
      challengesClient.$disconnect(),
      reviewDb && typeof reviewDb.end === 'function' ? reviewDb.end() : Promise.resolve()
    ])
  }
}

if (require.main === module) {
  main().catch((error) => {
    logError('recalculateMemberStats failed', error)
    process.exit(1)
  })
}

module.exports = {
  parseArgs,
  getUserIds,
  getExistingMemberUserIdSet,
  aggregateStatsForUser,
  initializeLegacyLookupCache,
  fetchLegacyRatingFields,
  writeStatsToDatabase,
  backfillHistoryFromLegacy,
  backfillHistoryFromCompletedChallenges,
  refreshHistoryMostRecentFlagsForUsers,
  mapWithConcurrency,
  toCsvValue,
  toIsoString,
  buildCsvWriter,
  buildProcessedUserIdsWriter,
  resolveLegacyDesignTypeId,
  buildAggregatedStatsFromReviewResults,
  buildSupplementalHistoryRowsFromCompletedChallenges,
  aggregateChallengeWinnerStatsForUser,
  buildCurrentMemberMaxRatingData,
  syncCurrentMemberMaxRatingsForUsers
}

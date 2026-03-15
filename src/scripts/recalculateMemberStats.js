#!/usr/bin/env node
'use strict'

/**
 * Recalculate member statistics from challenge winner data.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL (challenge database)
 * - REVIEW_DB_URL (review database, required unless --skip-ratings is set)
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
 *   node src/scripts/recalculateMemberStats.js --track-id <uuid>
 * - Skip unified history seeding:
 *   node src/scripts/recalculateMemberStats.js --skip-history
 * - Skip legacy rating enrichment:
 *   node src/scripts/recalculateMemberStats.js --skip-ratings
 *
 * Notes:
 * - Rating and rank fields are backfilled from the legacy member stat sub-tables.
 * - mostRecentSubmission is derived from ChallengeWinner timestamps.
 * - memberStatsHistory is seeded from memberDevelopHistoryStats and memberDataScienceHistoryStats.
 * - memberStatsHistory.mostRecent is recalculated from latest eventDate per (userId, trackId, typeId).
 * - memberStatsHistory.newRating on the mostRecent row is synchronized from memberStats.rating.
 * - --skip-history skips the legacy history backfill pass.
 * - --skip-ratings skips the legacy rating/rank enrichment pass and the Qubits rerate backfill.
 * - The script is idempotent and safe to run multiple times.
 * - Writes use upsert on (userId, trackId, typeId).
 * - Users missing in members.member are skipped to satisfy foreign key constraints.
 */

const fs = require('fs')
const path = require('path')

require('dotenv').config()

const { getMembersClient, getChallengesClient } = require('../common/prisma')
const reviewDb = require('../common/reviewDb')
const { rerateDevTrack } = require('../ratings/developRatingEngine')

const DEFAULT_ACTOR = process.env.UPDATED_BY || process.env.CREATED_BY || 'stats-migration'
const CREATED_BY = process.env.CREATED_BY || DEFAULT_ACTOR
const UPDATED_BY = process.env.UPDATED_BY || DEFAULT_ACTOR
const USER_BATCH_SIZE = 100
const TRACK_NAMES = {
  DEVELOP: 'DEVELOP',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE'
}
const TYPE_NAMES = {
  CHALLENGE: 'Challenge',
  FIRST2FINISH: 'First2Finish',
  TASK: 'Task',
  SRM: 'SRM',
  MARATHON_MATCH: 'MARATHON_MATCH'
}
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
 * Normalize lookup keys used for cached track and type resolution.
 * @param {*} value raw lookup value
 * @returns {string} normalized uppercase lookup key
 */
function normalizeLookupKey (value) {
  return String(value || '').trim().toUpperCase()
}

/**
 * Resolve a track identifier or label to the canonical unified track name.
 * @param {*} trackId raw track identifier or name
 * @returns {string|undefined} unified track name when recognized
 */
function resolveTrackName (trackId) {
  if (trackId === null || trackId === undefined) {
    return undefined
  }

  const normalized = normalizeLookupKey(trackId)
  if (!normalized) {
    return undefined
  }

  if (normalized.includes('DATA') && normalized.includes('SCIENCE')) {
    return TRACK_NAMES.DATA_SCIENCE
  }

  if (normalized.includes('DEVELOP') || normalized === 'DEV') {
    return TRACK_NAMES.DEVELOP
  }

  if (normalized.includes('DESIGN') || normalized === 'DES') {
    return TRACK_NAMES.DESIGN
  }

  return undefined
}

/**
 * Resolve a type identifier or label to the canonical unified type name.
 * @param {*} typeId raw type identifier or name
 * @returns {string|undefined} unified type name when recognized
 */
function resolveTypeName (typeId) {
  if (typeId === null || typeId === undefined) {
    return undefined
  }

  const normalized = normalizeLookupKey(typeId)
  if (!normalized) {
    return undefined
  }

  if (normalized.includes('MARATHON')) {
    return TYPE_NAMES.MARATHON_MATCH
  }

  if (normalized.includes('FIRST') || normalized.includes('F2F')) {
    return TYPE_NAMES.FIRST2FINISH
  }

  if (normalized.includes('TASK')) {
    return TYPE_NAMES.TASK
  }

  if (normalized.includes('SRM')) {
    return TYPE_NAMES.SRM
  }

  if (normalized.includes('CHALLENGE')) {
    return TYPE_NAMES.CHALLENGE
  }

  return String(typeId)
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
 * Preserve the first resolved lookup entry for a normalized cache key.
 * @param {Map<string, string>} map lookup map
 * @param {*} key lookup input
 * @param {*} value resolved id
 */
function addLookupEntry (map, key, value) {
  const normalizedKey = normalizeLookupKey(key)
  if (!normalizedKey || value === null || value === undefined || map.has(normalizedKey)) {
    return
  }

  map.set(normalizedKey, String(value))
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

  const [trackRows, typeRows] = await Promise.all([
    challengesClient.$queryRaw`
      SELECT "id", "name", "abbreviation"
      FROM "ChallengeTrack"
    `,
    challengesClient.$queryRaw`
      SELECT "id", "name", "abbreviation", "isTask"
      FROM "ChallengeType"
    `
  ])

  const trackIdsByName = new Map()
  const typeIdsByName = new Map()

  trackRows.forEach((row) => {
    addLookupEntry(trackIdsByName, row.name, row.id)
    addLookupEntry(trackIdsByName, row.abbreviation, row.id)
    addLookupEntry(trackIdsByName, resolveTrackName(row.name), row.id)
    addLookupEntry(trackIdsByName, resolveTrackName(row.abbreviation), row.id)
  })

  typeRows.forEach((row) => {
    addLookupEntry(typeIdsByName, row.name, row.id)
    addLookupEntry(typeIdsByName, row.abbreviation, row.id)
    addLookupEntry(typeIdsByName, resolveTypeName(row.name), row.id)
    addLookupEntry(typeIdsByName, resolveTypeName(row.abbreviation), row.id)

    if (row.isTask) {
      addLookupEntry(typeIdsByName, TYPE_NAMES.TASK, row.id)
    }
  })

  legacyLookupCache = {
    trackIds: {
      DEVELOP: trackIdsByName.get(TRACK_NAMES.DEVELOP) || null,
      DESIGN: trackIdsByName.get(TRACK_NAMES.DESIGN) || null,
      DATA_SCIENCE: trackIdsByName.get(TRACK_NAMES.DATA_SCIENCE) || null
    },
    typeIds: {
      CHALLENGE: typeIdsByName.get(normalizeLookupKey(TYPE_NAMES.CHALLENGE)) || null,
      FIRST2FINISH: typeIdsByName.get(normalizeLookupKey(TYPE_NAMES.FIRST2FINISH)) || null,
      TASK: typeIdsByName.get(normalizeLookupKey(TYPE_NAMES.TASK)) || null,
      SRM: typeIdsByName.get(normalizeLookupKey(TYPE_NAMES.SRM)) || null,
      MARATHON_MATCH: typeIdsByName.get(normalizeLookupKey(TYPE_NAMES.MARATHON_MATCH)) || null
    },
    typeIdsByName
  }

  if (!legacyLookupCache.trackIds.DEVELOP || !legacyLookupCache.trackIds.DATA_SCIENCE) {
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
    const normalized = normalizeLookupKey(candidate)
    if (normalized && legacyLookupCache.typeIdsByName.has(normalized)) {
      return legacyLookupCache.typeIdsByName.get(normalized)
    }
  }

  return null
}

/**
 * Merge non-null stat values into the keyed legacy rating lookup.
 * @param {Map<string, Object>} lookup keyed legacy stat lookup
 * @param {string} key unified track/type lookup key
 * @param {Object} values legacy rating/rank values for the key
 */
function mergeLegacyStatLookup (lookup, key, values) {
  const existing = lookup.get(key) || {}
  const merged = { ...existing }

  Object.keys(values).forEach((field) => {
    if (values[field] !== null && values[field] !== undefined) {
      merged[field] = values[field]
    }
  })

  lookup.set(key, merged)
}

/**
 * Load rating and rank fields from the legacy member stat sub-tables for one user.
 * The returned map is merged into the challenge-derived stats before unified writes.
 * @param {Object} membersClient prisma members client
 * @param {BigInt} userId member user id
 * @returns {Map<string, Object>} map keyed by trackId::typeId with legacy rating fields
 * @throws {Error} if the legacy challenge lookup cache is not initialized
 */
async function fetchLegacyRatingFields (membersClient, userId) {
  if (!legacyLookupCache) {
    throw new Error('Legacy challenge lookup cache is not initialized')
  }

  const parentRows = await membersClient.memberStats.findMany({
    where: { userId },
    select: { id: true }
  })

  if (parentRows.length === 0) {
    return new Map()
  }

  const memberStatsIds = parentRows.map((row) => row.id.toString())
  const idsSql = memberStatsIds.join(', ')
  const lookup = new Map()

  const developRows = await membersClient.$queryRawUnsafe(
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
  )

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

  const srmRows = await membersClient.$queryRawUnsafe(
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
  )

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

  const marathonRows = await membersClient.$queryRawUnsafe(
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

/**
 * Keep the latest event payload for duplicate legacy history keys.
 * @param {Map<string, Object>} lookup keyed legacy history lookup
 * @param {Object} row candidate history row
 */
function mergeLegacyHistoryRow (lookup, row) {
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
    return {
      upserted: 0,
      refreshed: options.refreshMostRecent === false
        ? 0
        : await refreshHistoryMostRecentFlagsForUsers(membersClient, [userId])
    }
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

    mergeLegacyHistoryRow(legacyHistoryLookup, {
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

    mergeLegacyHistoryRow(legacyHistoryLookup, {
      userId,
      trackId: legacyLookupCache.trackIds.DATA_SCIENCE,
      typeId,
      challengeId: String(row.challengeId),
      eventDate,
      newRating: toOptionalInt(row.rating)
    })
  })

  const historyRows = Array.from(legacyHistoryLookup.values())
  if (historyRows.length === 0) {
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
      challengeId: true
    }
  })

  const existingByKey = new Map(
    existingRows.map((row) => [
      buildHistoryKey(userId, row.trackId, row.typeId, row.challengeId),
      row
    ])
  )

  const queries = historyRows.map((row) => {
    const writeData = {
      eventDate: row.eventDate,
      newRating: row.newRating,
      updatedBy: UPDATED_BY
    }
    const existingRow = existingByKey.get(buildHistoryKey(userId, row.trackId, row.typeId, row.challengeId))

    if (existingRow) {
      return membersClient.memberStatsHistory.update({
        where: { id: existingRow.id },
        data: writeData
      })
    }

    return membersClient.memberStatsHistory.create({
      data: {
        userId,
        trackId: row.trackId,
        typeId: row.typeId,
        challengeId: row.challengeId,
        mostRecent: false,
        createdBy: CREATED_BY,
        ...writeData
      }
    })
  })

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

function parseArgs (argv) {
  const options = {
    csvOnly: false,
    csvPath: null,
    userIds: [],
    trackId: null,
    typeId: null,
    limit: null,
    skipHistory: false,
    skipRatings: false,
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

    if (arg === '--skip-history') {
      options.skipHistory = true
      continue
    }

    if (arg === '--skip-ratings') {
      options.skipRatings = true
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
  --user-id <id>            Process a single user (repeatable).
  --user-ids <id,id>        Comma-separated user IDs.
  --track-id <id>           Filter by track ID.
  --type-id <id>            Filter by type ID.
  --limit <n>               Limit number of users processed.
  --skip-history            Skip seeding memberStatsHistory from legacy history tables.
  --skip-ratings            Skip legacy rating enrichment and development rerating.
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
    addEqualsClause('c."trackId"', options.trackId)
  }

  if (options.typeId) {
    addEqualsClause('c."typeId"', options.typeId)
  }

  return {
    whereSql: clauses.join(' AND '),
    params
  }
}

async function getUserIds (challengesClient, options) {
  const explicitUserIds = Array.from(new Set(options.userIds))
  let userIds

  if (explicitUserIds.length > 0) {
    userIds = explicitUserIds.map((userId) => normalizeBigInt(userId, 'user id'))
  } else {
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

    userIds = rows.map((row) => normalizeBigInt(row.userId, 'user id'))
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

async function aggregateStatsForUser (challengesClient, userId, options) {
  const { whereSql, params } = buildFilterQuery(options, userId)

  const rows = await challengesClient.$queryRawUnsafe(
    `
    SELECT
      cw."userId" AS "userId",
      c."trackId" AS "trackId",
      c."typeId" AS "typeId",
      COUNT(DISTINCT c.id)::int AS "challenges",
      COUNT(CASE WHEN cw."type" = 'PLACEMENT' THEN 1 END)::int AS "wins",
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

  return rows.map((row) => ({
    userId: normalizeBigInt(row.userId, 'user id'),
    trackId: String(row.trackId),
    typeId: String(row.typeId),
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
  }))
}

async function writeStatsToDatabase (membersClient, statsRecords) {
  if (!statsRecords || statsRecords.length === 0) {
    return 0
  }

  try {
    const existingRows = await membersClient.memberStats.findMany({
      where: {
        OR: statsRecords.map((record) => ({
          userId: record.userId,
          trackId: record.trackId,
          typeId: record.typeId
        }))
      },
      select: MEMBER_STATS_PRESERVE_SELECT
    })
    const existingByKey = new Map(
      existingRows.map((row) => [buildUserTrackTypeKey(row.userId, row.trackId, row.typeId), row])
    )
    const queries = statsRecords.map((record) => {
      const existingRow = existingByKey.get(buildUserTrackTypeKey(record.userId, record.trackId, record.typeId)) || null

      return buildMemberStatsUpsertQuery(
        membersClient,
        record,
        existingRow
      )
    })

    // Use array transactions to avoid interactive transaction timeout closures on long write loops.
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

  const rows = await membersClient.$queryRawUnsafe(
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
    ),
    updated AS (
      UPDATE "members"."memberStatsHistory" msh
      SET
        "mostRecent" = CASE WHEN ranked."rowNum" = 1 THEN true ELSE false END,
        "oldRating" = CASE
          WHEN ranked."rowNum" = 1 THEN previous."newRating"
          ELSE msh."oldRating"
        END,
        "newRating" = CASE
          WHEN ranked."rowNum" = 1 AND ms."id" IS NOT NULL THEN ms."rating"
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
      RETURNING 1
    )
    SELECT COUNT(*)::int AS "updatedRows" FROM updated
    `,
    ...params
  )

  return rows && rows[0] ? toInt(rows[0].updatedRows) : 0
}

async function main () {
  const options = parseArgs(process.argv.slice(2))

  if (options.help) {
    printUsage()
    return
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required')
  }

  if (!process.env.CHALLENGES_DB_URL) {
    throw new Error('CHALLENGES_DB_URL is required')
  }

  if (!options.csvOnly && !options.skipRatings && !reviewDb) {
    throw new Error('REVIEW_DB_URL is required unless --skip-ratings is set')
  }

  const membersClient = getMembersClient()
  const challengesClient = getChallengesClient()

  let csvWriter
  let processedUsers = 0
  let writtenStats = 0
  let writtenHistory = 0
  let updatedHistoryFlags = 0
  let reratedChallenges = 0
  let reratedRatings = 0

  try {
    logInfo(`Starting recalculateMemberStats in ${options.csvOnly ? 'CSV-only' : 'write'} mode`)

    await membersClient.$connect()
    await challengesClient.$connect()

    if (!options.csvOnly && (!options.skipRatings || !options.skipHistory)) {
      await initializeLegacyLookupCache(challengesClient)
    }

    const userIds = await getUserIds(challengesClient, options)
    if (userIds.length === 0) {
      logInfo('No users found for the provided filters')
      return
    }

    if (options.csvOnly) {
      csvWriter = buildCsvWriter(options.csvPath)
      csvWriter.writeLine(['User ID', 'Track ID', 'Type ID', 'Challenges', 'Wins', 'Most Recent Event Date', 'Most Recent Submission'])
    }

    for (let batchStart = 0; batchStart < userIds.length; batchStart += USER_BATCH_SIZE) {
      const batchUserIds = userIds.slice(batchStart, batchStart + USER_BATCH_SIZE)
      const batchStatsRecords = []
      const existingUserIdSet = await getExistingMemberUserIdSet(membersClient, batchUserIds)
      const existingBatchUserIds = []
      const missingBatchUserIds = []

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

      for (const userId of batchUserIds) {
        processedUsers += 1
        if (!existingUserIdSet.has(userId.toString())) {
          if (processedUsers % 100 === 0 || processedUsers === userIds.length) {
            logInfo(`Processed ${processedUsers} of ${userIds.length} users`)
          }
          continue
        }

        const stats = await aggregateStatsForUser(challengesClient, userId, options)
        if (!options.csvOnly && !options.skipRatings && stats.length > 0) {
          const legacyRatingFields = await fetchLegacyRatingFields(membersClient, userId)
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
        } else {
          batchStatsRecords.push(...stats)
        }

        if (processedUsers % 100 === 0 || processedUsers === userIds.length) {
          logInfo(`Processed ${processedUsers} of ${userIds.length} users`)
        }
      }

      if (!options.csvOnly && batchStatsRecords.length > 0) {
        const written = await writeStatsToDatabase(membersClient, batchStatsRecords)
        writtenStats += written
        logInfo(`Created/updated ${written} memberStats rows for users ${batchStart + 1}-${processedUsers}`)
      }

      if (!options.csvOnly) {
        if (!options.skipHistory) {
          let batchHistoryWrites = 0
          let batchHistoryRefreshes = 0
          for (const userId of existingBatchUserIds) {
            const historyResult = await backfillHistoryFromLegacy(membersClient, userId)
            batchHistoryWrites += historyResult.upserted
            batchHistoryRefreshes += historyResult.refreshed
          }

          writtenHistory += batchHistoryWrites
          updatedHistoryFlags += batchHistoryRefreshes

          if (batchHistoryWrites > 0) {
            logInfo(`Seeded/updated ${batchHistoryWrites} memberStatsHistory rows for users ${batchStart + 1}-${processedUsers}`)
          }
          if (batchHistoryRefreshes > 0) {
            logInfo(`Recomputed memberStatsHistory.mostRecent on ${batchHistoryRefreshes} row(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        } else {
          const updatedRows = await refreshHistoryMostRecentFlagsForUsers(membersClient, existingBatchUserIds)
          updatedHistoryFlags += updatedRows
          if (updatedRows > 0) {
            logInfo(`Recomputed memberStatsHistory.mostRecent on ${updatedRows} row(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        }

        if (!options.skipRatings) {
          let batchReratedChallenges = 0
          let batchReratedRatings = 0

          for (const userId of existingBatchUserIds) {
            const rerateResult = await rerateDevTrack(membersClient, challengesClient, reviewDb, userId)
            batchReratedChallenges += rerateResult.challengesProcessed
            batchReratedRatings += rerateResult.ratingsUpdated
          }

          reratedChallenges += batchReratedChallenges
          reratedRatings += batchReratedRatings

          if (batchReratedChallenges > 0) {
            logInfo(`Re-rated ${batchReratedChallenges} development challenge(s) for users ${batchStart + 1}-${processedUsers}`)
          }
        }
      }
    }

    if (csvWriter) {
      await csvWriter.end()
    }

    logInfo(`Completed processing ${processedUsers} users, created/updated ${writtenStats} stat records, seeded/updated ${writtenHistory} history records, recomputed ${updatedHistoryFlags} history mostRecent flags, and rebuilt ${reratedRatings} rating update(s) across ${reratedChallenges} development challenge(s)`)
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
  refreshHistoryMostRecentFlagsForUsers,
  toCsvValue,
  toIsoString,
  buildCsvWriter
}

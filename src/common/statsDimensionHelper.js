'use strict'

const TRACK_NAMES = {
  DEVELOP: 'DEVELOP',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE',
  COPILOT: 'COPILOT'
}

const TYPE_NAMES = {
  CHALLENGE: 'Challenge',
  CODE: 'CODE',
  BUG_HUNT: 'BUG_HUNT',
  TEST_SCENARIOS: 'TEST_SCENARIOS',
  TEST_SUITES: 'TEST_SUITES',
  FIRST2FINISH: 'First2Finish',
  TASK: 'Task',
  SRM: 'SRM',
  MARATHON_MATCH: 'MARATHON_MATCH'
}

let cachedLookupPromise

/**
 * Normalize lookup keys used for track/type resolution.
 * @param {*} value raw lookup input
 * @returns {string} uppercase lookup key, or an empty string when not provided
 */
function normalizeLookupKey (value) {
  return String(value || '').trim().toUpperCase()
}

/**
 * Normalize track labels into the canonical API names used by unified stats.
 * Unknown values are returned as-is so hidden legacy track labels still round-trip.
 * @param {*} value raw track id, name, or abbreviation
 * @returns {string|undefined} canonical track label when present
 */
function getCanonicalTrackName (value) {
  if (value === null || value === undefined) {
    return undefined
  }

  const raw = String(value).trim()
  const normalized = normalizeLookupKey(value)
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

  if (normalized.includes('COPILOT')) {
    return TRACK_NAMES.COPILOT
  }

  return raw
}

/**
 * Normalize type labels into the canonical API names used by unified stats.
 * Unknown values are returned as-is so hidden legacy type labels still round-trip.
 * @param {*} value raw type id, name, or abbreviation
 * @returns {string|undefined} canonical type label when present
 */
function getCanonicalTypeName (value) {
  if (value === null || value === undefined) {
    return undefined
  }

  const raw = String(value).trim()
  const normalized = normalizeLookupKey(value)
  if (!normalized) {
    return undefined
  }

  if (normalized.includes('MARATHON')) {
    return TYPE_NAMES.MARATHON_MATCH
  }

  if (normalized.includes('FIRST') || normalized === 'F2F') {
    return TYPE_NAMES.FIRST2FINISH
  }

  if (normalized === 'CODE' || normalized === 'COD') {
    return TYPE_NAMES.CODE
  }

  if (normalized.includes('TASK') || normalized === 'TSK') {
    return TYPE_NAMES.TASK
  }

  if (normalized.includes('SRM')) {
    return TYPE_NAMES.SRM
  }

  if (normalized.includes('CHALLENGE') || normalized === 'CH') {
    return TYPE_NAMES.CHALLENGE
  }

  return raw
}

function addLookupEntry (map, key, value) {
  const normalizedKey = normalizeLookupKey(key)
  if (!normalizedKey || value === null || value === undefined || map.has(normalizedKey)) {
    return
  }

  map.set(normalizedKey, String(value))
}

/**
 * Build a process-local lookup that can translate between challenge UUID ids,
 * canonical API labels, abbreviations, and seeded legacy ids.
 * @param {Array<Object>} trackRows ChallengeTrack rows
 * @param {Array<Object>} typeRows ChallengeType rows
 * @returns {Object} lookup bundle shared by unified reads and migration scripts
 */
function buildChallengeDimensionLookup (trackRows, typeRows) {
  const trackIdsByLookup = new Map()
  const typeIdsByLookup = new Map()
  const trackNamesById = new Map()
  const typeNamesById = new Map()

  trackRows.forEach((row) => {
    const id = String(row.id)
    const canonicalTrackName = getCanonicalTrackName(row.name)

    trackNamesById.set(id, canonicalTrackName || String(row.name || '').trim())

    addLookupEntry(trackIdsByLookup, row.id, id)
    addLookupEntry(trackIdsByLookup, row.name, id)
    addLookupEntry(trackIdsByLookup, row.abbreviation, id)
    addLookupEntry(trackIdsByLookup, row.legacyId, id)
    addLookupEntry(trackIdsByLookup, canonicalTrackName, id)
  })

  typeRows.forEach((row) => {
    const id = String(row.id)
    const canonicalTypeName = getCanonicalTypeName(row.name)

    typeNamesById.set(id, canonicalTypeName || String(row.name || '').trim())

    addLookupEntry(typeIdsByLookup, row.id, id)
    addLookupEntry(typeIdsByLookup, row.name, id)
    addLookupEntry(typeIdsByLookup, row.abbreviation, id)
    addLookupEntry(typeIdsByLookup, row.legacyId, id)
    addLookupEntry(typeIdsByLookup, canonicalTypeName, id)

    if (row.isTask) {
      addLookupEntry(typeIdsByLookup, TYPE_NAMES.TASK, id)
    }
  })

  const lookup = {
    trackIdsByLookup,
    typeIdsByLookup,
    trackNamesById,
    typeNamesById
  }

  lookup.trackIds = {
    DEVELOP: resolveTrackIdFromLookup(lookup, TRACK_NAMES.DEVELOP) || null,
    DESIGN: resolveTrackIdFromLookup(lookup, TRACK_NAMES.DESIGN) || null,
    DATA_SCIENCE: resolveTrackIdFromLookup(lookup, TRACK_NAMES.DATA_SCIENCE) || null,
    COPILOT: resolveTrackIdFromLookup(lookup, TRACK_NAMES.COPILOT) || null
  }
  lookup.typeIds = {
    CHALLENGE: resolveTypeIdFromLookup(lookup, TYPE_NAMES.CHALLENGE) || null,
    CODE: resolveTypeIdFromLookup(lookup, TYPE_NAMES.CODE) || null,
    BUG_HUNT: resolveTypeIdFromLookup(lookup, TYPE_NAMES.BUG_HUNT) || null,
    TEST_SCENARIOS: resolveTypeIdFromLookup(lookup, TYPE_NAMES.TEST_SCENARIOS) || null,
    TEST_SUITES: resolveTypeIdFromLookup(lookup, TYPE_NAMES.TEST_SUITES) || null,
    FIRST2FINISH: resolveTypeIdFromLookup(lookup, TYPE_NAMES.FIRST2FINISH) || null,
    TASK: resolveTypeIdFromLookup(lookup, TYPE_NAMES.TASK) || null,
    SRM: resolveTypeIdFromLookup(lookup, TYPE_NAMES.SRM) || null,
    MARATHON_MATCH: resolveTypeIdFromLookup(lookup, TYPE_NAMES.MARATHON_MATCH) || null
  }

  return lookup
}

/**
 * Load and cache the challenge dimension lookup for the current process.
 * @param {Object} challengesClient Prisma challenge client
 * @returns {Promise<Object>} cached lookup data
 */
async function loadChallengeDimensionLookup (challengesClient) {
  if (cachedLookupPromise) {
    return cachedLookupPromise
  }

  cachedLookupPromise = (async () => {
    const [trackRows, typeRows] = await Promise.all([
      challengesClient.$queryRaw`
        SELECT "id", "name", "abbreviation", "legacyId"
        FROM "ChallengeTrack"
      `,
      challengesClient.$queryRaw`
        SELECT "id", "name", "abbreviation", "legacyId", "isTask"
        FROM "ChallengeType"
      `
    ])

    return buildChallengeDimensionLookup(trackRows, typeRows)
  })()

  try {
    return await cachedLookupPromise
  } catch (error) {
    cachedLookupPromise = null
    throw error
  }
}

function clearChallengeDimensionLookupCache () {
  cachedLookupPromise = null
}

/**
 * Resolve a track UUID from a UUID, canonical label, abbreviation, or legacy id.
 * @param {Object} lookup challenge dimension lookup
 * @param {*} value raw track identifier
 * @returns {string|undefined} resolved track UUID when known
 */
function resolveTrackIdFromLookup (lookup, value) {
  if (!lookup) {
    return undefined
  }

  return lookup.trackIdsByLookup.get(normalizeLookupKey(value))
}

/**
 * Resolve a type UUID from a UUID, canonical label, abbreviation, or legacy id.
 * @param {Object} lookup challenge dimension lookup
 * @param {*} value raw type identifier
 * @returns {string|undefined} resolved type UUID when known
 */
function resolveTypeIdFromLookup (lookup, value) {
  if (!lookup) {
    return undefined
  }

  return lookup.typeIdsByLookup.get(normalizeLookupKey(value))
}

/**
 * Resolve a canonical track name from either the stored UUID or a human-readable label.
 * @param {Object} lookup challenge dimension lookup
 * @param {*} value raw track identifier
 * @returns {string|undefined} canonical track label when known
 */
function resolveTrackNameFromLookup (lookup, value) {
  if (lookup && lookup.trackNamesById.has(String(value))) {
    return lookup.trackNamesById.get(String(value))
  }

  return getCanonicalTrackName(value)
}

/**
 * Resolve a canonical type name from either the stored UUID or a human-readable label.
 * @param {Object} lookup challenge dimension lookup
 * @param {*} value raw type identifier
 * @returns {string|undefined} canonical type label when known
 */
function resolveTypeNameFromLookup (lookup, value) {
  if (lookup && lookup.typeNamesById.has(String(value))) {
    return lookup.typeNamesById.get(String(value))
  }

  return getCanonicalTypeName(value)
}

module.exports = {
  TRACK_NAMES,
  TYPE_NAMES,
  normalizeLookupKey,
  getCanonicalTrackName,
  getCanonicalTypeName,
  buildChallengeDimensionLookup,
  loadChallengeDimensionLookup,
  clearChallengeDimensionLookupCache,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup,
  resolveTrackNameFromLookup,
  resolveTypeNameFromLookup
}

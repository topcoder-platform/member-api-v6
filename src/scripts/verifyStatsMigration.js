const _ = require('lodash')
const prismaManager = require('../common/prisma')

const prisma = prismaManager.getClient()
const { Prisma } = prismaManager

const LEGACY_TABLES = [
  'memberDevelopStats',
  'memberDevelopStatsItem',
  'memberDesignStats',
  'memberDataScienceStats',
  'memberSrmStats',
  'memberMarathonStats',
  'memberDevelopHistoryStats',
  'memberDataScienceHistoryStats',
  'memberHistoryStats'
]

function parseArgs (argv) {
  const args = { samples: 15 }
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--samples' && argv[i + 1]) {
      args.samples = _.clamp(Number(argv[i + 1]), 1, 50)
      i += 1
    }
  }
  return args
}

function toNumber (value) {
  if (_.isNil(value)) {
    return 0
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : 0
}

function toIso (value) {
  if (!value) {
    return null
  }
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date.toISOString()
}

async function tableExists (tableName) {
  const rows = await prisma.$queryRaw`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'members'
      AND table_name = ${tableName}
    ) AS "exists"
  `
  return !!(rows[0] && rows[0].exists)
}

async function countTable (tableName) {
  const exists = await tableExists(tableName)
  if (!exists) {
    return null
  }
  const rows = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::bigint AS count FROM "members"."${tableName}"`)
  return toNumber(rows[0] && rows[0].count)
}

function maxIso (values) {
  let latest = null
  values.forEach(value => {
    const iso = toIso(value)
    if (!iso) {
      return
    }
    if (!latest || iso > latest) {
      latest = iso
    }
  })
  return latest
}

function summarizeRows (rows) {
  return {
    challenges: _.sumBy(rows, row => toNumber(row.challenges)),
    wins: _.sumBy(rows, row => toNumber(row.wins)),
    mostRecentSubmission: maxIso(_.map(rows, 'mostRecentSubmission')),
    mostRecentEventDate: maxIso(_.map(rows, 'mostRecentEventDate'))
  }
}

const TRACK_NAMES = {
  DEVELOP: 'DEVELOP',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE',
  COPILOT: 'COPILOT'
}
const TYPE_NAMES = {
  CHALLENGE: 'Challenge',
  FIRST2FINISH: 'First2Finish',
  TASK: 'Task',
  SRM: 'SRM',
  MARATHON_MATCH: 'MARATHON_MATCH'
}
const DEFAULT_RATING_CLASSIFICATION = {
  ratingStatus: 'rated',
  ratingStatusSource: 'default-rated'
}

/**
 * Normalize challenge metadata names into comparison-friendly identifiers.
 * @param {*} value metadata name
 * @returns {string} normalized uppercase identifier with underscores
 */
function normalizeChallengeDimension (value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, '_')
}

/**
 * Parse a loosely-typed boolean-like metadata value.
 * @param {*} value metadata value
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
 * Decide whether a challenge should be treated as rated or explicitly unrated.
 * Missing or indeterminate metadata defaults to rated so null newRating values
 * still fail parity unless unrated intent is explicit.
 * @param {Array<Object>} metadataRows challenge metadata rows
 * @returns {Object} rating classification payload
 */
function classifyChallengeRating (metadataRows) {
  let unratedEntry
  let ratedEntry

  _.forEach(metadataRows || [], (row) => {
    const value = parseBooleanLike(row && row.metadataValue)
    if (value === undefined) {
      return
    }

    const name = normalizeChallengeDimension(row && row.metadataName)
    if (name === 'UNRATED' && _.isNil(unratedEntry)) {
      unratedEntry = { name: row.metadataName, value }
    }

    if ((name === 'RATED' || name === 'ISRATED' || name === 'IS_RATED') && _.isNil(ratedEntry)) {
      ratedEntry = { name: row.metadataName, value }
    }
  })

  if (unratedEntry) {
    return {
      ratingStatus: unratedEntry.value ? 'unrated' : 'rated',
      ratingStatusSource: `challenge-metadata:${unratedEntry.name}`
    }
  }

  if (ratedEntry) {
    return {
      ratingStatus: ratedEntry.value ? 'rated' : 'unrated',
      ratingStatusSource: `challenge-metadata:${ratedEntry.name}`
    }
  }

  return { ...DEFAULT_RATING_CLASSIFICATION }
}

/**
 * Resolve rating classification for challenge ids using challenge metadata and legacy id fallbacks.
 * @param {Array<*>} challengeIds challenge identifiers from memberStatsHistory
 * @returns {Promise<Map<string, Object>>} classification payload keyed by challenge id string
 */
async function fetchChallengeRatingClassificationMap (challengeIds) {
  const normalizedChallengeIds = _.chain(challengeIds)
    .map(challengeId => (_.isNil(challengeId) ? null : String(challengeId).trim()))
    .filter(Boolean)
    .uniq()
    .value()

  if (normalizedChallengeIds.length === 0) {
    return new Map()
  }

  const numericChallengeIds = _.chain(normalizedChallengeIds)
    .filter(challengeId => /^\d+$/.test(challengeId))
    .map(challengeId => Number(challengeId))
    .filter(Number.isSafeInteger)
    .uniq()
    .value()

  const whereClauses = [Prisma.sql`c."id" IN (${Prisma.join(normalizedChallengeIds)})`]
  if (numericChallengeIds.length > 0) {
    whereClauses.push(Prisma.sql`c."legacyId" IN (${Prisma.join(numericChallengeIds)})`)
  }

  const rows = await prisma.$queryRaw`
    SELECT
      c."id" AS "challengeId",
      c."legacyId" AS "legacyChallengeId",
      cm."name" AS "metadataName",
      cm."value" AS "metadataValue"
    FROM "challenges"."Challenge" c
    LEFT JOIN "challenges"."ChallengeMetadata" cm
      ON cm."challengeId" = c."id"
      AND LOWER(cm."name") IN ('rated', 'israted', 'unrated')
    WHERE ${Prisma.join(whereClauses, Prisma.sql` OR `)}
  `

  const classificationByChallengeId = new Map()
  _.forEach(_.groupBy(rows, row => String(row.challengeId)), (challengeRows) => {
    const classification = classifyChallengeRating(challengeRows)
    const challengeId = String(challengeRows[0].challengeId)
    classificationByChallengeId.set(challengeId, classification)

    if (!_.isNil(challengeRows[0].legacyChallengeId)) {
      classificationByChallengeId.set(String(challengeRows[0].legacyChallengeId), classification)
    }
  })

  return classificationByChallengeId
}

/**
 * Resolve rating classification for one history row.
 * @param {Object} row memberStatsHistory row
 * @param {Map<string, Object>} classificationByChallengeId classifications keyed by challenge id
 * @returns {Object} rating classification payload
 */
function getHistoryRowRatingClassification (row, classificationByChallengeId) {
  const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId)
  if (!challengeId) {
    return { ...DEFAULT_RATING_CLASSIFICATION }
  }

  return classificationByChallengeId.get(challengeId) || { ...DEFAULT_RATING_CLASSIFICATION }
}

function getUnifiedTrackName (trackId) {
  const normalized = String(trackId || '').toUpperCase().trim()
  if (TRACK_NAMES[normalized]) {
    return TRACK_NAMES[normalized]
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
  return normalized
}

/**
 * Resolve a unified type name from a type id, name, or abbreviation.
 * @param {*} typeId raw type value
 * @returns {string} canonical unified type name when recognized
 */
function getUnifiedTypeName (typeId) {
  const normalized = String(typeId || '').toUpperCase().trim()
  if (!normalized) {
    return normalized
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
  return typeId
}

function resolveUnifiedTrackName (row) {
  const candidates = [row.trackName, row.trackAbbreviation, row.trackId]
  for (const value of candidates) {
    const unifiedTrackName = getUnifiedTrackName(value)
    if (
      unifiedTrackName === TRACK_NAMES.DEVELOP ||
      unifiedTrackName === TRACK_NAMES.DESIGN ||
      unifiedTrackName === TRACK_NAMES.DATA_SCIENCE
    ) {
      return unifiedTrackName
    }
  }
  return null
}

/**
 * Resolve the canonical unified type name for a row joined to ChallengeType.
 * @param {Object} row unified stats or history row
 * @returns {string|null} canonical unified type name
 */
function resolveUnifiedTypeName (row) {
  const candidates = [row.typeName, row.typeAbbreviation, row.typeId]
  for (const value of candidates) {
    const unifiedTypeName = getUnifiedTypeName(value)
    if (
      unifiedTypeName === TYPE_NAMES.CHALLENGE ||
      unifiedTypeName === TYPE_NAMES.FIRST2FINISH ||
      unifiedTypeName === TYPE_NAMES.TASK ||
      unifiedTypeName === TYPE_NAMES.SRM ||
      unifiedTypeName === TYPE_NAMES.MARATHON_MATCH
    ) {
      return unifiedTypeName
    }
  }
  return null
}

function getRowsForTrack (rowsByTrackId, trackIdsByUnifiedTrack, trackName) {
  const trackIds = trackIdsByUnifiedTrack[trackName] || []
  return _.flatMap(trackIds, trackId => rowsByTrackId[trackId] || [])
}

/**
 * Convert nullable database values into comparable numbers.
 * @param {*} value raw database value
 * @returns {number|null} comparable numeric value or null
 */
function toComparableNumber (value) {
  if (_.isNil(value)) {
    return null
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

/**
 * Load the legacy memberStats parent ids used by the legacy child tables.
 * @param {BigInt} userId member user id
 * @returns {Array<*>} parent row ids in the native database type
 */
async function getLegacyMemberStatsIds (userId) {
  const legacyParentRows = await prisma.$queryRaw`
    SELECT id
    FROM "members"."memberStats"
    WHERE "userId" = ${userId}
  `

  return _.map(legacyParentRows, row => row.id)
}

/**
 * Build mismatch details for a legacy-to-unified field comparison.
 * @param {Object|null} legacyRow legacy source row
 * @param {Object|null} unifiedRow unified target row
 * @param {Array<Object>} fields field mapping configuration
 * @param {string} track track label for the mismatch
 * @param {string} type type label for the mismatch
 * @returns {Object|null} mismatch payload when any compared field differs
 */
function compareFieldSet (legacyRow, unifiedRow, fields, track, type) {
  if (!legacyRow && !unifiedRow) {
    return null
  }

  const diff = {}
  fields.forEach(({ legacyField, unifiedField }) => {
    const legacyValue = toComparableNumber(legacyRow && legacyRow[legacyField])
    const unifiedValue = toComparableNumber(unifiedRow && unifiedRow[unifiedField])
    if (legacyValue !== unifiedValue) {
      diff[unifiedField] = { old: legacyValue, new: unifiedValue }
    }
  })

  return _.isEmpty(diff) ? null : { track, type, diff }
}

/**
 * Merge legacy rating/rank rows using the same deterministic overwrite order as the migration backfill.
 * Rows must already be ordered with the same precedence as the migration queries.
 * @param {Array<Object>} rows legacy source rows
 * @param {Array<string>} fields legacy fields to merge
 * @returns {Object|null} merged legacy row for parity comparison
 */
function mergeLegacyFieldRows (rows, fields) {
  if (!rows || rows.length === 0) {
    return null
  }

  const merged = {}
  rows.forEach((row) => {
    fields.forEach((field) => {
      if (!_.isNil(row[field])) {
        merged[field] = row[field]
      }
    })
  })

  return _.isEmpty(merged) ? null : merged
}

function compareTrack (label, oldSummary, newSummary) {
  if (!oldSummary) {
    return null
  }

  const diff = {}
  if (toNumber(oldSummary.challenges) !== toNumber(newSummary.challenges)) {
    diff.challenges = { old: toNumber(oldSummary.challenges), new: toNumber(newSummary.challenges) }
  }
  if (toNumber(oldSummary.wins) !== toNumber(newSummary.wins)) {
    diff.wins = { old: toNumber(oldSummary.wins), new: toNumber(newSummary.wins) }
  }
  if (toIso(oldSummary.mostRecentSubmission) !== toIso(newSummary.mostRecentSubmission)) {
    diff.mostRecentSubmission = { old: toIso(oldSummary.mostRecentSubmission), new: toIso(newSummary.mostRecentSubmission) }
  }
  if (toIso(oldSummary.mostRecentEventDate) !== toIso(newSummary.mostRecentEventDate)) {
    diff.mostRecentEventDate = { old: toIso(oldSummary.mostRecentEventDate), new: toIso(newSummary.mostRecentEventDate) }
  }

  return _.isEmpty(diff) ? null : { track: label, diff }
}

/**
 * Add a violation type marker to a collection of verifier findings.
 * Warnings are preserved so operators can inspect them without failing parity.
 * @param {Array<Object>} mismatches mismatches produced by a verifier step
 * @param {string} violationType summary bucket label
 * @returns {Array<Object>} tagged findings
 */
function tagMismatches (mismatches, violationType) {
  return _.map(mismatches || [], mismatch => ({ ...mismatch, violationType }))
}

async function compareUser (userId, availableTables) {
  const newRows = await prisma.$queryRaw`
    SELECT
      ms."trackId",
      ms."typeId",
      ms."challenges",
      ms."wins",
      ms."mostRecentSubmission",
      ms."mostRecentEventDate",
      ct."name" AS "trackName",
      ct."abbreviation" AS "trackAbbreviation"
    FROM "members"."memberStats" ms
    LEFT JOIN "challenges"."ChallengeTrack" ct
      ON ct."id" = ms."trackId"
    WHERE ms."userId" = ${userId}
  `

  const rowsByTrackId = _.groupBy(newRows, row => String(row.trackId || ''))
  const trackIdsByUnifiedTrack = _.transform(Object.keys(rowsByTrackId), (acc, trackId) => {
    const unifiedTrackName = resolveUnifiedTrackName(rowsByTrackId[trackId][0])
    if (!unifiedTrackName) {
      return
    }
    if (!acc[unifiedTrackName]) {
      acc[unifiedTrackName] = []
    }
    acc[unifiedTrackName].push(trackId)
  }, {})

  const newDevelop = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DEVELOP))
  const newDesign = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DESIGN))
  const newDataScience = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DATA_SCIENCE))

  const legacyIds = await getLegacyMemberStatsIds(userId)

  const mismatches = []
  if (legacyIds.length === 0) {
    return mismatches
  }

  if (availableTables.memberDevelopStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDevelopStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DEVELOP', summarizeRows(oldRows), newDevelop)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  if (availableTables.memberDesignStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDesignStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DESIGN', summarizeRows(oldRows), newDesign)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  if (availableTables.memberDataScienceStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDataScienceStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DATA_SCIENCE', summarizeRows(oldRows), newDataScience)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  return mismatches
}

/**
 * Compare legacy rating/rank fields against the unified memberStats rows for one user.
 * This is used by the migration verifier after the existing counter/date checks.
 * @param {BigInt} userId member user id
 * @param {Object} availableTables legacy table availability map
 * @returns {Array<Object>} rating or rank mismatches for the sampled user
 */
async function compareRatings (userId, availableTables) {
  const legacyIds = await getLegacyMemberStatsIds(userId)
  if (legacyIds.length === 0) {
    return []
  }

  const idsSql = legacyIds.map(id => String(id)).join(', ')
  const mismatches = []
  const unifiedRows = await prisma.$queryRaw`
    SELECT
      ms."trackId",
      ms."typeId",
      ms."rating",
      ms."avgRank",
      ms."avgNumSubmissions",
      ms."bestRank",
      ms."globalRank",
      ms."countryRank",
      ms."schoolRank",
      ms."volatility",
      ms."maxRating",
      ms."minRating",
      ms."topFiveFinishes",
      ms."topTenFinishes",
      ct."name" AS "trackName",
      ct."abbreviation" AS "trackAbbreviation",
      cty."name" AS "typeName",
      cty."abbreviation" AS "typeAbbreviation"
    FROM "members"."memberStats" ms
    LEFT JOIN "challenges"."ChallengeTrack" ct
      ON ct."id" = ms."trackId"
    LEFT JOIN "challenges"."ChallengeType" cty
      ON cty."id" = ms."typeId"
    WHERE ms."userId" = ${userId}
  `

  if (availableTables.memberDevelopStats && availableTables.memberDevelopStatsItem) {
    const legacyDevelopRows = await prisma.$queryRawUnsafe(
      `
      SELECT MAX(mdsi."rating")::int AS "rating"
      FROM "members"."memberDevelopStats" mds
      INNER JOIN "members"."memberDevelopStatsItem" mdsi
        ON mdsi."developStatsId" = mds."id"
      WHERE mds."memberStatsId" IN (${idsSql})
      `
    )
    const unifiedDevelopRating = _.max(
      _.chain(unifiedRows)
        .filter(row => resolveUnifiedTrackName(row) === TRACK_NAMES.DEVELOP)
        .map(row => toComparableNumber(row.rating))
        .filter(value => !_.isNil(value))
        .value()
    )
    const mismatch = compareFieldSet(
      legacyDevelopRows[0] || null,
      { rating: unifiedDevelopRating },
      [{ legacyField: 'rating', unifiedField: 'rating' }],
      TRACK_NAMES.DEVELOP,
      'MAX'
    )
    if (mismatch) {
      mismatches.push(mismatch)
    }
  }

  if (availableTables.memberDataScienceStats && availableTables.memberSrmStats) {
    const srmFieldMappings = [
      { legacyField: 'rating', unifiedField: 'rating' },
      { legacyField: 'rank', unifiedField: 'globalRank' },
      { legacyField: 'countryRank', unifiedField: 'countryRank' },
      { legacyField: 'schoolRank', unifiedField: 'schoolRank' },
      { legacyField: 'volatility', unifiedField: 'volatility' },
      { legacyField: 'maximumRating', unifiedField: 'maxRating' },
      { legacyField: 'minimumRating', unifiedField: 'minRating' }
    ]
    const legacySrmRows = await prisma.$queryRawUnsafe(
      `
      SELECT
        srm."rating" AS "rating",
        srm."rank" AS "rank",
        srm."countryRank" AS "countryRank",
        srm."schoolRank" AS "schoolRank",
        srm."volatility" AS "volatility",
        srm."maximumRating" AS "maximumRating",
        srm."minimumRating" AS "minimumRating"
      FROM "members"."memberDataScienceStats" ds
      INNER JOIN "members"."memberSrmStats" srm
        ON srm."dataScienceStatsId" = ds."id"
      WHERE ds."memberStatsId" IN (${idsSql})
      ORDER BY srm."id" DESC
      `
    )
    const legacySrmRow = mergeLegacyFieldRows(
      legacySrmRows,
      srmFieldMappings.map(({ legacyField }) => legacyField)
    )
    const unifiedSrmRow = _.find(unifiedRows, row =>
      resolveUnifiedTrackName(row) === TRACK_NAMES.DATA_SCIENCE &&
      resolveUnifiedTypeName(row) === TYPE_NAMES.SRM
    )
    const mismatch = compareFieldSet(
      legacySrmRow,
      unifiedSrmRow || null,
      srmFieldMappings,
      TRACK_NAMES.DATA_SCIENCE,
      TYPE_NAMES.SRM
    )
    if (mismatch) {
      mismatches.push(mismatch)
    }
  }

  if (availableTables.memberDataScienceStats && availableTables.memberMarathonStats) {
    const marathonFieldMappings = [
      { legacyField: 'rating', unifiedField: 'rating' },
      { legacyField: 'rank', unifiedField: 'globalRank' },
      { legacyField: 'countryRank', unifiedField: 'countryRank' },
      { legacyField: 'schoolRank', unifiedField: 'schoolRank' },
      { legacyField: 'volatility', unifiedField: 'volatility' },
      { legacyField: 'maximumRating', unifiedField: 'maxRating' },
      { legacyField: 'minimumRating', unifiedField: 'minRating' },
      { legacyField: 'avgRank', unifiedField: 'avgRank' },
      { legacyField: 'avgNumSubmissions', unifiedField: 'avgNumSubmissions' },
      { legacyField: 'bestRank', unifiedField: 'bestRank' },
      { legacyField: 'topFiveFinishes', unifiedField: 'topFiveFinishes' },
      { legacyField: 'topTenFinishes', unifiedField: 'topTenFinishes' }
    ]
    const legacyMarathonRows = await prisma.$queryRawUnsafe(
      `
      SELECT
        marathon."rating" AS "rating",
        marathon."rank" AS "rank",
        marathon."countryRank" AS "countryRank",
        marathon."schoolRank" AS "schoolRank",
        marathon."volatility" AS "volatility",
        marathon."maximumRating" AS "maximumRating",
        marathon."minimumRating" AS "minimumRating",
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
    const legacyMarathonRow = mergeLegacyFieldRows(
      legacyMarathonRows,
      marathonFieldMappings.map(({ legacyField }) => legacyField)
    )
    const unifiedMarathonRow = _.find(unifiedRows, row =>
      resolveUnifiedTrackName(row) === TRACK_NAMES.DATA_SCIENCE &&
      resolveUnifiedTypeName(row) === TYPE_NAMES.MARATHON_MATCH
    )
    const mismatch = compareFieldSet(
      legacyMarathonRow,
      unifiedMarathonRow || null,
      marathonFieldMappings,
      TRACK_NAMES.DATA_SCIENCE,
      TYPE_NAMES.MARATHON_MATCH
    )
    if (mismatch) {
      mismatches.push(mismatch)
    }
  }

  return mismatches
}

/**
 * Compare legacy history row coverage against the unified memberStatsHistory rows.
 * Unified counts are allowed to be lower only when legacy duplicates collapse by challengeId.
 * @param {BigInt} userId member user id
 * @param {Object} availableTables legacy table availability map
 * @returns {Array<Object>} history parity mismatches for the sampled user
 */
async function compareHistoryCounts (userId, availableTables) {
  if (!availableTables.memberHistoryStats) {
    return []
  }

  const historyParentRows = await prisma.$queryRaw`
    SELECT id
    FROM "members"."memberHistoryStats"
    WHERE "userId" = ${userId}
  `
  if (historyParentRows.length === 0) {
    return []
  }

  const historyIds = _.map(historyParentRows, row => String(row.id))
  const idsSql = historyIds.join(', ')
  const mismatches = []
  const unifiedHistoryRows = await prisma.$queryRaw`
    SELECT
      msh."challengeId",
      msh."trackId",
      msh."typeId",
      ct."name" AS "trackName",
      ct."abbreviation" AS "trackAbbreviation"
    FROM "members"."memberStatsHistory" msh
    LEFT JOIN "challenges"."ChallengeTrack" ct
      ON ct."id" = msh."trackId"
    WHERE msh."userId" = ${userId}
  `

  if (availableTables.memberDevelopHistoryStats) {
    const legacyDevelopCounts = await prisma.$queryRawUnsafe(
      `
      SELECT
        COUNT(*)::int AS "count",
        COUNT(DISTINCT "challengeId")::int AS "distinctChallengeCount"
      FROM "members"."memberDevelopHistoryStats"
      WHERE "historyStatsId" IN (${idsSql})
      `
    )
    const unifiedDevelopCount = _.filter(
      unifiedHistoryRows,
      row => resolveUnifiedTrackName(row) === TRACK_NAMES.DEVELOP
    ).length
    const legacyDistinctCount = toNumber(legacyDevelopCounts[0] && legacyDevelopCounts[0].distinctChallengeCount)
    if (unifiedDevelopCount < legacyDistinctCount) {
      mismatches.push({
        track: TRACK_NAMES.DEVELOP,
        type: 'HISTORY',
        diff: {
          legacyCount: toNumber(legacyDevelopCounts[0] && legacyDevelopCounts[0].count),
          legacyDistinctChallenges: legacyDistinctCount,
          unifiedCount: unifiedDevelopCount
        }
      })
    }
  }

  if (availableTables.memberDataScienceHistoryStats) {
    const legacyScienceCounts = await prisma.$queryRawUnsafe(
      `
      SELECT
        COUNT(*)::int AS "count",
        COUNT(DISTINCT "challengeId")::int AS "distinctChallengeCount"
      FROM "members"."memberDataScienceHistoryStats"
      WHERE "historyStatsId" IN (${idsSql})
      `
    )
    const unifiedScienceCount = _.filter(
      unifiedHistoryRows,
      row => resolveUnifiedTrackName(row) === TRACK_NAMES.DATA_SCIENCE
    ).length
    const legacyDistinctCount = toNumber(legacyScienceCounts[0] && legacyScienceCounts[0].distinctChallengeCount)
    if (unifiedScienceCount < legacyDistinctCount) {
      mismatches.push({
        track: TRACK_NAMES.DATA_SCIENCE,
        type: 'HISTORY',
        diff: {
          legacyCount: toNumber(legacyScienceCounts[0] && legacyScienceCounts[0].count),
          legacyDistinctChallenges: legacyDistinctCount,
          unifiedCount: unifiedScienceCount
        }
      })
    }
  }

  return mismatches
}

/**
 * Verify history ordering and mostRecent parity per unified track/type group for one user.
 * Null newRating values fail parity unless the underlying challenge is explicitly unrated.
 * @param {BigInt} userId member user id
 * @returns {Array<Object>} per-user history ordering findings
 */
async function compareHistoryOrder (userId) {
  const historyRows = await prisma.$queryRaw`
    SELECT
      "challengeId",
      "trackId",
      "typeId",
      "eventDate" AS "ratingDate",
      "newRating",
      "mostRecent"
    FROM "members"."memberStatsHistory"
    WHERE "userId" = ${userId}
    ORDER BY "trackId" ASC, "typeId" ASC, "eventDate" ASC, "id" ASC
  `

  const violations = []
  const ratingClassificationByChallengeId = await fetchChallengeRatingClassificationMap(
    _.map(historyRows, 'challengeId')
  )
  const historyByTrackType = _.groupBy(historyRows, row => `${row.trackId}::${row.typeId}`)
  _.forEach(historyByTrackType, (rows, key) => {
    const [trackId, typeId] = key.split('::')
    const mostRecentRows = _.filter(rows, row => row.mostRecent)
    const latestRatingDate = maxIso(_.map(rows, 'ratingDate'))

    if (mostRecentRows.length !== 1) {
      violations.push({
        trackId,
        typeId,
        violation: `expected exactly one mostRecent row, found ${mostRecentRows.length}`
      })
    } else if (toIso(mostRecentRows[0].ratingDate) !== latestRatingDate) {
      violations.push({
        trackId,
        typeId,
        violation: 'mostRecent row does not have the latest ratingDate'
      })
    }

    _.forEach(rows, (row) => {
      if (_.isNil(row.newRating)) {
        const ratingClassification = getHistoryRowRatingClassification(row, ratingClassificationByChallengeId)
        const isExplicitlyUnrated = ratingClassification.ratingStatus === 'unrated'
        violations.push({
          trackId,
          typeId,
          challengeId: _.isNil(row.challengeId) ? null : String(row.challengeId),
          ratingStatus: ratingClassification.ratingStatus,
          ratingStatusSource: ratingClassification.ratingStatusSource,
          ...(isExplicitlyUnrated ? { severity: 'warning' } : {}),
          violation: isExplicitlyUnrated
            ? `challengeId=${row.challengeId} has null newRating for an explicitly unrated challenge`
            : `challengeId=${row.challengeId} has null newRating for a rated challenge`
        })
      }
    })
  })

  return violations
}

/**
 * Ensure that every rated unified memberStats row also has a global rank.
 * @param {BigInt} userId member user id
 * @returns {Array<Object>} rank completeness findings for the sampled user
 */
async function compareRankCompleteness (userId) {
  const statRows = await prisma.$queryRaw`
    SELECT
      "trackId",
      "typeId",
      "rating",
      "globalRank",
      "countryRank",
      "schoolRank",
      "volatility"
    FROM "members"."memberStats"
    WHERE "userId" = ${userId}
  `

  return _.chain(statRows)
    .filter(row => !_.isNil(row.rating) && _.isNil(row.globalRank))
    .map(row => ({
      trackId: row.trackId,
      typeId: row.typeId,
      field: 'globalRank',
      violation: 'rating present but rank null'
    }))
    .value()
}

/**
 * Count track/type groups in memberStatsHistory that do not have exactly one mostRecent row.
 * @returns {number} number of violating groups across the unified history table
 */
async function getMostRecentViolationCount () {
  const rows = await prisma.$queryRaw`
    SELECT COUNT(*)::int AS "violations"
    FROM (
      SELECT "userId", "trackId", "typeId"
      FROM "members"."memberStatsHistory"
      GROUP BY "userId", "trackId", "typeId"
      HAVING SUM(CASE WHEN "mostRecent" THEN 1 ELSE 0 END) != 1
    ) violations
  `

  return toNumber(rows[0] && rows[0].violations)
}

async function main () {
  const args = parseArgs(process.argv.slice(2))

  console.log('Checking table availability...')
  const availableTables = {}
  for (const tableName of LEGACY_TABLES) {
    // eslint-disable-next-line no-await-in-loop
    availableTables[tableName] = await tableExists(tableName)
  }
  console.log('Legacy tables found:', availableTables)

  console.log('\nRunning count checks...')
  const unifiedCount = await countTable('memberStats')
  const unifiedHistoryCount = await countTable('memberStatsHistory')
  const developCount = await countTable('memberDevelopStats')
  const designCount = await countTable('memberDesignStats')
  const dataScienceCount = await countTable('memberDataScienceStats')

  const legacyStatsTotal = _.sumBy([developCount, designCount, dataScienceCount], value => toNumber(value))

  console.log('memberStats (unified):', unifiedCount)
  console.log('memberDevelopStats (legacy):', developCount)
  console.log('memberDesignStats (legacy):', designCount)
  console.log('memberDataScienceStats (legacy):', dataScienceCount)
  console.log('legacy stats total:', legacyStatsTotal)
  console.log('memberStatsHistory (unified):', unifiedHistoryCount)

  const legacyHistoryCount = _.sumBy([
    await countTable('memberDevelopHistoryStats'),
    await countTable('memberDataScienceHistoryStats')
  ], value => toNumber(value))
  console.log('legacy history rows (develop + data science):', legacyHistoryCount)

  const rankRows = await prisma.$queryRaw`
    SELECT COUNT(*)::int AS "count"
    FROM "members"."memberStats"
    WHERE "rating" IS NOT NULL
      OR "avgRank" IS NOT NULL
      OR "globalRank" IS NOT NULL
      OR "countryRank" IS NOT NULL
      OR "schoolRank" IS NOT NULL
  `
  console.log('non-null rating/rank rows in unified memberStats:', toNumber(rankRows[0] && rankRows[0].count))

  console.log('\nRunning sampled user comparisons...')
  const sampledUsers = await prisma.$queryRaw`
    SELECT DISTINCT "userId"
    FROM "members"."memberStats"
    ORDER BY random()
    LIMIT ${args.samples}
  `

  let mismatchCount = 0
  let warningCount = 0
  const violationBreakdown = {
    'track-summary': 0,
    'rating-mismatch': 0,
    'history-count': 0,
    'history-order': 0,
    'rank-completeness': 0
  }
  for (const row of sampledUsers) {
    // eslint-disable-next-line no-await-in-loop
    const mismatches = tagMismatches(await compareUser(row.userId, availableTables), 'track-summary')
    // eslint-disable-next-line no-await-in-loop
    const ratingMismatches = tagMismatches(await compareRatings(row.userId, availableTables), 'rating-mismatch')
    // eslint-disable-next-line no-await-in-loop
    const historyMismatches = tagMismatches(await compareHistoryCounts(row.userId, availableTables), 'history-count')
    // eslint-disable-next-line no-await-in-loop
    const historyOrderMismatches = tagMismatches(await compareHistoryOrder(row.userId), 'history-order')
    // eslint-disable-next-line no-await-in-loop
    const rankCompletenessMismatches = tagMismatches(await compareRankCompleteness(row.userId), 'rank-completeness')
    const allMismatches = mismatches
      .concat(ratingMismatches, historyMismatches, historyOrderMismatches, rankCompletenessMismatches)

    _.forEach(allMismatches, (mismatch) => {
      if (mismatch.severity === 'warning') {
        warningCount += 1
      } else {
        mismatchCount += 1
        if (!_.isNil(violationBreakdown[mismatch.violationType])) {
          violationBreakdown[mismatch.violationType] += 1
        }
      }
    })

    if (allMismatches.length > 0) {
      console.log(`userId=${row.userId} mismatches:`, JSON.stringify(allMismatches))
    }
  }

  console.log('\nSampled violation breakdown:')
  console.log('history-order:', violationBreakdown['history-order'])
  console.log('rank-completeness:', violationBreakdown['rank-completeness'])
  console.log('rating-mismatch:', violationBreakdown['rating-mismatch'])
  console.log('history-count:', violationBreakdown['history-count'])
  console.log('track-summary:', violationBreakdown['track-summary'])
  if (warningCount > 0) {
    console.log('warnings:', warningCount)
  }

  console.log('\nRunning mostRecent sanity check...')
  const mostRecentViolationCount = await getMostRecentViolationCount()
  console.log('memberStatsHistory mostRecent violation groups:', mostRecentViolationCount)

  if (mismatchCount === 0) {
    console.log(`No sampled mismatches detected across ${sampledUsers.length} users.`)
  } else {
    console.log(`Detected ${mismatchCount} sampled mismatches across ${sampledUsers.length} users.`)
  }
}

main()
  .catch(err => {
    console.error('Verification failed:', err)
    process.exitCode = 1
  })
  .finally(async () => {
    await prisma.$disconnect()
  })

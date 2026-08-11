#!/usr/bin/env node
'use strict'

/**
 * Remove migrated numeric Marathon Match history rows that duplicate complete
 * canonical UUID history rows. The cleanup is intended for environments where
 * the members and challenges schemas are available in the same Postgres
 * database, because it verifies duplicates against official Challenge API
 * Marathon Match rows before deleting any history.
 *
 * Usage examples:
 * - Preview duplicate rows for every member:
 *   pnpm dedupe-marathon-match-history --dry-run
 * - Preview duplicate rows for one member:
 *   pnpm dedupe-marathon-match-history --dry-run --user-id 40562752
 * - Delete duplicate migrated rows after reviewing the dry-run output:
 *   pnpm dedupe-marathon-match-history --apply --user-id 40562752
 */

require('dotenv').config()

const { Prisma, getMembersClient } = require('../common/prisma')

const CLEANUP_ACTOR = 'dedupe-mm-history'
const DEFAULT_SAMPLE_LIMIT = 20

/**
 * Parse a positive integer CLI option.
 * @param {string} name option name used in error messages
 * @param {*} value option value
 * @returns {number} parsed integer
 * @throws {Error} when the value is missing or not a positive integer
 */
function parsePositiveIntegerOption (name, value) {
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`${name} requires a positive integer value`)
  }

  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return parsed
}

/**
 * Convert a user or history identifier to BigInt for raw Prisma parameters.
 * @param {*} value numeric identifier
 * @returns {BigInt} parsed identifier
 * @throws {Error} when the value is not an integer identifier
 */
function toBigIntId (value) {
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error('Expected a numeric identifier')
  }

  if (!/^\d+$/.test(String(value).trim())) {
    throw new Error(`Expected a numeric identifier, got ${value}`)
  }

  return global.BigInt(String(value).trim())
}

/**
 * Parse dedupeMarathonMatchHistory command-line arguments.
 * @param {Array<string>} argv command-line arguments without node/script names
 * @returns {Object} parsed options
 * @throws {Error} when an option is unknown or invalid
 */
function parseArgs (argv) {
  const options = {
    dryRun: true,
    userId: null,
    limit: null,
    help: false
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]

    if (arg === '--dry-run') {
      options.dryRun = true
    } else if (arg === '--') {
      continue
    } else if (arg === '--apply') {
      options.dryRun = false
    } else if (arg === '--user-id') {
      options.userId = String(parsePositiveIntegerOption('--user-id', argv[index + 1]))
      index += 1
    } else if (arg.startsWith('--user-id=')) {
      options.userId = String(parsePositiveIntegerOption('--user-id', arg.slice('--user-id='.length)))
    } else if (arg === '--limit') {
      options.limit = parsePositiveIntegerOption('--limit', argv[index + 1])
      index += 1
    } else if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveIntegerOption('--limit', arg.slice('--limit='.length))
    } else if (arg === '--help' || arg === '-h') {
      options.help = true
    } else {
      throw new Error(`Unknown option ${arg}`)
    }
  }

  return options
}

/**
 * Print command-line usage for the cleanup script.
 * @returns {void}
 */
function printUsage () {
  console.log(`
Usage:
  pnpm dedupe-marathon-match-history [options]

Options:
  --dry-run              Preview duplicate rows without deleting anything (default)
  --apply                Delete duplicate migrated numeric history rows
  --user-id <id>         Restrict cleanup to one member userId
  --limit <count>        Limit rows returned or deleted in this run
  --help                 Show this message

Examples:
  pnpm dedupe-marathon-match-history --dry-run --user-id 40562752
  pnpm dedupe-marathon-match-history --apply --user-id 40562752
`)
}

/**
 * Connect a Prisma-style client when the client exposes $connect.
 * @param {Object} client Prisma client or test double
 * @returns {Promise<void>} resolves when connected
 */
async function connectClient (client) {
  if (client && typeof client.$connect === 'function') {
    await client.$connect()
  }
}

/**
 * Disconnect a Prisma-style client when the client exposes $disconnect.
 * @param {Object} client Prisma client or test double
 * @returns {Promise<void>} resolves when disconnected
 */
async function disconnectClient (client) {
  if (client && typeof client.$disconnect === 'function') {
    await client.$disconnect()
  }
}

/**
 * Find migrated numeric Marathon Match history rows that have an authoritative
 * canonical UUID replacement for the same user and MM round number.
 * @param {Object} membersClient Prisma members client connected to the unified database
 * @param {Object} options parsed script options
 * @param {string|null} options.userId optional member userId filter
 * @param {number|null} options.limit optional maximum duplicate rows to return
 * @returns {Promise<Array<Object>>} duplicate row metadata
 */
async function findDuplicateHistoryRows (membersClient, options: any = {}) {
  const userFilter = options.userId
    ? Prisma.sql`AND h."userId" = ${toBigIntId(options.userId)}`
    : Prisma.empty
  const limitClause = options.limit
    ? Prisma.sql`LIMIT ${options.limit}`
    : Prisma.empty

  return membersClient.$queryRaw`
    WITH "nativeDimension" AS (
      SELECT
        (
          SELECT ct.id
          FROM "challenges"."ChallengeTrack" ct
          WHERE UPPER(ct.name) = 'DATA SCIENCE'
             OR UPPER(ct.abbreviation) = 'DS'
          LIMIT 1
        ) AS "trackId",
        (
          SELECT ctype.id
          FROM "challenges"."ChallengeType" ctype
          WHERE UPPER(ctype.name) IN ('MARATHON MATCH', 'MARATHON_MATCH')
             OR UPPER(ctype.abbreviation) = 'MM'
          LIMIT 1
        ) AS "typeId"
    ),
    "legacyRows" AS (
      SELECT DISTINCT ON (h.id)
        h.id AS "legacyHistoryId",
        h."userId",
        h."challengeId" AS "legacyChallengeId",
        h."eventDate" AS "legacyEventDate",
        h."newRating" AS "legacyNewRating",
        h.placement AS "legacyPlacement",
        dsh."challengeName" AS "legacyChallengeName",
        substring(
          dsh."challengeName"
          from '(?i)(?:MM|Marathon Match)[[:space:]]*#?[[:space:]]*([0-9]+)'
        ) AS "matchNumber"
      FROM "members"."memberStatsHistory" h
      JOIN "members"."memberHistoryStats" mhs
        ON mhs."userId" = h."userId"
      JOIN "members"."memberDataScienceHistoryStats" dsh
        ON dsh."historyStatsId" = mhs.id
       AND dsh."challengeId"::text = h."challengeId"
       AND dsh."subTrack" = 'MARATHON_MATCH'
      CROSS JOIN "nativeDimension" d
      WHERE h."trackId" IN (d."trackId", 'DATA_SCIENCE')
        AND h."typeId" IN (d."typeId", 'MARATHON_MATCH')
        AND h."challengeId" ~ '^[0-9]+$'
        ${userFilter}
      ORDER BY h.id, dsh."historyStatsId"
    ),
    "canonicalRows" AS (
      SELECT
        h.id AS "canonicalHistoryId",
        h."userId",
        h."challengeId" AS "canonicalChallengeId",
        h."eventDate" AS "canonicalEventDate",
        h."newRating" AS "canonicalNewRating",
        h.placement AS "canonicalPlacement",
        c.name AS "canonicalChallengeName",
        substring(
          c.name
          from '(?i)(?:MM|Marathon Match)[[:space:]]*#?[[:space:]]*([0-9]+)'
        ) AS "matchNumber"
      FROM "members"."memberStatsHistory" h
      JOIN "challenges"."Challenge" c
        ON c.id = h."challengeId"
      CROSS JOIN "nativeDimension" d
      WHERE h."trackId" IN (d."trackId", 'DATA_SCIENCE')
        AND h."typeId" IN (d."typeId", 'MARATHON_MATCH')
        AND c."typeId" = d."typeId"
        AND h."challengeId" !~ '^[0-9]+$'
        AND h."newRating" IS NOT NULL
        AND h.placement IS NOT NULL
        AND h.placement > 0
        AND (
          h."createdBy" IN ('rerate-mm-stats', 'rerate-member-stats')
          OR h."updatedBy" IN ('rerate-mm-stats', 'rerate-member-stats')
        )
        ${userFilter}
    ),
    "canonicalHistory" AS (
      SELECT DISTINCT ON ("userId", "matchNumber")
        *
      FROM "canonicalRows"
      WHERE "matchNumber" IS NOT NULL
      ORDER BY "userId", "matchNumber", "canonicalEventDate" DESC NULLS LAST, "canonicalHistoryId" DESC
    )
    SELECT
      legacy."legacyHistoryId",
      legacy."userId",
      legacy."matchNumber",
      legacy."legacyChallengeId",
      legacy."legacyChallengeName",
      legacy."legacyEventDate",
      legacy."legacyNewRating",
      legacy."legacyPlacement",
      canonical."canonicalHistoryId",
      canonical."canonicalChallengeId",
      canonical."canonicalChallengeName",
      canonical."canonicalEventDate",
      canonical."canonicalNewRating",
      canonical."canonicalPlacement"
    FROM "legacyRows" legacy
    JOIN "canonicalHistory" canonical
      ON canonical."userId" = legacy."userId"
     AND canonical."matchNumber" = legacy."matchNumber"
    WHERE legacy."matchNumber" IS NOT NULL
    ORDER BY legacy."userId", legacy."matchNumber"::integer, legacy."legacyEventDate", legacy."legacyHistoryId"
    ${limitClause}
  `
}

/**
 * Delete duplicate migrated history rows and repair mostRecent flags for
 * affected native Marathon Match histories.
 * @param {Object} membersClient Prisma members client connected to the unified database
 * @param {Array<Object>} duplicateRows duplicate rows returned by findDuplicateHistoryRows
 * @returns {Promise<Object>} deletion summary
 */
async function deleteDuplicateHistoryRows (membersClient, duplicateRows) {
  const legacyHistoryIds = Array.from(new Set(
    duplicateRows.map(row => String(row.legacyHistoryId))
  )).map(toBigIntId)
  const userIds = Array.from(new Set(
    duplicateRows.map(row => String(row.userId))
  )).map(toBigIntId)

  if (legacyHistoryIds.length === 0) {
    return {
      deletedRows: 0,
      usersRefreshed: 0
    }
  }

  return membersClient.$transaction(async (tx) => {
    const deletedRows = await tx.$executeRaw`
      DELETE FROM "members"."memberStatsHistory"
      WHERE id IN (${Prisma.join(legacyHistoryIds)})
    `

    await tx.$executeRaw`
      WITH "nativeDimension" AS (
        SELECT
          (
            SELECT ct.id
            FROM "challenges"."ChallengeTrack" ct
            WHERE UPPER(ct.name) = 'DATA SCIENCE'
               OR UPPER(ct.abbreviation) = 'DS'
            LIMIT 1
          ) AS "trackId",
          (
            SELECT ctype.id
            FROM "challenges"."ChallengeType" ctype
            WHERE UPPER(ctype.name) IN ('MARATHON MATCH', 'MARATHON_MATCH')
               OR UPPER(ctype.abbreviation) = 'MM'
            LIMIT 1
          ) AS "typeId"
      ),
      "rankedHistory" AS (
        SELECT
          h.id,
          ROW_NUMBER() OVER (
            PARTITION BY h."userId"
            ORDER BY h."eventDate" DESC NULLS LAST, h.id DESC
          ) AS "historyRank"
        FROM "members"."memberStatsHistory" h
        CROSS JOIN "nativeDimension" d
        WHERE h."userId" IN (${Prisma.join(userIds)})
          AND h."trackId" IN (d."trackId", 'DATA_SCIENCE')
          AND h."typeId" IN (d."typeId", 'MARATHON_MATCH')
      )
      UPDATE "members"."memberStatsHistory" h
      SET
        "mostRecent" = ranked."historyRank" = 1,
        "updatedAt" = NOW(),
        "updatedBy" = ${CLEANUP_ACTOR}
      FROM "rankedHistory" ranked
      WHERE h.id = ranked.id
        AND h."mostRecent" IS DISTINCT FROM (ranked."historyRank" = 1)
    `

    return {
      deletedRows,
      usersRefreshed: userIds.length
    }
  })
}

/**
 * Summarize duplicate rows for console output.
 * @param {Array<Object>} rows duplicate rows
 * @returns {Array<Object>} formatted sample rows
 */
function formatDuplicateSample (rows) {
  return rows.slice(0, DEFAULT_SAMPLE_LIMIT).map((row) => ({
    userId: String(row.userId),
    matchNumber: row.matchNumber,
    legacyHistoryId: String(row.legacyHistoryId),
    legacyChallengeId: String(row.legacyChallengeId),
    legacyChallengeName: row.legacyChallengeName,
    legacyPlacement: row.legacyPlacement,
    legacyNewRating: row.legacyNewRating,
    canonicalHistoryId: String(row.canonicalHistoryId),
    canonicalChallengeId: String(row.canonicalChallengeId),
    canonicalChallengeName: row.canonicalChallengeName,
    canonicalPlacement: row.canonicalPlacement,
    canonicalNewRating: row.canonicalNewRating
  }))
}

/**
 * Run the duplicate migrated Marathon Match history cleanup.
 * @param {Object} options parsed script options
 * @param {Object} dependencies optional clients for tests
 * @returns {Promise<Object>} cleanup summary
 */
async function run (options, dependencies: any = {}) {
  const membersClient = dependencies.membersClient || getMembersClient()
  const shouldDisconnect = dependencies.disconnect !== false

  try {
    await connectClient(membersClient)

    const duplicateRows = await findDuplicateHistoryRows(membersClient, options)
    console.log(`Found ${duplicateRows.length} duplicate migrated Marathon Match history row(s).`)
    const sample = formatDuplicateSample(duplicateRows)
    if (sample.length > 0) {
      console.table(sample)
    }

    if (options.dryRun) {
      console.log('Dry run complete. Re-run with --apply to delete these migrated duplicate rows.')
      return {
        dryRun: true,
        duplicateRows: duplicateRows.length,
        deletedRows: 0,
        usersRefreshed: 0
      }
    }

    const deletion = await deleteDuplicateHistoryRows(membersClient, duplicateRows)
    console.log(`Deleted ${deletion.deletedRows} migrated duplicate history row(s).`)

    return {
      dryRun: false,
      duplicateRows: duplicateRows.length,
      deletedRows: deletion.deletedRows,
      usersRefreshed: deletion.usersRefreshed
    }
  } finally {
    if (shouldDisconnect) {
      await disconnectClient(membersClient)
    }
  }
}

if (require.main === module) {
  try {
    const options = parseArgs(process.argv.slice(2))
    if (options.help) {
      printUsage()
    } else {
      run(options)
        .then((summary) => {
          console.log(JSON.stringify(summary, null, 2))
        })
        .catch((error) => {
          console.error('Marathon Match history dedupe failed:', error)
          process.exitCode = 1
        })
    }
  } catch (error) {
    console.error('Marathon Match history dedupe failed:', error)
    process.exitCode = 1
  }
}

module.exports = {
  parseArgs,
  printUsage,
  findDuplicateHistoryRows,
  deleteDuplicateHistoryRows,
  formatDuplicateSample,
  run
}

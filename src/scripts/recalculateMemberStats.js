#!/usr/bin/env node
'use strict'

/**
 * Recalculate member statistics from challenge winner data.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL (challenge database)
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
 *
 * Notes:
 * - Rating and rank fields are written as NULL for now.
 * - mostRecentSubmission is derived from ChallengeWinner timestamps.
 * - The script is idempotent and safe to run multiple times.
 * - Writes use upsert on (userId, trackId, typeId).
 */

const fs = require('fs')
const path = require('path')

require('dotenv').config()

const config = require('config')
const { Prisma, getMembersClient, getChallengesClient } = require('../common/prisma')

const DEFAULT_ACTOR = process.env.UPDATED_BY || process.env.CREATED_BY || 'stats-migration'
const CREATED_BY = process.env.CREATED_BY || DEFAULT_ACTOR
const UPDATED_BY = process.env.UPDATED_BY || DEFAULT_ACTOR
const USER_BATCH_SIZE = 100
const TRANSACTION_TIMEOUT_MS = Number.parseInt(
  process.env.MEMBER_STATS_TRANSACTION_TIMEOUT || `${config.MEMBER_SERVICE_PRISMA_TIMEOUT || 60000}`,
  10
)

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

function buildFilterClauses (options, includeUserFilter) {
  const clauses = [
    Prisma.sql`c."trackId" IS NOT NULL`,
    Prisma.sql`c."typeId" IS NOT NULL`
  ]

  if (includeUserFilter !== null && includeUserFilter !== undefined) {
    clauses.push(Prisma.sql`cw."userId" = ${includeUserFilter}`)
  }

  if (options.trackId) {
    clauses.push(Prisma.sql`c."trackId" = ${options.trackId}`)
  }

  if (options.typeId) {
    clauses.push(Prisma.sql`c."typeId" = ${options.typeId}`)
  }

  return clauses
}

async function getUserIds (challengesClient, options) {
  const explicitUserIds = Array.from(new Set(options.userIds))
  let userIds

  if (explicitUserIds.length > 0) {
    userIds = explicitUserIds.map((userId) => normalizeBigInt(userId, 'user id'))
  } else {
    const whereClauses = buildFilterClauses(options, null)

    const rows = await challengesClient.$queryRaw`
      SELECT DISTINCT cw."userId" AS "userId"
      FROM "ChallengeWinner" cw
      INNER JOIN "Challenge" c ON c.id = cw."challengeId"
      WHERE ${Prisma.join(whereClauses, Prisma.sql` AND `)}
      ORDER BY cw."userId" ASC
    `

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

async function aggregateStatsForUser (challengesClient, userId, options) {
  const whereClauses = buildFilterClauses(options, userId)

  const rows = await challengesClient.$queryRaw`
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
    WHERE ${Prisma.join(whereClauses, Prisma.sql` AND `)}
    GROUP BY cw."userId", c."trackId", c."typeId"
    ORDER BY c."trackId" ASC, c."typeId" ASC
  `

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
    await membersClient.$transaction(async (tx) => {
      for (const record of statsRecords) {
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

        await tx.memberStats.upsert({
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
    }, {
      timeout: TRANSACTION_TIMEOUT_MS
    })

    return statsRecords.length
  } catch (error) {
    logError(`Transaction failed. Rolled back ${statsRecords.length} pending records.`, error)
    throw error
  }
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

  const membersClient = getMembersClient()
  const challengesClient = getChallengesClient()

  let csvWriter
  let processedUsers = 0
  let writtenStats = 0

  try {
    logInfo(`Starting recalculateMemberStats in ${options.csvOnly ? 'CSV-only' : 'write'} mode`)

    await membersClient.$connect()
    await challengesClient.$connect()

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

      for (const userId of batchUserIds) {
        const stats = await aggregateStatsForUser(challengesClient, userId, options)
        processedUsers += 1

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
    }

    if (csvWriter) {
      await csvWriter.end()
    }

    logInfo(`Completed processing ${processedUsers} users, created/updated ${writtenStats} stat records`)
  } finally {
    await Promise.allSettled([
      membersClient.$disconnect(),
      challengesClient.$disconnect()
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
  aggregateStatsForUser,
  writeStatsToDatabase,
  toCsvValue,
  toIsoString,
  buildCsvWriter
}

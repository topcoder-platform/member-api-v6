#!/usr/bin/env node
'use strict'

/**
 * Bulk re-rate a configured historical rating path.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL or CHALLENGE_DB_URL (challenge database)
 * - REVIEW_DB_URL (review database)
 *
 * Usage examples:
 * - Dry-run discovery:
 *   node src/scripts/rerateRatingPath.js --rating-name AI --dry-run
 * - Re-rate every discovered member:
 *   node src/scripts/rerateRatingPath.js --rating-name AI --concurrency 5
 * - Re-rate a bounded sample:
 *   node src/scripts/rerateRatingPath.js --rating-name AI --limit 100
 * - Re-rate specific users by userId:
 *   node src/scripts/rerateRatingPath.js --rating-name AI --user-id 12345 --user-ids 67890,24680
 *
 * Notes:
 * - Handles are not required. The script discovers participating user IDs from
 *   configured rating path challenges and persists rows for each discovered user.
 * - Each member is replayed from the start of the configured path so complete
 *   path history is written without needing an explicit challengeId.
 * - Marathon Match events are read from reviewSummation only; the
 *   marathon-match-api schema is not required for historical rerates.
 * - The script is idempotent and safe to run multiple times. Writes use the
 *   existing configured rating path upserts in the rating engine.
 */

require('dotenv').config()

const fs = require('fs')
const path = require('path')
const config = require('config')

const reviewDb = require('../common/reviewDb')
const {
  getMembersClient,
  getChallengesClient
} = require('../common/prisma')
const { getConfiguredRatingPath } = require('../ratings/ratingPathConfig')
const {
  fetchRatingPathHistory,
  fetchRatingPathParticipantsForChallenge,
  resolveRatingPathParticipantId,
  rerateMmTrack
} = require('../ratings/mmRatingEngine')

const DEFAULT_CONCURRENCY = 4
const DEFAULT_PROCESSED_USER_IDS_PATH = 'rerateRatingPath.processedUserIds.json'

function logInfo (message) {
  console.log(`[INFO] ${new Date().toISOString()} ${message}`)
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

  const totalSeconds = Math.round(durationMs / 1000)
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  if (minutes === 0) {
    return `${seconds}s`
  }

  return `${minutes}m ${seconds}s`
}

/**
 * Parse a comma-separated or single user ID option value.
 * @param {*} value raw command-line option value
 * @returns {Array<string>} normalized user ID strings
 */
function parseUserIds (value) {
  return String(value || '')
    .split(',')
    .map(userId => userId.trim())
    .filter(Boolean)
}

/**
 * Parse a positive integer command-line option.
 * @param {string} optionName option name used in error messages
 * @param {*} value raw option value
 * @returns {number} parsed positive integer
 * @throws {Error} when the value is missing or not a positive integer
 */
function parsePositiveIntegerOption (optionName, value) {
  if (!value) {
    throw new Error(`${optionName} requires a value`)
  }

  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${optionName} must be a positive integer`)
  }

  return parsed
}

/**
 * Parse rerateRatingPath command-line arguments.
 * @param {Array<string>} argv process argv slice after the script path
 * @returns {Object} normalized script options
 * @throws {Error} when an option is unknown or invalid
 */
function parseArgs (argv) {
  const options = {
    ratingName: null,
    concurrency: DEFAULT_CONCURRENCY,
    limit: null,
    userIds: [],
    dryRun: false,
    processedUserIdsPath: DEFAULT_PROCESSED_USER_IDS_PATH,
    help: false
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]

    if (arg === '--') {
      continue
    }

    if (arg === '--rating-name' || arg === '--ratingName') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error(`${arg} requires a value`)
      }

      options.ratingName = next
      index += 1
      continue
    }

    if (arg === '--concurrency') {
      options.concurrency = parsePositiveIntegerOption(arg, argv[index + 1])
      index += 1
      continue
    }

    if (arg === '--limit') {
      options.limit = parsePositiveIntegerOption(arg, argv[index + 1])
      index += 1
      continue
    }

    if (arg === '--user-id') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--user-id requires a value')
      }

      options.userIds.push(...parseUserIds(next))
      index += 1
      continue
    }

    if (arg === '--user-ids') {
      const next = argv[index + 1]
      if (!next) {
        throw new Error('--user-ids requires a value')
      }

      options.userIds.push(...parseUserIds(next))
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

    if (arg === '--dry-run') {
      options.dryRun = true
      continue
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true
      continue
    }

    throw new Error(`Unknown option: ${arg}`)
  }

  options.userIds = Array.from(new Set(options.userIds))
  return options
}

function printUsage () {
  console.log(`
Usage:
  node src/scripts/rerateRatingPath.js --rating-name <name> [options]

Options:
  --rating-name <name>      Configured RATING_PATHS name to re-rate, for example AI.
  --user-id <id>            Re-rate a single user ID discovered in the path (repeatable).
  --user-ids <id,id>        Comma-separated user IDs discovered in the path.
  --limit <n>               Limit the number of discovered users processed.
  --concurrency <n>         Process up to n users in parallel (default: ${DEFAULT_CONCURRENCY}).
  --processed-user-ids-path <path>
                            Write successfully processed user IDs to JSON (default: ${DEFAULT_PROCESSED_USER_IDS_PATH}).
  --dry-run                 Discover matching path challenges and users without writing ratings.
  --help, -h                Show this help.
`)
}

/**
 * Process items with bounded concurrency while preserving result order.
 * @param {Array<*>} items items to process
 * @param {number} concurrency maximum number of in-flight operations
 * @param {Function} iteratee async item processor
 * @returns {Promise<Array<*>>} processor results in input order
 */
async function mapWithConcurrency (items, concurrency, iteratee) {
  const results = new Array(items.length)
  let nextIndex = 0

  async function worker () {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex
      nextIndex += 1
      results[currentIndex] = await iteratee(items[currentIndex], currentIndex)
    }
  }

  const workerCount = Math.min(Math.max(concurrency, 1), items.length)
  await Promise.all(Array.from({ length: workerCount }, worker))
  return results
}

/**
 * Build a checkpoint writer for successfully processed user IDs.
 * @param {string} outputPath relative or absolute output path
 * @returns {Object} writer with appendUserId and end methods
 */
function buildProcessedUserIdsWriter (outputPath) {
  const resolvedPath = path.resolve(process.cwd(), outputPath || DEFAULT_PROCESSED_USER_IDS_PATH)
  const processedUserIds = new Set()
  let writePromise = Promise.resolve()

  async function write () {
    await fs.promises.writeFile(
      resolvedPath,
      `${JSON.stringify(Array.from(processedUserIds), null, 2)}\n`,
      'utf8'
    )
  }

  function enqueueWrite () {
    writePromise = writePromise.then(write)
    return writePromise
  }

  return {
    outputPath: resolvedPath,
    async appendUserId (userId) {
      processedUserIds.add(String(userId))
      await enqueueWrite()
    },
    async end () {
      await enqueueWrite()
    }
  }
}

/**
 * Connect a Prisma-style client when it exposes $connect.
 * @param {Object} client database client
 * @returns {Promise<void>}
 */
async function connectClient (client) {
  if (client && typeof client.$connect === 'function') {
    await client.$connect()
  }
}

/**
 * Disconnect or close a database client when it exposes a close method.
 * @param {Object} client database client
 * @returns {Promise<void>}
 */
async function disconnectClient (client) {
  if (client && typeof client.$disconnect === 'function') {
    await client.$disconnect()
    return
  }

  if (client && typeof client.end === 'function') {
    await client.end()
  }
}

/**
 * Discover distinct members who participated in configured path events.
 * The first matching challenge is retained for logging and dry-run summaries,
 * but the rerate pass replays from the start of the full path for completeness.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Array<Object>} pathHistory ordered rating path event history
 * @param {Object} options discovery controls and optional test doubles
 * @param {Array<string>} options.userIds optional user ID allow-list
 * @param {number|null} options.limit optional maximum discovered users
 * @param {Function} options.fetchParticipants optional participant loader
 * @param {Function} options.resolveParticipantId optional participant id resolver
 * @returns {Promise<Object>} discovery summary containing members and scan counts
 */
async function discoverRatingPathMembers (reviewDbClient, pathHistory, options = {}) {
  const fetchParticipants = options.fetchParticipants || fetchRatingPathParticipantsForChallenge
  const resolveParticipantId = options.resolveParticipantId || resolveRatingPathParticipantId
  const userFilter = new Set((options.userIds || []).map(userId => String(userId)))
  const membersByUserId = new Map()
  let challengesScanned = 0
  let participantRowsScanned = 0

  for (const historyEntry of pathHistory) {
    const { participantRows } = await fetchParticipants(reviewDbClient, historyEntry)
    challengesScanned += 1
    participantRowsScanned += participantRows.length

    for (const row of participantRows) {
      const userId = resolveParticipantId(row, historyEntry.source)
      const userKey = String(userId)

      if (userFilter.size > 0 && !userFilter.has(userKey)) {
        continue
      }

      if (!membersByUserId.has(userKey)) {
        membersByUserId.set(userKey, {
          userId,
          firstChallengeId: historyEntry.challengeId,
          firstEventDate: historyEntry.eventDate
        })
      }
    }

    if (options.limit && membersByUserId.size >= options.limit) {
      break
    }
  }

  const members = Array.from(membersByUserId.values())
  return {
    members: options.limit ? members.slice(0, options.limit) : members,
    challengesScanned,
    participantRowsScanned
  }
}

/**
 * Run the configured rating path bulk rerate workflow.
 * @param {Object} options parsed script options
 * @param {Object} dependencies optional clients and functions for tests
 * @returns {Promise<Object>} bulk rerate summary
 */
async function run (options, dependencies = {}) {
  if (!options.ratingName) {
    throw new Error('--rating-name is required')
  }

  const scriptConfig = dependencies.config || config
  const ratingPath = getConfiguredRatingPath(scriptConfig.RATING_PATHS, options.ratingName)
  if (!ratingPath) {
    throw new Error(`Rating path '${options.ratingName}' is not configured`)
  }

  const membersClient = dependencies.membersClient || getMembersClient()
  const challengeClient = dependencies.challengeClient || getChallengesClient()
  const reviewDbClient = Object.prototype.hasOwnProperty.call(dependencies, 'reviewDbClient')
    ? dependencies.reviewDbClient
    : reviewDb
  const shouldDisconnect = dependencies.disconnect !== false
  const startedAt = startTimer()

  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate rating paths')
  }

  try {
    await connectClient(membersClient)
    await connectClient(challengeClient)

    logInfo(`Loading rating path '${ratingPath.name}' (${ratingPath.trackName} / ${ratingPath.name})`)
    const pathHistory = await (dependencies.fetchRatingPathHistory || fetchRatingPathHistory)(challengeClient, ratingPath)
    logInfo(`Loaded ${pathHistory.length} rated path challenge(s)`)

    if (pathHistory.length === 0) {
      return {
        ratingName: ratingPath.name,
        dryRun: options.dryRun,
        pathChallenges: 0,
        usersDiscovered: 0,
        usersProcessed: 0,
        usersFailed: 0,
        ratingsUpdated: 0,
        durationMs: getElapsedMilliseconds(startedAt)
      }
    }

    const discovery = await discoverRatingPathMembers(reviewDbClient, pathHistory, {
      userIds: options.userIds,
      limit: options.limit,
      fetchParticipants: dependencies.fetchParticipants,
      resolveParticipantId: dependencies.resolveParticipantId
    })
    const members = discovery.members

    logInfo(`Scanned ${discovery.challengesScanned} challenge(s) and ${discovery.participantRowsScanned} participant row(s)`)
    logInfo(`Discovered ${members.length} member(s) to rerate`)

    if (options.dryRun) {
      members.slice(0, 10).forEach((member) => {
        logInfo(`Dry-run member ${String(member.userId)} first appears in challenge ${member.firstChallengeId}`)
      })
      if (members.length > 10) {
        logInfo(`Dry-run output truncated to 10 of ${members.length} member(s)`)
      }

      return {
        ratingName: ratingPath.name,
        dryRun: true,
        pathChallenges: pathHistory.length,
        challengesScanned: discovery.challengesScanned,
        participantRowsScanned: discovery.participantRowsScanned,
        usersDiscovered: members.length,
        usersProcessed: 0,
        usersFailed: 0,
        ratingsUpdated: 0,
        durationMs: getElapsedMilliseconds(startedAt)
      }
    }

    const processedUserIdsWriter = buildProcessedUserIdsWriter(options.processedUserIdsPath)
    await processedUserIdsWriter.end()
    logInfo(`Writing successfully processed user IDs to ${processedUserIdsWriter.outputPath}`)

    const rerateResults = await mapWithConcurrency(members, options.concurrency, async (member, index) => {
      const userStartedAt = startTimer()
      try {
        const result = await (dependencies.rerateMmTrack || rerateMmTrack)(
          membersClient,
          challengeClient,
          null,
          reviewDbClient,
          member.userId,
          null,
          {
            ratingPath
          }
        )

        await processedUserIdsWriter.appendUserId(member.userId)
        logInfo(`Rerated ${index + 1}/${members.length} userId=${String(member.userId)} ratingsUpdated=${result.ratingsUpdated} duration=${formatDuration(getElapsedMilliseconds(userStartedAt))}`)
        return {
          userId: String(member.userId),
          ok: true,
          ...result,
          durationMs: getElapsedMilliseconds(userStartedAt)
        }
      } catch (error) {
        logError(`Failed to rerate userId=${String(member.userId)} after ${formatDuration(getElapsedMilliseconds(userStartedAt))}`, error)
        return {
          userId: String(member.userId),
          ok: false,
          error: error.message,
          durationMs: getElapsedMilliseconds(userStartedAt)
        }
      }
    })

    const usersProcessed = rerateResults.filter(result => result.ok).length
    const usersFailed = rerateResults.length - usersProcessed
    const ratingsUpdated = rerateResults.reduce((sum, result) => sum + (result.ok ? result.ratingsUpdated || 0 : 0), 0)
    const challengesProcessed = rerateResults.reduce((sum, result) => sum + (result.ok ? result.challengesProcessed || 0 : 0), 0)
    const durationMs = getElapsedMilliseconds(startedAt)

    logInfo(`Completed rating path '${ratingPath.name}': usersProcessed=${usersProcessed}, usersFailed=${usersFailed}, ratingsUpdated=${ratingsUpdated}, duration=${formatDuration(durationMs)}`)

    return {
      ratingName: ratingPath.name,
      dryRun: false,
      pathChallenges: pathHistory.length,
      challengesScanned: discovery.challengesScanned,
      participantRowsScanned: discovery.participantRowsScanned,
      usersDiscovered: members.length,
      usersProcessed,
      usersFailed,
      challengesProcessed,
      ratingsUpdated,
      durationMs
    }
  } finally {
    if (shouldDisconnect) {
      await disconnectClient(membersClient)
      await disconnectClient(challengeClient)
      await disconnectClient(reviewDbClient)
    }
  }
}

async function main () {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) {
    printUsage()
    return
  }

  const summary = await run(options)
  if (summary.usersFailed > 0) {
    process.exitCode = 1
  }
}

if (require.main === module) {
  main().catch((error) => {
    logError(error.message, error)
    process.exitCode = 1
  })
}

module.exports = {
  DEFAULT_CONCURRENCY,
  DEFAULT_PROCESSED_USER_IDS_PATH,
  parseArgs,
  parseUserIds,
  mapWithConcurrency,
  buildProcessedUserIdsWriter,
  discoverRatingPathMembers,
  run
}

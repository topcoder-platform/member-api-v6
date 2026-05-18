#!/usr/bin/env node
'use strict'

/**
 * Bulk re-rate Development Challenge history for every discovered competitor.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL or CHALLENGE_DB_URL (challenge database)
 * - REVIEW_DB_URL (review database with challengeResult data)
 *
 * Usage examples:
 * - Dry-run discovery:
 *   node src/scripts/rerateDevelopmentChallenge.js --dry-run
 * - Re-rate every discovered member from the start of their Development history:
 *   node src/scripts/rerateDevelopmentChallenge.js --concurrency 5
 * - Re-rate a bounded sample:
 *   node src/scripts/rerateDevelopmentChallenge.js --limit 100
 * - Re-rate specific users by userId:
 *   node src/scripts/rerateDevelopmentChallenge.js --user-id 12345 --user-ids 67890,24680
 *
 * Notes:
 * - Challenge discovery uses the Development ChallengeTrack id and includes both
 *   ChallengeType Challenge and CODE rows.
 * - Existing CODE rows are discovered for member coverage, but legacy numeric
 *   review aliases are skipped during full replay because legacy subtrack
 *   history already carries those ratings.
 * - Handles are not required. Participants are discovered from review-api
 *   challengeResult rows.
 * - Each member is replayed from the beginning by calling rerateDevTrack with no
 *   starting challenge id, so canonical Development Challenge history is persisted.
 * - Current Develop / Challenge ranks are recalculated once after the batch,
 *   not once per member, to avoid long repeated interactive transactions.
 */

require('dotenv').config()

const fs = require('fs')
const path = require('path')

const reviewDb = require('../common/reviewDb')
const {
  getMembersClient,
  getChallengesClient
} = require('../common/prisma')
const {
  TRACK_NAMES,
  TYPE_NAMES,
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup
} = require('../common/statsDimensionHelper')
const { recalculateRatingRanks } = require('../common/ratingRankHelper')
const {
  RATING_METADATA_SELECT,
  isChallengeRated
} = require('../ratings/challengeRatingStatus')
const {
  buildReviewChallengeIds,
  fetchParticipantsForChallenge,
  rerateDevTrack
} = require('../ratings/developRatingEngine')

const DEFAULT_CONCURRENCY = 4
const DEFAULT_PROCESSED_USER_IDS_PATH = 'rerateDevelopmentChallenge.processedUserIds.json'
const COMPLETED_CHALLENGE_STATUS = 'COMPLETED'
const MEMBER_LOOKUP_BATCH_SIZE = 5000
const RERATE_ACTOR = 'rerate-member-stats'

/**
 * Write an informational message with a timestamp for long-running operator logs.
 * @param {string} message message to print
 * @returns {void}
 */
function logInfo (message) {
  console.log(`[INFO] ${new Date().toISOString()} ${message}`)
}

/**
 * Write an error message with a timestamp and optional stack/detail object.
 * @param {string} message message to print
 * @param {Error} [error] optional error object to include
 * @returns {void}
 */
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
 * Normalize a date-like value with invalid values represented as null.
 * @param {*} value raw date value
 * @returns {Date|null} parsed Date or null
 */
function toDateOrNull (value) {
  if (!value) {
    return null
  }

  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
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
 * Parse rerateDevelopmentChallenge command-line arguments.
 * @param {Array<string>} argv process argv slice after the script path
 * @returns {Object} normalized script options
 * @throws {Error} when an option is unknown or invalid
 */
function parseArgs (argv) {
  const options = {
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

/**
 * Print command-line usage for the Development Challenge rerate helper.
 * @returns {void}
 */
function printUsage () {
  console.log(`
Usage:
  node src/scripts/rerateDevelopmentChallenge.js [options]

Options:
  --user-id <id>            Re-rate a single user ID discovered in Development history (repeatable).
  --user-ids <id,id>        Comma-separated user IDs discovered in Development history.
  --limit <n>               Limit the number of discovered users processed.
  --concurrency <n>         Process up to n users in parallel (default: ${DEFAULT_CONCURRENCY}).
  --processed-user-ids-path <path>
                            Write successfully processed user IDs to JSON (default: ${DEFAULT_PROCESSED_USER_IDS_PATH}).
  --dry-run                 Discover Development challenges and users without writing ratings.
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
 * Resolve the ChallengeType ids that are included in Development Challenge rating.
 * @param {Object} dimensionLookup cached challenge dimension lookup
 * @returns {Array<string>} unique ChallengeType ids for Challenge and CODE
 */
function resolveDevelopmentTypeIds (dimensionLookup) {
  return Array.from(new Set([
    resolveTypeIdFromLookup(dimensionLookup, TYPE_NAMES.CHALLENGE),
    resolveTypeIdFromLookup(dimensionLookup, TYPE_NAMES.CODE)
  ].filter(Boolean)))
}

/**
 * Load completed, rated Development challenges with Challenge or CODE type.
 * Challenge and CODE entries are both replayed by the Development rating engine
 * into the existing DEVELOP / Challenge rating stream.
 * @param {Object} challengeClient Prisma challenge client
 * @returns {Promise<Object>} resolved ids and ordered challenge history
 * @throws {Error} when required Development dimensions cannot be resolved
 */
async function fetchDevelopmentChallengeHistory (challengeClient) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const developmentTrackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAMES.DEVELOP)
  const developmentTypeIds = resolveDevelopmentTypeIds(dimensionLookup)
  const developmentRatingTypeId = resolveTypeIdFromLookup(dimensionLookup, TYPE_NAMES.CHALLENGE)

  if (!developmentTrackId) {
    throw new Error('Unable to resolve Development ChallengeTrack id')
  }
  if (developmentTypeIds.length === 0) {
    throw new Error('Unable to resolve Challenge or CODE ChallengeType ids')
  }

  const challenges = await challengeClient.challenge.findMany({
    where: {
      trackId: developmentTrackId,
      typeId: {
        in: developmentTypeIds
      },
      status: COMPLETED_CHALLENGE_STATUS
    },
    select: {
      id: true,
      legacyId: true,
      endDate: true,
      status: true,
      trackId: true,
      typeId: true,
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
    if (!challenge || String(challenge.status || '').trim().toUpperCase() !== COMPLETED_CHALLENGE_STATUS) {
      return
    }

    if (!isChallengeRated(challenge)) {
      return
    }

    const eventDate = toDateOrNull(challenge.endDate)
    if (!eventDate) {
      return
    }

    history.push({
      challengeId: String(challenge.id),
      reviewChallengeIds: buildReviewChallengeIds(challenge),
      eventDate,
      typeId: String(challenge.typeId),
      trackId: String(challenge.trackId)
    })
  })

  history.sort((left, right) => {
    const leftEventDate = left.eventDate.getTime()
    const rightEventDate = right.eventDate.getTime()
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return {
    trackId: developmentTrackId,
    ratingTrackId: developmentTrackId,
    ratingTypeId: developmentRatingTypeId,
    typeIds: developmentTypeIds,
    history
  }
}

/**
 * Resolve the unified memberStats dimensions that receive Development Challenge ratings.
 * CODE challenge rows are discovered for replay, but the rating stream persists
 * into the DEVELOP / Challenge memberStats dimension.
 * @param {Object} developmentHistoryResult result from fetchDevelopmentChallengeHistory
 * @returns {{trackId: string, typeId: string}} memberStats rank dimensions
 * @throws {Error} when required rank dimensions are missing
 */
function resolveDevelopmentRankDimensionIds (developmentHistoryResult) {
  const trackId = developmentHistoryResult && (developmentHistoryResult.ratingTrackId || developmentHistoryResult.trackId)
  const typeId = developmentHistoryResult && (developmentHistoryResult.ratingTypeId || (developmentHistoryResult.typeIds || [])[0])

  if (!trackId || !typeId) {
    throw new Error('Unable to resolve Develop Challenge rank dimensions')
  }

  return {
    trackId,
    typeId
  }
}

/**
 * Resolve a Development Challenge participant's member id.
 * @param {Object} row challengeResult participant row
 * @returns {BigInt} participant member id
 */
function resolveDevelopmentParticipantId (row) {
  return global.BigInt(String(row.userId))
}

/**
 * Discover distinct members who participated in Development Challenge events.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Array<Object>} developmentHistory ordered Development challenge history
 * @param {Object} options discovery controls and optional test doubles
 * @param {Array<string>} options.userIds optional user ID allow-list
 * @param {number|null} options.limit optional maximum discovered users
 * @param {Function} options.fetchParticipants optional participant loader
 * @param {Function} options.resolveParticipantId optional participant id resolver
 * @returns {Promise<Object>} discovery summary containing members and scan counts
 */
async function discoverDevelopmentChallengeMembers (reviewDbClient, developmentHistory, options = {}) {
  const fetchParticipants = options.fetchParticipants || fetchParticipantsForChallenge
  const resolveParticipantId = options.resolveParticipantId || resolveDevelopmentParticipantId
  const userFilter = new Set((options.userIds || []).map(userId => String(userId)))
  const membersByUserId = new Map()
  let challengesScanned = 0
  let participantRowsScanned = 0

  for (const historyEntry of developmentHistory) {
    const participantResult = await fetchParticipants(reviewDbClient, historyEntry)
    const participantRows = participantResult.participantRows || participantResult || []
    challengesScanned += 1
    participantRowsScanned += participantRows.length

    for (const row of participantRows) {
      const userId = resolveParticipantId(row)
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
 * Filter discovered users down to members present in member-api storage.
 * @param {Object} membersClient Prisma members client
 * @param {Array<Object>} members discovered member descriptors
 * @returns {Promise<Object>} existing and missing member descriptors
 */
async function filterExistingMembers (membersClient, members) {
  if (!members || members.length === 0) {
    return {
      existingMembers: [],
      skippedMembers: []
    }
  }

  const existingUserIds = new Set()
  for (let start = 0; start < members.length; start += MEMBER_LOOKUP_BATCH_SIZE) {
    const batch = members.slice(start, start + MEMBER_LOOKUP_BATCH_SIZE)
    const rows = await membersClient.member.findMany({
      where: {
        userId: {
          in: batch.map(member => member.userId)
        }
      },
      select: {
        userId: true
      }
    })

    rows.forEach((row) => {
      existingUserIds.add(String(row.userId))
    })
  }

  return {
    existingMembers: members.filter(member => existingUserIds.has(String(member.userId))),
    skippedMembers: members.filter(member => !existingUserIds.has(String(member.userId)))
  }
}

/**
 * Run the Development Challenge bulk rerate workflow.
 * @param {Object} options parsed script options
 * @param {Object} dependencies optional clients and functions for tests
 * @param {Function} [dependencies.rerateDevTrack] optional member rerate function
 * @param {Function} [dependencies.recalculateRatingRanks] optional final rank recalculation function
 * @returns {Promise<Object>} bulk rerate summary
 */
async function run (options, dependencies = {}) {
  const membersClient = dependencies.membersClient || getMembersClient()
  const challengeClient = dependencies.challengeClient || getChallengesClient()
  const reviewDbClient = Object.prototype.hasOwnProperty.call(dependencies, 'reviewDbClient')
    ? dependencies.reviewDbClient
    : reviewDb
  const shouldDisconnect = dependencies.disconnect !== false
  const startedAt = startTimer()

  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate Development Challenge stats')
  }

  try {
    await connectClient(membersClient)
    await connectClient(challengeClient)

    logInfo('Loading completed Development Challenge and CODE challenges')
    const developmentHistoryResult = await (dependencies.fetchDevelopmentChallengeHistory || fetchDevelopmentChallengeHistory)(challengeClient)
    const developmentHistory = developmentHistoryResult.history || developmentHistoryResult
    logInfo(`Loaded ${developmentHistory.length} Development challenge(s)`)

    if (developmentHistory.length === 0) {
      return {
        dryRun: options.dryRun,
        trackId: developmentHistoryResult.trackId,
        typeIds: developmentHistoryResult.typeIds,
        pathChallenges: 0,
        usersDiscovered: 0,
        usersProcessed: 0,
        usersFailed: 0,
        usersSkippedMissing: 0,
        ratingsUpdated: 0,
        durationMs: getElapsedMilliseconds(startedAt)
      }
    }

    const discovery = await discoverDevelopmentChallengeMembers(reviewDbClient, developmentHistory, {
      userIds: options.userIds,
      limit: options.limit,
      fetchParticipants: dependencies.fetchParticipants,
      resolveParticipantId: dependencies.resolveParticipantId
    })
    const memberFilterResult = await (dependencies.filterExistingMembers || filterExistingMembers)(
      membersClient,
      discovery.members
    )
    const members = memberFilterResult.existingMembers
    const skippedMembers = memberFilterResult.skippedMembers

    logInfo(`Scanned ${discovery.challengesScanned} challenge(s) and ${discovery.participantRowsScanned} participant row(s)`)
    logInfo(`Discovered ${discovery.members.length} member(s) to rerate; ${skippedMembers.length} missing from member storage`)

    if (options.dryRun) {
      members.slice(0, 10).forEach((member) => {
        logInfo(`Dry-run member ${String(member.userId)} first appears in challenge ${member.firstChallengeId}`)
      })
      if (members.length > 10) {
        logInfo(`Dry-run output truncated to 10 of ${members.length} existing member(s)`)
      }

      return {
        dryRun: true,
        trackId: developmentHistoryResult.trackId,
        typeIds: developmentHistoryResult.typeIds,
        pathChallenges: developmentHistory.length,
        challengesScanned: discovery.challengesScanned,
        participantRowsScanned: discovery.participantRowsScanned,
        usersDiscovered: discovery.members.length,
        usersProcessable: members.length,
        usersSkippedMissing: skippedMembers.length,
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
        const result = await (dependencies.rerateDevTrack || rerateDevTrack)(
          membersClient,
          challengeClient,
          reviewDbClient,
          member.userId,
          null,
          {
            recalculateRanks: false,
            skipLegacyReviewIds: true
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
    let rankRowsUpdated = 0

    if (ratingsUpdated > 0) {
      const rankStartedAt = startTimer()
      logInfo('Recalculating Development Challenge ranks after member rerates')
      rankRowsUpdated = await (dependencies.recalculateRatingRanks || recalculateRatingRanks)(
        membersClient,
        resolveDevelopmentRankDimensionIds(developmentHistoryResult),
        { updatedBy: RERATE_ACTOR }
      )
      logInfo(`Recalculated Development Challenge ranks rowsUpdated=${rankRowsUpdated} duration=${formatDuration(getElapsedMilliseconds(rankStartedAt))}`)
    }

    const durationMs = getElapsedMilliseconds(startedAt)

    logInfo(`Completed Development Challenge rerate: usersProcessed=${usersProcessed}, usersFailed=${usersFailed}, usersSkippedMissing=${skippedMembers.length}, ratingsUpdated=${ratingsUpdated}, duration=${formatDuration(durationMs)}`)

    return {
      dryRun: false,
      trackId: developmentHistoryResult.trackId,
      typeIds: developmentHistoryResult.typeIds,
      pathChallenges: developmentHistory.length,
      challengesScanned: discovery.challengesScanned,
      participantRowsScanned: discovery.participantRowsScanned,
      usersDiscovered: discovery.members.length,
      usersProcessable: members.length,
      usersSkippedMissing: skippedMembers.length,
      usersProcessed,
      usersFailed,
      challengesProcessed,
      ratingsUpdated,
      rankRowsUpdated,
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
          logError('Development Challenge rerate failed', error)
          process.exitCode = 1
        })
    }
  } catch (error) {
    logError('Development Challenge rerate failed', error)
    process.exitCode = 1
  }
}

module.exports = {
  parseArgs,
  printUsage,
  fetchDevelopmentChallengeHistory,
  discoverDevelopmentChallengeMembers,
  filterExistingMembers,
  run
}

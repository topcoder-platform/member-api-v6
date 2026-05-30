#!/usr/bin/env node
'use strict'

/**
 * Bulk re-rate native Marathon Match history for every discovered competitor.
 *
 * Required environment variables:
 * - DATABASE_URL (member database)
 * - CHALLENGES_DB_URL or CHALLENGE_DB_URL (challenge database)
 * - REVIEW_DB_URL (review database with reviewSummation/submission data)
 *
 * Usage examples:
 * - Dry-run discovery:
 *   node src/scripts/rerateMarathonMatches.js --dry-run
 * - Re-rate every discovered member from the start of their MM history:
 *   node src/scripts/rerateMarathonMatches.js --concurrency 5
 * - Re-rate a bounded sample:
 *   node src/scripts/rerateMarathonMatches.js --limit 100
 * - Re-rate specific users by userId:
 *   node src/scripts/rerateMarathonMatches.js --user-id 12345 --user-ids 67890,24680
 *
 * Notes:
 * - Challenge discovery is based only on the Marathon Match ChallengeType id.
 *   This intentionally includes MM challenges imported under either Data Science
 *   or Development tracks.
 * - Existing DATA_SCIENCE / MARATHON_MATCH memberStatsHistory challenge IDs are
 *   also included because migrated legacy MM history can predate ChallengeType
 *   classification in challenge-api.
 * - Handles are not required. Participants are discovered from the latest final
 *   reviewSummation row per member and challenge.
 * - Both canonical challenge UUIDs and legacy numeric challenge ids are used
 *   when looking up review submissions.
 * - The script does not read the marathon-match-api schema; historical scores
 *   come from reviewSummation.aggregateScore.
 * - Each member is replayed from the beginning by calling rerateMmTrack with no
 *   starting challenge id, so complete native MM history is persisted.
 * - Member rerates intentionally use each target member's own MM history rather
 *   than the full discovered path to avoid scanning every MM challenge once per
 *   user.
 * - Current Marathon Match ranks are recalculated once after the batch, not once
 *   per member, to avoid long repeated interactive transactions.
 */

require('dotenv').config()

const fs = require('fs')
const path = require('path')

const reviewDb = require('../common/reviewDb')
const {
  Prisma,
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
  fetchRatingPathParticipantsForChallenge,
  resolveRatingPathParticipantId,
  rerateMmTrack
} = require('../ratings/mmRatingEngine')

const DEFAULT_CONCURRENCY = 4
const DEFAULT_PROCESSED_USER_IDS_PATH = 'rerateMarathonMatches.processedUserIds.json'
const COMPLETED_CHALLENGE_STATUS = 'COMPLETED'
const MARATHON_MATCH_SOURCE = 'MARATHON_MATCH'
const MEMBER_LOOKUP_BATCH_SIZE = 5000
const RERATE_ACTOR = 'rerate-member-stats'

/**
 * Add a normalized challenge id candidate to a target set.
 * @param {Set<string>} candidates candidate ids
 * @param {*} value challenge id candidate
 * @returns {void}
 */
function addChallengeIdCandidate (candidates, value) {
  if (value === null || value === undefined) {
    return
  }

  const normalized = String(value).trim()
  if (normalized) {
    candidates.add(normalized)
  }
}

/**
 * Build the review-api challenge ids that may identify a challenge.
 * Challenge-api returns canonical UUIDs, but imported review submissions can
 * still store the legacy numeric challenge id.
 * @param {Object} challenge challenge-api row
 * @returns {Array<string>} unique challenge id candidates
 */
function buildReviewChallengeIds (challenge) {
  const candidates = new Set()
  addChallengeIdCandidate(candidates, challenge && challenge.id)
  addChallengeIdCandidate(candidates, challenge && challenge.legacyId)
  addChallengeIdCandidate(candidates, challenge && challenge.legacyRecord && challenge.legacyRecord.legacySystemId)
  return Array.from(candidates)
}

/**
 * Merge ordered Marathon Match history sources while preserving every known
 * challenge id alias for review-api lookups.
 * @param {...Array<Object>} historySources ordered history arrays
 * @returns {Array<Object>} merged and sorted challenge history
 */
function mergeMarathonHistories (...historySources) {
  const historyByKey = new Map()
  const aliasToKey = new Map()

  historySources.flat().forEach((historyEntry) => {
    if (!historyEntry || !historyEntry.challengeId || !historyEntry.eventDate) {
      return
    }

    const aliases = new Set()
    addChallengeIdCandidate(aliases, historyEntry.challengeId)
    ;(historyEntry.reviewChallengeIds || []).forEach((alias) => addChallengeIdCandidate(aliases, alias))

    let key
    for (const alias of aliases) {
      if (aliasToKey.has(alias)) {
        key = aliasToKey.get(alias)
        break
      }
    }
    if (!key) {
      key = String(historyEntry.challengeId)
    }

    const existing = historyByKey.get(key)
    if (existing) {
      const reviewChallengeIds = new Set(existing.reviewChallengeIds || [existing.challengeId])
      aliases.forEach((alias) => reviewChallengeIds.add(alias))
      existing.reviewChallengeIds = Array.from(reviewChallengeIds)
      if (historyEntry.eventDate.getTime() < existing.eventDate.getTime()) {
        existing.eventDate = historyEntry.eventDate
      }
      existing.typeId = existing.typeId || historyEntry.typeId || null
      existing.trackId = existing.trackId || historyEntry.trackId || null
    } else {
      historyByKey.set(key, {
        ...historyEntry,
        challengeId: String(historyEntry.challengeId),
        reviewChallengeIds: Array.from(aliases)
      })
    }

    aliases.forEach((alias) => aliasToKey.set(alias, key))
  })

  const history = Array.from(historyByKey.values())
  history.sort((left, right) => {
    const leftEventDate = left.eventDate.getTime()
    const rightEventDate = right.eventDate.getTime()
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return history
}

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
 * Parse rerateMarathonMatches command-line arguments.
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
 * Print command-line usage for the native Marathon Match rerate helper.
 * @returns {void}
 */
function printUsage () {
  console.log(`
Usage:
  node src/scripts/rerateMarathonMatches.js [options]

Options:
  --user-id <id>            Re-rate a single user ID discovered in MM history (repeatable).
  --user-ids <id,id>        Comma-separated user IDs discovered in MM history.
  --limit <n>               Limit the number of discovered users processed.
  --concurrency <n>         Process up to n users in parallel (default: ${DEFAULT_CONCURRENCY}).
  --processed-user-ids-path <path>
                            Write successfully processed user IDs to JSON (default: ${DEFAULT_PROCESSED_USER_IDS_PATH}).
  --dry-run                 Discover MM challenges and users without writing ratings.
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
 * Load completed, rated Marathon Match challenges using only ChallengeType id.
 * Track is intentionally ignored so MM rows imported under Development and Data
 * Science are replayed as one native MM stream. Each history entry includes
 * canonical and legacy challenge id aliases for review-api submission lookups.
 * @param {Object} challengeClient Prisma challenge client
 * @returns {Promise<Object>} resolved rank dimensions and ordered challenge history
 * @throws {Error} when the native MM memberStats dimensions cannot be resolved
 */
async function fetchMarathonMatchHistory (challengeClient) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const dataScienceTrackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAMES.DATA_SCIENCE)
  const marathonMatchTypeId = resolveTypeIdFromLookup(dimensionLookup, TYPE_NAMES.MARATHON_MATCH)

  if (!dataScienceTrackId || !marathonMatchTypeId) {
    throw new Error('Unable to resolve Marathon Match rank dimensions')
  }

  const challenges = await challengeClient.challenge.findMany({
    where: {
      typeId: marathonMatchTypeId,
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
      trackId: challenge.trackId ? String(challenge.trackId) : null
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
    trackId: dataScienceTrackId,
    typeId: marathonMatchTypeId,
    history
  }
}

/**
 * Resolve the unified memberStats dimensions that receive native MM ratings.
 * Challenge discovery intentionally ignores source challenge track, but native
 * MM ratings persist into DATA_SCIENCE / MARATHON_MATCH.
 * @param {Object} marathonHistoryResult result from fetchMarathonMatchHistory
 * @param {Object} persistedHistoryResult result from fetchPersistedMarathonMatchHistory
 * @returns {{trackId: string, typeId: string}} memberStats rank dimensions
 * @throws {Error} when required rank dimensions are missing
 */
function resolveMarathonMatchRankDimensionIds (marathonHistoryResult, persistedHistoryResult) {
  const trackId = (marathonHistoryResult && marathonHistoryResult.trackId) ||
    (persistedHistoryResult && persistedHistoryResult.trackId)
  const typeId = (marathonHistoryResult && marathonHistoryResult.typeId) ||
    (persistedHistoryResult && persistedHistoryResult.typeId)

  if (!trackId || !typeId) {
    throw new Error('Unable to resolve Marathon Match rank dimensions')
  }

  return {
    trackId,
    typeId
  }
}

/**
 * Load distinct Marathon Match challenge IDs already present in unified member
 * history. This catches migrated legacy MM rows that are visible in member stats
 * history but are not classified as ChallengeType MARATHON_MATCH in challenge-api.
 * @param {Object} membersClient Prisma members client
 * @param {Object} challengeClient Prisma challenge client for dimension lookup
 * @returns {Promise<Object>} resolved dimension ids and ordered persisted history
 */
async function fetchPersistedMarathonMatchHistory (membersClient, challengeClient) {
  const dimensionLookup = await loadChallengeDimensionLookup(challengeClient)
  const dataScienceTrackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAMES.DATA_SCIENCE)
  const marathonMatchTypeId = resolveTypeIdFromLookup(dimensionLookup, TYPE_NAMES.MARATHON_MATCH)

  if (!dataScienceTrackId || !marathonMatchTypeId) {
    return {
      trackId: dataScienceTrackId || null,
      typeId: marathonMatchTypeId || null,
      history: []
    }
  }

  const trackIds = Array.from(new Set([dataScienceTrackId, TRACK_NAMES.DATA_SCIENCE].filter(Boolean)))
  const typeIds = Array.from(new Set([marathonMatchTypeId, TYPE_NAMES.MARATHON_MATCH].filter(Boolean)))
  const rows = await membersClient.$queryRaw`
    SELECT "challengeId", MIN("eventDate") AS "eventDate"
    FROM "members"."memberStatsHistory"
    WHERE "trackId" IN (${Prisma.join(trackIds)})
      AND "typeId" IN (${Prisma.join(typeIds)})
      AND "challengeId" IS NOT NULL
      AND "eventDate" IS NOT NULL
    GROUP BY "challengeId"
  `

  const history = rows
    .map((row) => ({
      challengeId: String(row.challengeId),
      reviewChallengeIds: [String(row.challengeId)],
      eventDate: toDateOrNull(row.eventDate),
      typeId: marathonMatchTypeId,
      trackId: dataScienceTrackId
    }))
    .filter((row) => row.eventDate)

  history.sort((left, right) => {
    const leftEventDate = left.eventDate.getTime()
    const rightEventDate = right.eventDate.getTime()
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return {
    trackId: dataScienceTrackId,
    typeId: marathonMatchTypeId,
    history
  }
}

/**
 * Discover distinct members who participated in native Marathon Match events.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Array<Object>} marathonHistory ordered MM challenge history
 * @param {Object} options discovery controls and optional test doubles
 * @param {Array<string>} options.userIds optional user ID allow-list
 * @param {number|null} options.limit optional maximum discovered users
 * @param {Function} options.fetchParticipants optional participant loader
 * @param {Function} options.resolveParticipantId optional participant id resolver
 * @returns {Promise<Object>} discovery summary containing members and scan counts
 */
async function discoverMarathonMatchMembers (reviewDbClient, marathonHistory, options = {}) {
  const fetchParticipants = options.fetchParticipants || fetchRatingPathParticipantsForChallenge
  const resolveParticipantId = options.resolveParticipantId || resolveRatingPathParticipantId
  const userFilter = new Set((options.userIds || []).map(userId => String(userId)))
  const membersByUserId = new Map()
  let challengesScanned = 0
  let participantRowsScanned = 0

  for (const historyEntry of marathonHistory) {
    const participantResult = await fetchParticipants(reviewDbClient, {
      ...historyEntry,
      source: MARATHON_MATCH_SOURCE
    })
    const participantRows = participantResult.participantRows || []
    challengesScanned += 1
    participantRowsScanned += participantRows.length

    for (const row of participantRows) {
      const userId = resolveParticipantId(row, MARATHON_MATCH_SOURCE)
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
 * Run the native Marathon Match bulk rerate workflow.
 * @param {Object} options parsed script options
 * @param {Object} dependencies optional clients and functions for tests
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
    throw new Error('REVIEW_DB_URL must be configured to rerate Marathon Match stats')
  }

  try {
    await connectClient(membersClient)
    await connectClient(challengeClient)

    logInfo('Loading completed Marathon Match challenges by ChallengeType id')
    const marathonHistoryResult = await (dependencies.fetchMarathonMatchHistory || fetchMarathonMatchHistory)(challengeClient)
    const challengeApiHistory = marathonHistoryResult.history || marathonHistoryResult
    const shouldLoadPersistedHistory = !dependencies.fetchMarathonMatchHistory || dependencies.fetchPersistedMarathonMatchHistory
    const persistedHistoryResult = shouldLoadPersistedHistory
      ? await (dependencies.fetchPersistedMarathonMatchHistory || fetchPersistedMarathonMatchHistory)(membersClient, challengeClient)
      : { history: [] }
    const persistedHistory = persistedHistoryResult.history || persistedHistoryResult
    const marathonHistory = mergeMarathonHistories(challengeApiHistory, persistedHistory)
    logInfo(`Loaded ${marathonHistory.length} Marathon Match challenge(s): ${challengeApiHistory.length} from Challenge API, ${persistedHistory.length} from memberStatsHistory`)

    if (marathonHistory.length === 0) {
      return {
        dryRun: options.dryRun,
        typeId: marathonHistoryResult.typeId,
        pathChallenges: 0,
        challengeApiChallenges: challengeApiHistory.length,
        persistedHistoryChallenges: persistedHistory.length,
        usersDiscovered: 0,
        usersProcessed: 0,
        usersFailed: 0,
        usersSkippedMissing: 0,
        ratingsUpdated: 0,
        durationMs: getElapsedMilliseconds(startedAt)
      }
    }

    const discovery = await discoverMarathonMatchMembers(reviewDbClient, marathonHistory, {
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
        typeId: marathonHistoryResult.typeId,
        pathChallenges: marathonHistory.length,
        challengeApiChallenges: challengeApiHistory.length,
        persistedHistoryChallenges: persistedHistory.length,
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
        const result = await (dependencies.rerateMmTrack || rerateMmTrack)(
          membersClient,
          challengeClient,
          null,
          reviewDbClient,
          member.userId,
          null,
          {
            recalculateRanks: false
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
      logInfo('Recalculating native Marathon Match ranks after member rerates')
      rankRowsUpdated = await (dependencies.recalculateRatingRanks || recalculateRatingRanks)(
        membersClient,
        resolveMarathonMatchRankDimensionIds(marathonHistoryResult, persistedHistoryResult),
        { updatedBy: RERATE_ACTOR }
      )
      logInfo(`Recalculated native Marathon Match ranks rowsUpdated=${rankRowsUpdated} duration=${formatDuration(getElapsedMilliseconds(rankStartedAt))}`)
    }

    const durationMs = getElapsedMilliseconds(startedAt)

    logInfo(`Completed native Marathon Match rerate: usersProcessed=${usersProcessed}, usersFailed=${usersFailed}, usersSkippedMissing=${skippedMembers.length}, ratingsUpdated=${ratingsUpdated}, duration=${formatDuration(durationMs)}`)

    return {
      dryRun: false,
      typeId: marathonHistoryResult.typeId,
      pathChallenges: marathonHistory.length,
      challengeApiChallenges: challengeApiHistory.length,
      persistedHistoryChallenges: persistedHistory.length,
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
          logError('Marathon Match rerate failed', error)
          process.exitCode = 1
        })
    }
  } catch (error) {
    logError('Marathon Match rerate failed', error)
    process.exitCode = 1
  }
}

module.exports = {
  parseArgs,
  printUsage,
  fetchMarathonMatchHistory,
  fetchPersistedMarathonMatchHistory,
  mergeMarathonHistories,
  resolveMarathonMatchRankDimensionIds,
  discoverMarathonMatchMembers,
  filterExistingMembers,
  run
}

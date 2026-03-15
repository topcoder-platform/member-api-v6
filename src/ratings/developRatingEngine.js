/**
 * Re-rate unified member stats for rated DEVELOPMENT / Challenge results.
 *
 * The engine reads challengeResult rows from review-api, resolves challenge
 * metadata from challenge-api, applies the Qubits math one challenge at a
 * time, and persists the target member's rating state into memberStats,
 * memberStatsHistory, and memberMaxRating.
 */

'use strict'

const errors = require('../common/errors')
const { runQubitsRating, getRatingColor, DEFAULT_VOLATILITY } = require('./qubitsAlgorithm')

const TRACK_ID = 'DEVELOP'
const TYPE_ID = 'Challenge'
const CHALLENGE_TRACK_NAME = 'DEVELOPMENT'
const CHALLENGE_TYPE_NAME = 'Challenge'
const RERATE_ACTOR = 'rerate-member-stats'

function isBigIntValue (value) {
  return Object.prototype.toString.call(value) === '[object BigInt]'
}

function toBigIntUserId (value) {
  if (isBigIntValue(value)) {
    return value
  }

  if (typeof global.BigInt !== 'function') {
    throw new Error('BigInt is not supported in this runtime')
  }

  return global.BigInt(String(value).trim())
}

function normalizeDate (value, fallbackValue) {
  const date = value ? new Date(value) : new Date(fallbackValue)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date
}

function createDefaultState () {
  return {
    rating: 0,
    volatility: 0,
    numRatings: 0
  }
}

/**
 * Clone a rating state into the normalized shape expected by the Qubits engine.
 * @param {Object} state source state
 * @returns {Object} normalized rating state
 */
function cloneState (state) {
  if (!state) {
    return createDefaultState()
  }

  return {
    rating: Number.isFinite(Number(state.rating)) ? Number(state.rating) : 0,
    volatility: Number.isFinite(Number(state.volatility)) ? Number(state.volatility) : 0,
    numRatings: Number.isFinite(Number(state.numRatings)) ? Math.max(0, Number(state.numRatings)) : 0
  }
}

function buildUserStateKey (userId) {
  return String(userId)
}

/**
 * Build a rerate seed state from the latest authoritative history row before a challenge.
 * Unified memberStatsHistory does not checkpoint volatility, so rated history seeds fall
 * back to the default Qubits volatility until the rerate pass advances them.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {number} seedIndex index of the last history row before the target challenge
 * @returns {Object} seeded rating state
 */
function createHistorySeedState (historyRows, seedIndex) {
  if (!Array.isArray(historyRows) || seedIndex < 0 || !historyRows[seedIndex]) {
    return createDefaultState()
  }

  return {
    rating: Number.isFinite(Number(historyRows[seedIndex].newRating))
      ? Number(historyRows[seedIndex].newRating)
      : 0,
    volatility: DEFAULT_VOLATILITY,
    numRatings: seedIndex + 1
  }
}

/**
 * Find the history row index for a challenge within one participant timeline.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {string} challengeId challenge identifier
 * @returns {number} index of the matching row or -1 when absent
 */
function findHistoryIndexForChallenge (historyRows, challengeId) {
  const normalizedChallengeId = String(challengeId)

  for (let index = historyRows.length - 1; index >= 0; index -= 1) {
    if (String(historyRows[index].challengeId) === normalizedChallengeId) {
      return index
    }
  }

  return -1
}

/**
 * Find the last history row that is safely before the supplied challenge boundary.
 * When the current challenge already has a history row, the immediately preceding row is
 * authoritative. Otherwise the unified history table only exposes eventDate ordering, so
 * same-day rows without the current challenge checkpoint are excluded conservatively.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {Object} challengeEntry rerate challenge metadata
 * @returns {number} index of the latest pre-challenge history row or -1
 */
function findHistorySeedIndexForChallenge (historyRows, challengeEntry) {
  if (!Array.isArray(historyRows) || historyRows.length === 0) {
    return -1
  }

  const currentChallengeIndex = findHistoryIndexForChallenge(historyRows, challengeEntry.challengeId)
  if (currentChallengeIndex >= 0) {
    return currentChallengeIndex - 1
  }

  const challengeEventDate = challengeEntry && challengeEntry.eventDate
  if (!challengeEventDate) {
    return -1
  }

  const challengeTimestamp = challengeEventDate.getTime()
  let seedIndex = -1

  for (let index = 0; index < historyRows.length; index += 1) {
    if (historyRows[index].eventDate.getTime() < challengeTimestamp) {
      seedIndex = index
      continue
    }

    break
  }

  return seedIndex
}

/**
 * Compute the highest historical rating reached through the supplied history index.
 * @param {Array<Object>} historyRows participant history rows sorted by event date and id
 * @param {number} endIndex inclusive history index bound
 * @returns {number} maximum rating through that bound
 */
function computeMaxRatingFromHistory (historyRows, endIndex) {
  if (!Array.isArray(historyRows) || endIndex < 0) {
    return 0
  }

  let maxRating = 0

  for (let index = 0; index <= endIndex; index += 1) {
    const rating = Number(historyRows[index].newRating)
    if (Number.isFinite(rating) && rating > maxRating) {
      maxRating = rating
    }
  }

  return maxRating
}

function normalizeScore (row) {
  const finalScore = Number(row.finalScore)
  if (Number.isFinite(finalScore)) {
    return finalScore
  }

  const placement = Number(row.placement)
  if (Number.isFinite(placement)) {
    return -placement
  }

  return 0
}

async function fetchReviewResultsForUser (reviewDbClient, userId) {
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "createdAt"
      FROM "challengeResult"
      WHERE "userId" = $1
      ORDER BY "createdAt" ASC
    `,
    [String(userId)]
  )

  return result.rows
}

async function fetchParticipantsForChallenge (reviewDbClient, challengeId) {
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "finalScore", "placement", "rated", "createdAt"
      FROM "challengeResult"
      WHERE "challengeId" = $1
        AND "rated" = true
      ORDER BY "placement" ASC, "finalScore" DESC, "createdAt" ASC
    `,
    [String(challengeId)]
  )

  return result.rows
}

async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
  if (!challengeIds || challengeIds.length === 0) {
    return new Map()
  }

  const challenges = await challengeClient.challenge.findMany({
    where: {
      id: {
        in: challengeIds
      }
    },
    select: {
      id: true,
      endDate: true,
      track: {
        select: {
          name: true
        }
      },
      type: {
        select: {
          name: true
        }
      }
    }
  })

  return new Map(challenges.map((challenge) => [challenge.id, challenge]))
}

function buildTargetHistory (reviewRows, challengeMetadataById) {
  const history = []

  reviewRows.forEach((row) => {
    if (!row.rated) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !challenge.endDate) {
      return
    }

    if (!challenge.track || !challenge.type) {
      return
    }

    if (String(challenge.track.name).toUpperCase() !== CHALLENGE_TRACK_NAME || challenge.type.name !== CHALLENGE_TYPE_NAME) {
      return
    }

    const eventDate = normalizeDate(challenge.endDate, row.createdAt)
    if (!eventDate) {
      return
    }

    history.push({
      challengeId: String(row.challengeId),
      createdAt: normalizeDate(row.createdAt, challenge.endDate),
      endDate: normalizeDate(challenge.endDate, row.createdAt),
      eventDate
    })
  })

  history.sort((left, right) => {
    const leftEventDate = left.eventDate ? left.eventDate.getTime() : 0
    const rightEventDate = right.eventDate ? right.eventDate.getTime() : 0
    if (leftEventDate !== rightEventDate) {
      return leftEventDate - rightEventDate
    }

    const leftCreatedAt = left.createdAt ? left.createdAt.getTime() : 0
    const rightCreatedAt = right.createdAt ? right.createdAt.getTime() : 0
    if (leftCreatedAt !== rightCreatedAt) {
      return leftCreatedAt - rightCreatedAt
    }

    return left.challengeId.localeCompare(right.challengeId)
  })

  return history
}

/**
 * Cache the unified history timeline for participants needed during the rerate span.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds participant identifiers
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 */
async function loadParticipantHistoryCache (membersClient, participantIds, historyByUserId) {
  const idsToLoad = participantIds.filter((participantId) => !historyByUserId.has(buildUserStateKey(participantId)))
  if (idsToLoad.length === 0) {
    return
  }

  const historyRows = await membersClient.memberStatsHistory.findMany({
    where: {
      userId: {
        in: idsToLoad
      },
      trackId: TRACK_ID,
      typeId: TYPE_ID
    },
    orderBy: [{ userId: 'asc' }, { eventDate: 'asc' }, { id: 'asc' }],
    select: {
      id: true,
      userId: true,
      challengeId: true,
      newRating: true,
      eventDate: true
    }
  })

  idsToLoad.forEach((participantId) => {
    historyByUserId.set(buildUserStateKey(participantId), [])
  })

  historyRows.forEach((row) => {
    const stateKey = buildUserStateKey(row.userId)
    const cachedRows = historyByUserId.get(stateKey)
    const eventDate = normalizeDate(row.eventDate, row.eventDate)

    if (!cachedRows || !eventDate) {
      return
    }

    cachedRows.push({
      id: row.id,
      challengeId: String(row.challengeId),
      newRating: row.newRating,
      eventDate
    })
  })
}

/**
 * Build per-challenge participant seed states from authoritative history rows.
 * The target user's in-memory state is taken from the rerate replay so later challenges
 * in the rerate span see prior rerated values, while every other participant is loaded
 * fresh from history as of the current challenge boundary.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds challenge participant identifiers
 * @param {BigInt} targetUserId target member id
 * @param {Object} targetState current rerated target state
 * @param {Object} challengeEntry rerate challenge metadata
 * @param {Map<string, Array<Object>>} historyByUserId cached history rows per participant
 * @returns {Map<string, Object>} seed state keyed by participant id
 */
async function loadParticipantStates (membersClient, participantIds, targetUserId, targetState, challengeEntry, historyByUserId) {
  const targetStateKey = buildUserStateKey(targetUserId)
  const stateByUserId = new Map()
  const nonTargetParticipantIds = participantIds.filter((participantId) => buildUserStateKey(participantId) !== targetStateKey)

  if (nonTargetParticipantIds.length > 0) {
    await loadParticipantHistoryCache(membersClient, nonTargetParticipantIds, historyByUserId)
  }

  participantIds.forEach((participantId) => {
    const stateKey = buildUserStateKey(participantId)
    if (stateKey === targetStateKey) {
      stateByUserId.set(stateKey, cloneState(targetState))
      return
    }

    const historyRows = historyByUserId.get(stateKey) || []
    const seedIndex = findHistorySeedIndexForChallenge(historyRows, challengeEntry)
    stateByUserId.set(stateKey, createHistorySeedState(historyRows, seedIndex))
  })

  return stateByUserId
}

async function upsertHistoryRow (tx, userId, challengeId, oldRating, newRating, eventDate) {
  const existingHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID,
      challengeId
    },
    select: {
      id: true
    }
  })

  if (existingHistory) {
    await tx.memberStatsHistory.update({
      where: {
        id: existingHistory.id
      },
      data: {
        oldRating,
        newRating,
        eventDate,
        updatedBy: RERATE_ACTOR
      }
    })
    return
  }

  await tx.memberStatsHistory.create({
    data: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID,
      challengeId,
      mostRecent: false,
      oldRating,
      newRating,
      eventDate,
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    }
  })
}

/**
 * Decide whether a rerated peak should replace the stored global max rating row.
 * @param {Object|null} existingMaxRating current memberMaxRating row
 * @param {number} rating rerated peak rating for this track
 * @returns {boolean} true when the rerated peak should replace the stored row
 */
function shouldReplaceGlobalMaxRating (existingMaxRating, rating) {
  if (!existingMaxRating) {
    return true
  }

  const storedRating = Number(existingMaxRating.rating)
  return !Number.isFinite(storedRating) || rating > storedRating
}

/**
 * Persist the member's global max rating row when the rerated Develop peak exceeds it.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {number} rating recomputed peak rating
 * @returns {Promise<void>} resolves when max rating is stored
 */
async function updateMaxRating (tx, userId, rating) {
  const existingMaxRating = await tx.memberMaxRating.findFirst({
    where: {
      userId
    },
    select: {
      rating: true
    }
  })

  if (!shouldReplaceGlobalMaxRating(existingMaxRating, rating)) {
    return
  }

  await tx.memberMaxRating.upsert({
    where: {
      userId
    },
    create: {
      userId,
      rating,
      track: TRACK_ID,
      subTrack: TYPE_ID,
      ratingColor: getRatingColor(rating),
      createdBy: RERATE_ACTOR,
      updatedBy: RERATE_ACTOR
    },
    update: {
      rating,
      track: TRACK_ID,
      subTrack: TYPE_ID,
      ratingColor: getRatingColor(rating),
      updatedBy: RERATE_ACTOR
    }
  })
}

async function refreshMostRecentHistoryFlag (tx, userId) {
  await tx.memberStatsHistory.updateMany({
    where: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID,
      mostRecent: true
    },
    data: {
      mostRecent: false,
      updatedBy: RERATE_ACTOR
    }
  })

  const latestHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    select: {
      id: true
    }
  })

  if (!latestHistory) {
    return
  }

  const currentStats = await tx.memberStats.findFirst({
    where: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID
    },
    select: {
      rating: true
    }
  })

  const previousHistory = await tx.memberStatsHistory.findFirst({
    where: {
      userId,
      trackId: TRACK_ID,
      typeId: TYPE_ID
    },
    orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
    skip: 1,
    select: {
      newRating: true
    }
  })

  const latestUpdate = {
    mostRecent: true,
    oldRating: previousHistory ? previousHistory.newRating : null,
    updatedBy: RERATE_ACTOR
  }

  if (currentStats && currentStats.rating !== null) {
    latestUpdate.newRating = currentStats.rating
  }

  await tx.memberStatsHistory.update({
    where: {
      id: latestHistory.id
    },
    data: latestUpdate
  })
}

async function rerateDevTrack (membersClient, challengeClient, reviewDbClient, userId, fromChallengeId) {
  if (!reviewDbClient) {
    throw new Error('REVIEW_DB_URL must be configured to rerate development stats')
  }

  const normalizedUserId = toBigIntUserId(userId)
  const reviewRows = await fetchReviewResultsForUser(reviewDbClient, normalizedUserId)
  if (reviewRows.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengeClient,
    Array.from(new Set(reviewRows.map((row) => String(row.challengeId))))
  )

  const targetHistory = buildTargetHistory(reviewRows, challengeMetadataById)
  if (targetHistory.length === 0) {
    return {
      challengesProcessed: 0,
      ratingsUpdated: 0
    }
  }

  let startIndex = 0
  if (fromChallengeId) {
    startIndex = targetHistory.findIndex((entry) => entry.challengeId === String(fromChallengeId))
    if (startIndex < 0) {
      throw new errors.BadRequestError(`Challenge ${fromChallengeId} is not a rated ${TRACK_ID}/${TYPE_ID} event for this member`)
    }
  }

  const participantHistoryByUserId = new Map()
  let targetState = createDefaultState()
  let recomputedMaxRating = 0

  if (startIndex > 0) {
    await loadParticipantHistoryCache(membersClient, [normalizedUserId], participantHistoryByUserId)

    const targetHistoryRows = participantHistoryByUserId.get(buildUserStateKey(normalizedUserId)) || []
    let startSeedIndex = findHistoryIndexForChallenge(targetHistoryRows, targetHistory[startIndex - 1].challengeId)

    if (startSeedIndex < 0) {
      startSeedIndex = findHistorySeedIndexForChallenge(targetHistoryRows, targetHistory[startIndex])
    }

    targetState = createHistorySeedState(targetHistoryRows, startSeedIndex)
    recomputedMaxRating = computeMaxRatingFromHistory(targetHistoryRows, startSeedIndex)
  }

  let challengesProcessed = 0
  let ratingsUpdated = 0

  for (let index = startIndex; index < targetHistory.length; index += 1) {
    const historyEntry = targetHistory[index]
    const participantRows = await fetchParticipantsForChallenge(reviewDbClient, historyEntry.challengeId)
    if (participantRows.length === 0) {
      continue
    }

    const participantIds = participantRows.map((row) => toBigIntUserId(row.userId))
    const stateByUserId = await loadParticipantStates(
      membersClient,
      participantIds,
      normalizedUserId,
      targetState,
      historyEntry,
      participantHistoryByUserId
    )

    const targetStateBeforeRun = cloneState(targetState)
    const participants = participantRows.map((row) => {
      const participantUserId = toBigIntUserId(row.userId)
      const participantState = stateByUserId.get(buildUserStateKey(participantUserId)) || createDefaultState()

      return {
        coderId: String(participantUserId),
        rating: participantState.rating,
        volatility: participantState.volatility,
        numRatings: participantState.numRatings,
        score: normalizeScore(row)
      }
    })

    runQubitsRating(participants)

    const updatedTarget = participants.find((participant) => participant.coderId === String(normalizedUserId))
    if (!updatedTarget) {
      continue
    }

    targetState = cloneState(updatedTarget)
    challengesProcessed += 1
    ratingsUpdated += 1
    recomputedMaxRating = Math.max(recomputedMaxRating, updatedTarget.rating)

    await membersClient.$transaction(async (tx) => {
      await tx.memberStats.upsert({
        where: {
          userId_trackId_typeId: {
            userId: normalizedUserId,
            trackId: TRACK_ID,
            typeId: TYPE_ID
          }
        },
        create: {
          userId: normalizedUserId,
          trackId: TRACK_ID,
          typeId: TYPE_ID,
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          createdBy: RERATE_ACTOR,
          updatedBy: RERATE_ACTOR
        },
        update: {
          rating: updatedTarget.rating,
          volatility: updatedTarget.volatility,
          updatedBy: RERATE_ACTOR
        }
      })

      await upsertHistoryRow(
        tx,
        normalizedUserId,
        historyEntry.challengeId,
        targetStateBeforeRun.numRatings > 0 ? targetStateBeforeRun.rating : null,
        updatedTarget.rating,
        historyEntry.eventDate
      )
    })
  }

  if (ratingsUpdated > 0) {
    await membersClient.$transaction(async (tx) => {
      await refreshMostRecentHistoryFlag(tx, normalizedUserId)
      await updateMaxRating(tx, normalizedUserId, recomputedMaxRating)
    })
  }

  return {
    challengesProcessed,
    ratingsUpdated
  }
}

module.exports = {
  rerateDevTrack
}

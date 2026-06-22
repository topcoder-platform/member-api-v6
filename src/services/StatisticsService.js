/**
 * This service provides operations of statistics.
 */

const _ = require('lodash')
const Joi = require('joi')
const config = require('config')
const helper = require('../common/helper')
const logger = require('../common/logger')
const errors = require('../common/errors')
const prismaManager = require('../common/prisma')
const { Prisma } = prismaManager
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()
const prismaHelper = require('../common/prismaHelper')
const reviewDb = require('../common/reviewDb')
const { resolveChallengeResultRelation } = require('../common/reviewDbHelper')
const { rerateDevTrack } = require('../ratings/developRatingEngine')
const {
  RATING_METADATA_SELECT,
  isChallengeRated
} = require('../ratings/challengeRatingStatus')
const {
  fetchRatingPathParticipantsForChallenge,
  resolveRatingPathParticipantId,
  rerateMmTrack
} = require('../ratings/mmRatingEngine')
const {
  buildRatingPathTypeId,
  challengeMatchesRatingPath,
  getConfiguredRatingPath,
  getConfiguredRatingPathByTypeId,
  normalizeRatingPathConfigs
} = require('../ratings/ratingPathConfig')
const {
  TRACK_NAMES,
  TYPE_NAMES,
  getCanonicalTrackName,
  getCanonicalTypeName,
  loadChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup,
  resolveTrackNameFromLookup,
  resolveTypeNameFromLookup
} = require('../common/statsDimensionHelper')

const DISTRIBUTION_FIELDS = ['track', 'subTrack', 'distribution', 'createdAt', 'updatedAt',
  'createdBy', 'updatedBy']
const DISTRIBUTION_FIELDS_NO_DATE = ['track', 'subTrack', 'distribution']

const HISTORY_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE',
  'createdAt', 'updatedAt', 'createdBy', 'updatedBy']

const MEMBER_STATS_FIELDS = ['userId', 'groupId', 'handle', 'handleLower', 'maxRating',
  'challenges', 'wins', 'DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT', 'createdAt',
  'updatedAt', 'createdBy', 'updatedBy']

const LEGACY_STATS_READ_SOURCE = 'legacy'
const SUPPORTED_STATS_READ_SOURCES = ['unified', LEGACY_STATS_READ_SOURCE]
const DISTRIBUTION_RANGES = _.range(0, 4000, 100)
const DISTRIBUTION_MIN_RATING = 0
const DISTRIBUTION_MAX_RATING_EXCLUSIVE = 4000
const configuredStatsReadSource = _.toLower(String(config.STATS_READ_SOURCE || 'unified').trim())
if (!_.includes(SUPPORTED_STATS_READ_SOURCES, configuredStatsReadSource)) {
  logger.warn(`Invalid STATS_READ_SOURCE='${config.STATS_READ_SOURCE}'. Falling back to 'unified'.`)
}
const USE_LEGACY_STATS_READS = configuredStatsReadSource === LEGACY_STATS_READ_SOURCE
const RATING_SOURCE_DEVELOPMENT = 'DEVELOPMENT_CHALLENGE'
const RATING_SOURCE_DATA_SCIENCE_CHALLENGE = 'DATA_SCIENCE_CHALLENGE'
const RATING_SOURCE_MARATHON_MATCH = 'MARATHON_MATCH'
const RERATE_MARATHON_ACTOR = 'rerate-mm-stats'
const CHALLENGE_TRACK_QUALITY_ASSURANCE = 'QUALITY_ASSURANCE'
const CHALLENGE_WINNER_PLACEMENT_TYPE = 'PLACEMENT'
const CHALLENGE_WINNER_PASSED_REVIEW_TYPE = 'PASSED_REVIEW'
const CHALLENGE_WINNER_HISTORY_TYPES = [CHALLENGE_WINNER_PLACEMENT_TYPE, CHALLENGE_WINNER_PASSED_REVIEW_TYPE]
const CHALLENGE_WINNER_RATING_TYPES = [CHALLENGE_WINNER_PLACEMENT_TYPE]
const LEGACY_DEVELOP_SUBMISSION_FIELDS = [
  'numInquiries',
  'submissions',
  'passedScreening',
  'passedReview',
  'appeals'
]
const LEGACY_DEVELOP_SUBMISSION_RATE_FIELDS = [
  'submissionRate',
  'screeningSuccessRate',
  'reviewSuccessRate',
  'appealSuccessRate',
  'minScore',
  'maxScore',
  'avgScore',
  'avgPlacement',
  'winPercent'
]

/**
 * Join Prisma SQL condition fragments with a literal AND separator.
 * Prisma joins with a Prisma.sql separator stringify that separator to [object Object].
 * @param {Array<Object>} conditions Prisma SQL condition fragments
 * @returns {Object} joined Prisma SQL fragment
 */
function joinSqlConditions (conditions) {
  return Prisma.join(conditions, ' AND ')
}

function toOptionalInt (value) {
  if (_.isNil(value) || value === '') {
    return undefined
  }
  return _.toInteger(value)
}

function toOptionalFloat (value) {
  if (_.isNil(value) || value === '') {
    return undefined
  }
  return Number(value)
}

function toOptionalDate (value) {
  if (_.isNil(value)) {
    return undefined
  }
  return prismaHelper.convertDate(value)
}

/**
 * Normalize request challenge identifiers into the string form documented by the API.
 * Numeric compatibility inputs are echoed back as strings, while omitted values remain null.
 * @param {*} value request challenge identifier
 * @returns {string|null} normalized challenge identifier
 */
function normalizeChallengeIdForResponse (value) {
  if (_.isNil(value)) {
    return null
  }

  return String(value)
}

let challengeDimensionLookupPromise
const legacyChallengePageSummaryPromiseCache = new Map()

const LEGACY_CODE_PAGE_TIMEOUT_MS = 5000
const GENERIC_LEGACY_PAGE_TAG_NAMES = new Set([
  'OTHER',
  'DATA SCIENCE',
  'DEVELOPMENT',
  'DESIGN',
  'QUALITY ASSURANCE',
  'QA',
  'COPILOT'
])

function decodeBasicHtmlEntities (value) {
  if (_.isNil(value)) {
    return null
  }

  return String(value)
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, '\'')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&nbsp;/g, ' ')
    .trim()
}

function safeDecodeUriComponent (value) {
  if (_.isNil(value)) {
    return null
  }

  try {
    return decodeURIComponent(String(value))
  } catch (error) {
    return String(value)
  }
}

function stripLegacyChallengeTitlePrefix (value) {
  if (_.isNil(value)) {
    return null
  }

  return String(value).replace(/^\[[^\]]+\]\s*-\s*/, '').trim()
}

function parseLegacyChallengePageSummary (html, challengeId) {
  const normalizedChallengeId = _.isNil(challengeId) ? null : String(challengeId).trim()
  if (!normalizedChallengeId || !html) {
    return null
  }

  let title = null
  const titleMarker = 'name="twitter:title" content="'
  const titleMarkerIndex = html.indexOf(titleMarker)
  if (titleMarkerIndex >= 0) {
    title = html.slice(titleMarkerIndex + titleMarker.length).split('"', 1)[0]
  }

  if (!title) {
    const headingMarker = '<h1 class="'
    const headingMarkerIndex = html.indexOf(headingMarker)
    if (headingMarkerIndex >= 0) {
      const afterHeadingMarker = html.slice(headingMarkerIndex + headingMarker.length)
      const headingOpenTagEnd = afterHeadingMarker.indexOf('>')
      if (headingOpenTagEnd >= 0) {
        title = afterHeadingMarker.slice(headingOpenTagEnd + 1).split('</h1>', 1)[0]
      }
    }
  }

  const searchTags = []
  const searchTagNeedle = 'href="/challenges?search='
  let searchTagIndex = 0
  while (searchTagIndex >= 0) {
    searchTagIndex = html.indexOf(searchTagNeedle, searchTagIndex)
    if (searchTagIndex < 0) {
      break
    }

    const encodedTag = html
      .slice(searchTagIndex + searchTagNeedle.length)
      .split('"', 1)[0]
      .split('&', 1)[0]
    const decodedTag = decodeBasicHtmlEntities(safeDecodeUriComponent(encodedTag))
    if (decodedTag) {
      searchTags.push(decodedTag)
    }
    searchTagIndex += searchTagNeedle.length
  }

  return {
    challengeId: normalizedChallengeId,
    title: stripLegacyChallengeTitlePrefix(decodeBasicHtmlEntities(title)),
    searchTags: _.uniq(searchTags)
  }
}

function isLegacyCodeChallengePageSummary (summary) {
  if (!summary || !summary.title || summary.title === 'Topcoder') {
    return false
  }

  const specificTags = _.filter(summary.searchTags || [], (tag) => {
    const normalizedTag = String(tag || '').trim().toUpperCase()
    return normalizedTag && !GENERIC_LEGACY_PAGE_TAG_NAMES.has(normalizedTag)
  })

  return specificTags.length > 0
}

/**
 * Fetch one legacy challenge page summary from topcoder.com.
 * This is only used as a narrow fallback for older CODE history rows when
 * neither ChallengeLegacy nor ChallengeWinner data can map legacy review ids.
 * @param {string|number} challengeId legacy numeric challenge identifier
 * @returns {Promise<Object|null>} parsed title/tag summary when available
 */
async function fetchLegacyChallengePageSummary (challengeId) {
  const normalizedChallengeId = _.isNil(challengeId) ? null : String(challengeId).trim()
  if (!normalizedChallengeId || !/^\d+$/.test(normalizedChallengeId) || typeof global.fetch !== 'function') {
    return null
  }

  if (!legacyChallengePageSummaryPromiseCache.has(normalizedChallengeId)) {
    legacyChallengePageSummaryPromiseCache.set(normalizedChallengeId, (async () => {
      try {
        const fetchOptions = {
          headers: {
            'user-agent': 'member-api-v6/legacy-code-history'
          }
        }
        if (typeof global.AbortSignal !== 'undefined' &&
          typeof global.AbortSignal.timeout === 'function') {
          fetchOptions.signal = global.AbortSignal.timeout(LEGACY_CODE_PAGE_TIMEOUT_MS)
        }

        const response = await global.fetch(`https://www.topcoder.com/challenges/${normalizedChallengeId}`, fetchOptions)
        if (!response.ok) {
          logger.warn(`Unable to load legacy challenge page summary for challengeId=${normalizedChallengeId}: status ${response.status}`)
          return null
        }

        return parseLegacyChallengePageSummary(await response.text(), normalizedChallengeId)
      } catch (error) {
        logger.warn(`Unable to load legacy challenge page summary for challengeId=${normalizedChallengeId}: ${error.message}`)
        return null
      }
    })())
  }

  return legacyChallengePageSummaryPromiseCache.get(normalizedChallengeId)
}

/**
 * Load the shared challenge track/type lookup used by unified stats reads and writes.
 * The lookup translates between stored UUID ids and the canonical API labels used
 * by request payloads, filters, and response builders.
 * @returns {Promise<Object>} cached challenge dimension lookup
 */
async function getChallengeDimensionLookup () {
  if (!challengeDimensionLookupPromise) {
    challengeDimensionLookupPromise = loadChallengeDimensionLookup(prismaManager.getChallengesClient())
  }

  return challengeDimensionLookupPromise
}

/**
 * Normalize a track label into the canonical API name used by rerate endpoints.
 * @param {*} trackId raw track label
 * @returns {string|undefined} canonical track name when recognized
 */
function resolveTrackName (trackId) {
  return getCanonicalTrackName(trackId)
}

/**
 * Normalize a type label into the canonical API name used by rerate endpoints.
 * @param {*} typeId raw type label
 * @returns {string|undefined} canonical type name when recognized
 */
function resolveTypeName (typeId) {
  return getCanonicalTypeName(typeId)
}

/**
 * Resolve a configured rating path from the service config.
 * @param {*} ratingName requested rating path name
 * @returns {Object|null} normalized rating path config, or null when no name is supplied
 * @throws {errors.BadRequestError} when the requested rating path is not configured
 */
function resolveConfiguredRatingPath (ratingName) {
  if (_.isNil(ratingName) || String(ratingName).trim() === '') {
    return null
  }

  const ratingPath = getConfiguredRatingPath(config.RATING_PATHS, ratingName)
  if (!ratingPath) {
    throw new errors.BadRequestError(`Rating path '${ratingName}' is not configured.`)
  }

  return ratingPath
}

/**
 * Convert a member identifier into the BigInt shape used by Prisma relations.
 * @param {*} value raw member user id
 * @returns {BigInt} normalized user id
 */
function toBigIntUserId (value) {
  if (Object.prototype.toString.call(value) === '[object BigInt]') {
    return value
  }

  if (typeof global.BigInt !== 'function') {
    throw new Error('BigInt is not supported in this runtime')
  }

  return global.BigInt(String(value).trim())
}

/**
 * Convert a user id into a stable response/cache key.
 * @param {*} value raw user id
 * @returns {string} string user id
 */
function stringifyUserId (value) {
  return String(value)
}

/**
 * Resolve whether challenge metadata allows rating updates.
 * Missing rating metadata defaults to rated, matching the rating engines'
 * historical replay behavior for older challenges.
 * @param {Object} challenge challenge metadata row
 * @returns {boolean} true when the challenge should be considered rated
 */
function isChallengeRatingEnabled (challenge) {
  return isChallengeRated(challenge)
}

/**
 * Normalize challenge track labels for source-routing checks.
 * @param {*} value raw challenge track label, enum value, or abbreviation
 * @returns {string} uppercase source track key
 */
function normalizeChallengeSourceTrack (value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, '_')
}

/**
 * Check whether a challenge source track is Quality Assurance.
 * QA Challenge results are rated in the public Data Science Challenge bucket.
 * @param {*} value raw challenge track label, enum value, or abbreviation
 * @returns {boolean} true when the source track is QA
 */
function isQualityAssuranceChallengeSourceTrack (value) {
  const normalizedTrack = normalizeChallengeSourceTrack(value)
  return normalizedTrack === CHALLENGE_TRACK_QUALITY_ASSURANCE || normalizedTrack === 'QA'
}

/**
 * Build the source tracks replayed for Data Science Challenge ratings.
 * QA Challenges share the public Data Science Challenge rating bucket.
 * @returns {Array<string>} challenge source track labels
 */
function getDataScienceChallengeSourceTrackNames () {
  return [TRACK_NAMES.DATA_SCIENCE, CHALLENGE_TRACK_QUALITY_ASSURANCE]
}

/**
 * Load challenge metadata needed to decide which ratings apply.
 * @param {Object} challengeClient prisma challenge client
 * @param {string|number} challengeId challenge UUID or legacy numeric id
 * @returns {Promise<Object|null>} challenge metadata or null when absent
 */
async function fetchChallengeForRatingUpdate (challengeClient, challengeId) {
  const normalizedChallengeId = String(challengeId || '').trim()
  if (!normalizedChallengeId) {
    return null
  }

  const numericChallengeId = /^\d+$/.test(normalizedChallengeId)
    ? Number(normalizedChallengeId)
    : null
  const where = numericChallengeId && Number.isSafeInteger(numericChallengeId)
    ? {
      OR: [
        { id: normalizedChallengeId },
        { legacyId: numericChallengeId },
        {
          legacyRecord: {
            is: {
              legacySystemId: numericChallengeId
            }
          }
        }
      ]
    }
    : { id: normalizedChallengeId }

  return challengeClient.challenge.findFirst({
    where,
    select: {
      id: true,
      status: true,
      endDate: true,
      trackId: true,
      typeId: true,
      track: {
        select: {
          name: true,
          track: true
        }
      },
      type: {
        select: {
          name: true
        }
      },
      tags: true,
      skills: {
        select: {
          skillId: true
        }
      },
      metadata: RATING_METADATA_SELECT
    }
  })
}

/**
 * Resolve the rating source supported by the engines for a challenge.
 * Marathon Match challenges are type-driven because some Challenge API rows
 * carry the Development track while still belonging to the MM rating stream.
 * @param {Object} challenge challenge metadata row
 * @returns {string|null} source identifier or null when unsupported
 */
function resolveChallengeRatingSource (challenge) {
  const rawTrackName = _.get(challenge, 'track.track') || _.get(challenge, 'track.name') || _.get(challenge, 'trackId')
  const trackName = getCanonicalTrackName(rawTrackName)
  const typeName = getCanonicalTypeName(_.get(challenge, 'type.name') || _.get(challenge, 'typeId'))

  if (trackName === TRACK_NAMES.DEVELOP && typeName === TYPE_NAMES.CHALLENGE) {
    return RATING_SOURCE_DEVELOPMENT
  }

  if ((trackName === TRACK_NAMES.DATA_SCIENCE || isQualityAssuranceChallengeSourceTrack(rawTrackName)) &&
    typeName === TYPE_NAMES.CHALLENGE) {
    return RATING_SOURCE_DATA_SCIENCE_CHALLENGE
  }

  if (typeName === TYPE_NAMES.MARATHON_MATCH) {
    return RATING_SOURCE_MARATHON_MATCH
  }

  return null
}

/**
 * Build the native track/type rating job for a supported challenge.
 * @param {Object} challenge challenge metadata row
 * @param {string|null} source resolved challenge rating source
 * @returns {Object|null} rating job or null when unsupported/unrated
 */
function buildBaseRatingJob (challenge, source) {
  if (!source || !isChallengeRatingEnabled(challenge)) {
    return null
  }

  if (source === RATING_SOURCE_DEVELOPMENT) {
    return {
      source,
      trackId: TRACK_NAMES.DEVELOP,
      typeId: TYPE_NAMES.CHALLENGE
    }
  }

  if (source === RATING_SOURCE_DATA_SCIENCE_CHALLENGE) {
    return {
      source,
      trackId: TRACK_NAMES.DATA_SCIENCE,
      typeId: TYPE_NAMES.CHALLENGE
    }
  }

  if (source === RATING_SOURCE_MARATHON_MATCH) {
    return {
      source,
      trackId: TRACK_NAMES.DATA_SCIENCE,
      typeId: TYPE_NAMES.MARATHON_MATCH
    }
  }

  return null
}

/**
 * Build all rating jobs that apply to one completed challenge.
 * The base track/type job is included for supported rated challenges, and
 * configured named rating paths are included when their tags/skills match.
 * @param {Object} challenge challenge metadata row
 * @returns {Array<Object>} rating jobs to run
 */
function buildChallengeRatingJobs (challenge) {
  const source = resolveChallengeRatingSource(challenge)
  const jobs = []
  const baseJob = buildBaseRatingJob(challenge, source)
  if (baseJob) {
    jobs.push(baseJob)
  }

  if (!source || !isChallengeRatingEnabled(challenge)) {
    return jobs
  }

  normalizeRatingPathConfigs(config.RATING_PATHS).forEach((ratingPath) => {
    if (!challengeMatchesRatingPath(challenge, ratingPath)) {
      return
    }

    jobs.push({
      source,
      trackId: ratingPath.trackName,
      typeId: ratingPath.name,
      ratingName: ratingPath.name,
      ratingPath
    })
  })

  return _.uniqBy(jobs, (job) => `${job.ratingName || ''}::${job.trackId}::${job.typeId}`)
}

/**
 * Fetch review-api challengeResult participants with score or placement data.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|number} challengeId challenge identifier
 * @returns {Promise<Array<BigInt>>} participant user ids
 */
async function fetchChallengeResultParticipantIds (reviewDbClient, challengeId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT DISTINCT "userId"
      FROM ${challengeResultRelation}
      WHERE "challengeId" = $1
        AND "userId" IS NOT NULL
        AND "validSubmission" IS DISTINCT FROM FALSE
        AND "submissionId" IS NOT NULL
        AND (
          "finalScore" IS NOT NULL OR
          ("placement" IS NOT NULL AND "placement" > 0)
        )
      ORDER BY "userId" ASC
    `,
    [String(challengeId)]
  )

  return result.rows.map((row) => toBigIntUserId(row.userId))
}

/**
 * Fetch placement winner participants from challenge-api for completed
 * Development/Data Science rating rerates. This covers challenges where winners
 * can be assigned without a review-api challengeResult row for the same member.
 * @param {Object} challengeClient challenge Prisma client
 * @param {string|number} challengeId challenge identifier
 * @returns {Promise<Array<BigInt>>} winner user ids
 */
async function fetchChallengeWinnerParticipantIds (challengeClient, challengeId) {
  if (!challengeClient || !challengeClient.ChallengeWinner ||
    typeof challengeClient.ChallengeWinner.findMany !== 'function') {
    return []
  }

  const winnerRows = await challengeClient.ChallengeWinner.findMany({
    where: {
      challengeId: String(challengeId),
      type: {
        in: CHALLENGE_WINNER_RATING_TYPES
      }
    },
    select: {
      userId: true
    }
  })

  return winnerRows.map((row) => toBigIntUserId(row.userId))
}

/**
 * Fetch Marathon Match participants from review summations when challengeResult
 * rows are not available yet.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {string|number} challengeId challenge identifier
 * @returns {Promise<Array<BigInt>>} participant user ids
 */
async function fetchMarathonMatchParticipantIds (reviewDbClient, challengeId) {
  const { participantRows } = await fetchRatingPathParticipantsForChallenge(
    reviewDbClient,
    {
      challengeId: String(challengeId),
      source: RATING_SOURCE_MARATHON_MATCH
    }
  )

  return participantRows.map((row) => resolveRatingPathParticipantId(row, RATING_SOURCE_MARATHON_MATCH))
}

/**
 * Resolve submitter ids for the challenge and rating source.
 * Challenge ratings include placement winners so completed challenges without
 * challengeResult rows still rerate paid winners. Marathon Match submitters
 * are loaded from both challengeResult and final review summations so partially
 * synced result rows cannot omit lower-placed participants from rerating.
 * @param {Object} reviewDbClient raw pg review database client
 * @param {Object} challengeClient challenge Prisma client
 * @param {string|number} challengeId challenge identifier
 * @param {string} source rating source identifier
 * @returns {Promise<Array<BigInt>>} unique participant user ids
 */
async function fetchRatingParticipantIds (reviewDbClient, challengeClient, challengeId, source) {
  const challengeResultUserIds = await fetchChallengeResultParticipantIds(reviewDbClient, challengeId)
  if (source === RATING_SOURCE_DEVELOPMENT || source === RATING_SOURCE_DATA_SCIENCE_CHALLENGE) {
    return _.uniqBy(
      challengeResultUserIds.concat(await fetchChallengeWinnerParticipantIds(challengeClient, challengeId)),
      stringifyUserId
    )
  }

  if (source !== RATING_SOURCE_MARATHON_MATCH) {
    return _.uniqBy(challengeResultUserIds, stringifyUserId)
  }

  return _.uniqBy(
    challengeResultUserIds.concat(await fetchMarathonMatchParticipantIds(reviewDbClient, challengeId)),
    stringifyUserId
  )
}

/**
 * Filter discovered submitters down to members that exist in member-api storage.
 * @param {Object} membersClient prisma members client
 * @param {Array<BigInt>} participantIds discovered submitter ids
 * @returns {Promise<Object>} existing member ids and skipped ids
 */
async function filterExistingRatingParticipantIds (membersClient, participantIds) {
  const uniqueParticipantIds = _.uniqBy(participantIds, stringifyUserId)
  if (uniqueParticipantIds.length === 0) {
    return {
      existingParticipantIds: [],
      skippedParticipantIds: []
    }
  }

  const existingMembers = await membersClient.member.findMany({
    where: {
      userId: {
        in: uniqueParticipantIds
      }
    },
    select: {
      userId: true
    }
  })
  const existingIds = existingMembers.map((member) => member.userId)
  const existingIdSet = new Set(existingIds.map(stringifyUserId))

  return {
    existingParticipantIds: existingIds,
    skippedParticipantIds: uniqueParticipantIds.filter((userId) => !existingIdSet.has(stringifyUserId(userId)))
  }
}

/**
 * Run one rating job for one member.
 * @param {Object} challengeClient prisma challenge client
 * @param {Object} reviewDbClient raw pg review database client
 * @param {BigInt} userId target member id
 * @param {string} challengeId starting challenge id
 * @param {Object} job rating job to execute
 * @returns {Promise<Object>} engine rerate summary
 */
async function rerateChallengeRatingJobForMember (challengeClient, reviewDbClient, userId, challengeId, job) {
  if (job.ratingPath) {
    return rerateMmTrack(
      prisma,
      challengeClient,
      null,
      reviewDbClient,
      userId,
      challengeId,
      {
        ratingPath: job.ratingPath
      }
    )
  }

  if (job.source === RATING_SOURCE_DEVELOPMENT ||
    job.source === RATING_SOURCE_DATA_SCIENCE_CHALLENGE) {
    const rerateOptions = job.source === RATING_SOURCE_DATA_SCIENCE_CHALLENGE
      ? {
        targetTrackName: TRACK_NAMES.DATA_SCIENCE,
        targetTypeName: TYPE_NAMES.CHALLENGE,
        challengeTrackNames: getDataScienceChallengeSourceTrackNames(),
        challengeTypeNames: [TYPE_NAMES.CHALLENGE]
      }
      : undefined

    return rerateDevTrack(
      prisma,
      challengeClient,
      reviewDbClient,
      userId,
      challengeId,
      rerateOptions
    )
  }

  return rerateMmTrack(
    prisma,
    challengeClient,
    null,
    reviewDbClient,
    userId,
    challengeId
  )
}

function isLegacyMaxRatingPayload (value) {
  return _.isPlainObject(value) && !_.isNil(value.rating) && !_.isNil(value.ratingColor)
}

function normalizeUnifiedRecord (record, isPrivate, dimensionLookup) {
  if (!record || !record.trackId || !record.typeId) {
    return null
  }

  const normalized = _.omitBy({
    trackId: resolveTrackIdFromLookup(dimensionLookup, record.trackId),
    typeId: resolveTypeIdFromLookup(dimensionLookup, record.typeId),
    challenges: toOptionalInt(record.challenges),
    wins: toOptionalInt(record.wins),
    mostRecentSubmission: toOptionalDate(record.mostRecentSubmission),
    mostRecentEventDate: toOptionalDate(record.mostRecentEventDate),
    rating: toOptionalInt(record.rating),
    avgRank: toOptionalFloat(record.avgRank),
    avgNumSubmissions: toOptionalInt(record.avgNumSubmissions),
    bestRank: toOptionalInt(record.bestRank),
    globalRank: toOptionalInt(record.globalRank),
    countryRank: toOptionalInt(record.countryRank),
    schoolRank: toOptionalInt(record.schoolRank),
    volatility: toOptionalInt(record.volatility),
    maxRating: toOptionalInt(record.maxRating),
    minRating: toOptionalInt(record.minRating),
    topFiveFinishes: toOptionalInt(record.topFiveFinishes),
    topTenFinishes: toOptionalInt(record.topTenFinishes),
    isPrivate
  }, _.isUndefined)

  if (!normalized.trackId || !normalized.typeId) {
    return null
  }

  return normalized
}

function pushUnifiedRecord (collection, record, isPrivate, dimensionLookup) {
  const normalized = normalizeUnifiedRecord(record, isPrivate, dimensionLookup)
  if (normalized) {
    collection.push(normalized)
  }
}

function buildUnifiedStatsRecordsFromPayload (payload, isPrivate, dimensionLookup, options = {}) {
  const data = payload || {}
  const records = []
  const isPartial = !!options.partial
  const unifiedMaxRating = isLegacyMaxRatingPayload(data.maxRating) ? undefined : data.maxRating

  const rootPayload = {
    trackId: data.trackId,
    typeId: data.typeId,
    challenges: data.challenges,
    wins: data.wins,
    mostRecentSubmission: data.mostRecentSubmission,
    mostRecentEventDate: data.mostRecentEventDate,
    rating: data.rating,
    avgRank: data.avgRank,
    avgNumSubmissions: data.avgNumSubmissions,
    bestRank: data.bestRank,
    globalRank: data.globalRank,
    countryRank: data.countryRank,
    schoolRank: data.schoolRank,
    volatility: data.volatility,
    maxRating: unifiedMaxRating,
    minRating: data.minRating,
    topFiveFinishes: data.topFiveFinishes,
    topTenFinishes: data.topTenFinishes
  }

  if (rootPayload.trackId && rootPayload.typeId) {
    pushUnifiedRecord(records, rootPayload, isPrivate, dimensionLookup)
  }

  if (_.isArray(data.records)) {
    _.forEach(data.records, (record) => {
      pushUnifiedRecord(records, record, isPrivate, dimensionLookup)
    })
  }

  if (!isPartial && records.length === 0 && (!_.isNil(data.challenges) || !_.isNil(data.wins))) {
    pushUnifiedRecord(records, {
      trackId: data.trackId || TRACK_NAMES.DEVELOP,
      typeId: data.typeId || TYPE_NAMES.CHALLENGE,
      challenges: data.challenges,
      wins: data.wins,
      mostRecentSubmission: data.mostRecentSubmission,
      mostRecentEventDate: data.mostRecentEventDate,
      rating: data.rating,
      avgRank: data.avgRank,
      avgNumSubmissions: data.avgNumSubmissions,
      bestRank: data.bestRank,
      globalRank: data.globalRank,
      countryRank: data.countryRank,
      schoolRank: data.schoolRank,
      volatility: data.volatility,
      maxRating: unifiedMaxRating,
      minRating: data.minRating,
      topFiveFinishes: data.topFiveFinishes,
      topTenFinishes: data.topTenFinishes
    }, isPrivate, dimensionLookup)
  }

  // Last record wins for duplicate (trackId, typeId) keys.
  return _.values(_.keyBy(records, record => `${record.trackId}::${record.typeId}`))
}

function buildStatsTrackTypeKey (trackId, typeId) {
  return `${trackId}::${typeId}`
}

/**
 * Determine whether a unified stats row needs a computed global rank fallback.
 * Only positive ratings are rankable; missing, zero, and negative persisted ranks
 * are treated as unavailable because public legacy marathon data often stores
 * unrated rank placeholders as zero.
 * @param {Object} row unified memberStats row
 * @returns {Boolean} true when the row can be ranked from its rating
 */
function shouldComputeGlobalRank (row) {
  if (!row || !row.trackId || !row.typeId || _.isNil(row.rating)) {
    return false
  }

  const rating = Number(row.rating)
  const globalRank = _.isNil(row.globalRank) ? null : Number(row.globalRank)

  return Number.isFinite(rating) && rating > 0 &&
    (_.isNil(globalRank) || !Number.isFinite(globalRank) || globalRank <= 0)
}

/**
 * Build the cache key for a stats row's rank scope.
 * Rows sharing track, type, privacy, and rating share the same computed rank.
 * @param {Object} row unified memberStats row
 * @returns {String} cache key for computed rank lookups
 */
function buildGlobalRankScopeKey (row) {
  return [
    row.trackId,
    row.typeId,
    row.isPrivate ? 'private' : 'public',
    Number(row.rating)
  ].join('::')
}

/**
 * Fill invalid or missing globalRank values using current unified ratings.
 * The computed value matches SQL RANK semantics: one plus the number of rows in
 * the same track/type/privacy scope with a strictly higher positive rating.
 * @param {Array<Object>} statsRows unified memberStats rows returned for one member
 * @returns {Promise<Array<Object>>} rows with computed globalRank fallbacks applied
 * @throws {Error} propagates Prisma count failures
 */
async function hydrateComputedGlobalRanks (statsRows) {
  const rankTargets = _.filter(statsRows, shouldComputeGlobalRank)
  if (rankTargets.length === 0) {
    return statsRows
  }

  const uniqueRankTargets = _.uniqBy(rankTargets, buildGlobalRankScopeKey)
  const rankByScope = new Map()

  await Promise.all(_.map(uniqueRankTargets, async (row) => {
    const higherRatedCount = await prisma.memberStats.count({
      where: {
        trackId: row.trackId,
        typeId: row.typeId,
        isPrivate: row.isPrivate === true,
        rating: {
          gt: Number(row.rating)
        }
      }
    })

    rankByScope.set(buildGlobalRankScopeKey(row), higherRatedCount + 1)
  }))

  return _.map(statsRows, (row) => {
    const computedRank = shouldComputeGlobalRank(row)
      ? rankByScope.get(buildGlobalRankScopeKey(row))
      : undefined

    if (_.isNil(computedRank)) {
      return row
    }

    return {
      ...row,
      globalRank: computedRank
    }
  })
}

/**
 * Check whether a resolved challenge type is Marathon Match.
 * @param {string|undefined} typeName canonical or raw type name
 * @returns {boolean} true when the type should be exposed as Marathon Match
 */
function isMarathonMatchType (typeName) {
  return getCanonicalTypeName(typeName) === TYPE_NAMES.MARATHON_MATCH
}

/**
 * Resolve the public stats dimensions for a challenge-backed row.
 * Marathon Match is part of the public DATA_SCIENCE bucket even when imported
 * challenge metadata has a Development track.
 * @param {Object} row row containing trackId and typeId
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Object} normalized track/type ids and names
 */
function resolveStatsDimensionForChallengeRow (row, dimensionLookup) {
  const typeName = resolveTypeNameFromLookup(dimensionLookup, row.typeId)
  if (isMarathonMatchType(typeName)) {
    const dataScienceTrackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAMES.DATA_SCIENCE)
    return {
      trackId: dataScienceTrackId || row.trackId,
      typeId: row.typeId,
      trackName: TRACK_NAMES.DATA_SCIENCE,
      typeName: TYPE_NAMES.MARATHON_MATCH
    }
  }

  return {
    trackId: row.trackId,
    typeId: row.typeId,
    trackName: resolveTrackNameFromLookup(dimensionLookup, row.trackId),
    typeName
  }
}

/**
 * Resolve a legacy Development stats item to the unified ChallengeType id.
 * The legacy item can expose either a name such as FIRST_2_FINISH or a seeded
 * subTrackId, so both are tried against the challenge dimension lookup.
 * @param {Object} item legacy memberDevelopStatsItem row
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {string|undefined} unified ChallengeType id when resolvable
 */
function resolveLegacyDevelopStatsItemTypeId (item, dimensionLookup) {
  const candidates = [
    item && item.name,
    resolveTypeName(item && item.name),
    item && item.subTrackId
  ]

  for (const candidate of candidates) {
    const typeId = resolveTypeIdFromLookup(dimensionLookup, candidate)
    if (typeId) {
      return typeId
    }
  }

  return undefined
}

/**
 * Build actual legacy Development submission counters keyed by unified type id.
 * @param {Object|null} legacyStats latest legacy-shaped member stats row
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Map<string, Object>} submission counters keyed by ChallengeType id
 */
function buildLegacyDevelopSubmissionStatsByTypeId (legacyStats, dimensionLookup) {
  const lookup = new Map()
  const items = _.get(legacyStats, 'develop.items', [])

  _.forEach(items, (item) => {
    const typeId = resolveLegacyDevelopStatsItemTypeId(item, dimensionLookup)
    if (!typeId) {
      return
    }

    const submissionStats = {}
    LEGACY_DEVELOP_SUBMISSION_FIELDS.forEach((field) => {
      const value = toOptionalInt(item[field])
      if (!_.isNil(value)) {
        submissionStats[field] = value
      }
    })
    LEGACY_DEVELOP_SUBMISSION_RATE_FIELDS.forEach((field) => {
      const value = toOptionalFloat(item[field])
      if (!_.isNil(value)) {
        submissionStats[field] = value
      }
    })
    submissionStats.challenges = toOptionalInt(item.challenges)
    lookup.set(typeId, submissionStats)
  })

  return lookup
}

/**
 * Attach actual legacy Development submission counters to unified stats rows.
 * Unified rows store challenge counts, while the legacy child tables retain true
 * submission counts. When a unified row has newer review-result supplements, the
 * count beyond the legacy challenge baseline is added as one submission per row.
 * @param {Object} member member row
 * @param {String|Number} groupId requested group id
 * @param {Array<Object>} statsRows unified memberStats rows
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Promise<Array<Object>>} rows annotated with Development submission fields
 */
async function hydrateLegacyDevelopSubmissionStats (member, groupId, statsRows, dimensionLookup) {
  if (String(groupId) !== String(config.PUBLIC_GROUP_ID) || !statsRows || statsRows.length === 0) {
    return statsRows
  }

  const legacyStats = await getLegacyMemberStatsRow(member.userId, groupId)
  const legacySubmissionStatsByTypeId = buildLegacyDevelopSubmissionStatsByTypeId(legacyStats, dimensionLookup)
  if (legacySubmissionStatsByTypeId.size === 0) {
    return statsRows
  }

  const developTrackId = resolveTrackIdFromLookup(dimensionLookup, TRACK_NAMES.DEVELOP)

  return _.map(statsRows, (row) => {
    if (String(row.trackId) !== String(developTrackId)) {
      return row
    }

    const legacySubmissionStats = legacySubmissionStatsByTypeId.get(String(row.typeId))
    if (!legacySubmissionStats) {
      return row
    }

    const rowChallengeCount = toOptionalInt(row.challenges) || 0
    const legacyChallengeCount = toOptionalInt(legacySubmissionStats.challenges) || 0
    const supplementalSubmissionCount = Math.max(0, rowChallengeCount - legacyChallengeCount)
    const legacySubmissionCount = toOptionalInt(legacySubmissionStats.submissions)
    const submissions = _.isNil(legacySubmissionCount)
      ? undefined
      : legacySubmissionCount + supplementalSubmissionCount

    return _.omitBy({
      ...row,
      ..._.omit(legacySubmissionStats, ['challenges']),
      submissions
    }, _.isUndefined)
  })
}

/**
 * Fill missing unified aggregate win counters from rating history placements.
 * Existing explicit zero values are preserved; only null/undefined wins are
 * hydrated. This keeps stale configured rating-path rows from showing zero wins
 * when their memberStatsHistory rows already prove first-place finishes.
 * @param {BigInt} userId member identifier
 * @param {Array<Object>} statsRows unified memberStats rows
 * @returns {Promise<Array<Object>>} stats rows with missing wins hydrated
 */
async function hydrateMissingWinsFromHistory (userId, statsRows) {
  const targetRows = _.uniqBy(
    _.filter(statsRows || [], row => row && row.trackId && row.typeId && row.isPrivate !== true && _.isNil(row.wins)),
    row => buildStatsTrackTypeKey(row.trackId, row.typeId)
  )

  if (targetRows.length === 0) {
    return statsRows
  }

  const historyRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId,
      placement: 1,
      OR: _.map(targetRows, row => ({
        trackId: row.trackId,
        typeId: row.typeId
      }))
    },
    select: {
      trackId: true,
      typeId: true,
      placement: true
    }
  })

  const winsByPairKey = _.countBy(
    _.filter(historyRows || [], row => _.toInteger(row.placement) === 1),
    row => buildStatsTrackTypeKey(row.trackId, row.typeId)
  )

  return _.map(statsRows || [], (row) => {
    if (!row || !row.trackId || !row.typeId || row.isPrivate === true || !_.isNil(row.wins)) {
      return row
    }

    return {
      ...row,
      wins: winsByPairKey[buildStatsTrackTypeKey(row.trackId, row.typeId)] || 0
    }
  })
}

function getReviewDbClientOrThrow () {
  if (!reviewDb) {
    throw new Error('REVIEW_DB_URL must be configured to refresh or rerate member stats')
  }

  return reviewDb
}

/**
 * Determine whether a review-api challengeResult row represents a real submission.
 * Rows explicitly marked invalid, or rows from queries that expose an empty
 * submissionId, are placeholders and should not create stats/history activity.
 * Older in-memory callers that do not provide submission fields are treated as
 * unknown instead of invalid so legacy fallback tests can still exercise mapping.
 * @param {Object} row raw challengeResult row
 * @returns {boolean} true when the row can be used for stats/history fallback
 */
function isUsableReviewChallengeResultRow (row) {
  if (!row || row.validSubmission === false) {
    return false
  }

  if (Object.prototype.hasOwnProperty.call(row, 'submissionId')) {
    const submissionId = _.isNil(row.submissionId) ? '' : String(row.submissionId).trim()
    if (!submissionId) {
      return false
    }
  }

  return true
}

async function fetchReviewChallengeResultsForMember (reviewDbClient, userId) {
  const challengeResultRelation = await resolveChallengeResultRelation(reviewDbClient)
  const result = await reviewDbClient.query(
    `
      SELECT "challengeId", "userId", "submissionId", "finalScore",
             "placement", "rated", "validSubmission", "createdAt"
      FROM ${challengeResultRelation}
      WHERE "userId" = $1
        AND "validSubmission" IS DISTINCT FROM FALSE
        AND "submissionId" IS NOT NULL
      ORDER BY "createdAt" ASC
    `,
    [userId.toString()]
  )

  return result.rows
}

/**
 * Load placement-bearing winner rows for one member from challenge-api.
 * These rows provide a fallback history source for unrated tracks such as
 * First2Finish and MM imports whose history source is ChallengeWinner.
 * @param {Object} challengeClient prisma challenge client
 * @param {BigInt} userId member user id
 * @returns {Promise<Array<Object>>} winner rows with embedded challenge metadata
 */
async function fetchChallengeWinnerResultsForMember (challengeClient, userId) {
  try {
    return await challengeClient.ChallengeWinner.findMany({
      where: {
        userId: helper.bigIntToNumber(userId),
        type: {
          in: CHALLENGE_WINNER_HISTORY_TYPES
        }
      },
      select: {
        challengeId: true,
        type: true,
        placement: true,
        createdAt: true,
        challenge: {
          select: {
            id: true,
            legacyId: true,
            name: true,
            status: true,
            trackId: true,
            typeId: true,
            endDate: true,
            legacyRecord: {
              select: {
                legacySystemId: true
              }
            },
            winners: {
              where: {
                type: CHALLENGE_WINNER_PLACEMENT_TYPE
              },
              select: {
                placement: true
              },
              orderBy: {
                placement: 'asc'
              }
            }
          }
        }
      }
    })
  } catch (error) {
    logger.warn(`Unable to load challenge winner fallback rows for userId=${userId.toString()}: ${error.message}`)
    return []
  }
}

/**
 * Load challenge metadata keyed by both canonical UUID id and legacy numeric id.
 * Unified history rows may still carry legacy challenge identifiers from migrated
 * data, so callers can resolve names and canonical UUIDs without mutating storage.
 * @param {Object} challengeClient prisma challenge client
 * @param {Array<*>} challengeIds challenge identifiers from stats/history rows
 * @returns {Promise<Map<string, Object>>} metadata keyed by UUID and legacy id strings
 */
async function fetchChallengeMetadataMap (challengeClient, challengeIds) {
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

  const whereClauses = [{
    id: {
      in: normalizedChallengeIds
    }
  }]

  if (numericChallengeIds.length > 0) {
    whereClauses.push({
      legacyId: {
        in: numericChallengeIds
      }
    })
    whereClauses.push({
      legacyRecord: {
        is: {
          legacySystemId: {
            in: numericChallengeIds
          }
        }
      }
    })
  }

  const challenges = await challengeClient.challenge.findMany({
    where: whereClauses.length === 1 ? whereClauses[0] : { OR: whereClauses },
    select: {
      id: true,
      legacyId: true,
      name: true,
      status: true,
      trackId: true,
      typeId: true,
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
      },
      metadata: RATING_METADATA_SELECT,
      legacyRecord: {
        select: {
          legacySystemId: true
        }
      }
    }
  })

  const metadataByChallengeId = new Map()
  _.forEach(challenges, (challenge) => {
    metadataByChallengeId.set(String(challenge.id), challenge)
    if (!_.isNil(challenge.legacyId)) {
      metadataByChallengeId.set(String(challenge.legacyId), challenge)
    }
    if (!_.isNil(_.get(challenge, 'legacyRecord.legacySystemId'))) {
      metadataByChallengeId.set(String(challenge.legacyRecord.legacySystemId), challenge)
    }
  })

  return metadataByChallengeId
}

/**
 * Determine whether challenge metadata represents a completed challenge.
 * @param {Object} challenge challenge metadata row
 * @returns {boolean} true when the challenge status is COMPLETED
 */
function isCompletedChallenge (challenge) {
  return String(_.get(challenge, 'status') || '').trim().toUpperCase() === 'COMPLETED'
}

/**
 * Drop persisted history rows when challenge metadata proves the challenge is not completed.
 * Rows without matching challenge metadata are kept so legacy history can still surface.
 * @param {Array<Object>} rows unified history rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @returns {Array<Object>} history rows limited to completed challenges when status is known
 */
function filterUnifiedHistoryRowsToCompletedChallenges (rows, challengeMetadataById) {
  return _.filter(rows || [], (row) => {
    const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
    if (!challengeId) {
      return true
    }

    const challenge = challengeMetadataById.get(challengeId)
    if (!challenge) {
      return true
    }

    return isCompletedChallenge(challenge)
  })
}

/**
 * Attach canonical challenge ids and names to unified history rows before shaping
 * the response payload consumed by the profiles UI.
 * @param {Array<Object>} rows unified history rows loaded from members.memberStatsHistory
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @returns {Array<Object>} rows enriched with canonical challenge ids and names when available
 */
function enrichUnifiedHistoryRowsWithChallengeMetadata (rows, challengeMetadataById) {
  return _.map(rows || [], (row) => {
    const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
    if (!challengeId) {
      return row
    }

    const challenge = challengeMetadataById.get(challengeId)
    if (!challenge) {
      return row
    }

    const canonicalChallengeId = String(challenge.id)
    const legacyChallengeId = normalizeChallengeLookupKey(
      !_.isNil(challenge.legacyId)
        ? challenge.legacyId
        : _.get(challenge, 'legacyRecord.legacySystemId')
    )
    const preserveLegacyChallengeId = isLegacyNumericMarathonHistoryRow(row)

    return {
      ...row,
      challengeId: preserveLegacyChallengeId ? challengeId : canonicalChallengeId,
      canonicalChallengeId,
      legacyChallengeId,
      challengeName: row.challengeName || challenge.name
    }
  })
}

/**
 * Normalize placement values so only positive integer rankings are surfaced.
 * A zero placement is not meaningful in the profile history UI and is treated
 * as missing data that can be backfilled from challenge winners.
 * @param {*} value raw placement value
 * @returns {number|undefined} positive placement when available
 */
function toVisiblePlacement (value) {
  const placement = toOptionalInt(value)

  return Number.isInteger(placement) && placement > 0 ? placement : undefined
}

/**
 * Determine the absolute placement offset for a passed-review winner row.
 * ChallengeWinner stores PASSED_REVIEW placements relative to the passed-review
 * bucket, so visible rank must be shifted by the highest paid placement rank.
 * @param {Object} row challenge winner row with nested challenge placement winners
 * @returns {number} placement offset to add to PASSED_REVIEW placements
 */
function getPassedReviewPlacementOffset (row) {
  const placementWinners = _.get(row, 'challenge.winners')
  if (!_.isArray(placementWinners) || placementWinners.length === 0) {
    return 0
  }

  const placements = _.chain(placementWinners)
    .map(winner => toVisiblePlacement(winner.placement))
    .filter(placement => Number.isInteger(placement))
    .value()

  return placements.length > 0 ? _.max(placements) : 0
}

/**
 * Normalize a ChallengeWinner placement into the overall visible challenge rank.
 * PLACEMENT rows are already absolute; PASSED_REVIEW rows are offset by the
 * highest paid placement rank when that context is available.
 * @param {Object} row challenge winner row
 * @returns {number|undefined} visible placement when available
 */
function toVisibleChallengeWinnerPlacement (row) {
  const placement = toVisiblePlacement(row && row.placement)
  if (!placement) {
    return undefined
  }

  const winnerType = String(row.type || '').trim().toUpperCase()
  if (winnerType !== CHALLENGE_WINNER_PASSED_REVIEW_TYPE) {
    return placement
  }

  return placement + getPassedReviewPlacementOffset(row)
}

/**
 * Determine whether any history rows are missing a usable placement value.
 * @param {Array<Object>} rows history rows already shaped for response building
 * @returns {boolean} true when a row still needs placement enrichment
 */
function historyRowsNeedPlacementEnrichment (rows) {
  return _.some(rows || [], row => !_.isNil(row.challengeId) && !toVisiblePlacement(row.placement))
}

/**
 * Determine whether Marathon Match placements should be verified against ChallengeWinner.
 * Rerated canonical MM rows already store final-standing placement computed
 * from MM result scores, while ChallengeWinner rows can represent payment
 * placement and must not replace those rerated standings.
 * @param {Array<Object>} rows history rows already shaped for response building
 * @returns {boolean} true when MM placement verification should be attempted
 */
function marathonHistoryRowsNeedPlacementVerification (rows) {
  return _.some(rows || [], row =>
    !_.isNil(row.challengeId) &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH &&
    !(isReratedCanonicalMarathonHistoryRow(row) && toVisiblePlacement(row.placement))
  )
}

/**
 * Determine whether any history rows are missing a challenge display name.
 * @param {Array<Object>} rows history rows already shaped for response building
 * @returns {boolean} true when a row still needs challenge name enrichment
 */
function historyRowsNeedChallengeNameEnrichment (rows) {
  return _.some(rows || [], row => !_.isNil(row.challengeId) && !row.challengeName)
}

/**
 * Normalize one challenge lookup key for metadata maps.
 * @param {*} value challenge identifier candidate
 * @returns {string|null} trimmed challenge identifier, or null when unavailable
 */
function normalizeChallengeLookupKey (value) {
  if (_.isNil(value)) {
    return null
  }

  const normalized = String(value).trim()
  return normalized || null
}

/**
 * Build all challenge id aliases exposed by one challenge winner row.
 * @param {Object} row challenge winner row with embedded challenge metadata
 * @returns {Array<string>} unique challenge identifier candidates
 */
function buildChallengeWinnerChallengeIdCandidates (row) {
  return _.chain([
    row && row.challengeId,
    _.get(row, 'challenge.id'),
    _.get(row, 'challenge.legacyId'),
    _.get(row, 'challenge.legacyRecord.legacySystemId')
  ])
    .map(normalizeChallengeLookupKey)
    .filter(Boolean)
    .uniq()
    .value()
}

/**
 * Build a canonical challengeId -> placement lookup from challenge winner rows.
 * When duplicate winner rows exist, keep the best available placement.
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Map<string, number>} canonical challenge placements by challenge id
 */
function buildChallengeWinnerPlacementLookup (winnerRows) {
  const placementByChallengeId = new Map()

  _.forEach(winnerRows || [], (row) => {
    const placement = toVisibleChallengeWinnerPlacement(row)
    const challengeKeys = buildChallengeWinnerChallengeIdCandidates(row)

    if (!placement || challengeKeys.length === 0) {
      return
    }

    _.forEach(challengeKeys, (challengeKey) => {
      const existingPlacement = placementByChallengeId.get(challengeKey)
      if (_.isNil(existingPlacement) || placement < existingPlacement) {
        placementByChallengeId.set(challengeKey, placement)
      }
    })
  })

  return placementByChallengeId
}

/**
 * Fill or correct persisted placements from challenge-api winner rows.
 * Rerated canonical Marathon Match placements are preserved because they are
 * computed from final-standing scores; ChallengeWinner can store paid/winner
 * placement for MM and is only a fallback for non-rerated or missing rows.
 * @param {Array<Object>} rows persisted and/or synthesized history rows
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Array<Object>} history rows with corrected placements when available
 */
function mergeHistoryPlacementsFromChallengeWinners (rows, winnerRows) {
  const placementByChallengeId = buildChallengeWinnerPlacementLookup(winnerRows)

  if (placementByChallengeId.size === 0) {
    return rows || []
  }

  return _.map(rows || [], (row) => {
    const challengeKey = normalizeChallengeLookupKey(row.challengeId)
    const placement = challengeKey ? placementByChallengeId.get(challengeKey) : undefined
    const existingPlacement = toVisiblePlacement(row.placement)

    if ((isReratedCanonicalMarathonHistoryRow(row) && existingPlacement) ||
      !placement ||
      existingPlacement === placement) {
      return row
    }

    return {
      ...row,
      placement
    }
  })
}

/**
 * Build challenge metadata keyed by all aliases exposed in challenge winner rows.
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Map<string, Object>} challenge metadata keyed by canonical and legacy ids
 */
function buildChallengeWinnerMetadataLookup (winnerRows) {
  const metadataByChallengeId = new Map()

  _.forEach(winnerRows || [], (row) => {
    const challenge = row && row.challenge
    const challengeName = _.get(row, 'challenge.name')
    const canonicalChallengeId = normalizeChallengeLookupKey(_.get(row, 'challenge.id') || row.challengeId)
    const challengeKeys = buildChallengeWinnerChallengeIdCandidates(row)

    if (!challenge || !isCompletedChallenge(challenge) || !challengeName ||
      !canonicalChallengeId || challengeKeys.length === 0) {
      return
    }

    const metadata = {
      challengeId: canonicalChallengeId,
      challengeName
    }
    _.forEach(challengeKeys, (challengeKey) => {
      metadataByChallengeId.set(challengeKey, metadata)
    })
  })

  return metadataByChallengeId
}

/**
 * Fill missing challenge names from challenge-api winner rows.
 * @param {Array<Object>} rows persisted and/or synthesized history rows
 * @param {Array<Object>} winnerRows placement winner rows from challenge-api
 * @returns {Array<Object>} history rows with names and canonical ids when available
 */
function mergeHistoryChallengeMetadataFromChallengeWinners (rows, winnerRows) {
  const metadataByChallengeId = buildChallengeWinnerMetadataLookup(winnerRows)

  if (metadataByChallengeId.size === 0) {
    return rows || []
  }

  return _.map(rows || [], (row) => {
    const challengeKey = normalizeChallengeLookupKey(row.challengeId)
    const metadata = challengeKey ? metadataByChallengeId.get(challengeKey) : null

    if (!metadata) {
      return row
    }

    return {
      ...row,
      challengeId: metadata.challengeId,
      challengeName: row.challengeName || metadata.challengeName
    }
  })
}

/**
 * Aggregate completed review-api results into unified stats rows.
 * @param {Array<Object>} reviewRows review-api challenge result rows
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by challenge id
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Array<Object>} normalized aggregate rows
 */
function buildAggregatedStatsFromReviewResults (reviewRows, challengeMetadataById, dimensionLookup) {
  const aggregateByKey = new Map()

  _.forEach(reviewRows, (row) => {
    if (!isUsableReviewChallengeResultRow(row)) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const dimension = resolveStatsDimensionForChallengeRow({
      trackId: String(challenge.trackId),
      typeId: String(challenge.typeId)
    }, dimensionLookup)
    const trackId = dimension.trackId
    const typeId = dimension.typeId
    if (!trackId || !typeId) {
      return
    }

    const key = buildStatsTrackTypeKey(trackId, typeId)
    const existing = aggregateByKey.get(key) || {
      trackId,
      typeId,
      challenges: 0,
      wins: 0,
      mostRecentSubmission: null,
      mostRecentEventDate: null
    }

    existing.challenges += 1
    if (_.toInteger(row.placement) === 1) {
      existing.wins += 1
    }

    const submissionDate = row.createdAt ? new Date(row.createdAt) : null
    const eventDate = challenge.endDate ? new Date(challenge.endDate) : submissionDate

    if (submissionDate && !Number.isNaN(submissionDate.getTime()) &&
      (!existing.mostRecentSubmission || submissionDate > existing.mostRecentSubmission)) {
      existing.mostRecentSubmission = submissionDate
    }

    if (eventDate && !Number.isNaN(eventDate.getTime()) &&
      (!existing.mostRecentEventDate || eventDate > existing.mostRecentEventDate)) {
      existing.mostRecentEventDate = eventDate
    }

    aggregateByKey.set(key, existing)
  })

  return Array.from(aggregateByKey.values())
}

/**
 * Check whether the unified history response should surface the supplied track.
 * The public history contract currently exposes DEVELOPMENT, DESIGN, and DATA_SCIENCE groups.
 * @param {string|undefined} trackName canonical track label
 * @returns {boolean} true when the track should be included in history responses
 */
function isSupportedUnifiedHistoryTrack (trackName) {
  return _.includes([TRACK_NAMES.DEVELOP, TRACK_NAMES.DESIGN, TRACK_NAMES.DATA_SCIENCE], trackName)
}

/**
 * Identify aggregate track/type pairs that are visible in memberStats for the
 * current request scope.
 * @param {Array<Object>} aggregateRows unified memberStats rows for one member
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Set<string>} visible track/type pair keys
 */
function getVisibleUnifiedHistoryPairKeys (aggregateRows, dimensionLookup) {
  return new Set(
    _.chain(annotateUnifiedDimensionRows(aggregateRows || [], dimensionLookup))
      .filter(row => isSupportedUnifiedHistoryTrack(row.trackName))
      .map(row => buildStatsTrackTypeKey(row.trackId, row.typeId))
      .uniq()
      .value()
  )
}

/**
 * Identify aggregate track/type pairs that are visible in memberStats but missing from
 * memberStatsHistory for the current request scope.
 * @param {Array<Object>} aggregateRows unified memberStats rows for one member
 * @param {Array<Object>} historyRows unified memberStatsHistory rows for one member
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Set<string>} missing track/type pair keys
 */
function getMissingUnifiedHistoryPairKeys (aggregateRows, historyRows, dimensionLookup) {
  const persistedPairKeys = new Set(
    _.map(historyRows || [], row => buildStatsTrackTypeKey(row.trackId, row.typeId))
  )

  return new Set(
    _.chain(Array.from(getVisibleUnifiedHistoryPairKeys(aggregateRows, dimensionLookup)))
      .filter(pairKey => !persistedPairKeys.has(pairKey))
      .value()
  )
}

/**
 * Build transient unified history rows from completed review-api challenge results for
 * aggregate track/type pairs that do not yet have authoritative memberStatsHistory rows.
 * These fallback rows preserve challenge cards for non-rated tracks such as First2Finish
 * until a persistent backfill is written.
 * @param {Array<Object>} reviewRows review-api challenge results for the member
 * @param {Map<string, Object>} challengeMetadataById challenge metadata keyed by UUID and legacy ids
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {Set<string>} missingPairKeys track/type pairs that should be synthesized
 * @returns {Array<Object>} transient unified history rows ordered per pair
 */
function buildFallbackHistoryRowsFromReviewResults (reviewRows, challengeMetadataById, dimensionLookup, missingPairKeys) {
  const fallbackRowsByChallengeKey = new Map()

  _.forEach(reviewRows || [], (row) => {
    if (!isUsableReviewChallengeResultRow(row)) {
      return
    }

    const challenge = challengeMetadataById.get(String(row.challengeId))
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const dimension = resolveStatsDimensionForChallengeRow({
      trackId: String(challenge.trackId),
      typeId: String(challenge.typeId)
    }, dimensionLookup)
    const trackId = dimension.trackId
    const typeId = dimension.typeId
    const pairKey = buildStatsTrackTypeKey(trackId, typeId)
    if (missingPairKeys && !missingPairKeys.has(pairKey)) {
      return
    }

    const trackName = dimension.trackName
    if (!isSupportedUnifiedHistoryTrack(trackName)) {
      return
    }

    const typeName = dimension.typeName
    const eventDate = toOptionalDate(challenge.endDate || row.createdAt)
    if (!eventDate) {
      return
    }

    const createdAt = toOptionalDate(row.createdAt) || eventDate
    const placement = toVisiblePlacement(row.placement)
    const challengeId = String(challenge.id)
    const challengeKey = `${pairKey}::${challengeId}`
    const existing = fallbackRowsByChallengeKey.get(challengeKey)

    if (existing && createdAt <= existing.createdAt) {
      return
    }

    fallbackRowsByChallengeKey.set(challengeKey, {
      trackId,
      typeId,
      trackName,
      typeName,
      challengeId,
      challengeName: challenge.name || null,
      eventDate,
      placement,
      createdAt
    })
  })

  const fallbackRows = []
  const rowsByPairKey = _.groupBy(Array.from(fallbackRowsByChallengeKey.values()), row => buildStatsTrackTypeKey(row.trackId, row.typeId))

  _.forEach(rowsByPairKey, (pairRows) => {
    const orderedRows = _.orderBy(pairRows, [
      row => row.eventDate.getTime(),
      row => row.createdAt.getTime(),
      row => row.challengeId
    ], ['desc', 'desc', 'desc'])

    _.forEach(orderedRows, (row, index) => {
      fallbackRows.push(_.omit({
        ...row,
        mostRecent: index === 0
      }, ['createdAt']))
    })
  })

  return fallbackRows
}

/**
 * Build transient unified history rows from completed challenge winner placements.
 * This fallback is used when review-api does not expose challengeResult rows for
 * a member but challenge-api still records the member's placements.
 * @param {Array<Object>} winnerRows placement winner rows with embedded challenge metadata
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {Set<string>} missingPairKeys track/type pairs that should be synthesized
 * @returns {Array<Object>} transient unified history rows ordered per pair
 */
function buildFallbackHistoryRowsFromChallengeWinners (winnerRows, dimensionLookup, missingPairKeys) {
  const fallbackRowsByChallengeKey = new Map()

  _.forEach(winnerRows || [], (row) => {
    const challenge = row.challenge
    if (!challenge || !isCompletedChallenge(challenge) || !challenge.trackId || !challenge.typeId) {
      return
    }

    const dimension = resolveStatsDimensionForChallengeRow({
      trackId: String(challenge.trackId),
      typeId: String(challenge.typeId)
    }, dimensionLookup)
    const trackId = dimension.trackId
    const typeId = dimension.typeId
    const pairKey = buildStatsTrackTypeKey(trackId, typeId)
    if (missingPairKeys && !missingPairKeys.has(pairKey)) {
      return
    }

    const trackName = dimension.trackName
    if (!isSupportedUnifiedHistoryTrack(trackName)) {
      return
    }

    const typeName = dimension.typeName
    const eventDate = toOptionalDate(challenge.endDate || row.createdAt)
    if (!eventDate) {
      return
    }

    const createdAt = toOptionalDate(row.createdAt) || eventDate
    const placement = toVisibleChallengeWinnerPlacement(row)
    const challengeId = String(challenge.id || row.challengeId)
    const challengeKey = `${pairKey}::${challengeId}`
    const existing = fallbackRowsByChallengeKey.get(challengeKey)

    if (existing && createdAt <= existing.createdAt) {
      return
    }

    fallbackRowsByChallengeKey.set(challengeKey, {
      trackId,
      typeId,
      trackName,
      typeName,
      challengeId,
      challengeName: challenge.name || null,
      eventDate,
      placement,
      createdAt
    })
  })

  const fallbackRows = []
  const rowsByPairKey = _.groupBy(Array.from(fallbackRowsByChallengeKey.values()), row => buildStatsTrackTypeKey(row.trackId, row.typeId))

  _.forEach(rowsByPairKey, (pairRows) => {
    const orderedRows = _.orderBy(pairRows, [
      row => row.eventDate.getTime(),
      row => row.createdAt.getTime(),
      row => row.challengeId
    ], ['desc', 'desc', 'desc'])

    _.forEach(orderedRows, (row, index) => {
      fallbackRows.push(_.omit({
        ...row,
        mostRecent: index === 0
      }, ['createdAt']))
    })
  })

  return fallbackRows
}

/**
 * Recover missing legacy CODE history cards from the public challenge pages when
 * review rows exist but challenge-api metadata is unavailable for old numeric ids.
 * This keeps the development CODE details panel populated for members whose
 * aggregate counts survived migration but whose legacy challenge mappings did not.
 * @param {Array<Object>} reviewRows review-api challenge results for the member
 * @param {Array<Object>} aggregateRows visible memberStats rows for the member
 * @param {Array<Object>} existingRows persisted and/or synthesized history rows
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {Set<string>} missingPairKeys unresolved track/type pairs still needing history
 * @param {Function} [pageSummaryFetcher=fetchLegacyChallengePageSummary] legacy page fetcher used for testing
 * @returns {Promise<Array<Object>>} synthesized CODE history rows ordered per pair
 */
async function buildFallbackHistoryRowsFromLegacyCodePages (
  reviewRows,
  aggregateRows,
  existingRows,
  dimensionLookup,
  missingPairKeys,
  pageSummaryFetcher = fetchLegacyChallengePageSummary
) {
  if (!missingPairKeys || missingPairKeys.size === 0 || !reviewRows || reviewRows.length === 0) {
    return []
  }

  const candidatePairs = _.chain(annotateUnifiedDimensionRows(aggregateRows || [], dimensionLookup))
    .filter((row) => {
      const pairKey = buildStatsTrackTypeKey(row.trackId, row.typeId)
      return missingPairKeys.has(pairKey) &&
        row.trackName === TRACK_NAMES.DEVELOP &&
        row.typeName === TYPE_NAMES.CODE
    })
    .value()

  if (candidatePairs.length === 0) {
    return []
  }

  const existingChallengeIds = new Set(
    _.chain(existingRows || [])
      .map((row) => (_.isNil(row.challengeId) ? null : String(row.challengeId).trim()))
      .filter(Boolean)
      .value()
  )

  const orderedReviewRows = _.orderBy(reviewRows || [], [
    row => (row.createdAt ? new Date(row.createdAt).getTime() : 0),
    row => String(row.challengeId || '')
  ], ['desc', 'desc'])

  const fallbackRows = []
  for (const pair of candidatePairs) {
    const maxChallenges = Math.max(0, toOptionalInt(pair.challenges) || 0)
    if (maxChallenges === 0) {
      continue
    }

    const horizonDate = toOptionalDate(pair.mostRecentEventDate || pair.mostRecentSubmission)
    const horizonTimestamp = horizonDate
      ? horizonDate.getTime() + (45 * 24 * 60 * 60 * 1000)
      : null
    const pairRows = []

    for (const row of orderedReviewRows) {
      if (pairRows.length >= maxChallenges) {
        break
      }

      const challengeId = _.isNil(row.challengeId) ? null : String(row.challengeId).trim()
      if (!challengeId || existingChallengeIds.has(challengeId)) {
        continue
      }

      const createdAt = toOptionalDate(row.createdAt)
      if (!createdAt) {
        continue
      }

      if (horizonTimestamp && createdAt.getTime() > horizonTimestamp) {
        continue
      }

      const pageSummary = await pageSummaryFetcher(challengeId)
      if (!isLegacyCodeChallengePageSummary(pageSummary)) {
        continue
      }

      existingChallengeIds.add(challengeId)
      pairRows.push({
        trackId: pair.trackId,
        typeId: pair.typeId,
        trackName: pair.trackName,
        typeName: pair.typeName,
        challengeId,
        challengeName: pageSummary.title || null,
        eventDate: createdAt,
        placement: toVisiblePlacement(row.placement)
      })
    }

    const orderedPairRows = _.orderBy(pairRows, [
      row => row.eventDate.getTime(),
      row => row.challengeId
    ], ['desc', 'desc'])

    _.forEach(orderedPairRows, (row, index) => {
      fallbackRows.push({
        ...row,
        mostRecent: index === 0
      })
    })
  }

  return fallbackRows
}

/**
 * Remove track/type pairs from the pending fallback set once rows have been synthesized.
 * @param {Set<string>} pairKeys pending track/type pairs
 * @param {Array<Object>} rows synthesized history rows
 * @returns {Set<string>} unresolved pair keys
 */
function getUnresolvedHistoryPairKeys (pairKeys, rows) {
  const unresolvedPairKeys = new Set(pairKeys || [])
  _.forEach(rows || [], (row) => {
    unresolvedPairKeys.delete(buildStatsTrackTypeKey(row.trackId, row.typeId))
  })
  return unresolvedPairKeys
}

function buildHistoryChallengeKey (row) {
  return `${buildStatsTrackTypeKey(row.trackId, row.typeId)}::${row.challengeId}`
}

/**
 * Merge synthesized history rows without duplicating existing challenge cards.
 * @param {Array<Object>} existingRows persisted and/or synthesized history rows
 * @param {Array<Object>} fallbackRows synthesized candidate rows
 * @returns {Array<Object>} merged history rows
 */
function mergeMissingHistoryRows (existingRows, fallbackRows) {
  const mergedRows = existingRows ? existingRows.slice() : []
  const existingKeys = new Set(_.map(mergedRows, row => buildHistoryChallengeKey(row)))

  _.forEach(fallbackRows || [], (row) => {
    const key = buildHistoryChallengeKey(row)
    if (existingKeys.has(key)) {
      return
    }

    existingKeys.add(key)
    mergedRows.push(row)
  })

  return mergedRows
}

/**
 * Collapse duplicate history rows that point to the same normalized challenge card.
 * This protects reads while old rows stored as Development/MM coexist with rerun
 * rows normalized to DATA_SCIENCE/MM.
 * @param {Array<Object>} rows persisted and/or transient history rows
 * @returns {Array<Object>} history rows keyed by normalized track/type/challenge
 */
function dedupeUnifiedHistoryRows (rows) {
  const dedupedByKey = new Map()

  _.forEach(rows || [], (row) => {
    const key = buildHistoryChallengeKey(row)
    const existing = dedupedByKey.get(key)

    if (!existing ||
      (row.eventDate && existing.eventDate && row.eventDate > existing.eventDate) ||
      (_.isNil(existing.newRating) && !_.isNil(row.newRating))) {
      dedupedByKey.set(key, row)
    }
  })

  return Array.from(dedupedByKey.values())
}

/**
 * Reconcile imported Marathon Match legacy rows with canonical rerated rows.
 *
 * Imported numeric MM rows are the authoritative rating history for migrated
 * events. Canonical rerate rows are retained for new events that are outside the
 * migrated legacy window. Numeric rows that cannot be hydrated are dropped as
 * unresolved placeholders.
 *
 * @param {Array<Object>} rows persisted and/or transient history rows
 * @returns {Array<Object>} reconciled Marathon Match history rows
 */
function reconcileLegacyMarathonHistoryRows (rows) {
  const preparedRows = backfillCanonicalMarathonRatingsFromNextOldRating(rows)
  const visibleRows = selectVisibleMarathonHistoryRows(preparedRows)
  const hasReratedCanonicalRows = _.some(visibleRows, isReratedCanonicalMarathonHistoryRow)
  if (hasReratedCanonicalRows) {
    return visibleRows
  }

  const authoritativeLegacyRows = _.filter(visibleRows || [], row =>
    isLegacyNumericMarathonHistoryRow(row) &&
    !!row.challengeName &&
    !_.isNil(row.newRating)
  )
  const latestLegacyTimestamp = _.max(
    _.map(authoritativeLegacyRows, row => row.eventDate ? row.eventDate.getTime() : 0)
  ) || 0

  return _.filter(visibleRows || [], (row) => {
    if (!row ||
      row.trackName !== TRACK_NAMES.DATA_SCIENCE ||
      row.typeName !== TYPE_NAMES.MARATHON_MATCH) {
      return true
    }

    if (isLegacyNumericMarathonHistoryRow(row)) {
      return !!row.challengeName
    }

    const eventTimestamp = row.eventDate ? row.eventDate.getTime() : 0
    return !latestLegacyTimestamp ||
      eventTimestamp > latestLegacyTimestamp ||
      !_.isNil(row.newRating) ||
      !_.isNil(toVisiblePlacement(row.placement))
  })
}

/**
 * Recompute mostRecent flags after fallback and dimension normalization.
 * Persisted rows may still be split across legacy dimensions, so the response
 * needs one latest card per normalized track/type group.
 * @param {Array<Object>} rows persisted and/or transient history rows
 * @returns {Array<Object>} rows with normalized mostRecent flags
 */
function recomputeUnifiedHistoryMostRecentFlags (rows) {
  const rowsByPairKey = _.groupBy(rows || [], row => buildStatsTrackTypeKey(row.trackId, row.typeId))
  const normalizedRows = []

  _.forEach(rowsByPairKey, (pairRows) => {
    const orderedRows = _.orderBy(pairRows, [
      row => (row.eventDate ? row.eventDate.getTime() : 0),
      row => row.challengeId
    ], ['desc', 'desc'])

    _.forEach(orderedRows, (row, index) => {
      normalizedRows.push({
        ...row,
        mostRecent: index === 0
      })
    })
  })

  return normalizedRows
}

/**
 * Apply the stable ordering expected by the unified history response builders.
 * @param {Array<Object>} rows persisted and/or transient history rows
 * @returns {Array<Object>} rows ordered by mostRecent and event recency
 */
function orderUnifiedHistoryRows (rows) {
  return _.orderBy(rows || [], [
    row => (row.mostRecent ? 1 : 0),
    row => (row.eventDate ? row.eventDate.getTime() : 0),
    row => row.challengeId
  ], ['desc', 'desc', 'desc'])
}

/**
 * Convert a normalized rating bucket start into the response field name.
 * Ratings outside the documented 0-3999 distribution range are ignored.
 * @param {Number} rangeStart normalized inclusive lower bound for a 100-point rating bucket
 * @returns {String|null} distribution response key, or null when outside the supported range
 */
function getDistributionRangeKey (rangeStart) {
  if (rangeStart < DISTRIBUTION_MIN_RATING || rangeStart >= DISTRIBUTION_MAX_RATING_EXCLUSIVE) {
    return null
  }
  if (rangeStart === 0) {
    return 'ratingRange0To099'
  }
  return `ratingRange${rangeStart}To${rangeStart + 99}`
}

function createEmptyDistribution () {
  const distribution = {}
  _.forEach(DISTRIBUTION_RANGES, (rangeStart) => {
    distribution[getDistributionRangeKey(rangeStart)] = 0
  })
  return distribution
}

function toInteger (value) {
  if (_.isNil(value)) {
    return undefined
  }
  const parsed = Number(value)
  if (!Number.isInteger(parsed)) {
    return undefined
  }
  return parsed
}

/**
 * Convert legacy history stats data to response structure.
 * @param {Object} member member data
 * @param {Object} historyStats stats history row with nested develop/dataScience history
 * @param {Array} fields fields to return in response
 * @returns response
 */
function buildLegacyStatsHistoryResponse (member, historyStats, fields) {
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(historyStats.groupId),
    handle: member.handle,
    handleLower: member.handleLower
  }

  if (historyStats.develop && historyStats.develop.length > 0) {
    item.DEVELOP = { subTracks: [] }
    const subTrackGroupData = _.groupBy(historyStats.develop, 'subTrackId')
    _.forEach(subTrackGroupData, (trackHistory, subTrackId) => {
      const subTrackItem = {
        id: subTrackId,
        name: trackHistory[0].subTrack
      }
      subTrackItem.history = _.map(trackHistory, h => ({
        ..._.pick(h, ['challengeName', 'newRating']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        ratingDate: h.ratingDate ? h.ratingDate.getTime() : null
      }))
      item.DEVELOP.subTracks.push(subTrackItem)
    })
  }

  if (historyStats.dataScience && historyStats.dataScience.length > 0) {
    item.DATA_SCIENCE = {}
    const srmHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'SRM')
    const marathonHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'MARATHON_MATCH')
    if (srmHistory.length > 0) {
      item.DATA_SCIENCE.SRM = {}
      item.DATA_SCIENCE.SRM.history = _.map(srmHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
    if (marathonHistory.length > 0) {
      item.DATA_SCIENCE.MARATHON_MATCH = {}
      item.DATA_SCIENCE.MARATHON_MATCH.history = _.map(marathonHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
  }
  return fields ? _.pick(item, fields) : item
}

/**
 * Get distribution statistics from legacy table.
 * @param {Object} query the query parameters
 * @param {Array} fields selected fields
 * @returns {Object} the distribution statistics
 */
async function getLegacyDistribution (query, fields) {
  const whereConditions = []
  if (query.track) {
    whereConditions.push(Prisma.sql`UPPER("track") LIKE ${`%${query.track.toUpperCase()}%`}`)
  }
  if (query.subTrack) {
    whereConditions.push(Prisma.sql`UPPER("subTrack") LIKE ${`%${query.subTrack.toUpperCase()}%`}`)
  }

  const whereClause = whereConditions.length > 0
    ? Prisma.sql`WHERE ${joinSqlConditions(whereConditions)}`
    : Prisma.empty

  const items = await prisma.$queryRaw`
    SELECT *
    FROM "members"."distributionStats"
    ${whereClause}
  `

  if (!items || items.length === 0) {
    throw new errors.NotFoundError('No member distribution statistics is found.')
  }

  const records = []
  _.forEach(items, row => {
    const record = _.pick(row, DISTRIBUTION_FIELDS)
    record.distribution = createEmptyDistribution()
    _.forEach(DISTRIBUTION_RANGES, (rangeStart) => {
      const key = getDistributionRangeKey(rangeStart)
      record.distribution[key] = Number(row[key] || 0)
    })
    records.push(record)
  })

  let result = { track: query.track, subTrack: query.subTrack, distribution: {} }
  _.forEach(records, (record) => {
    _.forIn(record.distribution, (value, key) => {
      if (!result.distribution[key]) {
        result.distribution[key] = 0
      }
      result.distribution[key] += Number(value)
    })
    if (record.createdAt && (!result.createdAt || new Date(record.createdAt) < result.createdAt)) {
      result.createdAt = new Date(record.createdAt)
      result.createdBy = record.createdBy
    }
    if (record.updatedAt && (!result.updatedAt || new Date(record.updatedAt) > result.updatedAt)) {
      result.updatedAt = new Date(record.updatedAt)
      result.updatedBy = record.updatedBy
    }
  })

  if (fields) {
    result = _.pick(result, fields)
  }
  return result
}

/**
 * Load one legacy member stats aggregate row with nested legacy stats details.
 * @param {BigInt} userId member user id
 * @param {String|Number} groupId requested group id
 * @returns {Object|null} member stats row in legacy shape
 */
async function getLegacyMemberStatsRow (userId, groupId) {
  const isPrivate = String(groupId) !== String(config.PUBLIC_GROUP_ID)
  const statsRows = await prisma.$queryRaw`
    SELECT ms.*
    FROM "members"."memberStats" ms
    WHERE ms."userId" = ${userId}
      AND ms."isPrivate" = ${isPrivate}
    ORDER BY
      CASE
        WHEN EXISTS (SELECT 1 FROM "members"."memberDevelopStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberDesignStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" ds WHERE ds."memberStatsId" = ms."id")
          OR EXISTS (SELECT 1 FROM "members"."memberCopilotStats" cs WHERE cs."memberStatsId" = ms."id")
          THEN 0
        ELSE 1
      END,
      ms."id" DESC
    LIMIT 1
  `

  const stat = _.head(statsRows)
  if (!stat) {
    return null
  }

  const numericGroupId = _.toNumber(groupId)
  if (!Number.isNaN(numericGroupId)) {
    stat.groupId = numericGroupId
  }

  const [designRows, developRows, dataScienceRows, copilotRows] = await Promise.all([
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDesignStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDataScienceStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberCopilotStats"
      WHERE "memberStatsId" = ${stat.id}
      ORDER BY "id" ASC
      LIMIT 1
    `
  ])

  const design = _.head(designRows)
  if (design) {
    const designItems = await prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDesignStatsItem"
      WHERE "designStatsId" = ${design.id}
      ORDER BY "subTrackId" ASC, "id" ASC
    `
    design.items = designItems
  }
  stat.design = design

  const develop = _.head(developRows)
  if (develop) {
    const developItems = await prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopStatsItem"
      WHERE "developStatsId" = ${develop.id}
      ORDER BY "subTrackId" ASC, "id" ASC
    `
    develop.items = developItems
  }
  stat.develop = develop

  const dataScience = _.head(dataScienceRows)
  if (dataScience) {
    const [srmRows, marathonRows] = await Promise.all([
      prisma.$queryRaw`
        SELECT *
        FROM "members"."memberSrmStats"
        WHERE "dataScienceStatsId" = ${dataScience.id}
        ORDER BY "id" ASC
        LIMIT 1
      `,
      prisma.$queryRaw`
        SELECT *
        FROM "members"."memberMarathonStats"
        WHERE "dataScienceStatsId" = ${dataScience.id}
        ORDER BY "id" ASC
        LIMIT 1
      `
    ])
    const srm = _.head(srmRows)
    if (srm) {
      const [challengeDetails, divisions] = await Promise.all([
        prisma.$queryRaw`
          SELECT *
          FROM "members"."memberSrmChallengeDetail"
          WHERE "srmStatsId" = ${srm.id}
          ORDER BY "id" ASC
        `,
        prisma.$queryRaw`
          SELECT *
          FROM "members"."memberSrmDivisionDetail"
          WHERE "srmStatsId" = ${srm.id}
          ORDER BY "divisionName" ASC, "levelName" ASC, "id" ASC
        `
      ])
      srm.challengeDetails = challengeDetails
      srm.divisions = divisions
    }
    dataScience.srm = srm
    dataScience.marathon = _.head(marathonRows)
  }
  stat.dataScience = dataScience
  stat.copilot = _.head(copilotRows)

  return stat
}

/**
 * Load one legacy history stats aggregate row with nested history details.
 * @param {BigInt} userId member user id
 * @param {String|Number} groupId requested group id
 * @returns {Object|null} history stats row in legacy shape
 */
async function getLegacyHistoryStatsRow (userId, groupId) {
  const isPrivate = String(groupId) !== String(config.PUBLIC_GROUP_ID)
  const groupIdValue = toInteger(groupId)

  if (isPrivate && _.isNil(groupIdValue)) {
    return null
  }

  const whereConditions = [
    Prisma.sql`"userId" = ${userId}`,
    Prisma.sql`"isPrivate" = ${isPrivate}`
  ]
  if (isPrivate) {
    whereConditions.push(Prisma.sql`"groupId" = ${groupIdValue}`)
  }

  const historyRows = await prisma.$queryRaw`
    SELECT *
    FROM "members"."memberHistoryStats"
    WHERE ${joinSqlConditions(whereConditions)}
    ORDER BY "id" ASC
    LIMIT 1
  `

  const history = _.head(historyRows)
  if (!history) {
    return null
  }

  if (!isPrivate) {
    history.groupId = _.toNumber(groupId)
  }

  const [developRows, dataScienceRows] = await Promise.all([
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDevelopHistoryStats"
      WHERE "historyStatsId" = ${history.id}
      ORDER BY "subTrackId" ASC, "ratingDate" DESC, "id" DESC
    `,
    prisma.$queryRaw`
      SELECT *
      FROM "members"."memberDataScienceHistoryStats"
      WHERE "historyStatsId" = ${history.id}
      ORDER BY "subTrack" ASC, "date" DESC, "id" DESC
    `
  ])

  history.develop = developRows
  history.dataScience = dataScienceRows
  return history
}

function buildUnifiedHistoryRecordsFromPayload (payload, dimensionLookup) {
  const data = payload || {}
  const records = []
  const pushHistoryRecord = (item, fallbackTrackId, fallbackTypeId) => {
    const trackId = resolveTrackIdFromLookup(dimensionLookup, item.trackId || fallbackTrackId)
    const typeId = resolveTypeIdFromLookup(dimensionLookup, item.typeId || fallbackTypeId)
    if (!trackId || !typeId) {
      return
    }
    const eventDate = toOptionalDate(item.eventDate || item.date || item.ratingDate)
    if (!eventDate || _.isNil(item.challengeId)) {
      return
    }
    records.push(_.omitBy({
      trackId,
      typeId,
      challengeId: String(item.challengeId),
      oldRating: toOptionalInt(item.oldRating),
      newRating: toOptionalInt(item.newRating),
      placement: toOptionalInt(item.placement),
      percentile: toOptionalFloat(item.percentile),
      oldVolatility: toOptionalInt(item.oldVolatility),
      newVolatility: toOptionalInt(item.newVolatility),
      oldGlobalRank: toOptionalInt(item.oldGlobalRank),
      newGlobalRank: toOptionalInt(item.newGlobalRank),
      oldCountryRank: toOptionalInt(item.oldCountryRank),
      newCountryRank: toOptionalInt(item.newCountryRank),
      oldSchoolRank: toOptionalInt(item.oldSchoolRank),
      newSchoolRank: toOptionalInt(item.newSchoolRank),
      eventDate
    }, _.isUndefined))
  }

  if (!_.isNil(data.challengeId)) {
    pushHistoryRecord(data, data.trackId, data.typeId)
  }

  if (_.isArray(data.history) && data.history.length > 0) {
    _.forEach(data.history, (item) => {
      pushHistoryRecord(item, data.trackId, data.typeId)
    })
  }

  return _.values(_.keyBy(records, record => `${record.trackId}::${record.typeId}::${record.challengeId}`))
}

/**
 * Determine whether a history row is an imported Marathon Match legacy row that
 * still uses a numeric challenge id from legacy stats history.
 * @param {Object} row unified history row annotated with track/type names
 * @returns {boolean} true when the row is a legacy Marathon Match history row
 */
function isLegacyNumericMarathonHistoryRow (row) {
  const challengeId = normalizeChallengeLookupKey(row && row.challengeId)

  return !!challengeId &&
    /^\d+$/.test(challengeId) &&
    row &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH
}

/**
 * Determine whether a history row is a canonical rerated Marathon Match row.
 * Rerated UUID rows are the authoritative replay source after MM ratings are
 * regenerated, while numeric legacy rows remain a migration fallback.
 * @param {Object} row unified history row annotated with track/type names
 * @returns {boolean} true when the row is a rerated canonical Marathon Match row
 */
function isReratedCanonicalMarathonHistoryRow (row) {
  const challengeId = normalizeChallengeLookupKey(row && row.challengeId)

  return !!challengeId &&
    !/^\d+$/.test(challengeId) &&
    row &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH &&
    (row.createdBy === RERATE_MARATHON_ACTOR || row.updatedBy === RERATE_MARATHON_ACTOR) &&
    !_.isNil(row.newRating)
}

/**
 * Determine whether a Marathon Match history row uses a canonical UUID
 * challenge id instead of an imported numeric legacy challenge id.
 * @param {Object} row unified history row annotated with track/type names
 * @returns {boolean} true when the row is a canonical Marathon Match row
 */
function isCanonicalMarathonHistoryRow (row) {
  const challengeId = normalizeChallengeLookupKey(row && row.challengeId)

  return !!challengeId &&
    !/^\d+$/.test(challengeId) &&
    row &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH
}

/**
 * Extract a Marathon Match round number from canonical or legacy challenge names.
 * This lets migrated numeric legacy rows be reconciled with canonical Challenge
 * rows even when their challenge ids are unrelated.
 * @param {*} value challenge display name
 * @returns {string|null} Marathon Match number, or null when unavailable
 */
function extractMarathonMatchNumber (value) {
  const name = String(value || '').trim()
  if (!name) {
    return null
  }

  const match = name.match(/\b(?:MM|MARATHON\s+MATCH)\s*#?\s*(\d+)\b/i)
  return match ? match[1] : null
}

/**
 * Resolve a comparable Marathon Match number for a history row.
 * @param {Object} row unified history row
 * @returns {string|null} Marathon Match number, or null when unavailable
 */
function getMarathonHistoryMatchNumber (row) {
  if (!row ||
    row.trackName !== TRACK_NAMES.DATA_SCIENCE ||
    row.typeName !== TYPE_NAMES.MARATHON_MATCH) {
    return null
  }

  return extractMarathonMatchNumber(row.challengeName)
}

/**
 * Build a conservative title alias for MM rows that do not expose a round number.
 * Some legacy MM history names are truncated, so a normalized prefix is used
 * only for longer names where accidental matches are unlikely.
 * @param {*} value challenge display name
 * @returns {string|null} normalized title prefix alias, or null when unavailable
 */
function getMarathonHistoryTitleAlias (value) {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')

  return normalized.length >= 20 ? `title:${normalized.slice(0, 24)}` : null
}

/**
 * Build stable aliases used to identify duplicated migrated/canonical MM rows.
 * Imported legacy history uses numeric ids that do not always match challenge-api
 * legacy ids, so MM round numbers and longer normalized title prefixes are
 * retained as narrow fallbacks.
 * @param {Object} row unified history row
 * @returns {Array<string>} aliases for duplicate detection
 */
function getMarathonHistoryAliasKeys (row) {
  const keys = _.chain([
    row && row.challengeId,
    row && row.canonicalChallengeId,
    row && row.legacyChallengeId
  ])
    .map(normalizeChallengeLookupKey)
    .filter(Boolean)
    .map(key => `id:${key}`)
    .uniq()
    .value()

  const matchNumber = getMarathonHistoryMatchNumber(row)
  if (matchNumber) {
    keys.push(`match:${matchNumber}`)
  }

  const titleAlias = getMarathonHistoryTitleAlias(row && row.challengeName)
  if (titleAlias) {
    keys.push(titleAlias)
  }

  return keys
}

/**
 * Build aliases that let a rerated canonical row replace a stale legacy row.
 * Explicit id aliases intentionally stay out of this set because imported
 * legacy rating rows still win when challenge-api maps the same legacy id.
 * @param {Object} row unified history row
 * @returns {Array<string>} canonical replacement aliases
 */
function getMarathonHistoryCanonicalReplacementAliasKeys (row) {
  const keys = []
  const matchNumber = getMarathonHistoryMatchNumber(row)
  if (matchNumber) {
    keys.push(`match:${matchNumber}`)
  }

  const titleAlias = getMarathonHistoryTitleAlias(row && row.challengeName)
  if (titleAlias) {
    keys.push(titleAlias)
  }

  return keys
}

/**
 * Determine whether a numeric imported MM row has enough data to preserve as a
 * historical rating point.
 * @param {Object} row unified history row
 * @returns {boolean} true when the legacy row should win over rerated duplicates
 */
function isAuthoritativeLegacyMarathonHistoryRow (row) {
  return isLegacyNumericMarathonHistoryRow(row) &&
    !!row.challengeName &&
    !_.isNil(row.newRating)
}

/**
 * Determine whether a canonical MM row has enough rerated data to replace an
 * imported numeric legacy row.
 * @param {Object} row unified history row
 * @returns {boolean} true when the canonical row should win over legacy duplicates
 */
function isAuthoritativeCanonicalMarathonHistoryRow (row) {
  return !isLegacyNumericMarathonHistoryRow(row) &&
    isReratedCanonicalMarathonHistoryRow(row) &&
    !_.isNil(toVisiblePlacement(row.placement))
}

/**
 * Build the set of aliases covered by authoritative imported MM history rows.
 * @param {Array<Object>} rows unified history rows
 * @returns {Set<string>} duplicate-detection aliases
 */
function buildAuthoritativeLegacyMarathonAliasSet (rows) {
  const aliases = new Set()

  _.forEach(rows || [], (row) => {
    if (!isAuthoritativeLegacyMarathonHistoryRow(row)) {
      return
    }

    _.forEach(getMarathonHistoryAliasKeys(row), key => aliases.add(key))
  })

  return aliases
}

/**
 * Build the set of aliases covered by authoritative canonical MM rerate rows.
 * @param {Array<Object>} rows unified history rows
 * @returns {Set<string>} duplicate-detection aliases
 */
function buildAuthoritativeCanonicalMarathonAliasSet (rows) {
  const aliases = new Set()

  _.forEach(rows || [], (row) => {
    if (!isAuthoritativeCanonicalMarathonHistoryRow(row)) {
      return
    }

    _.forEach(getMarathonHistoryCanonicalReplacementAliasKeys(row), key => aliases.add(key))
  })

  return aliases
}

/**
 * Build aliases covered by any canonical Marathon Match row, including rows
 * that are incomplete rerate placeholders. This lets imported numeric rows
 * remain visible only when they are UUID-backed or are filling a known
 * canonical gap, while orphan numeric rows are hidden after native rerates.
 * @param {Array<Object>} rows unified history rows
 * @returns {Set<string>} aliases covered by canonical MM rows
 */
function buildAnyCanonicalMarathonAliasSet (rows) {
  const aliases = new Set()

  _.forEach(rows || [], (row) => {
    if (!isCanonicalMarathonHistoryRow(row)) {
      return
    }

    _.forEach(getMarathonHistoryCanonicalReplacementAliasKeys(row), key => aliases.add(key))
  })

  return aliases
}

/**
 * Determine whether a numeric legacy Marathon Match row should remain visible.
 * Legacy-only environments keep hydrated numeric rows, but once a member has
 * canonical MM history, numeric rows must either resolve to a Challenge API UUID
 * or fill a known canonical MM-number/title gap.
 * @param {Object} row candidate legacy numeric MM row
 * @param {boolean} hasCanonicalRows true when the response includes canonical MM history
 * @param {Set<string>} anyCanonicalAliases aliases covered by canonical MM rows
 * @returns {boolean} true when the numeric row should be shown
 */
function shouldKeepLegacyNumericMarathonHistoryRow (row, hasCanonicalRows, anyCanonicalAliases) {
  if (!row || !row.challengeName) {
    return false
  }

  if (!hasCanonicalRows) {
    return true
  }

  if (normalizeChallengeLookupKey(row.canonicalChallengeId)) {
    return true
  }

  return _.some(getMarathonHistoryCanonicalReplacementAliasKeys(row), key => anyCanonicalAliases.has(key))
}

/**
 * Determine whether a canonical MM row should be hidden because an imported
 * legacy row remains the authoritative historical rating point.
 * @param {Object} row candidate canonical history row
 * @param {Set<string>} legacyAliases aliases covered by imported legacy rows
 * @returns {boolean} true when the row is a duplicate historical rerate
 */
function isLegacyCoveredCanonicalMarathonRow (row, legacyAliases) {
  if (!row ||
    row.trackName !== TRACK_NAMES.DATA_SCIENCE ||
    row.typeName !== TYPE_NAMES.MARATHON_MATCH ||
    isLegacyNumericMarathonHistoryRow(row)) {
    return false
  }

  const matchingLegacyAliases = _.filter(getMarathonHistoryAliasKeys(row), key => legacyAliases.has(key))
  if (matchingLegacyAliases.length === 0) {
    return false
  }

  return !isAuthoritativeCanonicalMarathonHistoryRow(row) ||
    _.some(matchingLegacyAliases, key => key.indexOf('id:') === 0)
}

/**
 * Fill missing canonical Marathon Match ratings from the next canonical row's
 * oldRating. The migration stores many post-event MM ratings as the oldRating
 * for the following challenge, so this recovers visible graph points until a
 * native MM rerate rewrites canonical newRating values.
 * @param {Array<Object>} rows annotated history rows
 * @returns {Array<Object>} rows with recoverable canonical ratings filled
 */
function backfillCanonicalMarathonRatingsFromNextOldRating (rows) {
  const clonedRows = _.map(rows || [], row => ({ ...row }))
  const rowByIdentity = new Map(_.map(clonedRows, row => [row, row]))
  const canonicalRows = _.filter(clonedRows, row =>
    row &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH &&
    !isLegacyNumericMarathonHistoryRow(row)
  )

  _.forEach(_.groupBy(canonicalRows, row => buildStatsTrackTypeKey(row.trackId, row.typeId)), (groupRows) => {
    const orderedRows = _.orderBy(groupRows, [
      row => (row.eventDate ? row.eventDate.getTime() : 0),
      row => row.challengeId
    ], ['asc', 'asc'])

    for (let index = 0; index < orderedRows.length - 1; index += 1) {
      const row = rowByIdentity.get(orderedRows[index]) || orderedRows[index]
      const nextRow = orderedRows[index + 1]
      if (_.isNil(row.newRating) && !_.isNil(nextRow.oldRating)) {
        row.newRating = nextRow.oldRating
      }
      if (_.isNil(row.newVolatility) && !_.isNil(nextRow.oldVolatility)) {
        row.newVolatility = nextRow.oldVolatility
      }
    }
  })

  return clonedRows
}

/**
 * Select Marathon Match rows that should drive visible history and rating bounds.
 * Imported numeric rows keep historical ratings stable. Canonical rerate rows
 * are hidden only when they duplicate the migrated legacy window; newer
 * canonical rows remain visible as post-migration MM results.
 * @param {Array<Object>} rows annotated history rows for one response
 * @returns {Array<Object>} rows after applying canonical-vs-legacy precedence
 */
function selectVisibleMarathonHistoryRows (rows) {
  const legacyAliases = buildAuthoritativeLegacyMarathonAliasSet(rows)
  const canonicalAliases = buildAuthoritativeCanonicalMarathonAliasSet(rows)
  const anyCanonicalAliases = buildAnyCanonicalMarathonAliasSet(rows)
  const hasCanonicalRows = _.some(rows || [], isCanonicalMarathonHistoryRow)

  return _.filter(rows || [], (row) => {
    if (!row ||
      row.trackName !== TRACK_NAMES.DATA_SCIENCE ||
      row.typeName !== TYPE_NAMES.MARATHON_MATCH) {
      return true
    }

    if (isLegacyNumericMarathonHistoryRow(row)) {
      return shouldKeepLegacyNumericMarathonHistoryRow(row, hasCanonicalRows, anyCanonicalAliases) &&
        !_.some(getMarathonHistoryCanonicalReplacementAliasKeys(row), key => canonicalAliases.has(key))
    }

    return !isLegacyCoveredCanonicalMarathonRow(row, legacyAliases)
  })
}

/**
 * Select Marathon Match history rows that should drive aggregate rating bounds.
 * Bounds use the same legacy-over-canonical precedence as visible history after
 * challenge metadata enrichment has restored legacy names where possible.
 * @param {Array<Object>} rows annotated history rows for one Marathon Match stat
 * @returns {Array<Object>} rows used to compute min/max rating
 */
function selectMarathonHistoryRowsForRatingBounds (rows) {
  return selectVisibleMarathonHistoryRows(rows)
}

/**
 * Compute min/max rating values from selected history rows.
 * @param {Array<Object>} rows history rows with newRating values
 * @returns {{minRating: number|null, maxRating: number|null}} rating bounds
 */
function calculateHistoryRatingBounds (rows) {
  const bounds = {
    minRating: null,
    maxRating: null
  }

  _.forEach(selectMarathonHistoryRowsForRatingBounds(rows), (row) => {
    if (_.isNil(row && row.newRating)) {
      return
    }

    const rating = Number(row.newRating)
    if (!Number.isFinite(rating)) {
      return
    }

    bounds.minRating = bounds.minRating === null ? rating : Math.min(bounds.minRating, rating)
    bounds.maxRating = bounds.maxRating === null ? rating : Math.max(bounds.maxRating, rating)
  })

  return bounds
}

/**
 * Determine whether a unified stats row is Data Science / Marathon Match.
 * @param {Object} row annotated memberStats row
 * @returns {boolean} true when the row is the Marathon Match aggregate
 */
function isMarathonStatsRow (row) {
  return row &&
    row.trackName === TRACK_NAMES.DATA_SCIENCE &&
    row.typeName === TYPE_NAMES.MARATHON_MATCH &&
    row.trackId &&
    row.typeId
}

/**
 * Overlay Marathon Match aggregate min/max rating from history rows. This keeps
 * stats responses correct immediately after rerates even when an older aggregate
 * maxRating was left behind by migration or a previous rerate.
 * @param {BigInt} userId member user id
 * @param {Array<Object>} statsRows annotated memberStats rows
 * @returns {Promise<Array<Object>>} stats rows with corrected MM rating bounds
 */
async function hydrateMarathonRatingBoundsFromHistory (userId, statsRows) {
  const marathonStatsRows = _.filter(statsRows || [], isMarathonStatsRow)
  if (marathonStatsRows.length === 0) {
    return statsRows || []
  }

  const historyRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId,
      OR: _.map(marathonStatsRows, row => ({
        trackId: row.trackId,
        typeId: row.typeId
      }))
    },
    select: {
      trackId: true,
      typeId: true,
      challengeId: true,
      newRating: true,
      createdBy: true,
      updatedBy: true,
      eventDate: true
    }
  })

  if (!historyRows || historyRows.length === 0) {
    return statsRows || []
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    prismaManager.getChallengesClient(),
    _.uniq(_.map(historyRows, row => row.challengeId))
  )

  return _.map(statsRows || [], (row) => {
    if (!isMarathonStatsRow(row)) {
      return row
    }

    const annotatedHistoryRows = _.map(_.filter(historyRows, historyRow =>
      String(historyRow.trackId) === String(row.trackId) &&
      String(historyRow.typeId) === String(row.typeId)
    ), historyRow => ({
      ...historyRow,
      trackName: TRACK_NAMES.DATA_SCIENCE,
      typeName: TYPE_NAMES.MARATHON_MATCH
    }))
    const enrichedHistoryRows = enrichUnifiedHistoryRowsWithChallengeMetadata(
      annotatedHistoryRows,
      challengeMetadataById
    )
    const bounds = calculateHistoryRatingBounds(enrichedHistoryRows)

    if (bounds.minRating === null || bounds.maxRating === null) {
      return row
    }

    return {
      ...row,
      minRating: bounds.minRating,
      maxRating: bounds.maxRating
    }
  })
}

/**
 * Load legacy Marathon Match history details for numeric imported history rows.
 * The unified history table stores ratings and challenge ids but not the legacy
 * challenge names, so this narrow fallback lets the read path distinguish
 * authoritative imported legacy rows from unresolved numeric placeholders.
 * @param {BigInt} userId member user id
 * @param {Array<string>} challengeIds numeric legacy challenge ids to hydrate
 * @returns {Promise<Map<string, Object>>} legacy history fields keyed by challenge id
 */
async function fetchLegacyMarathonHistoryLookup (userId, challengeIds) {
  const normalizedChallengeIds = _.chain(challengeIds || [])
    .map(normalizeChallengeLookupKey)
    .filter(challengeId => challengeId && /^\d+$/.test(challengeId))
    .uniq()
    .value()

  if (normalizedChallengeIds.length === 0) {
    return new Map()
  }

  const rows = await prisma.$queryRaw`
    SELECT dsh."challengeId",
           dsh."challengeName",
           dsh."date",
           dsh."rating",
           dsh."placement",
           dsh."percentile"
    FROM "members"."memberHistoryStats" hs
    INNER JOIN "members"."memberDataScienceHistoryStats" dsh
      ON dsh."historyStatsId" = hs."id"
    WHERE hs."userId" = ${userId}
      AND hs."isPrivate" = false
      AND dsh."subTrack" = 'MARATHON_MATCH'
      AND dsh."challengeId"::text IN (${Prisma.join(normalizedChallengeIds)})
    ORDER BY dsh."date" DESC, dsh."id" DESC
  `

  const lookup = new Map()
  _.forEach(rows || [], (row) => {
    const challengeId = normalizeChallengeLookupKey(row.challengeId)
    if (challengeId && !lookup.has(challengeId)) {
      lookup.set(challengeId, row)
    }
  })

  return lookup
}

/**
 * Hydrate imported numeric Marathon Match history rows from legacy history
 * details when those source rows are still available.
 * @param {BigInt} userId member user id
 * @param {Array<Object>} rows annotated unified history rows
 * @returns {Promise<Array<Object>>} history rows with legacy names and metadata
 */
async function hydrateLegacyMarathonHistoryRows (userId, rows) {
  const legacyChallengeIds = _.chain(rows || [])
    .filter(isLegacyNumericMarathonHistoryRow)
    .map(row => normalizeChallengeLookupKey(row.challengeId))
    .filter(Boolean)
    .value()

  if (legacyChallengeIds.length === 0) {
    return rows || []
  }

  const legacyLookup = await fetchLegacyMarathonHistoryLookup(userId, legacyChallengeIds)
  if (legacyLookup.size === 0) {
    return rows || []
  }

  return _.map(rows || [], (row) => {
    if (!isLegacyNumericMarathonHistoryRow(row)) {
      return row
    }

    const legacyRow = legacyLookup.get(normalizeChallengeLookupKey(row.challengeId))
    if (!legacyRow) {
      return row
    }

    return {
      ...row,
      challengeName: row.challengeName || legacyRow.challengeName,
      eventDate: row.eventDate || legacyRow.date,
      newRating: _.isNil(row.newRating) ? legacyRow.rating : row.newRating,
      placement: toVisiblePlacement(row.placement) || toVisiblePlacement(legacyRow.placement),
      percentile: _.isNil(row.percentile) ? legacyRow.percentile : row.percentile
    }
  })
}

/**
 * Attach resolved canonical track/type labels to unified stats rows before response building.
 * @param {Array<Object>} rows unified stats or history rows from the database
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Array<Object>} rows annotated with trackName and typeName
 */
function annotateUnifiedDimensionRows (rows, dimensionLookup) {
  return _.map(rows || [], (row) => {
    const dimension = resolveStatsDimensionForChallengeRow(row, dimensionLookup)
    return {
      ...row,
      ...dimension
    }
  })
}

/**
 * Resolve a stats type filter to the stored value.
 * Known challenge types resolve through the dimension lookup. Configured rating
 * path names and deterministic path ids resolve through RATING_PATHS so freshly
 * created custom ChallengeType rows remain queryable even with a stale lookup.
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @param {*} value raw request filter value
 * @returns {string|undefined} stored filter value
 */
function resolveStatsTypeFilterValue (dimensionLookup, value) {
  const resolvedValue = resolveTypeIdFromLookup(dimensionLookup, value)
  if (resolvedValue) {
    return resolvedValue
  }

  const rawValue = String(value || '').trim()
  const configuredRatingPath = getConfiguredRatingPath(config.RATING_PATHS, rawValue) ||
    getConfiguredRatingPathByTypeId(config.RATING_PATHS, rawValue)
  if (configuredRatingPath) {
    return buildRatingPathTypeId(configuredRatingPath)
  }

  return rawValue || undefined
}

/**
 * Resolve optional unified stats filter parameters into stored ids.
 * Track filters resolve to challenge UUIDs. Type filters also accept configured
 * rating path names and deterministic path type ids.
 * @param {Object} query request query params
 * @param {Object} dimensionLookup shared challenge dimension lookup
 * @returns {Object} resolved filter payload
 */
function resolveUnifiedDimensionFilters (query, dimensionLookup) {
  const hasTrackFilter = !_.isNil(query.trackId) && String(query.trackId).trim() !== ''
  const hasTypeFilter = !_.isNil(query.typeId) && String(query.typeId).trim() !== ''

  return {
    hasTrackFilter,
    hasTypeFilter,
    trackId: hasTrackFilter ? resolveTrackIdFromLookup(dimensionLookup, query.trackId) : undefined,
    typeId: hasTypeFilter ? resolveStatsTypeFilterValue(dimensionLookup, query.typeId) : undefined
  }
}

function getUniqueTrackTypePairs (records) {
  return _.values(_.keyBy(_.map(records, record => ({
    trackId: record.trackId,
    typeId: record.typeId
  })), pair => `${pair.trackId}::${pair.typeId}`))
}

/**
 * Recompute the mostRecent marker for each affected (trackId, typeId) pair.
 * Exactly one row per pair is marked as mostRecent=true when rows exist.
 * The latest row's new rating and volatility are aligned with current memberStats
 * values when available. Old rating and volatility are aligned with the prior
 * history event for the same pair.
 *
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId user id
 * @param {Array} records history records that determine affected pairs
 * @param {String} operatorId operator id
 */
async function refreshMostRecentHistoryFlags (tx, userId, records, operatorId) {
  const pairs = getUniqueTrackTypePairs(records)
  for (const pair of pairs) {
    await tx.memberStatsHistory.updateMany({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId,
        mostRecent: true
      },
      data: {
        mostRecent: false,
        updatedBy: operatorId
      }
    })

    const latest = await tx.memberStatsHistory.findFirst({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId
      },
      orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
      select: { id: true }
    })

    if (latest) {
      const currentStats = await tx.memberStats.findFirst({
        where: {
          userId,
          trackId: pair.trackId,
          typeId: pair.typeId
        },
        select: {
          rating: true,
          volatility: true
        }
      })
      const previous = await tx.memberStatsHistory.findFirst({
        where: {
          userId,
          trackId: pair.trackId,
          typeId: pair.typeId
        },
        orderBy: [{ eventDate: 'desc' }, { id: 'desc' }],
        skip: 1,
        select: {
          newRating: true,
          newVolatility: true
        }
      })

      const latestUpdateData = {
        mostRecent: true,
        oldRating: previous ? previous.newRating : null,
        oldVolatility: previous ? previous.newVolatility : null,
        updatedBy: operatorId
      }
      if (currentStats) {
        latestUpdateData.newRating = currentStats.rating
        latestUpdateData.newVolatility = currentStats.volatility
      }

      await tx.memberStatsHistory.update({
        where: { id: latest.id },
        data: latestUpdateData
      })
    }
  }
}

/**
 * Synchronize new rating and volatility on the most recent history row per
 * (trackId, typeId) pair with the current values in memberStats.
 *
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId user id
 * @param {Array} records stats records that determine affected pairs
 * @param {String} operatorId operator id
 */
async function syncMostRecentHistoryRatings (tx, userId, records, operatorId) {
  const pairs = getUniqueTrackTypePairs(records)
  for (const pair of pairs) {
    const currentStats = await tx.memberStats.findFirst({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId
      },
      select: {
        rating: true,
        volatility: true
      }
    })
    if (!currentStats) {
      continue
    }

    await tx.memberStatsHistory.updateMany({
      where: {
        userId,
        trackId: pair.trackId,
        typeId: pair.typeId,
        mostRecent: true
      },
      data: {
        newRating: currentStats.rating,
        newVolatility: currentStats.volatility,
        updatedBy: operatorId
      }
    })
  }
}

/**
 * Get current member rating distribution statistics.
 * Resolves track/subTrack aliases to unified stats dimensions, aggregates rated
 * memberStats rows with positive ratings into the documented 100-point buckets,
 * and returns an empty histogram when matching stats exist but none have a rated
 * value.
 * @param {Object} query the query parameters
 * @param {String} [query.track] optional track filter
 * @param {String} [query.subTrack] optional subTrack/type filter
 * @param {String} [query.fields] optional comma-separated response fields
 * @returns {Object} distribution statistics for the requested filters
 * @throws {NotFoundError} when filters cannot be resolved or no matching stats exist
 */
async function getDistribution (query) {
  const startedAt = Date.now()
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, DISTRIBUTION_FIELDS_NO_DATE) || DISTRIBUTION_FIELDS_NO_DATE
  if (USE_LEGACY_STATS_READS) {
    return getLegacyDistribution(query, fields)
  }

  logger.info(`Calculating distribution on-the-fly for track='${query.track || ''}' subTrack='${query.subTrack || ''}'`)
  const dimensionStartedAt = Date.now()
  const dimensionLookup = await getChallengeDimensionLookup()
  const hasTrackFilter = !_.isNil(query.track) && String(query.track).trim() !== ''
  const hasTypeFilter = !_.isNil(query.subTrack) && String(query.subTrack).trim() !== ''
  const trackId = hasTrackFilter ? resolveTrackIdFromLookup(dimensionLookup, query.track) : undefined
  const typeId = hasTypeFilter ? resolveStatsTypeFilterValue(dimensionLookup, query.subTrack) : undefined
  logger.debug(`getDistribution resolved trackId='${trackId || ''}' typeId='${typeId || ''}' in ${Date.now() - dimensionStartedAt}ms`)

  if ((hasTrackFilter && !trackId) || (hasTypeFilter && !typeId)) {
    throw new errors.NotFoundError('No member distribution statistics is found.')
  }

  const whereConditions = [
    Prisma.sql`"rating" IS NOT NULL`,
    Prisma.sql`"rating" > ${DISTRIBUTION_MIN_RATING}`,
    Prisma.sql`"rating" < ${DISTRIBUTION_MAX_RATING_EXCLUSIVE}`
  ]
  if (trackId) {
    whereConditions.push(Prisma.sql`"trackId" = ${trackId}`)
  }
  if (typeId) {
    whereConditions.push(Prisma.sql`"typeId" = ${typeId}`)
  }

  const queryStartedAt = Date.now()
  const rows = await prisma.$queryRaw`
    SELECT
      (FLOOR("rating" / 100.0)::int * 100) AS "rangeStart",
      COUNT(*)::int AS "count"
    FROM "members"."memberStats"
    WHERE ${joinSqlConditions(whereConditions)}
    GROUP BY (FLOOR("rating" / 100.0)::int * 100)
    ORDER BY "rangeStart" ASC
  `
  const queryMs = Date.now() - queryStartedAt

  if (!rows || rows.length === 0) {
    logger.info(`getDistribution found no rated rows for track='${query.track || ''}' subTrack='${query.subTrack || ''}' queryMs=${queryMs} totalMs=${Date.now() - startedAt}`)
    const matchingStatsRow = await prisma.memberStats.findFirst({
      where: _.omitBy({
        trackId,
        typeId
      }, _.isUndefined),
      select: {
        id: true
      }
    })

    if (!matchingStatsRow) {
      throw new errors.NotFoundError('No member distribution statistics is found.')
    }

    let emptyResult = {
      track: query.track,
      subTrack: query.subTrack,
      distribution: createEmptyDistribution()
    }

    if (fields) {
      emptyResult = _.pick(emptyResult, fields)
    }

    return emptyResult
  }

  const distribution = createEmptyDistribution()
  _.forEach(rows, (row) => {
    const rangeStart = _.toInteger(row.rangeStart)
    const key = getDistributionRangeKey(rangeStart)
    if (key) {
      distribution[key] = Number(row.count)
    }
  })
  const totalRatedMembers = _.sumBy(rows, row => Number(row.count) || 0)

  let result = {
    track: query.track,
    subTrack: query.subTrack,
    distribution
  }

  if (fields) {
    result = _.pick(result, fields)
  }
  logger.info(`getDistribution calculated track='${query.track || ''}' subTrack='${query.subTrack || ''}' ranges=${rows.length} totalRatedMembers=${totalRatedMembers} queryMs=${queryMs} totalMs=${Date.now() - startedAt}`)
  return result
}

getDistribution.schema = {
  query: Joi.object().keys({
    track: Joi.string(),
    subTrack: Joi.string(),
    fields: Joi.string()
  })
}

/**
 * Get history statistics for completed challenges.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the history statistics
 */
async function getHistoryStats (currentUser, handle, query) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, HISTORY_STATS_FIELDS) || HISTORY_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)
  let result = []

  if (USE_LEGACY_STATS_READS) {
    const overallStat = []
    for (const groupId of groupIds) {
      const statsDb = await getLegacyHistoryStatsRow(member.userId, groupId)
      if (!_.isNil(statsDb)) {
        overallStat.push(statsDb)
      }
    }
    result = _.map(overallStat, t => buildLegacyStatsHistoryResponse(member, t, fields))
  } else {
    const dimensionLookup = await getChallengeDimensionLookup()
    const challengeClient = prismaManager.getChallengesClient()
    const where = {
      userId: member.userId
    }
    const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
    if (hasTrackFilter && !trackId) {
      return []
    }
    if (hasTypeFilter && !typeId) {
      return []
    }
    if (trackId) {
      where.trackId = trackId
    }
    if (typeId) {
      where.typeId = typeId
    }

    const historyRows = await prisma.memberStatsHistory.findMany({
      where,
      orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
    })
    const aggregateRows = await prisma.memberStats.findMany({
      where,
      select: {
        trackId: true,
        typeId: true,
        challenges: true,
        mostRecentSubmission: true,
        mostRecentEventDate: true
      }
    })

    const overallStat = []
    const visiblePairKeys = getVisibleUnifiedHistoryPairKeys(aggregateRows, dimensionLookup)
    const missingPairKeys = getMissingUnifiedHistoryPairKeys(aggregateRows, historyRows, dimensionLookup)

    if (historyRows.length > 0 || missingPairKeys.size > 0) {
      let reviewRows = []
      let unresolvedPairKeys = new Set(missingPairKeys)
      if (unresolvedPairKeys.size > 0 && reviewDb) {
        reviewRows = await fetchReviewChallengeResultsForMember(reviewDb, member.userId)
      }

      const challengeMetadataById = await fetchChallengeMetadataMap(
        challengeClient,
        _.uniq(_.map(historyRows, row => row.challengeId).concat(_.map(reviewRows, row => row.challengeId)))
      )

      let annotatedRows = dedupeUnifiedHistoryRows(filterUnifiedHistoryRowsToCompletedChallenges(
        enrichUnifiedHistoryRowsWithChallengeMetadata(
          annotateUnifiedDimensionRows(historyRows, dimensionLookup),
          challengeMetadataById
        ),
        challengeMetadataById
      ))

      if (unresolvedPairKeys.size > 0 && reviewRows.length > 0) {
        const reviewFallbackRows = buildFallbackHistoryRowsFromReviewResults(
          reviewRows,
          challengeMetadataById,
          dimensionLookup,
          unresolvedPairKeys
        )
        annotatedRows = mergeMissingHistoryRows(annotatedRows, reviewFallbackRows)
        unresolvedPairKeys = getUnresolvedHistoryPairKeys(unresolvedPairKeys, reviewFallbackRows)
      }

      if (
        missingPairKeys.size > 0 ||
        historyRowsNeedPlacementEnrichment(annotatedRows) ||
        marathonHistoryRowsNeedPlacementVerification(annotatedRows) ||
        historyRowsNeedChallengeNameEnrichment(annotatedRows)
      ) {
        const winnerRows = await fetchChallengeWinnerResultsForMember(challengeClient, member.userId)

        annotatedRows = dedupeUnifiedHistoryRows(mergeHistoryChallengeMetadataFromChallengeWinners(annotatedRows, winnerRows))
        annotatedRows = mergeHistoryPlacementsFromChallengeWinners(annotatedRows, winnerRows)
        const winnerFallbackPairKeys = new Set(
          Array.from(visiblePairKeys).concat(
            _.map(annotatedRows, row => buildStatsTrackTypeKey(row.trackId, row.typeId))
          )
        )

        const winnerFallbackRows = buildFallbackHistoryRowsFromChallengeWinners(
          winnerRows,
          dimensionLookup,
          winnerFallbackPairKeys
        )
        annotatedRows = mergeMissingHistoryRows(annotatedRows, winnerFallbackRows)
        unresolvedPairKeys = getUnresolvedHistoryPairKeys(unresolvedPairKeys, winnerFallbackRows)
      }

      if (unresolvedPairKeys.size > 0) {
        const legacyCodeFallbackRows = await buildFallbackHistoryRowsFromLegacyCodePages(
          reviewRows,
          aggregateRows,
          annotatedRows,
          dimensionLookup,
          unresolvedPairKeys
        )
        annotatedRows = mergeMissingHistoryRows(annotatedRows, legacyCodeFallbackRows)
        unresolvedPairKeys = getUnresolvedHistoryPairKeys(unresolvedPairKeys, legacyCodeFallbackRows)
      }

      annotatedRows = await hydrateLegacyMarathonHistoryRows(member.userId, annotatedRows)
      annotatedRows = reconcileLegacyMarathonHistoryRows(annotatedRows)

      const orderedRows = orderUnifiedHistoryRows(recomputeUnifiedHistoryMostRecentFlags(annotatedRows))
      if (orderedRows.length > 0) {
        _.forEach(groupIds, (groupId) => {
          const scopedRows = _.map(orderedRows, row => ({ ...row, groupId: _.toNumber(groupId) }))
          overallStat.push(scopedRows)
        })
      }
    }

    result = _.map(overallStat, rows => prismaHelper.buildUnifiedStatsHistoryResponse(member, rows, fields))
  }

  if (!helper.canManageMember(currentUser, member)) {
    result = _.map(result, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return result
}

getHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    groupIds: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    fields: Joi.string()
  })
}

/**
 * Create history stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the history stats data to create
 * @returns {Object} the created history stats
 */
async function createHistoryStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const operatorId = currentUser.userId || currentUser.sub
  const dimensionLookup = await getChallengeDimensionLookup()
  const challengeClient = prismaManager.getChallengesClient()
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data, dimensionLookup)
  if (!unifiedHistoryRecords || unifiedHistoryRecords.length === 0) {
    throw new errors.BadRequestError('No valid history records provided for unified history stats.')
  }

  logger.info(`Creating unified history stats for userId=${member.userId.toString()} with ${unifiedHistoryRecords.length} record(s)`)

  await prisma.$transaction(async (tx) => {
    const existingClauses = _.map(unifiedHistoryRecords, record => ({
      userId: member.userId,
      trackId: record.trackId,
      typeId: record.typeId,
      challengeId: record.challengeId
    }))

    const existingCount = await tx.memberStatsHistory.count({
      where: { OR: existingClauses }
    })
    if (existingCount > 0) {
      throw new errors.BadRequestError('History stats already exists')
    }

    await tx.memberStatsHistory.createMany({
      data: _.map(unifiedHistoryRecords, record => ({
        ...record,
        userId: member.userId,
        createdBy: operatorId,
        updatedBy: operatorId
      }))
    })

    await refreshMostRecentHistoryFlags(tx, member.userId, unifiedHistoryRecords, operatorId)
  })

  const createdRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId: member.userId,
      OR: _.map(unifiedHistoryRecords, record => ({
        trackId: record.trackId,
        typeId: record.typeId,
        challengeId: record.challengeId
      }))
    },
    orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
  })

  const challengeMetadataById = await fetchChallengeMetadataMap(challengeClient, _.map(createdRows, row => row.challengeId))
  const scopedRows = _.map(enrichUnifiedHistoryRowsWithChallengeMetadata(
    annotateUnifiedDimensionRows(createdRows, dimensionLookup),
    challengeMetadataById
  ), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsHistoryResponse(member, scopedRows, HISTORY_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

createHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challengeId: Joi.alternatives().try(Joi.string(), Joi.number()),
    mostRecent: Joi.boolean(),
    oldRating: Joi.number(),
    newRating: Joi.number(),
    placement: Joi.number(),
    percentile: Joi.number(),
    oldVolatility: Joi.number(),
    newVolatility: Joi.number(),
    oldGlobalRank: Joi.number(),
    newGlobalRank: Joi.number(),
    oldCountryRank: Joi.number(),
    newCountryRank: Joi.number(),
    oldSchoolRank: Joi.number(),
    newSchoolRank: Joi.number(),
    eventDate: Joi.number().positive(),
    date: Joi.number().positive(),
    ratingDate: Joi.number().positive(),
    history: Joi.array().items(Joi.object().keys({
      trackId: Joi.string(),
      typeId: Joi.string(),
      challengeId: Joi.alternatives().try(Joi.string(), Joi.number()).required(),
      mostRecent: Joi.boolean(),
      oldRating: Joi.number(),
      newRating: Joi.number(),
      placement: Joi.number(),
      percentile: Joi.number(),
      oldVolatility: Joi.number(),
      newVolatility: Joi.number(),
      oldGlobalRank: Joi.number(),
      newGlobalRank: Joi.number(),
      oldCountryRank: Joi.number(),
      newCountryRank: Joi.number(),
      oldSchoolRank: Joi.number(),
      newSchoolRank: Joi.number(),
      eventDate: Joi.number().positive(),
      date: Joi.number().positive(),
      ratingDate: Joi.number().positive()
    }))
  }).required()
}

/**
 * Partially update history stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the history stats data to update
 * @returns {Object} the updated history stats
 */
async function partiallyUpdateHistoryStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const operatorId = currentUser.userId || currentUser.sub
  const dimensionLookup = await getChallengeDimensionLookup()
  const challengeClient = prismaManager.getChallengesClient()
  const unifiedHistoryRecords = buildUnifiedHistoryRecordsFromPayload(data, dimensionLookup)
  if (!unifiedHistoryRecords || unifiedHistoryRecords.length === 0) {
    throw new errors.BadRequestError('No valid history records provided for unified history stats.')
  }

  await prisma.$transaction(async (tx) => {
    logger.info(`Upserting unified history stats for userId=${member.userId.toString()} with ${unifiedHistoryRecords.length} record(s)`)
    for (const record of unifiedHistoryRecords) {
      const existingRecord = await tx.memberStatsHistory.findFirst({
        where: {
          userId: member.userId,
          trackId: record.trackId,
          typeId: record.typeId,
          challengeId: record.challengeId
        }
      })
      if (existingRecord) {
        await tx.memberStatsHistory.update({
          where: { id: existingRecord.id },
          data: {
            ..._.omit(record, ['trackId', 'typeId', 'challengeId']),
            updatedBy: operatorId
          }
        })
      } else {
        await tx.memberStatsHistory.create({
          data: {
            ...record,
            userId: member.userId,
            createdBy: operatorId,
            updatedBy: operatorId
          }
        })
      }
    }

    await refreshMostRecentHistoryFlags(tx, member.userId, unifiedHistoryRecords, operatorId)
  })

  const updatedRows = await prisma.memberStatsHistory.findMany({
    where: {
      userId: member.userId,
      OR: _.map(unifiedHistoryRecords, record => ({
        trackId: record.trackId,
        typeId: record.typeId,
        challengeId: record.challengeId
      }))
    },
    orderBy: [{ mostRecent: 'desc' }, { eventDate: 'desc' }]
  })

  const challengeMetadataById = await fetchChallengeMetadataMap(challengeClient, _.map(updatedRows, row => row.challengeId))
  const scopedRows = _.map(enrichUnifiedHistoryRowsWithChallengeMetadata(
    annotateUnifiedDimensionRows(updatedRows, dimensionLookup),
    challengeMetadataById
  ), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsHistoryResponse(member, scopedRows, HISTORY_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

partiallyUpdateHistoryStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challengeId: Joi.alternatives().try(Joi.string(), Joi.number()),
    mostRecent: Joi.boolean(),
    oldRating: Joi.number(),
    newRating: Joi.number(),
    placement: Joi.number(),
    percentile: Joi.number(),
    oldVolatility: Joi.number(),
    newVolatility: Joi.number(),
    oldGlobalRank: Joi.number(),
    newGlobalRank: Joi.number(),
    oldCountryRank: Joi.number(),
    newCountryRank: Joi.number(),
    oldSchoolRank: Joi.number(),
    newSchoolRank: Joi.number(),
    eventDate: Joi.number().positive(),
    date: Joi.number().positive(),
    ratingDate: Joi.number().positive(),
    history: Joi.array().items(Joi.object().keys({
      trackId: Joi.string(),
      typeId: Joi.string(),
      challengeId: Joi.alternatives().try(Joi.string(), Joi.number()).required(),
      mostRecent: Joi.boolean(),
      oldRating: Joi.number(),
      newRating: Joi.number(),
      placement: Joi.number(),
      percentile: Joi.number(),
      oldVolatility: Joi.number(),
      newVolatility: Joi.number(),
      oldGlobalRank: Joi.number(),
      newGlobalRank: Joi.number(),
      oldCountryRank: Joi.number(),
      newCountryRank: Joi.number(),
      oldSchoolRank: Joi.number(),
      newSchoolRank: Joi.number(),
      eventDate: Joi.number().positive(),
      date: Joi.number().positive(),
      ratingDate: Joi.number().positive()
    }))
  }).required()
}

/**
 * Load member statistics from unified table.
 * @param {Object} member member row
 * @param {Array} groupIds requested group ids
 * @param {Object} query the query parameters
 * @param {Array} fields fields to return in response
 * @returns {Array} member statistics
 */
async function getUnifiedMemberStats (member, groupIds, query, fields) {
  const dimensionLookup = await getChallengeDimensionLookup()
  const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
  const stats = []

  if (hasTrackFilter && !trackId) {
    return stats
  }
  if (hasTypeFilter && !typeId) {
    return stats
  }

  for (const groupId of groupIds) {
    const where = {
      userId: member.userId,
      isPrivate: String(groupId) !== String(config.PUBLIC_GROUP_ID)
    }
    if (trackId) {
      where.trackId = trackId
    }
    if (typeId) {
      where.typeId = typeId
    }

    const unifiedStats = await prisma.memberStats.findMany({
      where,
      include: prismaHelper.unifiedStatsIncludeParams
    })

    if (unifiedStats && unifiedStats.length > 0) {
      const rankedStats = await hydrateComputedGlobalRanks(unifiedStats)
      const boundedStats = await hydrateMarathonRatingBoundsFromHistory(
        member.userId,
        annotateUnifiedDimensionRows(rankedStats, dimensionLookup)
      )
      const winHydratedStats = await hydrateMissingWinsFromHistory(member.userId, boundedStats)
      const responseStats = await hydrateLegacyDevelopSubmissionStats(member, groupId, winHydratedStats, dimensionLookup)
      const scopedStats = _.map(responseStats, stat => ({
        ...stat,
        groupId: _.toNumber(groupId)
      }))
      stats.push(prismaHelper.buildUnifiedStatsResponse(member, scopedStats, fields))
    }
  }

  return stats
}

/**
 * Load member statistics using legacy mapper from memberStats and nested legacy tables.
 * @param {Object} member member row
 * @param {Array} groupIds requested group ids
 * @param {Array} fields fields to return in response
 * @returns {Array} member statistics
 */
async function getLegacyMemberStats (member, groupIds, fields) {
  const stats = []
  for (const groupId of groupIds) {
    const stat = await getLegacyMemberStatsRow(member.userId, groupId)
    if (!_.isNil(stat)) {
      stats.push(prismaHelper.buildStatsResponse(member, stat, fields))
    }
  }
  return stats
}

/**
 * Get member statistics.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member statistics
 */
async function getMemberStats (currentUser, handle, query, throwError) {
  // validate and parse query parameter
  const fields = helper.parseCommaSeparatedString(query.fields, MEMBER_STATS_FIELDS) || MEMBER_STATS_FIELDS
  // get member by handle
  const member = await helper.getMemberByHandle(handle)

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, query.groupIds)
  let stats = []
  if (USE_LEGACY_STATS_READS) {
    stats = await getLegacyMemberStats(member, groupIds, fields)
    if (stats.length === 0) {
      logger.warn(`Legacy member stats lookup returned no rows for handle='${handle}', groupIds='${groupIds}'. Falling back to unified memberStats lookup.`)
      stats = await getUnifiedMemberStats(member, groupIds, query, fields)
    }
  } else {
    const dimensionLookup = await getChallengeDimensionLookup()
    const { hasTrackFilter, hasTypeFilter, trackId, typeId } = resolveUnifiedDimensionFilters(query, dimensionLookup)
    if (hasTrackFilter && !trackId) {
      return []
    }
    if (hasTypeFilter && !typeId) {
      return []
    }
    for (const groupId of groupIds) {
      const where = {
        userId: member.userId,
        isPrivate: groupId !== config.PUBLIC_GROUP_ID
      }
      if (trackId) {
        where.trackId = trackId
      }
      if (typeId) {
        where.typeId = typeId
      }

      const unifiedStats = await prisma.memberStats.findMany({
        where,
        include: prismaHelper.unifiedStatsIncludeParams
      })

      if (unifiedStats && unifiedStats.length > 0) {
        const rankedStats = await hydrateComputedGlobalRanks(unifiedStats)
        const boundedStats = await hydrateMarathonRatingBoundsFromHistory(
          member.userId,
          annotateUnifiedDimensionRows(rankedStats, dimensionLookup)
        )
        const winHydratedStats = await hydrateMissingWinsFromHistory(member.userId, boundedStats)
        const responseStats = await hydrateLegacyDevelopSubmissionStats(member, groupId, winHydratedStats, dimensionLookup)
        const scopedStats = _.map(responseStats, stat => ({
          ...stat,
          groupId: _.toNumber(groupId)
        }))
        stats.push(prismaHelper.buildUnifiedStatsResponse(member, scopedStats, fields))
      }
    }
  }

  if (throwError && stats.length === 0) {
    throw new errors.NotFoundError('Member stats not found')
  }

  if (!helper.canManageMember(currentUser, member)) {
    stats = _.map(stats, (item) => _.omit(item, config.STATISTICS_SECURE_FIELDS))
  }
  return stats
}

getMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  query: Joi.object().keys({
    groupIds: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    fields: Joi.string()
  }),
  throwError: Joi.boolean()
}

/**
 * Create member stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the stats data to create
 * @returns {Object} the updated member stats
 */
async function createMemberStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const isPrivate = groupIds[0] !== config.PUBLIC_GROUP_ID
  const rawData = _.cloneDeep(data)
  const dimensionLookup = await getChallengeDimensionLookup()
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate, dimensionLookup)
  const legacyMaxRatingData = isLegacyMaxRatingPayload(rawData.maxRating) ? rawData.maxRating : null

  let existingStat
  if (groupIds[0] === config.PUBLIC_GROUP_ID) {
    // get statistics by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: false }
    })
  } else {
    // get statistics private by member user id from db
    existingStat = await prisma.memberStats.findFirst({
      where: { userId: member.userId, isPrivate: true }
    })
  }

  if (existingStat) {
    throw new errors.BadRequestError('Member stats already exists')
  }

  if (!unifiedRecords || unifiedRecords.length === 0) {
    throw new errors.BadRequestError('No valid unified member stats payload provided.')
  }
  const operatorId = currentUser.userId || currentUser.sub
  logger.info(`Creating unified memberStats rows for userId=${member.userId.toString()} with ${unifiedRecords.length} row(s)`)
  await prisma.$transaction(async (tx) => {
    for (const record of unifiedRecords) {
      await tx.memberStats.create({
        data: {
          ...record,
          userId: member.userId,
          createdBy: operatorId,
          updatedBy: operatorId
        }
      })
    }
    await syncMostRecentHistoryRatings(tx, member.userId, unifiedRecords, operatorId)

    if (legacyMaxRatingData) {
      await prismaHelper.updateOrCreateModel(legacyMaxRatingData, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }
  })

  const allStats = await prisma.memberStats.findMany({
    where: { userId: member.userId, isPrivate },
    include: prismaHelper.unifiedStatsIncludeParams
  })
  const scopedStats = _.map(annotateUnifiedDimensionRows(allStats, dimensionLookup), stat => ({
    ...stat,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsResponse(member, scopedStats, MEMBER_STATS_FIELDS)
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  if (legacyMaxRatingData) {
    result.maxRating = {
      ...result.maxRating,
      ...legacyMaxRatingData
    }
  }
  return result
}

createMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challenges: Joi.number().positive(),
    wins: Joi.number().positive(),
    mostRecentSubmission: Joi.number().positive(),
    mostRecentEventDate: Joi.number().positive(),
    rating: Joi.number(),
    avgRank: Joi.number(),
    avgNumSubmissions: Joi.number(),
    bestRank: Joi.number(),
    globalRank: Joi.number(),
    countryRank: Joi.number(),
    schoolRank: Joi.number(),
    volatility: Joi.number(),
    minRating: Joi.number(),
    topFiveFinishes: Joi.number(),
    topTenFinishes: Joi.number(),
    records: Joi.array().items(Joi.object().keys({
      trackId: Joi.string().required(),
      typeId: Joi.string().required(),
      challenges: Joi.number(),
      wins: Joi.number(),
      mostRecentSubmission: Joi.number().positive(),
      mostRecentEventDate: Joi.number().positive(),
      rating: Joi.number(),
      avgRank: Joi.number(),
      avgNumSubmissions: Joi.number(),
      bestRank: Joi.number(),
      globalRank: Joi.number(),
      countryRank: Joi.number(),
      schoolRank: Joi.number(),
      volatility: Joi.number(),
      maxRating: Joi.number(),
      minRating: Joi.number(),
      topFiveFinishes: Joi.number(),
      topTenFinishes: Joi.number()
    })),
    maxRating: Joi.alternatives().try(
      Joi.object().keys({
        rating: Joi.number().positive().required(),
        track: Joi.string(),
        subTrack: Joi.string(),
        ratingColor: Joi.string().required()
      }),
      Joi.number()
    )
  }).required()
}

/**
 * Partially update member stats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the stats data to update
 * @returns {Object} the updated member stats
 */
async function partiallyUpdateMemberStats (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const groupIdsArr = []
  if (data.groupId) {
    groupIdsArr.push(data.groupId)
  }

  const groupIds = await helper.getAllowedGroupIds(currentUser, member, groupIdsArr)
  const isPrivate = groupIds[0] !== config.PUBLIC_GROUP_ID
  const rawData = _.cloneDeep(data)
  const dimensionLookup = await getChallengeDimensionLookup()
  const unifiedRecords = buildUnifiedStatsRecordsFromPayload(rawData, isPrivate, dimensionLookup, { partial: true })
  const legacyMaxRatingData = isLegacyMaxRatingPayload(rawData.maxRating) ? rawData.maxRating : null

  if ((!unifiedRecords || unifiedRecords.length === 0) && !legacyMaxRatingData) {
    throw new errors.BadRequestError('No valid unified member stats update payload provided.')
  }

  const operatorId = currentUser.userId || currentUser.sub

  await prisma.$transaction(async (tx) => {
    logger.info(`Upserting unified memberStats rows for userId=${member.userId.toString()} with ${unifiedRecords.length} row(s)`)
    for (const record of unifiedRecords) {
      await tx.memberStats.upsert({
        where: {
          userId_trackId_typeId: {
            userId: member.userId,
            trackId: record.trackId,
            typeId: record.typeId
          }
        },
        create: {
          ...record,
          userId: member.userId,
          createdBy: operatorId,
          updatedBy: operatorId
        },
        update: {
          ..._.omit(record, ['trackId', 'typeId', 'isPrivate']),
          isPrivate: record.isPrivate,
          updatedBy: operatorId
        }
      })
    }
    if (unifiedRecords.length > 0) {
      await syncMostRecentHistoryRatings(tx, member.userId, unifiedRecords, operatorId)
    }

    if (legacyMaxRatingData) {
      await prismaHelper.updateOrCreateModel(legacyMaxRatingData, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }
  })

  const updatedRows = await prisma.memberStats.findMany({
    where: { userId: member.userId, isPrivate },
    include: prismaHelper.unifiedStatsIncludeParams
  })
  const scopedRows = _.map(annotateUnifiedDimensionRows(updatedRows, dimensionLookup), row => ({
    ...row,
    groupId: _.toNumber(groupIds[0])
  }))
  let result = prismaHelper.buildUnifiedStatsResponse(member, scopedRows, MEMBER_STATS_FIELDS)
  if (legacyMaxRatingData) {
    result.maxRating = {
      ...result.maxRating,
      ...legacyMaxRatingData
    }
  }
  if (!helper.canManageMember(currentUser, member)) {
    result = _.omit(result, config.STATISTICS_SECURE_FIELDS)
  }
  return result
}

partiallyUpdateMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    groupId: Joi.string(),
    trackId: Joi.string(),
    typeId: Joi.string(),
    challenges: Joi.number().positive(),
    wins: Joi.number().positive(),
    mostRecentSubmission: Joi.number().positive(),
    mostRecentEventDate: Joi.number().positive(),
    rating: Joi.number(),
    avgRank: Joi.number(),
    avgNumSubmissions: Joi.number(),
    bestRank: Joi.number(),
    globalRank: Joi.number(),
    countryRank: Joi.number(),
    schoolRank: Joi.number(),
    volatility: Joi.number(),
    minRating: Joi.number(),
    topFiveFinishes: Joi.number(),
    topTenFinishes: Joi.number(),
    records: Joi.array().items(Joi.object().keys({
      trackId: Joi.string().required(),
      typeId: Joi.string().required(),
      challenges: Joi.number(),
      wins: Joi.number(),
      mostRecentSubmission: Joi.number().positive(),
      mostRecentEventDate: Joi.number().positive(),
      rating: Joi.number(),
      avgRank: Joi.number(),
      avgNumSubmissions: Joi.number(),
      bestRank: Joi.number(),
      globalRank: Joi.number(),
      countryRank: Joi.number(),
      schoolRank: Joi.number(),
      volatility: Joi.number(),
      maxRating: Joi.number(),
      minRating: Joi.number(),
      topFiveFinishes: Joi.number(),
      topTenFinishes: Joi.number()
    })),
    maxRating: Joi.alternatives().try(
      Joi.object().keys({
        rating: Joi.number().positive().required(),
        track: Joi.string(),
        subTrack: Joi.string(),
        ratingColor: Joi.string().required()
      }),
      Joi.number()
    )
  }).required()
}

/**
 * Refresh unified memberStats aggregates for a member from completed review-api challenge
 * results. Challenge metadata is resolved from challenge-api so counts and timestamps are
 * grouped by the existing unified track/type identifiers used in memberStats.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data optional payload echoed in the summary response with a string challengeId
 * @returns {Object} summary describing the refresh work that was completed
 * @throws {errors.ForbiddenError} if the caller is not allowed to manage the member
 */
async function refreshMemberStats (currentUser, handle, data) {
  const payload = data || {}
  const member = await helper.getMemberByHandle(handle)
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const operatorId = currentUser.userId || currentUser.sub
  const reviewDbClient = getReviewDbClientOrThrow()
  const challengeClient = prismaManager.getChallengesClient()
  const reviewRows = await fetchReviewChallengeResultsForMember(reviewDbClient, member.userId)

  if (reviewRows.length === 0) {
    return {
      handle,
      refreshed: true,
      challengeId: normalizeChallengeIdForResponse(payload.challengeId),
      challengeResultsProcessed: 0,
      statsUpdated: 0
    }
  }

  const challengeMetadataById = await fetchChallengeMetadataMap(
    challengeClient,
    _.uniq(_.map(reviewRows, row => String(row.challengeId)))
  )
  const dimensionLookup = await getChallengeDimensionLookup()
  const aggregateRows = buildAggregatedStatsFromReviewResults(reviewRows, challengeMetadataById, dimensionLookup)

  if (aggregateRows.length > 0) {
    await prisma.$transaction(async (tx) => {
      for (const aggregateRow of aggregateRows) {
        await tx.memberStats.upsert({
          where: {
            userId_trackId_typeId: {
              userId: member.userId,
              trackId: aggregateRow.trackId,
              typeId: aggregateRow.typeId
            }
          },
          create: {
            userId: member.userId,
            trackId: aggregateRow.trackId,
            typeId: aggregateRow.typeId,
            challenges: aggregateRow.challenges,
            wins: aggregateRow.wins,
            mostRecentSubmission: aggregateRow.mostRecentSubmission,
            mostRecentEventDate: aggregateRow.mostRecentEventDate,
            isPrivate: false,
            createdBy: operatorId,
            updatedBy: operatorId
          },
          update: {
            challenges: aggregateRow.challenges,
            wins: aggregateRow.wins,
            mostRecentSubmission: aggregateRow.mostRecentSubmission,
            mostRecentEventDate: aggregateRow.mostRecentEventDate,
            isPrivate: false,
            updatedBy: operatorId
          }
        })
      }

      await refreshMostRecentHistoryFlags(tx, member.userId, aggregateRows, operatorId)
    })
  }

  return {
    handle,
    refreshed: true,
    challengeId: normalizeChallengeIdForResponse(payload.challengeId),
    challengeResultsProcessed: reviewRows.length,
    statsUpdated: aggregateRows.length
  }
}

refreshMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    challengeId: Joi.alternatives().try(Joi.string().uuid(), Joi.number().integer().strict())
  })
}

/**
 * Re-rate every existing submitter on a completed challenge for all applicable
 * rating dimensions. This includes the native challenge track/type rating when
 * supported and any configured named rating paths whose tags/skills match the
 * challenge, such as the default AI path. Quality Assurance Challenge rows are
 * replayed into the public DATA_SCIENCE / Challenge rating bucket.
 * @param {Object} currentUser the user who performs operation
 * @param {Object} data rerate payload containing the completed challenge id
 * @returns {Object} summary of participants, rating jobs, updates, and per-member failures
 * @throws {errors.ForbiddenError} when the caller is not admin or M2M
 * @throws {errors.NotFoundError} when the challenge does not exist
 */
async function rerateChallengeSubmitterRatings (currentUser, data) {
  if (!currentUser || (!currentUser.isMachine && !helper.hasAdminRole(currentUser))) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const payload = data || {}
  if (_.isNil(payload.challengeId) || String(payload.challengeId).trim() === '') {
    throw new errors.BadRequestError('challengeId is required.')
  }

  const challengeClient = prismaManager.getChallengesClient()
  const reviewDbClient = getReviewDbClientOrThrow()
  const challenge = await fetchChallengeForRatingUpdate(challengeClient, payload.challengeId)
  if (!challenge) {
    throw new errors.NotFoundError(`Challenge with id: ${payload.challengeId} doesn't exist`)
  }

  const challengeId = String(challenge.id)
  const responseChallengeId = normalizeChallengeIdForResponse(challengeId)
  const baseResponse = {
    challengeId: responseChallengeId,
    rerated: false,
    ratings: [],
    participantIds: [],
    skippedParticipantIds: [],
    membersProcessed: 0,
    ratingsAttempted: 0,
    ratingsUpdated: 0,
    ratingFailures: []
  }

  if (!isCompletedChallenge(challenge)) {
    return {
      ...baseResponse,
      skippedReason: 'challenge-not-completed'
    }
  }

  const ratingJobs = buildChallengeRatingJobs(challenge)
  const ratings = ratingJobs.map((job) => _.omitBy({
    trackId: job.trackId,
    typeId: job.typeId,
    ratingName: job.ratingName,
    ratingTags: job.ratingPath ? job.ratingPath.tags : undefined,
    ratingSkillIds: job.ratingPath ? job.ratingPath.skillIds : undefined
  }, _.isUndefined))

  if (ratingJobs.length === 0) {
    return {
      ...baseResponse,
      ratings,
      skippedReason: 'no-supported-ratings'
    }
  }

  const participantIdsBySource = new Map()
  for (const job of ratingJobs) {
    if (participantIdsBySource.has(job.source)) {
      continue
    }

    participantIdsBySource.set(
      job.source,
      await fetchRatingParticipantIds(reviewDbClient, challengeClient, challengeId, job.source)
    )
  }

  const participantIds = _.uniqBy(
    Array.from(participantIdsBySource.values()).flat(),
    stringifyUserId
  )
  if (participantIds.length === 0) {
    return {
      ...baseResponse,
      ratings,
      skippedReason: 'no-submitters'
    }
  }

  const { existingParticipantIds, skippedParticipantIds } = await filterExistingRatingParticipantIds(
    prisma,
    participantIds
  )

  if (existingParticipantIds.length === 0) {
    return {
      ...baseResponse,
      ratings,
      participantIds: participantIds.map(stringifyUserId),
      skippedParticipantIds: skippedParticipantIds.map(stringifyUserId),
      skippedReason: 'no-existing-members'
    }
  }

  const ratingFailures = []
  let ratingsAttempted = 0
  let ratingsUpdated = 0

  for (const userId of existingParticipantIds) {
    for (const job of ratingJobs) {
      const sourceParticipantIds = participantIdsBySource.get(job.source) || []
      const sourceParticipantIdSet = new Set(sourceParticipantIds.map(stringifyUserId))
      if (!sourceParticipantIdSet.has(stringifyUserId(userId))) {
        continue
      }

      ratingsAttempted += 1

      try {
        const result = await rerateChallengeRatingJobForMember(
          challengeClient,
          reviewDbClient,
          userId,
          challengeId,
          job
        )
        ratingsUpdated += Number(result && result.ratingsUpdated) || 0
      } catch (error) {
        logger.warn(
          `Unable to rerate ${job.ratingName || `${job.trackId}/${job.typeId}`} for userId=${stringifyUserId(userId)} challengeId=${challengeId}: ${error.message}`
        )
        ratingFailures.push(_.omitBy({
          userId: stringifyUserId(userId),
          trackId: job.trackId,
          typeId: job.typeId,
          ratingName: job.ratingName,
          message: error.message
        }, _.isUndefined))
      }
    }
  }

  return {
    challengeId: responseChallengeId,
    rerated: ratingsAttempted > 0 && ratingFailures.length < ratingsAttempted,
    ratings,
    participantIds: participantIds.map(stringifyUserId),
    skippedParticipantIds: skippedParticipantIds.map(stringifyUserId),
    membersProcessed: existingParticipantIds.length,
    ratingsAttempted,
    ratingsUpdated,
    ratingFailures
  }
}

rerateChallengeSubmitterRatings.schema = {
  currentUser: Joi.any(),
  data: Joi.object().keys({
    challengeId: Joi.alternatives().try(Joi.string().uuid(), Joi.number().integer().strict()).required()
  }).required()
}

/**
 * Trigger a DEVELOPMENT / Challenge, DATA_SCIENCE / Challenge,
 * DATA_SCIENCE / MARATHON_MATCH, or configured tag- or skill-based rating path
 * re-rating pass beginning with the supplied challenge.
 * DATA_SCIENCE / Challenge rerates also replay Quality Assurance Challenge
 * source rows because QA history is surfaced in that public rating bucket.
 * The relevant review-api results are reprocessed in chronological order and
 * persisted into the existing unified rating tables for the member.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the rerate payload whose challengeId is echoed back as a string
 * @returns {Object} summary describing the rerate work that was completed
 * @throws {errors.ForbiddenError} if the caller is not allowed to manage the member
 */
async function rerateMemberStats (currentUser, handle, data) {
  const payload = data || {}
  const member = await helper.getMemberByHandle(handle)
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member stats.')
  }

  const ratingPath = resolveConfiguredRatingPath(payload.ratingName)
  const trackId = ratingPath ? ratingPath.trackName : resolveTrackName(payload.trackId || TRACK_NAMES.DEVELOP)
  const typeId = ratingPath ? ratingPath.name : resolveTypeName(payload.typeId || TYPE_NAMES.CHALLENGE)
  const challengeClient = prismaManager.getChallengesClient()
  const reviewDbClient = getReviewDbClientOrThrow()

  let result
  if (ratingPath) {
    result = await rerateMmTrack(
      prisma,
      challengeClient,
      null,
      reviewDbClient,
      member.userId,
      payload.challengeId,
      {
        ratingPath
      }
    )
  } else if (trackId === TRACK_NAMES.DEVELOP && typeId === TYPE_NAMES.CHALLENGE) {
    result = await rerateDevTrack(
      prisma,
      challengeClient,
      reviewDbClient,
      member.userId,
      payload.challengeId
    )
  } else if (trackId === TRACK_NAMES.DATA_SCIENCE && typeId === TYPE_NAMES.CHALLENGE) {
    result = await rerateDevTrack(
      prisma,
      challengeClient,
      reviewDbClient,
      member.userId,
      payload.challengeId,
      {
        targetTrackName: TRACK_NAMES.DATA_SCIENCE,
        targetTypeName: TYPE_NAMES.CHALLENGE,
        challengeTrackNames: getDataScienceChallengeSourceTrackNames(),
        challengeTypeNames: [TYPE_NAMES.CHALLENGE]
      }
    )
  } else if (trackId === TRACK_NAMES.DATA_SCIENCE && typeId === TYPE_NAMES.MARATHON_MATCH) {
    result = await rerateMmTrack(
      prisma,
      challengeClient,
      null,
      reviewDbClient,
      member.userId,
      payload.challengeId
    )
  } else {
    throw new errors.BadRequestError('Only DEVELOP / Challenge, DATA_SCIENCE / Challenge, and DATA_SCIENCE / MARATHON_MATCH rerates are currently supported.')
  }

  return {
    handle,
    rerated: true,
    challengeId: normalizeChallengeIdForResponse(payload.challengeId),
    trackId,
    typeId,
    ratingName: ratingPath ? ratingPath.name : undefined,
    ratingTags: ratingPath ? ratingPath.tags : undefined,
    ratingSkillIds: ratingPath ? ratingPath.skillIds : undefined,
    challengesRerated: Math.max(result.challengesProcessed - 1, 0),
    challengesProcessed: result.challengesProcessed,
    ratingPathChallengesProcessed: result.ratingPathChallengesProcessed,
    ratingsUpdated: result.ratingsUpdated
  }
}

rerateMemberStats.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    challengeId: Joi.alternatives().try(Joi.string().uuid(), Joi.number().integer().strict()).required(),
    ratingName: Joi.string(),
    trackId: Joi.string().valid(TRACK_NAMES.DEVELOP, TRACK_NAMES.DATA_SCIENCE).insensitive(),
    typeId: Joi.string().valid(TYPE_NAMES.CHALLENGE, TYPE_NAMES.MARATHON_MATCH).insensitive()
  }).required()
}

/**
 * Get member skills.
 * @param {String} handle the member handle
 * @param {Object} query the query parameters
 * @returns {Object} the member skills
 */
async function getMemberSkills (handle) {
  // validate member
  const member = await helper.getMemberByHandle(handle)
  const skillList = await skillsPrisma.userSkill.findMany({
    where: {
      userId: helper.bigIntToNumber(member.userId)
    },
    include: prismaHelper.skillsIncludeParams
  })
  // convert to response format
  return prismaHelper.buildMemberSkills(skillList)
}

getMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required()
}

/**
 * Check create/update member skill data
 * @param {Object} data request body
 */
async function validateMemberSkillData (data) {
  // Check displayMode
  if (data.displayModeId) {
    const modeCount = await skillsPrisma.userSkillDisplayMode.count({
      where: { id: data.displayModeId }
    })
    if (modeCount <= 0) {
      throw new errors.BadRequestError(`Display mode ${data.displayModeId} does not exist`)
    }
  }
  if (data.levels && data.levels.length > 0) {
    const levelCount = await skillsPrisma.userSkillLevel.count({
      where: { id: { in: data.levels } }
    })
    if (levelCount < data.levels.length) {
      throw new errors.BadRequestError(`Please make sure skill level exists`)
    }
  }
}

async function createMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate request
  const existingCount = await skillsPrisma.userSkill.count({
    where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId }
  })
  if (existingCount > 0) {
    throw new errors.BadRequestError('This member skill exists')
  }
  await validateMemberSkillData(data)

  // save to db
  // Determine target levels: provided, or default to 'self-declared'
  let levelIds = data.levels && data.levels.length > 0 ? data.levels : null
  if (!levelIds) {
    const selfDeclared = await skillsPrisma.userSkillLevel.findFirst({ where: { name: 'self-declared' } })
    if (!selfDeclared) {
      throw new errors.NotFoundError('Default skill level "self-declared" not found')
    }
    levelIds = [selfDeclared.id]
  }
  const modeId = data.displayModeId || (await (async () => {
    const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
    return principal ? principal.id : undefined
  })())
  if (!modeId) {
    throw new errors.BadRequestError('Display mode is required and default mode not found')
  }

  for (const levelId of levelIds) {
    await skillsPrisma.userSkill.create({
      data: {
        userId: helper.bigIntToNumber(member.userId),
        skillId: data.skillId,
        userSkillLevelId: levelId,
        userSkillDisplayModeId: modeId
      }
    })
  }

  // get skills by member handle
  const memberSkill = await this.getMemberSkills(handle)
  return memberSkill
}

createMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillId: Joi.string().uuid().required(),
    displayModeId: Joi.string().uuid(),
    levels: Joi.array().items(Joi.string().uuid())
  }).required()
}

/**
 * Partially update member skills.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the skills data to update
 * @returns {Object} the updated member skills
 */
async function partiallyUpdateMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate request
  const existingUserSkills = await skillsPrisma.userSkill.findMany({
    where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId }
  })
  if (!existingUserSkills || existingUserSkills.length === 0) {
    throw new errors.NotFoundError('Member skill not found')
  }
  await validateMemberSkillData(data)

  if (data.levels && data.levels.length > 0) {
    // Replace all existing with new set
    await skillsPrisma.userSkill.deleteMany({ where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId } })
    const modeId = data.displayModeId || (existingUserSkills[0] && existingUserSkills[0].userSkillDisplayModeId) || (await (async () => {
      const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
      return principal ? principal.id : undefined
    })())
    if (!modeId) {
      throw new errors.BadRequestError('Display mode is required and default mode not found')
    }
    for (const levelId of data.levels) {
      await skillsPrisma.userSkill.create({
        data: {
          userId: helper.bigIntToNumber(member.userId),
          skillId: data.skillId,
          userSkillLevelId: levelId,
          userSkillDisplayModeId: modeId
        }
      })
    }
  } else if (data.displayModeId) {
    // Update display mode on all existing records for this skill
    await skillsPrisma.userSkill.updateMany({
      where: { userId: helper.bigIntToNumber(member.userId), skillId: data.skillId },
      data: { userSkillDisplayModeId: data.displayModeId }
    })
  }

  // get skills by member handle
  const memberSkill = await this.getMemberSkills(handle)
  return memberSkill
}

partiallyUpdateMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillId: Joi.string().uuid().required(),
    displayModeId: Joi.string().uuid(),
    levels: Joi.array().items(Joi.string().uuid())
  }).required()
}

/**
 * Bulk verify member skills for a completed course.
 * Ensures each provided skill is associated with the member and has level 'verified'.
 * Replaces any existing levels for those skills with 'verified'.
 * @param {Object} currentUser the user who performs operation
 * @param {String} handle the member handle
 * @param {Object} data the payload containing skillIds: string[]
 * @returns {Object} the updated member skills
 */
async function verifyMemberSkills (currentUser, handle, data) {
  // get member by handle
  const member = await helper.getMemberByHandle(handle)
  // check authorization
  if (!helper.canManageMember(currentUser, member)) {
    throw new errors.ForbiddenError('You are not allowed to update the member skills.')
  }

  // validate input
  if (!data || !Array.isArray(data.skillIds) || data.skillIds.length === 0) {
    throw new errors.BadRequestError('skillIds is required and must be a non-empty array')
  }

  // ensure all skills exist
  const skillsCount = await skillsPrisma.skill.count({ where: { id: { in: data.skillIds } } })
  if (skillsCount < data.skillIds.length) {
    throw new errors.BadRequestError('One or more provided skills do not exist')
  }

  // find the 'verified' skill level id
  const verifiedLevel = await skillsPrisma.userSkillLevel.findFirst({ where: { name: 'verified' } })
  if (!verifiedLevel || !verifiedLevel.id) {
    throw new errors.NotFoundError('Verified skill level not found')
  }

  // process each skill: upsert memberSkill and set levels to verified only
  for (const skillId of data.skillIds) {
    const existing = await skillsPrisma.userSkill.findMany({
      where: { userId: helper.bigIntToNumber(member.userId), skillId }
    })
    // preserve display mode if any existing record
    let modeId = existing[0] ? existing[0].userSkillDisplayModeId : undefined
    if (!modeId) {
      const principal = await skillsPrisma.userSkillDisplayMode.findFirst({ where: { name: 'principal' } })
      modeId = principal ? principal.id : undefined
    }
    if (!modeId) {
      throw new errors.BadRequestError('Display mode is required and default mode not found')
    }
    // replace all with a single verified record
    await skillsPrisma.userSkill.deleteMany({ where: { userId: helper.bigIntToNumber(member.userId), skillId } })
    await skillsPrisma.userSkill.create({
      data: {
        userId: helper.bigIntToNumber(member.userId),
        skillId,
        userSkillLevelId: verifiedLevel.id,
        userSkillDisplayModeId: modeId
      }
    })
  }

  // return the updated skills set
  return this.getMemberSkills(handle)
}

verifyMemberSkills.schema = {
  currentUser: Joi.any(),
  handle: Joi.string().required(),
  data: Joi.object().keys({
    skillIds: Joi.array().items(Joi.string().uuid()).required()
  }).required()
}

module.exports = {
  getDistribution,
  getHistoryStats,
  createHistoryStats,
  partiallyUpdateHistoryStats,
  getMemberStats,
  createMemberStats,
  partiallyUpdateMemberStats,
  refreshMemberStats,
  rerateChallengeSubmitterRatings,
  rerateMemberStats,
  getMemberSkills,
  createMemberSkills,
  partiallyUpdateMemberSkills,
  verifyMemberSkills
}

logger.buildService(module.exports)

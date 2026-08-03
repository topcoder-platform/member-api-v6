/**
 * Build the Topcoder activity summary used by downloaded member profiles.
 *
 * Profiles derives its displayed track totals from member stats and stats history.
 * This helper applies the same grouping and history fallbacks so the PDF does not
 * invent a second set of counters from challenge registrations or winner rows.
 */

const TESTING_SUBTRACK_NAMES = new Set([
  'BUG_HUNT',
  'TEST_SCENARIOS',
  'TEST_SUITES'
])
const AI_ENGINEERING_TRACK_NAMES = new Set([
  'AI',
  'AI_ENGINEER',
  'AI_ENGINEERING'
])
const NATIVE_DATA_SCIENCE_SUBTRACK_NAMES = [
  'Challenge',
  'MARATHON_MATCH'
]
const NATIVE_DATA_SCIENCE_STATS_KEYS = new Set([
  ...NATIVE_DATA_SCIENCE_SUBTRACK_NAMES,
  'SRM',
  'challenges',
  'mostRecentEventDate',
  'mostRecentEventName',
  'mostRecentSubmission',
  'wins'
])

/**
 * Return a finite numeric value without coercing strings or nullish values.
 * PDF profile summaries use this to distinguish missing counters from zero.
 * @param {*} value candidate numeric value from the member stats response
 * @returns {number|undefined} the finite number, or undefined when unavailable
 */
function getFiniteNumber (value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

/**
 * Normalize a track name for matching Profiles aliases such as AI Engineering.
 * @param {*} value raw track or rating-path name
 * @returns {string} uppercase underscore-delimited track token
 */
function normalizeTrackName (value) {
  return String(value || '').trim().toUpperCase().replace(/[\s-]+/g, '_')
}

/**
 * Determine whether a track or rating-path name represents AI Engineering.
 * Profiles groups these aliases into Development activity.
 * @param {*} value raw track or rating-path name
 * @returns {boolean} true for a supported AI Engineering alias
 */
function isAIEngineeringTrackName (value) {
  return AI_ENGINEERING_TRACK_NAMES.has(normalizeTrackName(value))
}

/**
 * Read an explicit submission count from a member stats subtrack.
 * Both legacy numeric values and unified nested submission objects are supported.
 * @param {Object} subTrack member stats subtrack
 * @returns {number|undefined} explicit submission count when present
 */
function getSubTrackSubmissionCount (subTrack) {
  const nestedCount = subTrack && subTrack.submissions && typeof subTrack.submissions === 'object'
    ? subTrack.submissions.submissions
    : undefined
  return getFiniteNumber(nestedCount) ?? getFiniteNumber(subTrack && subTrack.submissions)
}

/**
 * Resolve the submission count displayed by Profiles for a subtrack.
 * Positive explicit counts win; otherwise challenge participation is the fallback.
 * @param {Object} subTrack member stats subtrack
 * @returns {number|undefined} displayable submission count when available
 */
function getSubTrackDisplaySubmissionCount (subTrack) {
  const submissionCount = getSubTrackSubmissionCount(subTrack)
  if (submissionCount !== undefined && submissionCount > 0) {
    return submissionCount
  }

  const challengeCount = getFiniteNumber(subTrack && subTrack.challenges)
  return challengeCount !== undefined && challengeCount > 0
    ? challengeCount
    : submissionCount
}

/**
 * Determine whether a member stats subtrack has activity visible in Profiles.
 * @param {Object} subTrack member stats subtrack
 * @returns {boolean} true when submissions or challenges are positive
 */
function isActiveSubTrack (subTrack) {
  return (getSubTrackDisplaySubmissionCount(subTrack) ?? 0) > 0 ||
    (getFiniteNumber(subTrack && subTrack.challenges) ?? 0) > 0
}

/**
 * Load history rows for a stats subtrack from the member history response.
 * Unified DEVELOPMENT, DESIGN, and QA history is stored in named subtrack arrays.
 * @param {Object|undefined} statsHistory member stats history response
 * @param {string} trackName API track key containing the subtrack
 * @param {string} subTrackName API subtrack name
 * @returns {Array<Object>} matching history rows, or an empty array
 */
function getSubTrackHistory (statsHistory, trackName, subTrackName) {
  const trackHistory = statsHistory && statsHistory[trackName]
  if (!trackHistory) {
    return []
  }

  if (Array.isArray(trackHistory.history)) {
    return trackHistory.history
  }

  if (Array.isArray(trackHistory.subTracks)) {
    const matchingSubTrack = trackHistory.subTracks.find(subTrack => subTrack && subTrack.name === subTrackName)
    return matchingSubTrack && Array.isArray(matchingSubTrack.history)
      ? matchingSubTrack.history
      : []
  }

  const keyedHistory = trackHistory[subTrackName]
  return keyedHistory && Array.isArray(keyedHistory.history) ? keyedHistory.history : []
}

/**
 * Pick the rated AI Engineering stats source that Profiles groups under Development.
 * A DATA_SCIENCE rating path is preferred over compatible top-level AI payloads.
 * @param {Object} stats member stats response for one public group
 * @returns {Object|undefined} subtrack plus its API history track key
 */
function getAIEngineeringSource (stats) {
  const dataScienceStats: Record<string, any> = stats.DATA_SCIENCE || {}
  const dataScienceCandidates = Object.entries(dataScienceStats)
    .filter(([name, value]) => (
      isAIEngineeringTrackName(name) &&
      value && typeof value === 'object' &&
      getFiniteNumber(value.rank && value.rank.rating) !== undefined
    ))
    .map(([name, subTrack]) => ({
      subTrack: { ...subTrack, name },
      trackName: 'DATA_SCIENCE'
    }))

  if (dataScienceCandidates.length > 0) {
    return getDataScienceSummarySource(dataScienceCandidates)
  }

  const topLevelName = ['AI_ENGINEERING', 'AI', 'AI_ENGINEER']
    .find(name => stats[name] && typeof stats[name] === 'object')
  if (!topLevelName) {
    return undefined
  }

  return {
    subTrack: { ...stats[topLevelName], name: topLevelName },
    trackName: topLevelName
  }
}

/**
 * Build the win and submission totals displayed for one Profiles subtrack.
 * Placement-bearing history is authoritative for wins, while history length is
 * the minimum submission count when aggregate counters are stale or incomplete.
 * @param {Object} source subtrack plus the API track key used to find its history
 * @param {Object|undefined} statsHistory member stats history response
 * @returns {{wins: number, submissions: number}} display-safe subtrack totals
 */
function getSubTrackSummary (source, statsHistory) {
  const history = getSubTrackHistory(statsHistory, source.trackName, source.subTrack.name)
  const historyWithPlacements = history.filter(row => getFiniteNumber(row && row.placement) !== undefined)

  return {
    submissions: Math.max(getSubTrackDisplaySubmissionCount(source.subTrack) ?? 0, history.length),
    wins: historyWithPlacements.length > 0
      ? historyWithPlacements.filter(row => row.placement === 1).length
      : getFiniteNumber(source.subTrack.wins) ?? 0
  }
}

/**
 * Build the stable challenge identity used by Profiles to de-duplicate history.
 * @param {Object} historyRow member stats history row
 * @returns {string} composite challenge identity
 */
function getHistoryChallengeKey (historyRow) {
  return [
    historyRow && historyRow.challengeId,
    historyRow && historyRow.challengeName,
    historyRow && (historyRow.ratingDate ?? historyRow.date)
  ].map(value => String(value ?? '')).join('::')
}

/**
 * Sum a track without de-duplicating activity across its subtracks.
 * Design and Testing detail views use raw challenge totals plus history-aware
 * win and submission summaries for each child card.
 * @param {Array<Object>} sources active subtracks with their API track keys
 * @param {Object|undefined} statsHistory member stats history response
 * @returns {{challenges: number, wins: number, submissions: number}} track totals
 */
function getStandardTrackSummary (sources, statsHistory) {
  return sources.reduce((summary, source) => {
    const subTrackSummary = getSubTrackSummary(source, statsHistory)
    summary.challenges += getFiniteNumber(source.subTrack.challenges) ?? 0
    summary.wins += subTrackSummary.wins
    summary.submissions += subTrackSummary.submissions
    return summary
  }, { challenges: 0, wins: 0, submissions: 0 })
}

/**
 * Build Development totals using the Profiles history de-duplication rules.
 * This prevents overlapping rating paths from counting the same challenge twice
 * while retaining aggregate-only CODE and First2Finish activity.
 * @param {Array<Object>} sources active Development subtracks
 * @param {Object|undefined} statsHistory member stats history response
 * @returns {{challenges: number, wins: number, submissions: number}} Development totals
 */
function getDevelopmentTrackSummary (sources, statsHistory) {
  const summaries = sources.map(source => ({
    history: getSubTrackHistory(statsHistory, source.trackName, source.subTrack.name),
    stats: getSubTrackSummary(source, statsHistory),
    subTrack: source.subTrack
  }))
  const historySummaries = summaries.filter(summary => summary.history.length > 0)

  if (historySummaries.length === 0) {
    return getStandardTrackSummary(sources, statsHistory)
  }

  const uniqueHistoryByChallenge = new Map()
  let hasDuplicateHistory = false
  historySummaries.forEach((summary) => {
    summary.history.forEach((historyRow) => {
      const key = getHistoryChallengeKey(historyRow)
      const existingHistory = uniqueHistoryByChallenge.get(key)
      if (existingHistory) {
        hasDuplicateHistory = true
      }
      if (!existingHistory || existingHistory.placement !== 1) {
        uniqueHistoryByChallenge.set(key, historyRow)
      }
    })
  })

  const uniqueHistory = Array.from(uniqueHistoryByChallenge.values())
  const noHistorySources = sources.filter((source) => (
    getSubTrackHistory(statsHistory, source.trackName, source.subTrack.name).length === 0
  ))
  const noHistoryStats = getStandardTrackSummary(noHistorySources, statsHistory)
  const historyChallengeExtras = historySummaries.reduce((total, summary) => (
    total + Math.max(0, (getFiniteNumber(summary.subTrack.challenges) ?? 0) - summary.history.length)
  ), 0)
  const historySubmissionExtras = historySummaries.reduce((total, summary) => (
    total + Math.max(0, summary.stats.submissions - summary.history.length)
  ), 0)
  const uniqueHistoryWins = uniqueHistory.filter(historyRow => historyRow.placement === 1).length
  const historyStatsWins = hasDuplicateHistory
    ? Math.max(...historySummaries.map(summary => summary.stats.wins))
    : historySummaries.reduce((total, summary) => total + summary.stats.wins, 0)

  return {
    challenges: uniqueHistory.length + historyChallengeExtras + noHistoryStats.challenges,
    submissions: uniqueHistory.length + historySubmissionExtras + noHistoryStats.submissions,
    wins: (uniqueHistoryWins > 0 ? uniqueHistoryWins : historyStatsWins) + noHistoryStats.wins
  }
}

/**
 * Pick the Data Science subtrack whose rating Profiles displays.
 * Rating, percentile, and challenge count are descending tie breakers.
 * @param {Array<Object>} sources active rated Data Science sources
 * @returns {Object|undefined} source with the strongest visible rating
 */
function getDataScienceSummarySource (sources) {
  return [...sources].sort((left, right) => {
    const leftRank = left.subTrack.rank || {}
    const rightRank = right.subTrack.rank || {}

    return (getFiniteNumber(rightRank.rating) ?? 0) - (getFiniteNumber(leftRank.rating) ?? 0) ||
      (getFiniteNumber(rightRank.percentile) ?? 0) - (getFiniteNumber(leftRank.percentile) ?? 0) ||
      (getFiniteNumber(right.subTrack.challenges) ?? 0) - (getFiniteNumber(left.subTrack.challenges) ?? 0)
  })[0]
}

/**
 * Build the independently rated, non-native Data Science rows shown by Profiles.
 * Native Challenge, Marathon Match, and SRM rows are handled by their parent
 * tracks, while AI Engineering aliases remain grouped under Development.
 * @param {Object} stats member stats response for one public group
 * @param {Object|undefined} statsHistory member stats history response for the same group
 * @returns {Array<{trackName: string, rating: number, wins: number, submissions: number, challenges: number}>} custom rated rows
 */
function getDataScienceRatingPathRows (stats, statsHistory) {
  const dataScienceStats: Record<string, any> = stats.DATA_SCIENCE
  if (!dataScienceStats || typeof dataScienceStats !== 'object') {
    return []
  }

  return Object.entries(dataScienceStats)
    .filter(([name, subTrack]) => (
      !NATIVE_DATA_SCIENCE_STATS_KEYS.has(name) &&
      !isAIEngineeringTrackName(name) &&
      subTrack && typeof subTrack === 'object' &&
      getFiniteNumber(subTrack.rank && subTrack.rank.rating) !== undefined
    ))
    .sort(([, left], [, right]) => (
      (getFiniteNumber(right.wins) ?? 0) - (getFiniteNumber(left.wins) ?? 0) ||
      (getSubTrackDisplaySubmissionCount(right) ?? 0) - (getSubTrackDisplaySubmissionCount(left) ?? 0)
    ))
    .map(([name, subTrack]) => {
      const source = {
        subTrack: { ...subTrack, name },
        trackName: 'DATA_SCIENCE'
      }

      return {
        trackName: name,
        rating: getFiniteNumber(subTrack.rank && subTrack.rank.rating) ?? 0,
        ...getSubTrackSummary(source, statsHistory),
        challenges: getFiniteNumber(subTrack.challenges) ?? 0
      }
    })
}

/**
 * Convert member stats and history responses into downloaded-profile activity rows.
 * The PDF uses this mapper to match the Development, Design, Testing, Data
 * Science, configured rating-path, and Competitive Programming values shown by
 * Profiles. Competitive Programming is emitted only for active SRM stats.
 * This function does not mutate its inputs or throw for missing response fields.
 * @param {Object|undefined} stats member stats response for one public group
 * @param {Object|undefined} statsHistory member stats history response for the same group
 * @returns {Array<{trackName: string, wins: number, submissions?: number, challenges?: number, rating?: number, competitions?: number}>} PDF activity rows
 */
function buildProfileActivityStats (stats, statsHistory) {
  if (!stats) {
    return []
  }

  const developSources = Array.isArray(stats.DEVELOP && stats.DEVELOP.subTracks)
    ? stats.DEVELOP.subTracks.map(subTrack => ({ subTrack, trackName: 'DEVELOP' }))
    : []
  const developmentSources = developSources.filter(source => (
    !TESTING_SUBTRACK_NAMES.has(source.subTrack.name) && isActiveSubTrack(source.subTrack)
  ))
  if (!developmentSources.some(source => isAIEngineeringTrackName(source.subTrack.name))) {
    const aiEngineeringSource = getAIEngineeringSource(stats)
    if (aiEngineeringSource && isActiveSubTrack(aiEngineeringSource.subTrack)) {
      developmentSources.push(aiEngineeringSource)
    }
  }
  const designSources = Array.isArray(stats.DESIGN && stats.DESIGN.subTracks)
    ? stats.DESIGN.subTracks
      .filter(isActiveSubTrack)
      .map(subTrack => ({ subTrack, trackName: 'DESIGN' }))
    : []
  const qaSources = Array.isArray(stats.QA && stats.QA.subTracks)
    ? stats.QA.subTracks
      .filter(isActiveSubTrack)
      .map(subTrack => ({ subTrack, trackName: 'QA' }))
    : []
  const testingSources = developSources
    .filter(source => TESTING_SUBTRACK_NAMES.has(source.subTrack.name) && isActiveSubTrack(source.subTrack))
    .concat(qaSources)
  const result = []

  if (developmentSources.length > 0) {
    result.push({
      trackName: 'Development',
      ...getDevelopmentTrackSummary(developmentSources, statsHistory)
    })
  }
  if (designSources.length > 0) {
    result.push({
      trackName: 'Design',
      ...getStandardTrackSummary(designSources, statsHistory)
    })
  }
  if (testingSources.length > 0) {
    result.push({
      trackName: 'Testing',
      ...getStandardTrackSummary(testingSources, statsHistory)
    })
  }

  const dataScienceStats: Record<string, any> = stats.DATA_SCIENCE || {}
  const dataScienceSources = NATIVE_DATA_SCIENCE_SUBTRACK_NAMES
    .filter(name => (
      dataScienceStats[name] &&
      typeof dataScienceStats[name] === 'object' &&
      (getFiniteNumber(dataScienceStats[name].challenges) ?? 0) > 0
    ))
    .map(name => ({
      subTrack: { ...dataScienceStats[name], name },
      trackName: 'DATA_SCIENCE'
    }))
  if (dataScienceSources.length > 0) {
    const summarySource = getDataScienceSummarySource(dataScienceSources)
    result.push({
      trackName: 'Data Science',
      rating: getFiniteNumber(summarySource && summarySource.subTrack.rank && summarySource.subTrack.rank.rating) ?? 0,
      ...getStandardTrackSummary(dataScienceSources, statsHistory)
    })
  }

  const srmStats = stats.DATA_SCIENCE && stats.DATA_SCIENCE.SRM
  const competitions = getFiniteNumber(srmStats && srmStats.challenges) ?? 0
  if (competitions > 0) {
    result.push({
      trackName: 'Competitive Programming',
      rating: getFiniteNumber(srmStats && srmStats.rank && srmStats.rank.rating) ?? 0,
      wins: getFiniteNumber(srmStats && srmStats.wins) ?? 0,
      competitions
    })
  }

  result.push(...getDataScienceRatingPathRows(stats, statsHistory))

  return result
}

/**
 * Resolve stats requests and build downloaded-profile activity rows.
 * Member stats are required; history is optional and falls back to aggregate
 * counters when its request fails. Promise.all is compatible with the Bluebird
 * global used by the application bootstrap.
 * @param {Promise<Array<Object>>} statsRequest member stats request
 * @param {Promise<Array<Object>>} historyRequest member stats history request
 * @param {Function} [onHistoryFailure] optional history error callback
 * @returns {Promise<Array<{trackName: string, wins: number, submissions?: number, challenges?: number, rating?: number, competitions?: number}>>} PDF activity rows
 * @throws {*} when the required member stats request fails
 */
async function buildProfileActivityStatsFromRequests (statsRequest, historyRequest, onHistoryFailure) {
  const safeHistoryRequest = Promise.resolve(historyRequest).catch((error) => {
    if (typeof onHistoryFailure === 'function') {
      onHistoryFailure(error)
    }
    return []
  })
  const [statsResult, historyResult] = await Promise.all([
    statsRequest,
    safeHistoryRequest
  ])

  return buildProfileActivityStats(
    Array.isArray(statsResult) ? statsResult[0] : undefined,
    Array.isArray(historyResult) ? historyResult[0] : undefined
  )
}

module.exports = {
  buildProfileActivityStats,
  buildProfileActivityStatsFromRequests
}

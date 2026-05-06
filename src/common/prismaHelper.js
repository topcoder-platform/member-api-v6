const _ = require('lodash')
const helper = require('./helper')
const errors = require('./errors')
const {
  getCanonicalTrackName,
  getCanonicalTypeName
} = require('./statsDimensionHelper')

const designBasicFields = [
  'name', 'numInquiries', 'submissions', 'passedScreening', 'avgPlacement',
  'screeningSuccessRate', 'submissionRate', 'winPercent'
]

const developSubmissionFields = [
  'appealSuccessRate', 'minScore', 'avgPlacement', 'reviewSuccessRate',
  'maxScore', 'avgScore', 'screeningSuccessRate', 'submissionRate', 'winPercent'
]

const developSubmissionBigIntFields = [
  'numInquiries', 'submissions', 'passedScreening', 'passedReview', 'appeals'
]

const developRankFields = [
  'overallPercentile', 'activeRank', 'overallCountryRank', 'reliability', 'rating',
  'minRating', 'volatility', 'overallSchoolRank', 'overallRank', 'activeSchoolRank',
  'activeCountryRank', 'maxRating', 'activePercentile'
]

const copilotFields = [
  'contests', 'projects', 'failures', 'reposts', 'activeContests', 'activeProjects', 'fulfillment'
]

const srmRankFields = [
  'rating', 'percentile', 'rank', 'countryRank', 'schoolRank',
  'volatility', 'maximumRating', 'minimumRating', 'defaultLanguage', 'competitions'
]

const srmDivisionFields = [
  'problemsSubmitted', 'problemsSysByTest', 'problemsFailed', 'levelName'
]

const marathonRankFields = [
  'rating', 'competitions', 'avgRank', 'avgNumSubmissions', 'bestRank',
  'topFiveFinishes', 'topTenFinishes', 'rank', 'percentile', 'volatility',
  'minimumRating', 'maximumRating', 'countryRank', 'schoolRank', 'defaultLanguage'
]

const auditFields = [
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy'
]

function getUnifiedTrackName (trackId) {
  const canonical = getCanonicalTrackName(trackId)
  return canonical || String(trackId || '').toUpperCase().trim()
}

function getUnifiedTypeName (typeId) {
  const canonical = getCanonicalTypeName(typeId)
  return canonical || typeId
}

function toUnixTime (value) {
  return value ? value.getTime() : null
}

function toNumber (value) {
  if (_.isNil(value)) {
    return 0
  }
  return helper.bigIntToNumber(value)
}

function mergeTrackCounters (trackItem, stat) {
  trackItem.challenges = toNumber(trackItem.challenges) + toNumber(stat.challenges)
  trackItem.wins = toNumber(trackItem.wins) + toNumber(stat.wins)
  const submissionDate = toUnixTime(stat.mostRecentSubmission)
  const eventDate = toUnixTime(stat.mostRecentEventDate)
  if (submissionDate && (!trackItem.mostRecentSubmission || submissionDate > trackItem.mostRecentSubmission)) {
    trackItem.mostRecentSubmission = submissionDate
  }
  if (eventDate && (!trackItem.mostRecentEventDate || eventDate > trackItem.mostRecentEventDate)) {
    trackItem.mostRecentEventDate = eventDate
  }
}

/**
 * Build the maxRating response object while recomputing the rating color from
 * the canonical color-band helper instead of trusting persisted color data.
 * @param {Object} maxRating memberMaxRating row
 * @returns {Object|null} normalized maxRating payload for API responses
 */
function buildMaxRatingResponse (maxRating) {
  if (!maxRating || _.isNil(maxRating.rating)) {
    return null
  }

  const rating = toNumber(maxRating.rating)
  return _.omitBy({
    rating,
    track: maxRating.track,
    subTrack: maxRating.subTrack,
    ratingColor: helper.getRatingColor(rating)
  }, _.isNil)
}

function toOptionalNumber (value) {
  if (_.isNil(value)) {
    return null
  }

  const numericValue = helper.bigIntToNumber(value)
  return Number.isFinite(Number(numericValue)) ? Number(numericValue) : null
}

function toComparableTimestamp (value) {
  if (!value) {
    return 0
  }

  if (_.isDate(value)) {
    return value.getTime()
  }

  const timestamp = new Date(value).getTime()
  return Number.isFinite(timestamp) ? timestamp : 0
}

/**
 * Resolve the highest current rating from unified memberStats rows.
 * When stats rows are loaded, only their current `rating` values count toward the
 * response. Rows missing unified track/type ids are ignored so partially-migrated
 * legacy data does not break maxRating resolution. The persisted memberMaxRating
 * row is used as a fallback when no current rating can be derived from loaded rows.
 * @param {Object|null} maxRating persisted memberMaxRating row
 * @param {Array<Object>} statsRows loaded memberStats rows
 * @returns {Object|null} normalized max rating candidate for API responses
 */
function resolveCurrentMaxRating (maxRating, statsRows) {
  if (!Array.isArray(statsRows) || statsRows.length === 0) {
    return maxRating || null
  }

  let selectedRating = null
  statsRows.forEach((row) => {
    const trackId = _.isNil(row && row.trackId) ? '' : String(row.trackId).trim()
    const typeId = _.isNil(row && row.typeId) ? '' : String(row.typeId).trim()
    if (!trackId || !typeId) {
      return
    }

    const rating = toOptionalNumber(row && row.rating)
    if (rating === null) {
      return
    }

    const candidate = {
      rating,
      track: getUnifiedTrackName(row.trackName || trackId),
      subTrack: getUnifiedTypeName(row.typeName || typeId),
      mostRecentEventDate: toComparableTimestamp(row.mostRecentEventDate)
    }

    if (!selectedRating || rating > selectedRating.rating) {
      selectedRating = candidate
      return
    }

    if (rating === selectedRating.rating) {
      const currentTimestamp = selectedRating.mostRecentEventDate || 0
      if (candidate.mostRecentEventDate > currentTimestamp) {
        selectedRating = candidate
      }
    }
  })

  return selectedRating || maxRating || null
}

/**
 * Build the user-facing max rating payload from current memberStats rows when
 * available, otherwise fall back to the persisted memberMaxRating row.
 * @param {Object|null} maxRating persisted memberMaxRating row
 * @param {Array<Object>} statsRows loaded memberStats rows
 * @returns {Object|null} normalized maxRating payload for API responses
 */
function buildCurrentMaxRatingResponse (maxRating, statsRows) {
  return buildMaxRatingResponse(resolveCurrentMaxRating(maxRating, statsRows))
}

/**
 * Convert member db data to response data
 * @param {Object} member member data from db
 */
function convertMember (member) {
  member.userId = helper.bigIntToNumber(member.userId)
  member.createdAt = member.createdAt.getTime()
  member.updatedAt = member.updatedAt.getTime()
  const statsRows = _.isArray(member.memberStats) ? member.memberStats : undefined
  const maxRating = member.maxRating
    ? _.omit(member.maxRating, ['id', 'userId', ...auditFields])
    : null
  member.maxRating = buildCurrentMaxRatingResponse(maxRating, statsRows)
  delete member.memberStats
  if (member.addresses) {
    member.addresses = _.map(member.addresses, d => _.omit(d,
      ['id', 'userId', ...auditFields]))
  }
  if (member.phones) {
    member.phones = _.map(member.phones, d => _.omit(d,
      ['id', 'userId', ...auditFields]))
  }
  member.verified = member.verified || false
}

/**
 * Build skill list data with data from db
 * @param {Array} skillList skill list from db
 * @returns skill list in response
 */
function buildMemberSkills (skillList) {
  if (!skillList || skillList.length === 0) {
    return []
  }
  // Detect standardized shape (UserSkill records) vs legacy (memberSkill with nested levels)
  const isStandardized = !!_.get(skillList, '[0].userSkillLevel') || !!_.get(skillList, '[0].userSkillDisplayMode')

  if (!isStandardized) {
    return _.map(skillList, item => {
      const ret = _.pick(item.skill, ['id', 'name', 'updatedAt'])
      ret.category = _.pick(item.skill.category, ['id', 'name'])
      if (item.displayMode) {
        ret.displayMode = _.pick(item.displayMode, ['id', 'name'])
      }
      if (item.levels && item.levels.length > 0) {
        ret.levels = _.map(item.levels, lvl => _.pick(lvl.skillLevel, ['id', 'name', 'description']))
      }
      return ret
    })
  }

  // Standardized: one UserSkill per (userId, skillId, level). Group by skill and aggregate levels
  const bySkill = _.groupBy(skillList, (i) => i.skill.id)
  const skills = _.map(bySkill, (items) => {
    const first = items[0]
    const ret = _.pick(first.skill, ['id', 'name'])
    // keep userSkill's created & updated fields
    ret.createdAt = first.createdAt
    ret.updatedAt = first.updatedAt
    ret.category = _.pick(first.skill.category, ['id', 'name'])
    if (first.userSkillDisplayMode) {
      ret.displayMode = _.pick(first.userSkillDisplayMode, ['id', 'name'])
    }

    if (first.skill && first.skill.skillEvents && first.skill.skillEvents.length) {
      const events = _.orderBy(first.skill.skillEvents || [], 'createdAt', 'desc')
      const grouped = _.groupBy(events, 'sourceType.name')
      ret.lastUsedDate = events[0].createdAt

      ret.activity = _.mapValues(grouped, (v) => ({
        sources: v
      }))
    }

    const levels = _.uniqBy(
      _.map(items, (i) => _.pick(i.userSkillLevel, ['id', 'name', 'description'])),
      'id'
    )
    if (levels.length > 0) {
      ret.levels = levels
    }
    return ret
  })

  return skills
}
/**
 * Build prisma filter with member search query
 * @param {Object} query request query parameters
 * @returns member filter used in prisma
 */
function buildSearchMemberFilter (query, options = {}) {
  const handles = _.isArray(query.handles) ? query.handles : []
  const handlesLower = _.isArray(query.handlesLower) ? query.handlesLower : []
  const userIds = _.isArray(query.userIds) ? query.userIds : []

  const filterList = []
  const restrictStatus = _.get(options, 'restrictStatus', true)
  if (restrictStatus) {
    filterList.push({ status: 'ACTIVE' })
  }
  if (query.userId) {
    filterList.push({ userId: query.userId })
  }
  if (query.handleLower) {
    filterList.push({ handleLower: query.handleLower })
  }
  if (query.handle) {
    filterList.push({ handle: query.handle })
  }
  if (query.email) {
    filterList.push({ email: query.email })
  }
  if (userIds.length > 0) {
    filterList.push({ userId: { in: userIds } })
  }
  if (handlesLower.length > 0) {
    filterList.push({ handleLower: { in: handlesLower } })
  }
  if (handles.length > 0) {
    filterList.push({ handle: { in: handles } })
  }

  const prismaFilter = {
    where: { AND: filterList }
  }
  return prismaFilter
}

/**
 * Convert db data to response structure for member stats
 * @param {Object} member member data
 * @param {Object} statsData stats data from db
 * @param {Array} fields fields return in response
 * @returns Member stats response
 */
function buildStatsResponse (member, statsData, fields) {
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(statsData.groupId),
    handle: member.handle,
    handleLower: member.handleLower,
    challenges: statsData.challenges,
    wins: statsData.wins
  }
  const maxRating = buildCurrentMaxRatingResponse(member.maxRating)
  if (maxRating) {
    item.maxRating = maxRating
  }
  if (statsData.design) {
    item.DESIGN = {
      challenges: helper.bigIntToNumber(statsData.design.challenges),
      wins: helper.bigIntToNumber(statsData.design.wins),
      mostRecentSubmission: statsData.design.mostRecentSubmission
        ? statsData.design.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.design.mostRecentEventDate
        ? statsData.design.mostRecentEventDate.getTime() : null,
      subTracks: []
    }
    const items = _.get(statsData, 'design.items', [])
    if (items.length > 0) {
      item.DESIGN.subTracks = _.map(items, t => ({
        ..._.pick(t, designBasicFields),
        challenges: helper.bigIntToNumber(t.challenges),
        wins: helper.bigIntToNumber(t.wins),
        id: t.subTrackId,
        mostRecentSubmission: t.mostRecentSubmission
          ? t.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: t.mostRecentEventDate
          ? t.mostRecentEventDate.getTime() : null
      }))
    }
  }
  if (statsData.develop) {
    item.DEVELOP = {
      challenges: helper.bigIntToNumber(statsData.develop.challenges),
      wins: helper.bigIntToNumber(statsData.develop.wins),
      mostRecentSubmission: statsData.develop.mostRecentSubmission
        ? statsData.develop.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.develop.mostRecentEventDate
        ? statsData.develop.mostRecentEventDate.getTime() : null,
      subTracks: []
    }
    const items = _.get(statsData, 'develop.items', [])
    if (items.length > 0) {
      item.DEVELOP.subTracks = _.map(items, t => ({
        challenges: helper.bigIntToNumber(t.challenges),
        wins: helper.bigIntToNumber(t.wins),
        id: t.subTrackId,
        name: t.name,
        mostRecentSubmission: t.mostRecentSubmission ? t.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: t.mostRecentEventDate ? t.mostRecentEventDate.getTime() : null,
        submissions: {
          ..._.pick(t, developSubmissionFields),
          ..._.mapValues(_.pick(t, developSubmissionBigIntFields), v => helper.bigIntToNumber(v))
        },
        rank: _.pick(t, developRankFields)
      }))
    }
  }
  if (statsData.copilot) {
    item.COPILOT = _.pick(statsData.copilot, copilotFields)
  }
  if (statsData.dataScience) {
    item.DATA_SCIENCE = {
      challenges: helper.bigIntToNumber(statsData.dataScience.challenges),
      wins: helper.bigIntToNumber(statsData.dataScience.wins),
      mostRecentSubmission: statsData.dataScience.mostRecentSubmission
        ? statsData.dataScience.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.dataScience.mostRecentEventDate
        ? statsData.dataScience.mostRecentEventDate.getTime() : null,
      mostRecentEventName: statsData.dataScience.mostRecentEventName
    }
    if (statsData.dataScience.srm) {
      const srmData = statsData.dataScience.srm
      item.DATA_SCIENCE.SRM = {
        challenges: helper.bigIntToNumber(srmData.challenges),
        wins: helper.bigIntToNumber(srmData.wins),
        mostRecentSubmission: srmData.mostRecentSubmission
          ? srmData.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: srmData.mostRecentEventDate
          ? srmData.mostRecentEventDate.getTime() : null,
        mostRecentEventName: srmData.mostRecentEventName,
        rank: _.pick(srmData, srmRankFields)
      }
      if (srmData.challengeDetails && srmData.challengeDetails.length > 0) {
        item.DATA_SCIENCE.SRM.challengeDetails = _.map(srmData.challengeDetails,
          t => _.pick(t, ['challenges', 'levelName', 'failedChallenges']))
      }
      if (srmData.divisions && srmData.divisions.length > 0) {
        const div1Data = _.filter(srmData.divisions, t => t.divisionName === 'division1')
        const div2Data = _.filter(srmData.divisions, t => t.divisionName === 'division2')
        if (div1Data.length > 0) {
          item.DATA_SCIENCE.SRM.division1 = _.map(div1Data, t => _.pick(t, srmDivisionFields))
        }
        if (div2Data.length > 0) {
          item.DATA_SCIENCE.SRM.division2 = _.map(div2Data, t => _.pick(t, srmDivisionFields))
        }
      }
    }
    if (statsData.dataScience.marathon) {
      const marathonData = statsData.dataScience.marathon
      item.DATA_SCIENCE.MARATHON_MATCH = {
        challenges: helper.bigIntToNumber(marathonData.challenges),
        wins: helper.bigIntToNumber(marathonData.wins),
        mostRecentSubmission: marathonData.mostRecentSubmission
          ? marathonData.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: marathonData.mostRecentEventDate
          ? marathonData.mostRecentEventDate.getTime() : null,
        mostRecentEventName: marathonData.mostRecentEventName,
        rank: _.pick(marathonData, marathonRankFields)
      }
    }
  }

  return fields ? _.pick(item, fields) : item
}

/**
 * Convert db data from unified memberStats table to response structure
 * @param {Object} member member data
 * @param {Array|Object} statsData stats data from db
 * @param {Array} fields fields return in response
 * @returns Member stats response
 */
function buildUnifiedStatsResponse (member, statsData, fields) {
  const rows = _.isArray(statsData) ? statsData : [statsData]
  const validRows = _.chain(rows)
    .filter(row => !_.isNil(row))
    .map(row => ({
      ...row,
      resolvedTrackName: getUnifiedTrackName(row.trackName || row.trackId),
      resolvedTypeName: getUnifiedTypeName(row.typeName || row.typeId)
    }))
    .filter(row => _.includes(['DEVELOP', 'DESIGN', 'DATA_SCIENCE', 'COPILOT'], row.resolvedTrackName))
    .value()
  const first = _.head(validRows) || {}
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: _.isNil(first.groupId) ? undefined : helper.bigIntToNumber(first.groupId),
    handle: member.handle,
    handleLower: member.handleLower,
    challenges: _.sumBy(validRows, row => toNumber(row.challenges)),
    wins: _.sumBy(validRows, row => toNumber(row.wins))
  }
  const maxRating = buildCurrentMaxRatingResponse(member.maxRating, validRows)
  if (maxRating) {
    item.maxRating = maxRating
  }

  _.forEach(validRows, (row) => {
    const trackName = row.resolvedTrackName
    const typeName = row.resolvedTypeName
    if (trackName === 'DEVELOP') {
      const challengeCount = toNumber(row.challenges)
      if (!item.DEVELOP) {
        item.DEVELOP = {
          challenges: 0,
          wins: 0,
          mostRecentSubmission: null,
          mostRecentEventDate: null,
          subTracks: []
        }
      }
      mergeTrackCounters(item.DEVELOP, row)
      const subTrackItem = {
        id: typeName,
        name: typeName,
        challenges: challengeCount,
        wins: toNumber(row.wins),
        mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
        mostRecentEventDate: toUnixTime(row.mostRecentEventDate),
        // Unified stats do not persist legacy submission counters, but each counted
        // development challenge necessarily represents one submission-level result.
        submissions: {
          submissions: challengeCount
        }
      }
      const rank = {}
      if (!_.isNil(row.rating)) {
        rank.rating = row.rating
      }
      if (!_.isNil(row.globalRank)) {
        rank.overallRank = row.globalRank
      }
      if (!_.isNil(row.countryRank)) {
        rank.overallCountryRank = row.countryRank
      }
      if (!_.isNil(row.schoolRank)) {
        rank.overallSchoolRank = row.schoolRank
      }
      if (!_.isNil(row.volatility)) {
        rank.volatility = row.volatility
      }
      if (!_.isNil(row.maxRating)) {
        rank.maxRating = row.maxRating
      }
      if (!_.isNil(row.minRating)) {
        rank.minRating = row.minRating
      }
      subTrackItem.rank = rank
      item.DEVELOP.subTracks.push(subTrackItem)
    } else if (trackName === 'DESIGN') {
      if (!item.DESIGN) {
        item.DESIGN = {
          challenges: 0,
          wins: 0,
          mostRecentSubmission: null,
          mostRecentEventDate: null,
          subTracks: []
        }
      }
      mergeTrackCounters(item.DESIGN, row)
      item.DESIGN.subTracks.push({
        id: typeName,
        name: typeName,
        challenges: toNumber(row.challenges),
        wins: toNumber(row.wins),
        mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
        mostRecentEventDate: toUnixTime(row.mostRecentEventDate)
      })
    } else if (trackName === 'DATA_SCIENCE') {
      if (!item.DATA_SCIENCE) {
        item.DATA_SCIENCE = {
          challenges: 0,
          wins: 0,
          mostRecentSubmission: null,
          mostRecentEventDate: null
        }
      }
      mergeTrackCounters(item.DATA_SCIENCE, row)
      if (typeName === 'SRM') {
        const srmItem = {
          challenges: toNumber(row.challenges),
          wins: toNumber(row.wins),
          mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
          mostRecentEventDate: toUnixTime(row.mostRecentEventDate),
          rank: _.omitBy({
            rating: row.rating,
            rank: row.globalRank,
            countryRank: row.countryRank,
            schoolRank: row.schoolRank,
            volatility: row.volatility,
            maximumRating: row.maxRating,
            minimumRating: row.minRating
          }, _.isNil)
        }
        item.DATA_SCIENCE.SRM = srmItem
      } else if (typeName === 'MARATHON_MATCH') {
        const marathonItem = {
          challenges: toNumber(row.challenges),
          wins: toNumber(row.wins),
          mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
          mostRecentEventDate: toUnixTime(row.mostRecentEventDate),
          rank: _.omitBy({
            rating: row.rating,
            rank: row.globalRank,
            countryRank: row.countryRank,
            schoolRank: row.schoolRank,
            volatility: row.volatility,
            maximumRating: row.maxRating,
            minimumRating: row.minRating,
            avgRank: row.avgRank,
            avgNumSubmissions: row.avgNumSubmissions,
            bestRank: row.bestRank,
            topFiveFinishes: row.topFiveFinishes,
            topTenFinishes: row.topTenFinishes
          }, _.isNil)
        }
        item.DATA_SCIENCE.MARATHON_MATCH = marathonItem
      } else if (typeName) {
        item.DATA_SCIENCE[typeName] = {
          challenges: toNumber(row.challenges),
          wins: toNumber(row.wins),
          mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
          mostRecentEventDate: toUnixTime(row.mostRecentEventDate),
          rank: _.omitBy({
            rating: row.rating,
            rank: row.globalRank,
            countryRank: row.countryRank,
            schoolRank: row.schoolRank,
            volatility: row.volatility,
            maximumRating: row.maxRating,
            minimumRating: row.minRating,
            avgRank: row.avgRank,
            avgNumSubmissions: row.avgNumSubmissions,
            bestRank: row.bestRank,
            topFiveFinishes: row.topFiveFinishes,
            topTenFinishes: row.topTenFinishes
          }, _.isNil)
        }
      }
    } else if (trackName === 'COPILOT') {
      item.COPILOT = _.omitBy({
        challenges: toNumber(row.challenges),
        wins: toNumber(row.wins),
        mostRecentSubmission: toUnixTime(row.mostRecentSubmission),
        mostRecentEventDate: toUnixTime(row.mostRecentEventDate)
      }, _.isNil)
    }
  })

  return fields ? _.pick(item, fields) : item
}

/**
 * Convert prisma data from unified memberStatsHistory table
 * @param {Object} member member data
 * @param {Array|Object} historyStats stats history
 * @param {Array} fields fields to return in response
 * @returns response
 */
function buildUnifiedStatsHistoryResponse (member, historyStats, fields) {
  const rows = _.isArray(historyStats) ? historyStats : [historyStats]
  const validRows = _.chain(rows)
    .filter(row => !_.isNil(row))
    .map(row => ({
      ...row,
      resolvedTrackName: getUnifiedTrackName(row.trackName || row.trackId),
      resolvedTypeName: getUnifiedTypeName(row.typeName || row.typeId)
    }))
    .filter(row => _.includes(['DEVELOP', 'DESIGN', 'DATA_SCIENCE'], row.resolvedTrackName))
    .value()
  const first = _.head(validRows) || {}
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(first.groupId),
    handle: member.handle,
    handleLower: member.handleLower
  }

  const groupedByTrackType = _.groupBy(validRows, row => `${row.resolvedTrackName}::${row.resolvedTypeName}`)
  _.forEach(groupedByTrackType, (trackHistory, key) => {
    const [trackName, typeName] = key.split('::')
    if (trackName === 'DEVELOP' || trackName === 'DESIGN') {
      const historyTrackName = trackName === 'DESIGN' ? 'DESIGN' : 'DEVELOP'
      if (!item[historyTrackName]) {
        item[historyTrackName] = { subTracks: [] }
      }
      item[historyTrackName].subTracks.push({
        id: typeName,
        name: typeName,
        history: _.map(trackHistory, h => {
          const historyDate = h.ratingDate || h.date || h.eventDate
          const placement = _.toInteger(h.placement)
          return _.omitBy({
            challengeId: _.isFinite(_.toNumber(h.challengeId)) ? _.toNumber(h.challengeId) : h.challengeId,
            challengeName: h.challengeName,
            placement: Number.isInteger(placement) && placement > 0 ? placement : undefined,
            percentile: h.percentile,
            rating: _.isNil(h.rating) ? h.newRating : h.rating,
            newRating: h.newRating,
            ratingDate: historyDate ? historyDate.getTime() : null,
            mostRecent: !!h.mostRecent,
            oldRating: h.oldRating,
            oldGlobalRank: h.oldGlobalRank,
            newGlobalRank: h.newGlobalRank,
            oldCountryRank: h.oldCountryRank,
            newCountryRank: h.newCountryRank,
            oldSchoolRank: h.oldSchoolRank,
            newSchoolRank: h.newSchoolRank
          }, _.isNil)
        })
      })
    } else if (trackName === 'DATA_SCIENCE') {
      if (!item.DATA_SCIENCE) {
        item.DATA_SCIENCE = {}
      }
      if (!item.DATA_SCIENCE[typeName]) {
        item.DATA_SCIENCE[typeName] = {}
      }
      item.DATA_SCIENCE[typeName].history = _.map(trackHistory, h => {
        const historyDate = h.ratingDate || h.date || h.eventDate
        const placement = _.toInteger(h.placement)
        return _.omitBy({
          challengeId: _.isFinite(_.toNumber(h.challengeId)) ? _.toNumber(h.challengeId) : h.challengeId,
          challengeName: h.challengeName,
          date: historyDate ? historyDate.getTime() : null,
          ratingDate: historyDate ? historyDate.getTime() : null,
          rating: _.isNil(h.rating) ? h.newRating : h.rating,
          newRating: h.newRating,
          placement: Number.isInteger(placement) && placement > 0 ? placement : undefined,
          percentile: h.percentile,
          mostRecent: !!h.mostRecent,
          oldRating: h.oldRating,
          oldGlobalRank: h.oldGlobalRank,
          newGlobalRank: h.newGlobalRank,
          oldCountryRank: h.oldCountryRank,
          newCountryRank: h.newCountryRank,
          oldSchoolRank: h.oldSchoolRank,
          newSchoolRank: h.newSchoolRank
        }, _.isNil)
      })
    }
  })

  return fields ? _.pick(item, fields) : item
}

// include parameters used to get unified member stats
const unifiedStatsIncludeParams = {}

// Minimal memberStats fields required to derive the highest current rating.
const currentMaxRatingStatsSelect = {
  trackId: true,
  typeId: true,
  rating: true,
  mostRecentEventDate: true
}

// include parameters used to get all member skills
// Standardized skills schema: userSkill has singular level and display mode
const skillsIncludeParams = {
  userSkillLevel: true,
  skill: { include: { category: true } },
  userSkillDisplayMode: true
}

/**
 * Convert number to date
 * @param {Number} dateNum date number
 * @returns date instance or undefined
 */
function convertDate (dateNum) {
  return dateNum ? new Date(dateNum) : undefined
}

/**
 * Update or Create item.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 * @returns new created item data or undefined
 */
async function updateOrCreateModel (itemData, existingData, txModel, parentId, operatorId) {
  if (existingData) {
    await txModel.update({
      where: {
        id: existingData.id
      },
      data: {
        ...itemData,
        updatedBy: operatorId
      }
    })
  } else {
    const newItemData = await txModel.create({
      data: {
        ...itemData,
        ...parentId,
        createdBy: operatorId
      }
    })
    return newItemData
  }
}

/**
 * Validate subTrack items data
 * @param {Array} updateItems the subTrack data to update
 * @param {Array} existingItems the existing subTrack data
 * @param {String} modelName the model name
 * @returns subTrack items data to create
 */
function validateSubTrackData (updateItems, existingItems, modelName) {
  const itemIds = []
  const itemNames = []
  const toCreateItems = []

  updateItems.forEach(item => {
    if (_.find(itemIds, id => id === item.id)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate id: '${item.id}'`)
    }
    if (item.name && _.find(itemNames, name => name === item.name)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate name: '${item.name}'`)
    }
    itemIds.push(item.id)
    if (item.name) {
      itemNames.push(item.name)
    }
    const foundItem = existingItems.find(eItem => eItem.subTrackId === item.id)
    const nameItem = existingItems.find(eItem => {
      if (eItem.subTrack) {
        return eItem.subTrackId !== item.id && eItem.subTrack === item.name
      }
      return eItem.subTrackId !== item.id && eItem.name === item.name
    })

    if (foundItem && (item.name && (foundItem.subTrack ? item.name !== foundItem.subTrack : item.name !== foundItem.name))) {
      throw new errors.BadRequestError(`${modelName} item with name '${item.name}' is not same as the DB one with same id`)
    }
    if (nameItem) {
      throw new errors.BadRequestError(`${modelName} item has duplicated name '${item.name}' in DB`)
    }
    if (!foundItem && !(item.id && item.name)) {
      throw new errors.BadRequestError(`${modelName} new item must have id and name both`)
    }
    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  return toCreateItems
}

/**
 * Validate level items data
 * @param {Array} updateItems the level data to update
 * @param {Array} existingItems the level subTrack data
 * @param {String} modelName the model name
 * @param {String} itemName the item name
 * @param {Object} schema the joi schema
 * @returns level items data to create
 */
function validateLevelItemsData (updateItems, existingItems, modelName, itemName, schema) {
  const itemLevelNames = []
  const toCreateItems = []
  updateItems.forEach(item => {
    if (_.find(itemLevelNames, ln => ln === item.levelName)) {
      throw new errors.BadRequestError(`${modelName} ${itemName} items contains duplicate level name: '${item.levelName}'`)
    }
    itemLevelNames.push(item.levelName)

    const foundItem = existingItems.find(eItem => {
      if (itemName === 'challengeDetail') {
        return eItem.levelName === item.levelName
      } else {
        return eItem.levelName === item.levelName && eItem.divisionName === itemName
      }
    })
    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  if (toCreateItems.length > 0) {
    const validateRes = schema.validate(toCreateItems)

    if (validateRes.error) {
      throw new errors.BadRequestError(validateRes.error.error)
    }
  }
}

/**
 * Validate history items data
 * @param {Array} updateItems the history data to update
 * @param {Array} existingItems the existing history data
 * @param {String} modelName the model name
 * @returns history items data to create
 */
function validateHistoryData (updateItems, existingItems, modelName) {
  const itemIds = []
  const toCreateItems = []
  updateItems.forEach(item => {
    if (_.find(itemIds, id => id === item.challengeId)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate id: '${item.challengeId}'`)
    }
    itemIds.push(item.challengeId)

    const foundItem = existingItems.find(eItem => helper.bigIntToNumber(eItem.challengeId) === item.challengeId && eItem.subTrack === item.subTrack)

    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  return toCreateItems
}

/**
 * Update array items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.subTrackId === item.subTrackId)
    if (foundItem) {
      item.id = foundItem.id
      toUpdate.push(item)
    } else {
      toCreate.push(item)
    }
  })
  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ..._.omit(elem, ['id', 'subTrackId', 'name']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update array level items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayLevelItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName)
    if (foundItem) {
      item.id = foundItem.id
      toUpdate.push(item)
    } else {
      toCreate.push(item)
    }
  })
  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ..._.omit(elem, ['id']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update array division items.
 * @param {Array} updateD1Items division1 items to be updated
 * @param {Array} updateD2Items division2 items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayDivisionItems (updateD1Items, updateD2Items, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if ((!updateD1Items || updateD1Items.length === 0) && (!updateD2Items || updateD2Items.length === 0)) {
    return
  }

  if (updateD1Items) {
    updateD1Items.forEach(item => {
      const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName && eItem.divisionName === 'division1')
      if (foundItem) {
        item.id = foundItem.id
        toUpdate.push(item)
      } else {
        item.divisionName = 'division1'
        toCreate.push(item)
      }
    })
  }

  if (updateD2Items) {
    updateD2Items.forEach(item => {
      const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName && eItem.divisionName === 'division2')
      if (foundItem) {
        item.id = foundItem.id
        toUpdate.push(item)
      } else {
        item.divisionName = 'division2'
        toCreate.push(item)
      }
    })
  }

  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ..._.omit(elem, ['id']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update history items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateHistoryItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []

  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.subTrack === item.subTrack && helper.bigIntToNumber(eItem.challengeId) === item.challengeId)
    if (foundItem) {
      item.id = foundItem.id
      toUpdate.push(item)
    } else {
      toCreate.push(item)
    }
  })
  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ..._.omit(elem, ['id', 'subTrackId', 'subTrack', 'challengeId']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

module.exports = {
  convertMember,
  buildCurrentMaxRatingResponse,
  buildMemberSkills,
  buildStatsResponse,
  buildUnifiedStatsResponse,
  buildSearchMemberFilter,
  buildUnifiedStatsHistoryResponse,
  currentMaxRatingStatsSelect,
  unifiedStatsIncludeParams,
  skillsIncludeParams,
  convertDate,
  updateOrCreateModel,
  validateSubTrackData,
  validateLevelItemsData,
  validateHistoryData,
  updateArrayItems,
  updateArrayLevelItems,
  updateArrayDivisionItems,
  updateHistoryItems
}

'use strict'

const DEFAULT_RATING_PATH_TRACK = 'DATA_SCIENCE'
const RATING_PATH_TRACK_NAMES = {
  DATA_SCIENCE: 'DATA_SCIENCE',
  DEVELOP: 'DEVELOP'
}

/**
 * Normalize a configured rating path name for case-insensitive lookups.
 * @param {*} value raw rating path name
 * @returns {string} normalized lookup key
 */
function normalizeRatingPathName (value) {
  return String(value || '').trim().toUpperCase()
}

/**
 * Normalize a challenge tag or skill id for case-insensitive matching.
 * @param {*} value raw challenge tag or skill id
 * @returns {string} normalized lookup key
 */
function normalizeRatingPathLookupValue (value) {
  return String(value || '').trim().toUpperCase()
}

/**
 * Normalize a challenge tag for case-insensitive matching.
 * @param {*} value raw challenge tag
 * @returns {string} normalized tag key
 */
function normalizeRatingPathTag (value) {
  return normalizeRatingPathLookupValue(value)
}

/**
 * Normalize a challenge skill id for case-insensitive matching.
 * @param {*} value raw challenge skill id
 * @returns {string} normalized skill id key
 */
function normalizeRatingPathSkillId (value) {
  return normalizeRatingPathLookupValue(value)
}

/**
 * Normalize a configured rating path destination track.
 * Config accepts `DATA_SCIENCE`, `DEVELOP`, or `DEVELOPMENT`; unified member stats
 * store development ratings under `DEVELOP`.
 * @param {*} value raw rating path track
 * @returns {string|undefined} normalized unified stats track name
 */
function normalizeRatingPathTrack (value) {
  const normalizedTrack = String(value || DEFAULT_RATING_PATH_TRACK)
    .trim()
    .toUpperCase()
    .replace(/[\s-]+/g, '_')

  if (normalizedTrack === RATING_PATH_TRACK_NAMES.DATA_SCIENCE) {
    return RATING_PATH_TRACK_NAMES.DATA_SCIENCE
  }

  if (normalizedTrack === RATING_PATH_TRACK_NAMES.DEVELOP || normalizedTrack === 'DEVELOPMENT') {
    return RATING_PATH_TRACK_NAMES.DEVELOP
  }

  return undefined
}

/**
 * Convert one raw rating path config entry into the engine-ready shape.
 * Invalid entries and entries without tags or skill ids are ignored by returning null.
 * Tags are treated as any-of filters, while skill ids are treated as an all-of
 * combination filter so a path can target challenges requiring multiple skills.
 * @param {Object} entry raw config entry with name, optional tags, optional skillIds, and optional track
 * @returns {Object|null} normalized rating path config
 */
function normalizeRatingPathConfig (entry) {
  if (!entry || !entry.name) {
    return null
  }

  const name = String(entry.name).trim()
  const trackName = normalizeRatingPathTrack(entry.track || entry.trackId || entry.trackName)
  const tags = []
  const normalizedTagSet = new Set()
  const skillIds = []
  const normalizedSkillIdSet = new Set()

  const rawTags = Array.isArray(entry.tags) ? entry.tags : []
  rawTags.forEach((tag) => {
    const trimmedTag = String(tag || '').trim()
    const normalizedTag = normalizeRatingPathTag(trimmedTag)
    if (!trimmedTag || normalizedTagSet.has(normalizedTag)) {
      return
    }

    tags.push(trimmedTag)
    normalizedTagSet.add(normalizedTag)
  })

  const rawSkillIds = Array.isArray(entry.skillIds) ? entry.skillIds : (Array.isArray(entry.skills) ? entry.skills : [])
  rawSkillIds.forEach((skillId) => {
    const trimmedSkillId = String(skillId || '').trim()
    const normalizedSkillId = normalizeRatingPathSkillId(trimmedSkillId)
    if (!trimmedSkillId || normalizedSkillIdSet.has(normalizedSkillId)) {
      return
    }

    skillIds.push(trimmedSkillId)
    normalizedSkillIdSet.add(normalizedSkillId)
  })

  if (!name || !trackName || (tags.length === 0 && skillIds.length === 0)) {
    return null
  }

  return {
    name,
    normalizedName: normalizeRatingPathName(name),
    trackName,
    tags,
    normalizedTags: Array.from(normalizedTagSet),
    skillIds,
    normalizedSkillIds: Array.from(normalizedSkillIdSet)
  }
}

/**
 * Normalize a rating path config collection, dropping invalid entries.
 * @param {Array<Object>} entries raw rating path config entries
 * @returns {Array<Object>} normalized rating path configs
 */
function normalizeRatingPathConfigs (entries) {
  if (!Array.isArray(entries)) {
    return []
  }

  return entries
    .map(normalizeRatingPathConfig)
    .filter(Boolean)
}

/**
 * Find a configured rating path by name.
 * @param {Array<Object>} entries raw or normalized rating path config entries
 * @param {*} ratingName requested rating path name
 * @returns {Object|null} normalized rating path config or null when not configured
 */
function getConfiguredRatingPath (entries, ratingName) {
  const normalizedName = normalizeRatingPathName(ratingName)
  if (!normalizedName) {
    return null
  }

  return normalizeRatingPathConfigs(entries)
    .find((entry) => entry.normalizedName === normalizedName) || null
}

/**
 * Check whether a challenge's tags and skills match a configured rating path.
 * Configured tags match when any tag is present. Configured skill ids match only
 * when every configured skill id is present on the challenge.
 * @param {Object} challenge challenge metadata containing tags and skills
 * @param {Object} ratingPath normalized rating path config
 * @returns {boolean} true when all configured rating path predicates match
 */
function challengeMatchesRatingPath (challenge, ratingPath) {
  if (!ratingPath || !challenge) {
    return false
  }

  const hasTagPredicates = Array.isArray(ratingPath.normalizedTags) && ratingPath.normalizedTags.length > 0
  const hasSkillPredicates = Array.isArray(ratingPath.normalizedSkillIds) && ratingPath.normalizedSkillIds.length > 0
  if (!hasTagPredicates && !hasSkillPredicates) {
    return false
  }

  if (hasTagPredicates) {
    if (!Array.isArray(challenge.tags)) {
      return false
    }

    const normalizedPathTags = new Set(ratingPath.normalizedTags)
    if (!challenge.tags.some((tag) => normalizedPathTags.has(normalizeRatingPathTag(tag)))) {
      return false
    }
  }

  if (hasSkillPredicates) {
    if (!Array.isArray(challenge.skills)) {
      return false
    }

    const challengeSkillIds = new Set(challenge.skills.map((skill) => normalizeRatingPathSkillId(skill && skill.skillId)))
    if (!ratingPath.normalizedSkillIds.every((skillId) => challengeSkillIds.has(skillId))) {
      return false
    }
  }

  return true
}

module.exports = {
  DEFAULT_RATING_PATH_TRACK,
  RATING_PATH_TRACK_NAMES,
  normalizeRatingPathName,
  normalizeRatingPathTag,
  normalizeRatingPathSkillId,
  normalizeRatingPathTrack,
  normalizeRatingPathConfig,
  normalizeRatingPathConfigs,
  getConfiguredRatingPath,
  challengeMatchesRatingPath
}

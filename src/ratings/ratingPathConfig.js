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
 * Normalize a challenge tag for case-insensitive matching.
 * @param {*} value raw challenge tag
 * @returns {string} normalized tag key
 */
function normalizeRatingPathTag (value) {
  return String(value || '').trim().toUpperCase()
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
 * Invalid entries and entries without tags are ignored by returning null.
 * @param {Object} entry raw config entry with name, tags, and optional track
 * @returns {Object|null} normalized rating path config
 */
function normalizeRatingPathConfig (entry) {
  if (!entry || !entry.name || !Array.isArray(entry.tags)) {
    return null
  }

  const name = String(entry.name).trim()
  const trackName = normalizeRatingPathTrack(entry.track || entry.trackId || entry.trackName)
  const tags = []
  const normalizedTagSet = new Set()

  entry.tags.forEach((tag) => {
    const trimmedTag = String(tag || '').trim()
    const normalizedTag = normalizeRatingPathTag(trimmedTag)
    if (!trimmedTag || normalizedTagSet.has(normalizedTag)) {
      return
    }

    tags.push(trimmedTag)
    normalizedTagSet.add(normalizedTag)
  })

  if (!name || !trackName || tags.length === 0) {
    return null
  }

  return {
    name,
    normalizedName: normalizeRatingPathName(name),
    trackName,
    tags,
    normalizedTags: Array.from(normalizedTagSet)
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
 * Check whether a challenge's tag array matches a configured rating path.
 * @param {Object} challenge challenge metadata containing a tags array
 * @param {Object} ratingPath normalized rating path config
 * @returns {boolean} true when any configured tag is present on the challenge
 */
function challengeMatchesRatingPath (challenge, ratingPath) {
  if (!ratingPath || !Array.isArray(ratingPath.normalizedTags) || !Array.isArray(challenge && challenge.tags)) {
    return false
  }

  const normalizedPathTags = new Set(ratingPath.normalizedTags)
  return challenge.tags.some((tag) => normalizedPathTags.has(normalizeRatingPathTag(tag)))
}

module.exports = {
  DEFAULT_RATING_PATH_TRACK,
  RATING_PATH_TRACK_NAMES,
  normalizeRatingPathName,
  normalizeRatingPathTag,
  normalizeRatingPathTrack,
  normalizeRatingPathConfig,
  normalizeRatingPathConfigs,
  getConfiguredRatingPath,
  challengeMatchesRatingPath
}

'use strict'

const RATING_METADATA_SELECT = Object.freeze({
  select: {
    name: true,
    value: true
  }
})

const RATED_METADATA_NAMES = new Set(['RATED', 'ISRATED', 'IS_RATED'])
const UNRATED_METADATA_NAMES = new Set(['UNRATED'])

/**
 * Parse a loosely typed challenge rating flag.
 * @param {*} value raw rating flag value from challenge metadata or review rows
 * @returns {boolean|undefined} parsed boolean, or undefined when the value is not boolean-like
 */
function parseBooleanLike (value) {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (normalized === 'true') {
      return true
    }
    if (normalized === 'false') {
      return false
    }
  }

  return undefined
}

/**
 * Normalize a challenge metadata flag name for case-insensitive comparisons.
 * @param {*} value raw metadata flag name
 * @returns {string} normalized metadata flag name
 */
function normalizeRatingMetadataName (value) {
  return String(value || '').trim().toUpperCase().replace(/[\s-]+/g, '_')
}

/**
 * Resolve the key/value pair from one metadata row-shaped object.
 * @param {Object} entry challenge metadata entry
 * @returns {Object|null} normalized metadata pair, or null when the entry is unusable
 */
function readMetadataEntry (entry) {
  if (!entry || typeof entry !== 'object') {
    return null
  }

  const rawName = Object.prototype.hasOwnProperty.call(entry, 'name')
    ? entry.name
    : (Object.prototype.hasOwnProperty.call(entry, 'metadataName') ? entry.metadataName : entry.key)
  if (rawName === undefined) {
    return null
  }

  const rawValue = Object.prototype.hasOwnProperty.call(entry, 'value')
    ? entry.value
    : (Object.prototype.hasOwnProperty.call(entry, 'metadataValue') ? entry.metadataValue : entry.val)

  return {
    name: normalizeRatingMetadataName(rawName),
    value: parseBooleanLike(rawValue)
  }
}

/**
 * Capture one normalized rating metadata flag into the aggregate state.
 * @param {Object} flags mutable aggregate flag state
 * @param {*} rawName raw metadata flag name
 * @param {*} rawValue raw metadata flag value
 * @returns {void}
 */
function captureRatingFlag (flags, rawName, rawValue) {
  const name = normalizeRatingMetadataName(rawName)
  const value = parseBooleanLike(rawValue)
  if (value === undefined) {
    return
  }

  if (UNRATED_METADATA_NAMES.has(name)) {
    if (value) {
      flags.unratedTrue = true
    } else {
      flags.unratedFalse = true
    }
    return
  }

  if (RATED_METADATA_NAMES.has(name)) {
    if (value) {
      flags.ratedTrue = true
    } else {
      flags.ratedFalse = true
    }
  }
}

/**
 * Collect rating-related metadata flags from array or object-shaped metadata.
 * @param {*} metadata challenge metadata relation rows or a metadata object
 * @returns {Object} aggregate booleans for rated and unrated flags
 */
function collectRatingFlags (metadata) {
  const flags = {
    ratedTrue: false,
    ratedFalse: false,
    unratedTrue: false,
    unratedFalse: false
  }

  if (Array.isArray(metadata)) {
    metadata.forEach((entry) => {
      const pair = readMetadataEntry(entry)
      if (pair) {
        captureRatingFlag(flags, pair.name, pair.value)
      }
    })
    return flags
  }

  if (metadata && typeof metadata === 'object') {
    const pair = readMetadataEntry(metadata)
    if (pair) {
      captureRatingFlag(flags, pair.name, pair.value)
    }

    Object.keys(metadata).forEach((name) => {
      captureRatingFlag(flags, name, metadata[name])
    })
  }

  return flags
}

/**
 * Resolve whether a challenge should receive rating and volatility updates.
 * Missing or indeterminate metadata defaults to rated so legacy challenges still
 * replay unless challenge metadata explicitly marks them unrated.
 * @param {Object} challenge challenge metadata record from Challenge API
 * @returns {boolean} true when the challenge should be rerated
 */
function isChallengeRated (challenge) {
  if (!challenge) {
    return false
  }

  let explicitlyRated = false
  const directRated = parseBooleanLike(challenge.isRated)
  if (directRated === false) {
    return false
  }
  if (directRated === true) {
    explicitlyRated = true
  }

  const legacyRated = parseBooleanLike(challenge.rated)
  if (legacyRated === false) {
    return false
  }
  if (legacyRated === true) {
    explicitlyRated = true
  }

  const directUnrated = parseBooleanLike(challenge.unrated)
  if (directUnrated === true) {
    return false
  }
  if (directUnrated === false) {
    explicitlyRated = true
  }

  const flags = collectRatingFlags(challenge.metadata)
  if (flags.ratedFalse || flags.unratedTrue) {
    return false
  }

  if (explicitlyRated || flags.ratedTrue || flags.unratedFalse) {
    return true
  }

  return true
}

module.exports = {
  RATING_METADATA_SELECT,
  isChallengeRated,
  parseBooleanLike
}

/**
 * Synchronize memberMaxRating from current unified memberStats rating rows.
 */

'use strict'

const _ = require('lodash')
const {
  resolveTrackNameFromLookup,
  resolveTypeNameFromLookup
} = require('../common/statsDimensionHelper')
const { getRatingColor } = require('./qubitsAlgorithm')

/**
 * Convert a date-like value into a sortable timestamp.
 * @param {*} value date, date string, or nullish value from memberStats
 * @returns {number} millisecond timestamp, or 0 when the value is missing/invalid
 */
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
 * Normalize a rating value for memberMaxRating comparisons.
 * @param {*} value persisted rating value
 * @returns {number|null} integer rating, or null when the input is not a finite rating
 */
function toOptionalRating (value) {
  if (_.isNil(value)) {
    return null
  }

  const rating = Number(value)
  return Number.isFinite(rating) ? Math.trunc(rating) : null
}

/**
 * Build a deterministic comparison key for rating dimension tie-breakers.
 * @param {*} trackId memberStats track id
 * @param {*} typeId memberStats type id
 * @returns {string} stable combined key
 */
function buildTrackTypeKey (trackId, typeId) {
  return `${trackId || ''}::${typeId || ''}`
}

/**
 * Check whether a stats row is for the dimension currently being rerated.
 * @param {Object} row memberStats row
 * @param {Object} currentDimension just-updated track/type id and label
 * @returns {boolean} true when the row belongs to the current dimension
 */
function matchesCurrentDimension (row, currentDimension) {
  return currentDimension &&
    String(row.trackId) === String(currentDimension.trackId) &&
    String(row.typeId) === String(currentDimension.typeId)
}

/**
 * Resolve canonical track/type labels for a memberStats row.
 * @param {Object} row memberStats row
 * @param {Object} options label resolution options
 * @returns {{track: string, subTrack: string}} labels for memberMaxRating
 */
function resolveRatingLabels (row, options) {
  if (matchesCurrentDimension(row, options.currentDimension)) {
    return {
      track: options.currentDimension.trackName || row.trackId,
      subTrack: options.currentDimension.typeName || row.typeId
    }
  }

  return {
    track: resolveTrackNameFromLookup(options.dimensionLookup, row.trackId) || row.trackId,
    subTrack: resolveTypeNameFromLookup(options.dimensionLookup, row.typeId) || row.typeId
  }
}

/**
 * Pick the memberStats row that should drive memberMaxRating.
 * Only current public rows with finite ratings are eligible. The highest rating
 * wins; ties use the most recent event date and finally a stable track/type key.
 * @param {Array<Object>} statsRows current memberStats rows for one member
 * @returns {Object|null} selected stats row or null when none are rated
 */
function selectCurrentMaxRatingRow (statsRows) {
  let selectedRow = null
  let selectedKey = null

  _.forEach(statsRows || [], (row) => {
    if (!row || row.isPrivate === true) {
      return
    }

    const rating = toOptionalRating(row.rating)
    if (_.isNil(rating)) {
      return
    }

    const candidate = {
      ...row,
      rating,
      mostRecentEventDate: toComparableTimestamp(row.mostRecentEventDate)
    }
    const candidateKey = buildTrackTypeKey(candidate.trackId, candidate.typeId)

    if (!selectedRow ||
      candidate.rating > selectedRow.rating ||
      (candidate.rating === selectedRow.rating &&
        candidate.mostRecentEventDate > selectedRow.mostRecentEventDate) ||
      (candidate.rating === selectedRow.rating &&
        candidate.mostRecentEventDate === selectedRow.mostRecentEventDate &&
        candidateKey < selectedKey)) {
      selectedRow = candidate
      selectedKey = candidateKey
    }
  })

  return selectedRow
}

/**
 * Synchronize a member's persisted maxRating from current public memberStats.
 * This is intended to run in the same transaction that follows rating updates,
 * so services that read member.maxRating directly see the updated rating value.
 * @param {Object} tx prisma transaction client
 * @param {BigInt} userId member identifier
 * @param {Object} options synchronization options
 * @param {Object} [options.dimensionLookup] shared challenge dimension lookup
 * @param {Object} [options.currentDimension] just-updated track/type id and label
 * @param {string} [options.actor] audit actor for createdBy/updatedBy
 * @returns {Promise<Object|null>} persisted maxRating payload or null when no current rating exists
 */
async function syncCurrentMemberMaxRating (tx, userId, options: any = {}) {
  const statsRows = await tx.memberStats.findMany({
    where: {
      userId
    },
    select: {
      userId: true,
      trackId: true,
      typeId: true,
      rating: true,
      mostRecentEventDate: true,
      isPrivate: true
    }
  })
  const selectedRow = selectCurrentMaxRatingRow(statsRows)

  if (!selectedRow) {
    return null
  }

  const labels = resolveRatingLabels(selectedRow, options)
  const actor = options.actor || 'rating-sync'
  const data = {
    userId,
    rating: selectedRow.rating,
    track: labels.track,
    subTrack: labels.subTrack,
    ratingColor: getRatingColor(selectedRow.rating)
  }

  await tx.memberMaxRating.upsert({
    where: {
      userId
    },
    create: {
      ...data,
      createdBy: actor,
      updatedBy: actor
    },
    update: {
      rating: data.rating,
      track: data.track,
      subTrack: data.subTrack,
      ratingColor: data.ratingColor,
      updatedBy: actor
    }
  })

  return data
}

module.exports = {
  selectCurrentMaxRatingRow,
  syncCurrentMemberMaxRating
}

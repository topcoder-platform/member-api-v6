/**
 * Port of the Topcoder Qubits rating math for challenge participants.
 *
 * Input rows must be shaped as:
 *   { coderId, rating, volatility, numRatings, score }
 *
 * The function mutates and returns the same participant array with updated
 * integer `rating`, `volatility`, and `numRatings` values. New players are
 * initialized from the default Qubits baseline and the rating delta is capped
 * by `150 + 1500 / (timesPlayed + 2)` before the new volatility is derived.
 */

'use strict'

const INITIAL_SCORE = 1200.0
const ONE_STD_DEV_EQUALS = 1200.0
const INITIAL_WEIGHT = 0.60
const FINAL_WEIGHT = 0.18
const FIRST_VOLATILITY = 385
const DEFAULT_RATING = 1200
const DEFAULT_VOLATILITY = 515

const NORMINV_A = [
  -39.6968302866538,
  220.946098424521,
  -275.928510446969,
  138.357751867269,
  -30.6647980661472,
  2.50662827745924
]

const NORMINV_B = [
  -54.4760987982241,
  161.585836858041,
  -155.698979859887,
  66.8013118877197,
  -13.2806815528857
]

const NORMINV_C = [
  -0.00778489400243029,
  -0.322396458041136,
  -2.40075827716184,
  -2.54973253934373,
  4.37466414146497,
  2.93816398269878
]

const NORMINV_D = [
  0.00778469570904146,
  0.32246712907004,
  2.445134137143,
  3.75440866190742
]

const P_LOW = 0.02425
const P_HIGH = 1 - P_LOW

const RATING_COLORS = [{
  color: '#9D9FA0',
  limit: 900
}, {
  color: '#69C329',
  limit: 1200
}, {
  color: '#616BD5',
  limit: 1500
}, {
  color: '#FCD617',
  limit: 2200
}, {
  color: '#EF3A3A',
  limit: Infinity
}]

function sqr (value) {
  return value * value
}

function erf (value) {
  const sign = value < 0 ? -1 : 1
  const absValue = Math.abs(value)
  const t = 1 / (1 + 0.5 * absValue)
  const polynomial = 1.00002368 +
    t * (0.37409196 +
      t * (0.09678418 +
        t * (-0.18628806 +
          t * (0.27886807 +
            t * (-1.13520398 +
              t * (1.48851587 +
                t * (-0.82215223 +
                  t * 0.17087277)))))))
  const tau = t * Math.exp(
    -absValue * absValue - 1.26551223 + t * polynomial
  )

  return sign === 1 ? 1 - tau : tau - 1
}

function erfc (value) {
  return 1 - erf(value)
}

function refine (estimate, probability) {
  if (probability <= 0 || probability >= 1) {
    return estimate
  }

  const error = 0.5 * erfc(-estimate / Math.SQRT2) - probability
  const update = error * Math.sqrt(2 * Math.PI) * Math.exp(sqr(estimate) / 2)
  return estimate - update / (1 + estimate * update / 2)
}

function normsinv (probability) {
  if (probability <= 0) {
    return -Infinity
  }

  if (probability >= 1) {
    return Infinity
  }

  let q
  let r
  let estimate

  if (probability < P_LOW) {
    q = Math.sqrt(-2 * Math.log(probability))
    estimate = (((((NORMINV_C[0] * q + NORMINV_C[1]) * q + NORMINV_C[2]) * q + NORMINV_C[3]) * q + NORMINV_C[4]) * q + NORMINV_C[5]) /
      ((((NORMINV_D[0] * q + NORMINV_D[1]) * q + NORMINV_D[2]) * q + NORMINV_D[3]) * q + 1)
    return refine(estimate, probability)
  }

  if (probability <= P_HIGH) {
    q = probability - 0.5
    r = q * q
    estimate = (((((NORMINV_A[0] * r + NORMINV_A[1]) * r + NORMINV_A[2]) * r + NORMINV_A[3]) * r + NORMINV_A[4]) * r + NORMINV_A[5]) * q /
      (((((NORMINV_B[0] * r + NORMINV_B[1]) * r + NORMINV_B[2]) * r + NORMINV_B[3]) * r + NORMINV_B[4]) * r + 1)
    return refine(estimate, probability)
  }

  q = Math.sqrt(-2 * Math.log(1 - probability))
  estimate = -(((((NORMINV_C[0] * q + NORMINV_C[1]) * q + NORMINV_C[2]) * q + NORMINV_C[3]) * q + NORMINV_C[4]) * q + NORMINV_C[5]) /
    ((((NORMINV_D[0] * q + NORMINV_D[1]) * q + NORMINV_D[2]) * q + NORMINV_D[3]) * q + 1)
  return refine(estimate, probability)
}

function winprobability (rating1, volatility1, rating2, volatility2) {
  const denominator = Math.sqrt(2 * (sqr(volatility1) + sqr(volatility2)))
  if (!denominator) {
    if (rating1 === rating2) {
      return 0.5
    }
    return rating1 > rating2 ? 1 : 0
  }

  return 0.5 * (erf((rating1 - rating2) / denominator) + 1)
}

function getNormalizedRating (participant) {
  if (participant && participant.numRatings > 0 && Number.isFinite(Number(participant.rating))) {
    return Number(participant.rating)
  }
  return DEFAULT_RATING
}

function getNormalizedVolatility (participant) {
  if (participant && participant.numRatings > 0 && Number.isFinite(Number(participant.volatility))) {
    return Number(participant.volatility)
  }
  return DEFAULT_VOLATILITY
}

function getWeight (rating, timesPlayed) {
  let weight = 1 / (1 - ((INITIAL_WEIGHT - FINAL_WEIGHT) / (timesPlayed + 1) + FINAL_WEIGHT)) - 1

  if (rating > 2500) {
    weight *= 0.8
  } else if (rating >= 2000) {
    weight *= 0.9
  }

  return weight
}

function getChallengeFactor (preparedParticipants) {
  if (preparedParticipants.length === 0) {
    return 0
  }

  let totalRating = 0
  let totalVolatility = 0

  preparedParticipants.forEach((participant) => {
    totalRating += participant.oldRating
    totalVolatility += sqr(participant.oldVolatility / ONE_STD_DEV_EQUALS)
  })

  const averageRating = totalRating / preparedParticipants.length

  let ratingVariance = 0
  preparedParticipants.forEach((participant) => {
    ratingVariance += sqr((participant.oldRating - averageRating) / ONE_STD_DEV_EQUALS)
  })

  const denominator = preparedParticipants.length > 1
    ? preparedParticipants.length - 1
    : 1

  return Math.sqrt((totalVolatility / preparedParticipants.length) + (ratingVariance / denominator)) * ONE_STD_DEV_EQUALS
}

function buildActualRankMap (participants) {
  const sortedParticipants = participants.slice().sort((left, right) => {
    if (right.score !== left.score) {
      return right.score - left.score
    }

    return String(left.coderId).localeCompare(String(right.coderId))
  })

  const rankByCoderId = new Map()
  let index = 0

  while (index < sortedParticipants.length) {
    let nextIndex = index + 1
    while (nextIndex < sortedParticipants.length && sortedParticipants[nextIndex].score === sortedParticipants[index].score) {
      nextIndex += 1
    }

    const firstRank = index + 1
    const lastRank = nextIndex
    const averageRank = (firstRank + lastRank) / 2

    for (let groupIndex = index; groupIndex < nextIndex; groupIndex += 1) {
      rankByCoderId.set(String(sortedParticipants[groupIndex].coderId), averageRank)
    }

    index = nextIndex
  }

  return rankByCoderId
}

function createPreparedParticipant (participant) {
  const numRatings = Number.isFinite(Number(participant.numRatings)) ? Math.max(0, Number(participant.numRatings)) : 0
  return {
    participant,
    coderId: String(participant.coderId),
    score: Number.isFinite(Number(participant.score)) ? Number(participant.score) : 0,
    oldRating: numRatings > 0 ? getNormalizedRating(participant) : INITIAL_SCORE,
    oldVolatility: numRatings > 0 ? getNormalizedVolatility(participant) : DEFAULT_VOLATILITY,
    oldNumRatings: numRatings
  }
}

function applyRatingUpdate (preparedParticipants, comparisonParticipants) {
  if (preparedParticipants.length === 0 || comparisonParticipants.length === 0) {
    return
  }

  const actualRankMap = buildActualRankMap(comparisonParticipants)
  const challengeFactor = getChallengeFactor(comparisonParticipants.map(createPreparedParticipant))

  preparedParticipants.forEach((preparedParticipant) => {
    let expectedRank = 0.5

    comparisonParticipants.forEach((comparisonParticipant) => {
      const comparison = createPreparedParticipant(comparisonParticipant)
      expectedRank += winprobability(
        comparison.oldRating,
        comparison.oldVolatility,
        preparedParticipant.oldRating,
        preparedParticipant.oldVolatility
      )
    })

    const actualRank = actualRankMap.get(preparedParticipant.coderId) || comparisonParticipants.length
    const expectedPerformance = -normsinv((expectedRank - 0.5) / comparisonParticipants.length)
    const actualPerformance = -normsinv((actualRank - 0.5) / comparisonParticipants.length)
    const performedAs = preparedParticipant.oldRating + challengeFactor * (actualPerformance - expectedPerformance)
    const weight = getWeight(preparedParticipant.oldRating, preparedParticipant.oldNumRatings)
    const cap = 150 + 1500 / (preparedParticipant.oldNumRatings + 2)

    let newRating = (preparedParticipant.oldRating + weight * performedAs) / (1 + weight)
    const ratingDelta = newRating - preparedParticipant.oldRating
    if (Math.abs(ratingDelta) > cap) {
      newRating = preparedParticipant.oldRating + Math.sign(ratingDelta) * cap
    }

    let newVolatility = Math.sqrt(
      sqr(newRating - preparedParticipant.oldRating) / weight +
      sqr(preparedParticipant.oldVolatility) / (weight + 1)
    )

    if (preparedParticipant.oldNumRatings === 0) {
      newVolatility = FIRST_VOLATILITY
    }

    preparedParticipant.participant.rating = Math.round(newRating)
    preparedParticipant.participant.volatility = Math.round(newVolatility)
    preparedParticipant.participant.numRatings = preparedParticipant.oldNumRatings + 1
  })
}

function runQubitsRating (participants) {
  if (!Array.isArray(participants) || participants.length === 0) {
    return []
  }

  const ratedParticipants = []
  const newParticipants = []

  participants.forEach((participant) => {
    const preparedParticipant = createPreparedParticipant(participant)
    if (preparedParticipant.oldNumRatings > 0) {
      ratedParticipants.push(preparedParticipant)
    } else {
      newParticipants.push(preparedParticipant)
    }
  })

  if (ratedParticipants.length > 0) {
    applyRatingUpdate(ratedParticipants, ratedParticipants.map((item) => item.participant))
  }

  if (newParticipants.length > 0) {
    applyRatingUpdate(newParticipants, participants)
  }

  return participants
}

function getRatingColor (rating) {
  const numericRating = Number(rating)
  if (!Number.isFinite(numericRating)) {
    return RATING_COLORS[RATING_COLORS.length - 1].color
  }

  let index = 0
  while (index < RATING_COLORS.length && RATING_COLORS[index].limit <= numericRating) {
    index += 1
  }

  return (RATING_COLORS[index] && RATING_COLORS[index].color) || RATING_COLORS[RATING_COLORS.length - 1].color
}

module.exports = {
  INITIAL_SCORE,
  ONE_STD_DEV_EQUALS,
  INITIAL_WEIGHT,
  FINAL_WEIGHT,
  FIRST_VOLATILITY,
  DEFAULT_RATING,
  DEFAULT_VOLATILITY,
  runQubitsRating,
  getRatingColor
}

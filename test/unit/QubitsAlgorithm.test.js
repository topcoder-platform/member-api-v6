/*
 * Unit tests for the shared Topcoder Qubits rating algorithm.
 */

require('../../app-bootstrap')
const chai = require('chai')

const {
  runQubitsRating
} = require('../../src/ratings/qubitsAlgorithm')

const should = chai.should()

function cloneParticipants (participants) {
  return participants.map((participant) => ({ ...participant }))
}

function buildStateByCoderId (participants) {
  return participants.reduce((lookup, participant) => {
    lookup[participant.coderId] = {
      rating: participant.rating,
      volatility: participant.volatility,
      numRatings: participant.numRatings
    }
    return lookup
  }, {})
}

describe('qubits rating algorithm unit tests', () => {
  it('runQubitsRating should use frozen pre-event states regardless of participant order', () => {
    const participants = [
      { coderId: '10', rating: 1800, volatility: 300, numRatings: 5, score: 90 },
      { coderId: '20', rating: 2200, volatility: 250, numRatings: 8, score: 80 },
      { coderId: '30', rating: 0, volatility: 0, numRatings: 0, score: 100 },
      { coderId: '40', rating: 1500, volatility: 400, numRatings: 3, score: 70 }
    ]

    const forward = cloneParticipants(participants)
    const reverse = cloneParticipants(participants).reverse()

    runQubitsRating(forward)
    runQubitsRating(reverse)

    const expectedState = {
      10: { rating: 1927, volatility: 341, numRatings: 6 },
      20: { rating: 2130, volatility: 261, numRatings: 9 },
      30: { rating: 1821, volatility: 385, numRatings: 1 },
      40: { rating: 1447, volatility: 348, numRatings: 4 }
    }

    buildStateByCoderId(forward).should.deep.equal(expectedState)
    buildStateByCoderId(reverse).should.deep.equal(expectedState)
  })

  it('runQubitsRating should apply the legacy 2500 weight boundary', () => {
    const participants = [
      { coderId: '1', rating: 2500, volatility: 250, numRatings: 10, score: 0 },
      { coderId: '2', rating: 2400, volatility: 250, numRatings: 10, score: 100 }
    ]

    runQubitsRating(participants)

    buildStateByCoderId(participants).should.deep.equal({
      1: { rating: 2461, volatility: 240, numRatings: 11 },
      2: { rating: 2442, volatility: 239, numRatings: 11 }
    })
  })

  it('runQubitsRating should not persist ratings below the legacy minimum', () => {
    const participants = [
      { coderId: '1', rating: 10, volatility: 1000, numRatings: 5, score: 0 },
      { coderId: '2', rating: 3000, volatility: 1000, numRatings: 5, score: 100 },
      { coderId: '3', rating: 3000, volatility: 1000, numRatings: 5, score: 90 },
      { coderId: '4', rating: 3000, volatility: 1000, numRatings: 5, score: 80 }
    ]

    runQubitsRating(participants)

    should.equal(participants[0].rating, 1)
  })
})

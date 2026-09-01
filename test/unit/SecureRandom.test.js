/*
 * Unit tests for secure random identifier generation.
 */

const chai = require('chai')
const crypto = require('crypto')
const { generateSecureRandomString } = require('../../src/common/secureRandom')

const should = chai.should()

describe('secure random utility unit tests', () => {
  it('generateSecureRandomString should select every character with an unbiased random integer', () => {
    const alphabet = 'abcd'
    const originalRandomInt = crypto.randomInt
    const requestedBounds = []
    let nextIndex = 0

    crypto.randomInt = (maximum) => {
      requestedBounds.push(maximum)
      const result = nextIndex % maximum
      nextIndex += 1
      return result
    }

    try {
      generateSecureRandomString(8, alphabet).should.equal('abcdabcd')
      requestedBounds.should.deep.equal(Array(8).fill(alphabet.length))
    } finally {
      crypto.randomInt = originalRandomInt
    }
  })

  it('generateSecureRandomString should reject invalid size and alphabet inputs', () => {
    should.throw(() => generateSecureRandomString(0), RangeError)
    should.throw(() => generateSecureRandomString(1.5), RangeError)
    should.throw(() => generateSecureRandomString(4, null), TypeError)
    should.throw(() => generateSecureRandomString(4, 'a'), RangeError)
    should.throw(() => generateSecureRandomString(4, 'aab'), RangeError)
  })
})

/*
 * Unit tests for migrate-dynamo-data helpers.
 */

require('../../app-bootstrap')
const chai = require('chai')

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.DATABASE_URL = process.env.DATABASE_URL || placeholderDbUrl
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl

const { resolveMaxRatingColor } = require('../../src/scripts/migrate-dynamo-data')

const should = chai.should()

describe('migrate-dynamo-data helper unit tests', () => {
  describe('resolveMaxRatingColor tests', () => {
    it('should keep valid imported color', () => {
      const color = resolveMaxRatingColor({
        rating: 1173,
        ratingColor: '#ef3a3a'
      })
      should.equal(color, '#EF3A3A')
    })

    it('should calculate color from rating when color is missing', () => {
      const color = resolveMaxRatingColor({
        rating: 1173
      })
      should.equal(color, '#69C329')
    })

    it('should calculate color from rating when color is blank', () => {
      const color = resolveMaxRatingColor({
        rating: 1500,
        ratingColor: ' '
      })
      should.equal(color, '#FCD617')
    })
  })
})

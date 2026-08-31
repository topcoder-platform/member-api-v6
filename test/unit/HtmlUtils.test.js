/*
 * Unit tests for HTML text helpers.
 */

const chai = require('chai')
const { decodeBasicHtmlEntitiesOnce } = require('../../src/common/htmlUtils')

const should = chai.should()

describe('HTML utility unit tests', () => {
  it('decodeBasicHtmlEntitiesOnce should decode supported entities in one pass', () => {
    decodeBasicHtmlEntitiesOnce('&quot;Topcoder&#39;s&quot;&nbsp;&amp;&nbsp;&lt;API&gt;')
      .should.equal('"Topcoder\'s" & <API>')
  })

  it('decodeBasicHtmlEntitiesOnce should preserve a nested encoded entity for the next trust boundary', () => {
    const result = decodeBasicHtmlEntitiesOnce('&amp;lt;script&amp;gt;')

    result.should.equal('&lt;script&gt;')
    result.should.not.equal('<script>')
  })

  it('decodeBasicHtmlEntitiesOnce should preserve nullish input', () => {
    should.equal(decodeBasicHtmlEntitiesOnce(null), null)
    should.equal(decodeBasicHtmlEntitiesOnce(undefined), null)
  })
})

/*
 * Unit tests of prisma helper utilities.
 */

/* global BigInt */

require('../../app-bootstrap')
const chai = require('chai')
const prismaHelper = require('../../src/common/prismaHelper')

const should = chai.should()

describe('prisma helper unit tests', () => {
  it('convertMember should prefer the highest current memberStats rating over persisted maxRating', () => {
    const member = {
      userId: BigInt(100000218),
      createdAt: new Date('2026-03-26T00:00:00.000Z'),
      updatedAt: new Date('2026-03-26T00:00:00.000Z'),
      verified: false,
      maxRating: {
        id: BigInt(1),
        userId: BigInt(100000218),
        rating: 1237,
        track: 'DEVELOP',
        subTrack: 'Challenge',
        ratingColor: '#FCD617',
        createdAt: new Date('2026-01-01T00:00:00.000Z'),
        createdBy: 'test',
        updatedAt: new Date('2026-01-01T00:00:00.000Z'),
        updatedBy: 'test'
      },
      memberStats: [
        {
          trackId: 'DEVELOP',
          typeId: 'Challenge',
          rating: 224,
          mostRecentEventDate: new Date('2026-03-01T00:00:00.000Z')
        },
        {
          trackId: 'DATA_SCIENCE',
          typeId: 'SRM',
          rating: 180,
          mostRecentEventDate: new Date('2026-02-01T00:00:00.000Z')
        }
      ]
    }

    prismaHelper.convertMember(member)

    member.maxRating.should.deep.equal({
      rating: 224,
      track: 'DEVELOP',
      subTrack: 'Challenge',
      ratingColor: '#9D9FA0'
    })
    should.equal(member.userId, 100000218)
    should.equal(member.createdAt, new Date('2026-03-26T00:00:00.000Z').getTime())
    should.equal(member.updatedAt, new Date('2026-03-26T00:00:00.000Z').getTime())
    should.equal(member.verified, false)
    should.equal(Object.prototype.hasOwnProperty.call(member, 'memberStats'), false)
  })
})

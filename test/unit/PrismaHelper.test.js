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

  it('convertMember should ignore memberStats rows missing unified track/type ids', () => {
    const member = {
      userId: BigInt(100000219),
      createdAt: new Date('2026-03-26T00:00:00.000Z'),
      updatedAt: new Date('2026-03-26T00:00:00.000Z'),
      verified: false,
      maxRating: {
        id: BigInt(2),
        userId: BigInt(100000219),
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
          trackId: null,
          typeId: 'Challenge',
          rating: 9999,
          mostRecentEventDate: new Date('2026-03-20T00:00:00.000Z')
        },
        {
          trackId: 'DEVELOP',
          typeId: 'Challenge',
          rating: 224,
          mostRecentEventDate: new Date('2026-03-01T00:00:00.000Z')
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
  })

  it('convertMember should fall back to persisted maxRating when loaded memberStats rows are malformed', () => {
    const member = {
      userId: BigInt(100000220),
      createdAt: new Date('2026-03-26T00:00:00.000Z'),
      updatedAt: new Date('2026-03-26T00:00:00.000Z'),
      verified: false,
      maxRating: {
        id: BigInt(3),
        userId: BigInt(100000220),
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
          trackId: null,
          typeId: 'Challenge',
          rating: 9999,
          mostRecentEventDate: new Date('2026-03-20T00:00:00.000Z')
        },
        {
          trackId: '   ',
          typeId: 'Challenge',
          rating: 5000,
          mostRecentEventDate: new Date('2026-03-21T00:00:00.000Z')
        }
      ]
    }

    prismaHelper.convertMember(member)

    member.maxRating.should.deep.equal({
      rating: 1237,
      track: 'DEVELOP',
      subTrack: 'Challenge',
      ratingColor: '#616BD5'
    })
  })
})

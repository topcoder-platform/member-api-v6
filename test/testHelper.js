/**
 * This file defines common helper methods used for tests
 */
const _ = require('lodash')
const prisma = require('../src/common/prisma').getClient()

const member1 = {
  maxRating: {
    rating: 1000,
    track: 'dev',
    subTrack: 'code',
    ratingColor: ''
  },
  userId: 123,
  firstName: 'first name',
  lastName: 'last name',
  description: 'desc',
  otherLangName: 'en',
  handle: 'denis',
  handleLower: 'denis',
  status: 'ACTIVE',
  email: 'denis@topcoder.com',
  newEmail: 'denis2@topcoder.com',
  emailVerifyToken: 'abcdefg',
  emailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
  newEmailVerifyToken: 'abc123123',
  newEmailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
  addresses: [
    {
      streetAddr1: 'addr1',
      streetAddr2: 'addr2',
      city: 'NY',
      zip: '123123',
      stateCode: 'A',
      type: 'type',
      updatedAt: '2020-02-06T07:38:50.088Z',
      createdAt: '2020-02-06T07:38:50.088Z',
      createdBy: 'test',
      updatedBy: 'test'
    }
  ],
  homeCountryCode: 'US',
  competitionCountryCode: 'US',
  photoURL: 'http://test.com/abc.png',
  tracks: ['code'],
  updatedAt: '2020-02-06T07:38:50.088Z',
  createdAt: '2020-02-06T07:38:50.088Z',
  createdBy: 'test1',
  updatedBy: 'test2'
}

const member2 = {
  maxRating: {
    rating: 1500,
    track: 'dev',
    subTrack: 'code',
    ratingColor: ' '
  },
  userId: 456,
  firstName: 'first name 2',
  lastName: 'last name 2',
  description: 'desc 2',
  otherLangName: 'en',
  handle: 'testing',
  handleLower: 'testing',
  status: 'ACTIVE',
  email: 'testing@topcoder.com',
  newEmail: 'testing2@topcoder.com',
  emailVerifyToken: 'abcdefg',
  emailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
  newEmailVerifyToken: 'abc123123',
  newEmailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
  addresses: [
    {
      streetAddr1: 'addr1',
      streetAddr2: 'addr2',
      city: 'NY',
      zip: '123123',
      stateCode: 'A',
      type: 'type',
      updatedAt: '2020-02-06T07:38:50.088Z',
      createdAt: '2020-02-06T07:38:50.088Z',
      createdBy: 'test',
      updatedBy: 'test'
    }
  ],
  homeCountryCode: 'US',
  competitionCountryCode: 'US',
  photoURL: 'http://test.com/def.png',
  tracks: ['code'],
  updatedAt: '2020-02-06T07:38:50.088Z',
  createdAt: '2020-02-06T07:38:50.088Z',
  createdBy: 'test1',
  updatedBy: 'test2'
}

function testDataToPrisma (data) {
  const ret = _.omit(data, ['addresses', 'maxRating'])
  ret.maxRating = {
    create: {
      ...data.maxRating,
      createdBy: 'test'
    }
  }
  ret.addresses = {
    create: {
      ...data.addresses[0],
      createdBy: 'test'
    }
  }
  return ret
}

/**
 * Create test data
 */
async function createData () {
  // create data in DB
  await prisma.member.create({
    data: testDataToPrisma(member1)
  })
  await prisma.member.create({
    data: testDataToPrisma(member2)
  })
  await prisma.memberTraits.create({
    data: {
      userId: member1.userId,
      subscriptions: ['abc'],
      createdBy: 'test'
    }
  })
}

/**
 * Clear test data
 */
async function clearData () {
  // remove data in DB
  const memberIds = [member1.userId, member2.userId]
  const filter = { where: { userId: { in: memberIds } } }

  await prisma.memberTraits.deleteMany(filter)
  await prisma.memberAddress.deleteMany(filter)
  await prisma.memberMaxRating.deleteMany(filter)
  await prisma.member.deleteMany(filter)
}

/**
 * Get test data.
 */
function getData () {
  return { member1, member2 }
}

/**
 * Get dates difference in milliseconds
 */
function getDatesDiff (d1, d2) {
  return new Date(d1).getTime() - new Date(d2).getTime()
}

module.exports = {
  createData,
  clearData,
  getData,
  getDatesDiff
}

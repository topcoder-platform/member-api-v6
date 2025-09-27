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
  availableForGigs: false,
  firstName: 'first name 2',
  lastName: 'last name 2',
  // description: 'desc 2',
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
  // photoURL: 'http://test.com/def.png',
  tracks: ['code'],
  updatedAt: '2020-02-06T07:38:50.088Z',
  createdAt: '2020-02-06T07:38:50.088Z',
  createdBy: 'test1',
  updatedBy: 'test2',
}

const member3 = {
  maxRating: {
    rating: 1000,
    track: 'dev',
    subTrack: 'code',
    ratingColor: ' '
  },
  userId: 111,
  firstName: 'first other 2',
  lastName: 'last other 2',
  description: 'desc 2',
  otherLangName: 'en',
  handle: 'member3',
  handleLower: 'member3',
  status: 'ACTIVE',
  email: 'other@topcoder.com',
  newEmail: 'other@topcoder.com',
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
    create: data.addresses.map(addr => ({
      ...addr,
      createdBy: 'test'
    }))
  }
  return ret
}

/**
 * Create skills for member2
 */
async function createMember2Skills () {
  // Create skill categories
  const programmingCategory = await prisma.skillCategory.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440001' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440001', 
      name: 'Programming Languages', 
      createdBy: 'test' 
    },
    update: { name: 'Programming Languages' }
  })

  const webCategory = await prisma.skillCategory.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440002' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440002', 
      name: 'Web Development', 
      createdBy: 'test' 
    },
    update: { name: 'Web Development' }
  })

  // Create display modes
  const principalMode = await prisma.displayMode.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440010' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440010', 
      name: 'principal', 
      createdBy: 'test' 
    },
    update: { name: 'principal' }
  })

  const additionalMode = await prisma.displayMode.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440011' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440011', 
      name: 'additional', 
      createdBy: 'test' 
    },
    update: { name: 'additional' }
  })

  // Create skill levels
  const verifiedLevel = await prisma.skillLevel.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440020' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440020', 
      name: 'verified', 
      description: 'Verified skill level',
      createdBy: 'test' 
    },
    update: { name: 'verified', description: 'Verified skill level' }
  })

  const selfDeclaredLevel = await prisma.skillLevel.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440021' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440021', 
      name: 'self-declared', 
      description: 'Self-declared skill level',
      createdBy: 'test' 
    },
    update: { name: 'self-declared', description: 'Self-declared skill level' }
  })

  // Create skills
  const javascriptSkill = await prisma.skill.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440100' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440100', 
      name: 'JavaScript', 
      categoryId: programmingCategory.id,
      createdBy: 'test' 
    },
    update: { name: 'JavaScript' }
  })

  const reactSkill = await prisma.skill.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440101' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440101', 
      name: 'React', 
      categoryId: webCategory.id,
      createdBy: 'test' 
    },
    update: { name: 'React' }
  })

  const nodejsSkill = await prisma.skill.upsert({
    where: { id: '550e8400-e29b-41d4-a716-446655440102' },
    create: { 
      id: '550e8400-e29b-41d4-a716-446655440102', 
      name: 'Node.js', 
      categoryId: programmingCategory.id,
      createdBy: 'test' 
    },
    update: { name: 'Node.js' }
  })

  // Create member skills for member2
  const memberSkill1 = await prisma.memberSkill.create({
    data: {
      id: '550e8400-e29b-41d4-a716-446655440200',
      userId: member2.userId,
      skillId: javascriptSkill.id,
      displayModeId: principalMode.id,
      createdBy: 'test'
    }
  })

  const memberSkill2 = await prisma.memberSkill.create({
    data: {
      id: '550e8400-e29b-41d4-a716-446655440201',
      userId: member2.userId,
      skillId: reactSkill.id,
      displayModeId: principalMode.id,
      createdBy: 'test'
    }
  })

  const memberSkill3 = await prisma.memberSkill.create({
    data: {
      id: '550e8400-e29b-41d4-a716-446655440202',
      userId: member2.userId,
      skillId: nodejsSkill.id,
      displayModeId: additionalMode.id,
      createdBy: 'test'
    }
  })

  // Create member skill levels
  await prisma.memberSkillLevel.createMany({
    data: [
      {
        memberSkillId: memberSkill1.id,
        skillLevelId: verifiedLevel.id,
        createdBy: 'test'
      },
      {
        memberSkillId: memberSkill2.id,
        skillLevelId: verifiedLevel.id,
        createdBy: 'test'
      },
      {
        memberSkillId: memberSkill3.id,
        skillLevelId: selfDeclaredLevel.id,
        createdBy: 'test'
      }
    ]
  })
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
  await prisma.member.create({
    data: testDataToPrisma(member3)
  })

  
  // Create member traits record with subscriptions and education trait
  await prisma.memberTraits.create({
    data: {
      userId: member1.userId,
      subscriptions: ['abc'],
      createdBy: 'test'
    }
  })
  await prisma.memberTraits.create({
    data: {
      userId: member2.userId,
      subscriptions: ['abc'],
      education: {
        create: [
          {
            collegeName: 'MIT',
            degree: 'Bachelor of Science',
            endYear: 2015,
            createdBy: 'test'
          },
          {
            collegeName: 'Stanford University',
            degree: 'Master of Science',
            endYear: 2017,
            createdBy: 'test'
          }
        ]
      },
      work: { 
        create: [
          {
            industry: 'Banking',
            companyName: 'Test Company',
            position: 'Developer',
            createdBy: 'test'
          }
        ]
      },
      createdBy: 'test'
    }
  })

  // Create skills for member2
  await createMember2Skills()
}

/**
 * Clear test data
 */
async function clearData () {
  // remove data in DB
  const memberIds = [member1.userId, member2.userId, member3.userId]
  const filter = { where: { userId: { in: memberIds } } }

  // Clear member skills first (due to foreign key constraints)
  await prisma.memberSkillLevel.deleteMany({
    where: {
      memberSkill: {
        userId: { in: memberIds }
      }
    }
  })
  await prisma.memberSkill.deleteMany(filter)
  
  await prisma.memberTraits.deleteMany(filter)
  await prisma.memberAddress.deleteMany(filter)
  await prisma.memberMaxRating.deleteMany(filter)
  await prisma.member.deleteMany(filter)
}

/**
 * Get test data.
 */
function getData () {
  return { member1, member2, member3 }
}

/**
 * Get dates difference in milliseconds
 */
function getDatesDiff (d1, d2) {
  return new Date(d1).getTime() - new Date(d2).getTime()
}

/**
 * Create a member with expired token for testing
 */
async function createMemberWithExpiredToken (expiredDate) {
  const randomSuffix = Math.floor(Math.random() * 10000)
  const member = {
    maxRating: {
      rating: 1000,
      track: 'dev',
      subTrack: 'code',
      ratingColor: ''
    },
    userId: 9999 + randomSuffix, // Use random userId to avoid conflicts
    firstName: 'expired',
    lastName: 'token',
    description: 'desc',
    otherLangName: 'en',
    handle: 'expiredtoken' + randomSuffix, // Use random handle to avoid conflicts
    handleLower: 'expiredtoken' + randomSuffix, // Use random handle to avoid conflicts
    status: 'ACTIVE',
    email: 'expired' + randomSuffix + '@topcoder.com', // Use random email to avoid conflicts
    newEmail: 'expired2@topcoder.com',
    emailVerifyToken: 'expiredtoken123',
    emailVerifyTokenDate: expiredDate,
    newEmailVerifyToken: 'expiredtoken456',
    newEmailVerifyTokenDate: expiredDate,
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
    photoURL: 'http://test.com/expired.png',
    tracks: ['code'],
    updatedAt: '2020-02-06T07:38:50.088Z',
    createdAt: '2020-02-06T07:38:50.088Z',
    createdBy: 'test1',
    updatedBy: 'test2'
  }

  await prisma.member.create({
    data: testDataToPrisma(member)
  })

  return member
}

/**
 * Create a member with null address fields for testing
 */
async function createMemberWithNullAddresses () {
  const randomSuffix = Math.floor(Math.random() * 10000)
  const member = {
    maxRating: {
      rating: 1000,
      track: 'dev',
      subTrack: 'code',
      ratingColor: ''
    },
    userId: 888 + randomSuffix, // Use random userId to avoid conflicts
    firstName: 'null',
    lastName: 'address',
    description: 'desc',
    otherLangName: 'en',
    handle: 'nulladdress' + randomSuffix, // Use random handle to avoid conflicts
    handleLower: 'nulladdress' + randomSuffix, // Use random handle to avoid conflicts
    status: 'ACTIVE',
    email: 'null' + randomSuffix + '@topcoder.com', // Use random email to avoid conflicts
    newEmail: 'null2@topcoder.com',
    emailVerifyToken: 'nulltoken123',
    emailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
    newEmailVerifyToken: 'nulltoken456',
    newEmailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
    addresses: [
      {
        streetAddr1: null,
        streetAddr2: null,
        city: null,
        zip: null,
        stateCode: null,
        type: 'type',
        updatedAt: '2020-02-06T07:38:50.088Z',
        createdAt: '2020-02-06T07:38:50.088Z',
        createdBy: 'test',
        updatedBy: 'test'
      }
    ],
    homeCountryCode: 'US',
    competitionCountryCode: 'US',
    photoURL: 'http://test.com/null.png',
    tracks: ['code'],
    updatedAt: '2020-02-06T07:38:50.088Z',
    createdAt: '2020-02-06T07:38:50.088Z',
    createdBy: 'test1',
    updatedBy: 'test2'
  }

  await prisma.member.create({
    data: testDataToPrisma(member)
  })

  return member
}

/**
 * Clear member traits for a specific user
 */
async function clearMemberTraits (userId) {
  await prisma.memberTraits.deleteMany({
    where: { userId }
  })
}

/**
 * Create a member with empty work history for testing
 */
async function createMemberWithEmptyWorkHistory () {
  const randomSuffix = Math.floor(Math.random() * 10000)
  const member = {
    maxRating: {
      rating: 1000,
      track: 'dev',
      subTrack: 'code',
      ratingColor: ''
    },
    userId: 777 + randomSuffix, // Use random userId to avoid conflicts
    firstName: 'empty',
    lastName: 'work',
    description: 'desc',
    otherLangName: 'en',
    handle: 'emptywork' + randomSuffix, // Use random handle to avoid conflicts
    handleLower: 'emptywork' + randomSuffix, // Use random handle to avoid conflicts
    status: 'ACTIVE',
    email: 'emptywork' + randomSuffix + '@topcoder.com', // Use random email to avoid conflicts
    newEmail: 'emptywork2@topcoder.com',
    emailVerifyToken: 'emptyworktoken123',
    emailVerifyTokenDate: '2028-02-06T07:38:50.088Z',
    newEmailVerifyToken: 'emptyworktoken456',
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
    photoURL: 'http://test.com/emptywork.png',
    tracks: ['code'],
    updatedAt: '2020-02-06T07:38:50.088Z',
    createdAt: '2020-02-06T07:38:50.088Z',
    createdBy: 'test1',
    updatedBy: 'test2'
  }

  await prisma.member.create({
    data: testDataToPrisma(member)
  })

  // Create member traits with empty work history
  await prisma.memberTraits.create({
    data: {
      userId: member.userId,
      work: {
        create: [] // Empty work history
      },
      createdBy: 'test'
    }
  })

  return member
}

/**
 * Clear additional test data
 */
async function clearAdditionalData () {
  // Clear by handle pattern since handles are now random
  const filter = { where: { handle: { startsWith: 'expiredtoken' } } }
  const filter2 = { where: { handle: { startsWith: 'nulladdress' } } }
  const filter3 = { where: { handle: { startsWith: 'emptywork' } } }
  
  // Get member IDs first
  const members = await prisma.member.findMany({
    where: {
      OR: [
        { handle: { startsWith: 'expiredtoken' } },
        { handle: { startsWith: 'nulladdress' } },
        { handle: { startsWith: 'emptywork' } }
      ]
    },
    select: { userId: true }
  })
  
  const memberIds = members.map(m => m.userId)
  
  if (memberIds.length > 0) {
    // Clear member traits first (due to foreign key constraints)
    await prisma.memberTraits.deleteMany({
      where: { userId: { in: memberIds } }
    })
    
    // Then clear members
    await prisma.member.deleteMany({
      where: { userId: { in: memberIds } }
    })
  }
}

module.exports = {
  
  createData,
  clearData,
  getData,
  getDatesDiff,
  createMemberWithExpiredToken,
  createMemberWithNullAddresses,
  createMemberWithEmptyWorkHistory,
  clearAdditionalData,
  clearMemberTraits
}

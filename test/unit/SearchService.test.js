/*
 * Unit tests of search service.
 */

/* global BigInt */

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.DATABASE_URL = process.env.DATABASE_URL || placeholderDbUrl
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl
process.env.RESOURCES_DB_URL = process.env.RESOURCES_DB_URL || placeholderDbUrl

const appConfig = require('config')
appConfig.DATABASE_URL = appConfig.DATABASE_URL || placeholderDbUrl
appConfig.SKILLS_DB_URL = appConfig.SKILLS_DB_URL || placeholderDbUrl
appConfig.RESOURCES_DB_URL = appConfig.RESOURCES_DB_URL || placeholderDbUrl

require('../../app-bootstrap')
const chai = require('chai')

const _ = require('lodash')
delete require.cache[require.resolve('../../src/common/prisma')]
const prismaManager = require('../../src/common/prisma')
const prismaHelper = require('../../src/common/prismaHelper')
const statsDimensionHelper = require('../../src/common/statsDimensionHelper')
const service = require('../../src/services/SearchService')

const should = chai.should()

describe('search service unit tests', () => {
  it('searchMembers should accept a JSON-array string for userIds', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    let memberFilter

    try {
      prisma.member.count = async (filter) => {
        memberFilter = filter
        return 0
      }

      await service.searchMembers(
        { isMachine: true },
        {
          userIds: '[100000013,40158994,89770408]',
          fields: 'handle,userId'
        }
      )

      memberFilter.where.AND.should.deep.equal([
        { userId: { in: [100000013, 40158994, 89770408] } }
      ])
    } finally {
      prisma.member.count = originalMemberCount
    }
  })

  it('searchMembers should accept qs decimal-string userIds without losing precision', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    let memberFilter

    try {
      prisma.member.count = async (filter) => {
        memberFilter = filter
        return 0
      }

      await service.searchMembers(
        { isMachine: true },
        {
          userIds: ['100000013', '9223372036854775807'],
          fields: 'handle,userId'
        }
      )

      memberFilter.where.AND.should.deep.equal([
        { userId: { in: ['100000013', '9223372036854775807'] } }
      ])
    } finally {
      prisma.member.count = originalMemberCount
    }
  })

  it('searchMembers should preserve an empty userIds array', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    let memberFilter

    try {
      prisma.member.count = async (filter) => {
        memberFilter = filter
        return 0
      }

      await service.searchMembers(
        { isMachine: true },
        {
          userIds: [],
          fields: 'handle,userId'
        }
      )

      memberFilter.where.AND.should.deep.equal([])
    } finally {
      prisma.member.count = originalMemberCount
    }
  })

  it('searchMembers should reject invalid userIds before querying Prisma', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    let memberCountCalled = false
    let validationError

    try {
      prisma.member.count = async () => {
        memberCountCalled = true
        return 0
      }

      for (const userIds of [
        ['TCConnCopilot'],
        [Number.MAX_SAFE_INTEGER + 1],
        ['9223372036854775808'],
        ['']
      ]) {
        validationError = undefined

        try {
          await service.searchMembers(
            { isMachine: true },
            {
              userIds,
              fields: 'handle,userId'
            }
          )
        } catch (err) {
          validationError = err
        }

        should.exist(validationError)
        validationError.isJoi.should.equal(true)
        validationError.details[0].path.should.deep.equal(['query', 'userIds'])
        validationError.details[0].type.should.equal('queryArray.items')
      }

      memberCountCalled.should.equal(false)
    } finally {
      prisma.member.count = originalMemberCount
    }
  })

  it('searchMembers should skip stats and skills hydration for explicit field-limited lookups', async () => {
    const prisma = prismaManager.getClient()
    const skillsPrisma = prismaManager.getSkillsClient()

    const originalMemberCount = prisma.member.count
    const originalMemberFindMany = prisma.member.findMany
    const originalMemberStatsFindMany = prisma.memberStats.findMany
    const originalUserSkillFindMany = skillsPrisma.userSkill.findMany

    let memberStatsRequested = false
    let userSkillsRequested = false

    try {
      prisma.member.count = async () => 2
      prisma.member.findMany = async (args) => {
        args.select.should.deep.equal({
          userId: true,
          createdAt: true,
          updatedAt: true,
          verified: true,
          handle: true,
          email: true
        })
        should.not.exist(args.include)

        return [
          {
            userId: BigInt(1001),
            handle: 'alpha',
            email: 'alpha@example.com',
            createdAt: new Date('2026-04-01T00:00:00.000Z'),
            updatedAt: new Date('2026-04-02T00:00:00.000Z'),
            verified: true
          },
          {
            userId: BigInt(1002),
            handle: 'beta',
            email: 'beta@example.com',
            createdAt: new Date('2026-04-03T00:00:00.000Z'),
            updatedAt: new Date('2026-04-04T00:00:00.000Z'),
            verified: false
          }
        ]
      }
      prisma.memberStats.findMany = async () => {
        memberStatsRequested = true
        return []
      }
      skillsPrisma.userSkill.findMany = async () => {
        userSkillsRequested = true
        return []
      }

      const response = await service.searchMembers(
        { isMachine: true },
        {
          userIds: [1001, 1002],
          page: 1,
          perPage: 20,
          fields: 'userId,handle,email',
          includeStats: 'false'
        }
      )

      should.equal(response.total, 2)
      response.result.should.deep.equal([
        { userId: 1001, handle: 'alpha', email: 'alpha@example.com' },
        { userId: 1002, handle: 'beta', email: 'beta@example.com' }
      ])
      should.equal(memberStatsRequested, false)
      should.equal(userSkillsRequested, false)
    } finally {
      prisma.member.count = originalMemberCount
      prisma.member.findMany = originalMemberFindMany
      prisma.memberStats.findMany = originalMemberStatsFindMany
      skillsPrisma.userSkill.findMany = originalUserSkillFindMany
    }
  })

  it('searchMembers should display configured rating path names for included stats', async () => {
    const prisma = prismaManager.getClient()
    const skillsPrisma = prismaManager.getSkillsClient()

    const originalMemberCount = prisma.member.count
    const originalMemberFindMany = prisma.member.findMany
    const originalMemberStatsFindMany = prisma.memberStats.findMany
    const originalUserSkillFindMany = skillsPrisma.userSkill.findMany
    const originalGetChallengesClient = prismaManager.getChallengesClient

    try {
      statsDimensionHelper.clearChallengeDimensionLookupCache()
      prisma.member.count = async () => 1
      prisma.member.findMany = async () => [
        {
          userId: BigInt(1001),
          handle: 'alpha',
          handleLower: 'alpha',
          createdAt: new Date('2026-04-01T00:00:00.000Z'),
          updatedAt: new Date('2026-04-02T00:00:00.000Z'),
          verified: true
        }
      ]
      prisma.memberStats.findMany = async () => [
        {
          userId: BigInt(1001),
          trackId: 'track-ds-id',
          typeId: 'rating-path-ai-engineering',
          challenges: 3,
          wins: 0,
          rating: 1517,
          globalRank: 4,
          volatility: 331,
          mostRecentEventDate: new Date('2024-06-01T00:00:00.000Z'),
          isPrivate: false
        }
      ]
      skillsPrisma.userSkill.findMany = async () => []
      prismaManager.getChallengesClient = () => ({
        $queryRaw: async (query) => {
          const sql = _.castArray(query).join('')
          if (sql.includes('"ChallengeTrack"')) {
            return [
              { id: 'track-ds-id', name: 'Data Science', abbreviation: 'DS', legacyId: null }
            ]
          }
          return [
            { id: 'rating-path-ai-engineering', name: 'AI Engineering', abbreviation: 'AI Engineering', legacyId: null, isTask: false }
          ]
        }
      })

      const response = await service.searchMembers(
        { isMachine: true },
        {
          page: 1,
          perPage: 20,
          fields: 'userId,handle,handleLower,createdAt,updatedAt,stats,numberOfChallengesWon,numberOfChallengesPlaced'
        }
      )

      response.result.should.have.length(1)
      should.exist(response.result[0].stats[0].DATA_SCIENCE['AI Engineering'])
      should.not.exist(response.result[0].stats[0].DATA_SCIENCE['rating-path-ai-engineering'])
      response.result[0].stats[0].DATA_SCIENCE['AI Engineering'].rank.rating.should.equal(1517)
      response.result[0].numberOfChallengesPlaced.should.equal(3)
    } finally {
      prisma.member.count = originalMemberCount
      prisma.member.findMany = originalMemberFindMany
      prisma.memberStats.findMany = originalMemberStatsFindMany
      skillsPrisma.userSkill.findMany = originalUserSkillFindMany
      prismaManager.getChallengesClient = originalGetChallengesClient
      statsDimensionHelper.clearChallengeDimensionLookupCache()
    }
  })

  it('searchMembersBySkills should chunk member queries for skill score searches', async () => {
    const prisma = prismaManager.getClient()
    const skillsPrisma = prismaManager.getSkillsClient()

    const originalMemberCount = prisma.member.count
    const originalMemberFindMany = prisma.member.findMany
    const originalUserSkillFindMany = skillsPrisma.userSkill.findMany
    const originalQueryRaw = skillsPrisma.$queryRaw
    const originalConvertMember = prismaHelper.convertMember

    const candidateBatchSizes = []

    try {
      const allUserIds = Array.from({ length: 1001 }, (_, idx) => idx + 1)

      prisma.member.count = async () => {
        throw new Error('member.count should not be called for sortBy=skillScore path')
      }

      skillsPrisma.$queryRaw = async () => allUserIds.map(userId => ({ userId }))

      prisma.member.findMany = async (args) => {
        if (args.select && args.where && args.where.userId && args.where.availableForGigs) {
          const ids = args.where.userId.in
          candidateBatchSizes.push(ids.length)
          return ids.map(userId => ({
            userId,
            availableForGigs: true,
            description: 'desc',
            photoURL: 'http://test.com/pic.png',
            lastLoginDate: new Date('2025-01-01T00:00:00.000Z'),
            skillScoreDeduction: 0
          }))
        }

        if (args.include && args.where && args.where.userId) {
          const ids = args.where.userId.in
          return ids.map(userId => ({
            userId,
            handle: `user-${userId.toString()}`,
            handleLower: `user-${userId.toString()}`,
            maxRating: null,
            addresses: [],
            verified: false,
            availableForGigs: true,
            description: 'desc',
            photoURL: 'http://test.com/pic.png',
            lastLoginDate: new Date('2025-01-01T00:00:00.000Z'),
            skillScoreDeduction: 0
          }))
        }

        return []
      }

      skillsPrisma.userSkill.findMany = async () => []
      prismaHelper.convertMember = () => {}

      const response = await service.searchMembersBySkills(
        { isMachine: true },
        {
          id: 'skill-java',
          page: 1,
          perPage: 10,
          includeStats: 'false',
          sortBy: 'skillScore',
          sortOrder: 'desc'
        }
      )

      should.equal(response.total, 1001)
      should.equal(response.result.length, 10)
      candidateBatchSizes.should.deep.equal([1000, 1])
    } finally {
      prisma.member.count = originalMemberCount
      prisma.member.findMany = originalMemberFindMany
      skillsPrisma.userSkill.findMany = originalUserSkillFindMany
      skillsPrisma.$queryRaw = originalQueryRaw
      prismaHelper.convertMember = originalConvertMember
    }
  })

  it('autocomplete should place exact handle match first on the first page', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    const originalMemberFindMany = prisma.member.findMany
    const originalMemberFindFirst = prisma.member.findFirst

    const findManyArgs = []
    try {
      prisma.member.count = async () => 3
      prisma.member.findFirst = async () => ({
        userId: BigInt(1001),
        handleLower: 'vinod_a'
      })
      prisma.member.findMany = async (args) => {
        findManyArgs.push(args)
        return [
          { userId: BigInt(1002), handleLower: 'vinod_ab' },
          { userId: BigInt(1003), handleLower: 'vinod_az' }
        ]
      }

      const response = await service.autocomplete(
        { isMachine: true },
        {
          term: 'vinod_a',
          page: 1,
          perPage: 3,
          sortOrder: 'desc',
          fields: 'userId,handleLower'
        }
      )

      should.equal(response.total, 3)
      response.result.map(item => item.handleLower).should.deep.equal(['vinod_a', 'vinod_ab', 'vinod_az'])
      response.result.map(item => item.userId).should.deep.equal([1001, 1002, 1003])
      should.equal(findManyArgs[0].where.handleLower.not, 'vinod_a')
      should.equal(findManyArgs[0].skip, 0)
      should.equal(findManyArgs[0].take, 2)
    } finally {
      prisma.member.count = originalMemberCount
      prisma.member.findMany = originalMemberFindMany
      prisma.member.findFirst = originalMemberFindFirst
    }
  })

  it('autocomplete should offset non-exact results by one on later pages when exact match exists', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberCount = prisma.member.count
    const originalMemberFindMany = prisma.member.findMany
    const originalMemberFindFirst = prisma.member.findFirst

    const allNonExact = ['vinod_ab', 'vinod_ac', 'vinod_ad', 'vinod_ae']
    const findManyArgs = []
    try {
      prisma.member.count = async () => 5
      prisma.member.findFirst = async () => ({
        userId: BigInt(2001),
        handleLower: 'vinod_a'
      })
      prisma.member.findMany = async (args) => {
        findManyArgs.push(args)
        return allNonExact
          .slice(args.skip, args.skip + args.take)
          .map((handleLower, index) => ({
            userId: BigInt(2002 + args.skip + index),
            handleLower
          }))
      }

      const response = await service.autocomplete(
        { isMachine: true },
        {
          term: 'vinod_a',
          page: 2,
          perPage: 2,
          sortOrder: 'asc',
          fields: 'handleLower'
        }
      )

      should.equal(response.total, 5)
      response.result.map(item => item.handleLower).should.deep.equal(['vinod_ac', 'vinod_ad'])
      should.equal(findManyArgs[0].where.handleLower.not, 'vinod_a')
      should.equal(findManyArgs[0].skip, 1)
      should.equal(findManyArgs[0].take, 2)
    } finally {
      prisma.member.count = originalMemberCount
      prisma.member.findMany = originalMemberFindMany
      prisma.member.findFirst = originalMemberFindFirst
    }
  })

  it('autocompleteByHandlePrefix should return the highest current rating from memberStats', async () => {
    const prisma = prismaManager.getClient()
    const originalMemberFindMany = prisma.member.findMany

    try {
      prisma.member.findMany = async () => ([
        {
          userId: BigInt(100000218),
          handle: 'testmfa1',
          firstName: 'Test',
          lastName: 'Mfa',
          photoURL: 'https://example.com/photo.png',
          maxRating: {
            rating: 1237,
            track: 'DEVELOP',
            subTrack: 'Challenge',
            ratingColor: '#FCD617'
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
      ])

      const response = await service.autocompleteByHandlePrefix(
        { roles: ['administrator'] },
        'test'
      )

      should.equal(response.length, 1)
      response[0].maxRating.should.deep.equal({
        rating: 224,
        track: 'DEVELOP',
        subTrack: 'Challenge',
        ratingColor: '#9D9FA0'
      })
    } finally {
      prisma.member.findMany = originalMemberFindMany
    }
  })
})

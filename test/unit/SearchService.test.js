/*
 * Unit tests of search service.
 */

require('../../app-bootstrap')
const chai = require('chai')

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.DATABASE_URL = process.env.DATABASE_URL || placeholderDbUrl
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl
process.env.RESOURCES_DB_URL = process.env.RESOURCES_DB_URL || placeholderDbUrl

const prismaManager = require('../../src/common/prisma')
const prismaHelper = require('../../src/common/prismaHelper')
const service = require('../../src/services/SearchService')

const should = chai.should()

describe('search service unit tests', () => {
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
})

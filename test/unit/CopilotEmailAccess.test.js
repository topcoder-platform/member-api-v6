/*
 * Unit tests of copilot email access rules.
 */

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.DATABASE_URL = process.env.DATABASE_URL || placeholderDbUrl
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl
process.env.CHALLENGES_DB_URL = process.env.CHALLENGES_DB_URL || placeholderDbUrl
process.env.ACADEMY_DB_URL = process.env.ACADEMY_DB_URL || placeholderDbUrl
process.env.RESOURCES_DB_URL = process.env.RESOURCES_DB_URL || placeholderDbUrl
process.env.ENGAGEMENTS_DB_URL = process.env.ENGAGEMENTS_DB_URL || placeholderDbUrl

const appConfig = require('config')
appConfig.DATABASE_URL = appConfig.DATABASE_URL || placeholderDbUrl
appConfig.RESOURCES_DB_URL = appConfig.RESOURCES_DB_URL || placeholderDbUrl

require('../../app-bootstrap')
const chai = require('chai')

const prismaManager = require('../../src/common/prisma')
const copilotEmailAccess = require('../../src/common/copilotEmailAccess')

const should = chai.should()

describe('copilot email access unit tests', () => {
  const resourcesPrisma = prismaManager.getResourcesClient()
  let originalFindMany
  let findManyCalls

  beforeEach(() => {
    originalFindMany = resourcesPrisma.resource.findMany
    findManyCalls = 0
    // Simulate a copilot who shares no challenges with the requested members.
    resourcesPrisma.resource.findMany = async () => {
      findManyCalls += 1
      return []
    }
  })

  afterEach(() => {
    resourcesPrisma.resource.findMany = originalFindMany
  })

  it('limits email access for a copilot without a sensitive data role', async () => {
    const currentUser = { userId: 1001, roles: ['Topcoder User', 'copilot'] }

    copilotEmailAccess.shouldLimitCopilotEmailAccess(currentUser).should.equal(true)
    const canAccess = await copilotEmailAccess.canCopilotAccessMemberEmail(currentUser, 2002)
    canAccess.should.equal(false)
    findManyCalls.should.equal(1)
  })

  it('does not limit email access for a Talent Manager who is also a copilot', async () => {
    const currentUser = {
      userId: 1001,
      roles: ['Project Manager', 'Talent Manager', 'Topcoder Talent', 'Topcoder User', 'copilot']
    }

    copilotEmailAccess.shouldLimitCopilotEmailAccess(currentUser).should.equal(false)
    const canAccess = await copilotEmailAccess.canCopilotAccessMemberEmail(currentUser, 2002)
    canAccess.should.equal(true)

    const members = [{ userId: 2002, email: 'member@topcoder.com' }]
    await copilotEmailAccess.stripUnauthorizedCopilotEmails(currentUser, members)
    should.equal(members[0].email, 'member@topcoder.com')
    findManyCalls.should.equal(0)
  })

  it('does not limit email access for an administrator who is also a copilot', async () => {
    const currentUser = { userId: 1001, roles: ['administrator', 'copilot'] }

    copilotEmailAccess.shouldLimitCopilotEmailAccess(currentUser).should.equal(false)
    const canAccess = await copilotEmailAccess.canCopilotAccessMemberEmail(currentUser, 2002)
    canAccess.should.equal(true)
    findManyCalls.should.equal(0)
  })
})

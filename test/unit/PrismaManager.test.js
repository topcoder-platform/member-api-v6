/*
 * Unit tests for Prisma client manager utilities.
 */

const path = require('path')
const chai = require('chai')

const should = chai.should()

const prismaPath = path.resolve(__dirname, '../../src/common/prisma.ts')

describe('prisma manager unit tests', () => {
  const originalChallengeDbUrl = process.env.CHALLENGE_DB_URL
  const originalChallengesDbUrl = process.env.CHALLENGES_DB_URL

  afterEach(() => {
    if (originalChallengeDbUrl === undefined) {
      delete process.env.CHALLENGE_DB_URL
    } else {
      process.env.CHALLENGE_DB_URL = originalChallengeDbUrl
    }

    if (originalChallengesDbUrl === undefined) {
      delete process.env.CHALLENGES_DB_URL
    } else {
      process.env.CHALLENGES_DB_URL = originalChallengesDbUrl
    }

    delete require.cache[prismaPath]
  })

  it('getChallengesClient should accept CHALLENGE_DB_URL as a fallback', async () => {
    delete process.env.CHALLENGES_DB_URL
    process.env.CHALLENGE_DB_URL = 'postgresql://user:password@localhost:5432/topcoder?schema=challenges'
    delete require.cache[prismaPath]

    const prismaManager = require('../../src/common/prisma')
    const challengesClient = prismaManager.getChallengesClient()

    should.exist(challengesClient)
    await challengesClient.$disconnect()
  })
})

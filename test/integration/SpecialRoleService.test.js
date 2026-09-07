/**
 * PostgreSQL regression coverage for PM-5966. Set SPECIAL_ROLE_TEST_DB_URL to
 * an empty test database; the suite creates both schemas inside a transaction
 * and rolls back all fixtures on completion. See ReadMe.md for the command.
 */
const path = require('path')
const { expect } = require('chai')
const { Client } = require('pg')
const { Prisma } = require('../../prisma/generated/challenge-client')

const describePostgres = process.env.SPECIAL_ROLE_TEST_DB_URL ? describe : describe.skip
const servicePath = path.resolve(__dirname, '../../src/services/SpecialRoleService.ts')
const helperPath = path.resolve(__dirname, '../../src/common/helper.ts')
const loggerPath = path.resolve(__dirname, '../../src/common/logger.ts')
const prismaPath = path.resolve(__dirname, '../../src/common/prisma.ts')
const cancelledStatuses = [
  'CANCELLED',
  'CANCELLED_FAILED_REVIEW',
  'CANCELLED_FAILED_SCREENING',
  'CANCELLED_ZERO_SUBMISSIONS',
  'CANCELLED_WINNER_UNRESPONSIVE',
  'CANCELLED_CLIENT_REQUEST',
  'CANCELLED_REQUIREMENTS_INFEASIBLE',
  'CANCELLED_ZERO_REGISTRATIONS',
  'CANCELLED_PAYMENT_FAILED'
]

describePostgres('special role service PostgreSQL regressions', function () {
  this.timeout(20000)
  let client
  let service
  const originalModules = new Map()

  before(async () => {
    client = new Client({ connectionString: process.env.SPECIAL_ROLE_TEST_DB_URL })
    await client.connect()
    await client.query('BEGIN')
    // Existing schemas cause setup to fail; no existing application tables are reused.
    await client.query(`
      CREATE SCHEMA challenges;
      CREATE SCHEMA resources;
      CREATE TYPE challenges."ChallengeStatusEnum" AS ENUM (
        'NEW', 'DRAFT', 'APPROVED', 'ACTIVE', 'COMPLETED', 'DELETED',
        'CANCELLED', 'CANCELLED_FAILED_REVIEW', 'CANCELLED_FAILED_SCREENING',
        'CANCELLED_ZERO_SUBMISSIONS', 'CANCELLED_WINNER_UNRESPONSIVE',
        'CANCELLED_CLIENT_REQUEST', 'CANCELLED_REQUIREMENTS_INFEASIBLE',
        'CANCELLED_ZERO_REGISTRATIONS', 'CANCELLED_PAYMENT_FAILED'
      );
      CREATE TABLE challenges."Challenge" (
        "id" text PRIMARY KEY, "name" text,
        "status" challenges."ChallengeStatusEnum",
        "trackId" text, "typeId" text DEFAULT 'challenge',
        "startDate" timestamptz, "endDate" timestamptz,
        "createdAt" timestamptz DEFAULT '2026-08-01T00:00:00Z',
        "groups" text[] DEFAULT '{}', "taskIsTask" boolean DEFAULT false
      );
      CREATE TABLE challenges."ChallengeTrack" (
        "id" text PRIMARY KEY, "name" text, "track" text, "abbreviation" text
      );
      CREATE TABLE challenges."ChallengeType" ("id" text PRIMARY KEY, "name" text);
      CREATE TABLE challenges."ChallengeUserWhitelist" ("challengeId" text);
      CREATE TABLE resources."ResourceRole" ("id" text PRIMARY KEY, "nameLower" text);
      CREATE TABLE resources."Resource" (
        "challengeId" text, "memberId" text, "roleId" text,
        "createdAt" timestamptz DEFAULT '2026-08-01T00:00:00Z'
      );
      INSERT INTO challenges."ChallengeType" VALUES ('challenge', 'Challenge');
      INSERT INTO challenges."ChallengeTrack" VALUES
        ('dev', 'Development', 'DEVELOPMENT', 'DEV'),
        ('design', 'Design', 'DESIGN', 'DES'),
        ('ds', 'Data Science', 'DATA_SCIENCE', 'DS'),
        ('qa', 'Quality Assurance', 'QUALITY_ASSURANCE', 'QA');
      INSERT INTO resources."ResourceRole" VALUES
        ('copilot', 'copilot'), ('reviewer', 'reviewer'), ('screener', 'primary screener');
    `)

    const stubs = {
      [helperPath]: { getMemberByHandle: async () => ({ userId: BigInt(1) }) },
      [loggerPath]: { buildService: () => {} },
      [prismaPath]: {
        ChallengesPrisma: Prisma,
        getChallengesClient: () => ({
          $queryRaw: async query => (await client.query(query.text, query.values)).rows
        })
      }
    }
    for (const modulePath of [servicePath, ...Object.keys(stubs)]) {
      originalModules.set(modulePath, require.cache[modulePath])
      delete require.cache[modulePath]
      if (stubs[modulePath]) {
        require.cache[modulePath] = {
          id: modulePath, filename: modulePath, loaded: true, exports: stubs[modulePath]
        }
      }
    }
    service = require(servicePath)
  })

  beforeEach(async () => {
    await client.query(`
      TRUNCATE challenges."Challenge", challenges."ChallengeUserWhitelist", resources."Resource";
    `)
  })

  after(async () => {
    for (const [modulePath, originalModule] of originalModules) {
      delete require.cache[modulePath]
      if (originalModule) require.cache[modulePath] = originalModule
    }
    if (client) {
      try {
        await client.query('ROLLBACK')
      } finally {
        await client.end()
      }
    }
  })

  /**
   * Seed real challenge and resource rows for a service regression scenario.
   * @param {Array<Object>} challenges rows with id, status, and optional track,
   * groups, task, or whitelisted visibility attributes
   * @param {String} role resource role assigned to the test member (default copilot)
   * @returns {Promise<void>} resolves after every fixture has been inserted
   * @throws {Error} propagates PostgreSQL fixture insertion failures
   */
  async function seedChallenges (challenges, role = 'copilot') {
    for (const challenge of challenges) {
      await client.query(`
        INSERT INTO challenges."Challenge" ("id", "name", "status", "trackId", "groups", "taskIsTask")
        VALUES ($1, $1, $2, $3, $4, $5)
      `, [challenge.id, challenge.status, challenge.track || 'dev', challenge.groups || [], !!challenge.task])
      await client.query(`
        INSERT INTO resources."Resource" ("challengeId", "memberId", "roleId") VALUES ($1, '1', $2)
      `, [challenge.id, role])
      if (challenge.whitelisted) {
        await client.query('INSERT INTO challenges."ChallengeUserWhitelist" VALUES ($1)', [challenge.id])
      }
    }
  }

  it('returns five terminal challenges and 60% fulfillment for the eight-challenge QA example', async () => {
    await seedChallenges([
      { id: 'completed-1', status: 'COMPLETED' },
      { id: 'completed-2', status: 'COMPLETED' },
      { id: 'completed-3', status: 'COMPLETED' },
      { id: 'cancelled-review', status: 'CANCELLED_FAILED_REVIEW', track: 'design' },
      { id: 'cancelled-submissions', status: 'CANCELLED_ZERO_SUBMISSIONS', track: 'design' },
      { id: 'draft-1', status: 'DRAFT', track: 'ds' },
      { id: 'draft-2', status: 'DRAFT', track: 'ds' },
      // Post mortem is a phase of an ACTIVE challenge, not a terminal status.
      { id: 'post-mortem', status: 'ACTIVE', track: 'qa' }
    ])

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({ copilot: { challengeCount: 5 } })
    const result = await service.getMemberRoleChallenges('copilot', 'copilot')
    expect(result.total).to.equal(5)
    expect(result.challenges.map(challenge => challenge.id)).to.deep.equal([
      'cancelled-review', 'cancelled-submissions', 'completed-1', 'completed-2', 'completed-3'
    ])
    expect(result.trackCounts).to.deep.equal({ DEVELOPMENT: 3, DESIGN: 2 })
    expect(result.fulfillment).to.deep.equal({ completed: 3, cancelled: 2, total: 5, rate: 60 })
  })

  it('omits the Copilot badge and metrics when every challenge is non-terminal', async () => {
    await seedChallenges(['NEW', 'DRAFT', 'APPROVED', 'ACTIVE', 'DELETED'].map(status => ({ id: status, status })))

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({})
    expect(await service.getMemberRoleChallenges('copilot', 'copilot')).to.deep.equal({
      role: 'copilot', total: 0, challenges: [], trackCounts: {},
      fulfillment: { completed: 0, cancelled: 0, total: 0, rate: 0 }
    })
  })

  it('retains every cancellation type while excluding client requests from failures', async () => {
    await seedChallenges(cancelledStatuses.map(status => ({ id: status, status })))

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({ copilot: { challengeCount: 9 } })
    const result = await service.getMemberRoleChallenges('copilot', 'copilot')
    expect(result.total).to.equal(9)
    expect(result.challenges.map(challenge => challenge.status)).to.have.members(cancelledStatuses)
    expect(result.trackCounts).to.deep.equal({ DEVELOPMENT: 9 })
    expect(result.fulfillment).to.deep.equal({ completed: 0, cancelled: 8, total: 8, rate: 0 })
  })

  it('returns zero fulfillment with a visible history when only client cancellations exist', async () => {
    await seedChallenges([{ id: 'client', status: 'CANCELLED_CLIENT_REQUEST' }])

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({ copilot: { challengeCount: 1 } })
    const result = await service.getMemberRoleChallenges('copilot', 'copilot')
    expect(result.total).to.equal(1)
    expect(result.challenges[0].id).to.equal('client')
    expect(result.trackCounts).to.deep.equal({ DEVELOPMENT: 1 })
    expect(result.fulfillment).to.deep.equal({ completed: 0, cancelled: 0, total: 0, rate: 0 })
  })

  it('keeps fulfillment at 99.5% when client requests account for most cancellations', async () => {
    await seedChallenges([
      ...Array.from({ length: 199 }, (_, index) => ({ id: `completed-${index}`, status: 'COMPLETED' })),
      ...Array.from({ length: 24 }, (_, index) => ({ id: `client-${index}`, status: 'CANCELLED_CLIENT_REQUEST' })),
      { id: 'failed-review', status: 'CANCELLED_FAILED_REVIEW' }
    ])

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({ copilot: { challengeCount: 224 } })
    const result = await service.getMemberRoleChallenges('copilot', 'copilot')
    expect(result.total).to.equal(224)
    expect(result.challenges).to.have.length(224)
    expect(result.fulfillment).to.deep.equal({ completed: 199, cancelled: 1, total: 200, rate: 99.5 })
  })

  it('continues to hide restricted challenges and deduplicate resource assignments', async () => {
    await seedChallenges([
      { id: 'public', status: 'COMPLETED' },
      { id: 'group', status: 'COMPLETED', groups: ['private'] },
      { id: 'task', status: 'COMPLETED', task: true },
      { id: 'whitelist', status: 'CANCELLED_ZERO_SUBMISSIONS', whitelisted: true }
    ])
    await client.query(`INSERT INTO resources."Resource" ("challengeId", "memberId", "roleId")
      VALUES ('public', '1', 'copilot')`)

    expect(await service.getMemberRoleStats('copilot')).to.deep.equal({ copilot: { challengeCount: 1 } })
    const result = await service.getMemberRoleChallenges('copilot', 'copilot')
    expect(result.challenges.map(challenge => challenge.id)).to.deep.equal(['public'])
    expect(result.total).to.equal(1)
    expect(result.trackCounts).to.deep.equal({ DEVELOPMENT: 1 })
    expect(result.fulfillment).to.deep.equal({ completed: 1, cancelled: 0, total: 1, rate: 100 })
  })

  it('preserves Reviewer history across statuses and multiple review roles', async () => {
    await seedChallenges([
      { id: 'draft', status: 'DRAFT' },
      { id: 'active', status: 'ACTIVE' },
      { id: 'completed', status: 'COMPLETED' },
      { id: 'cancelled', status: 'CANCELLED_CLIENT_REQUEST' }
    ], 'reviewer')
    await client.query(`INSERT INTO resources."Resource" ("challengeId", "memberId", "roleId")
      VALUES ('completed', '1', 'screener')`)

    expect(await service.getMemberRoleStats('reviewer')).to.deep.equal({ reviewer: { challengeCount: 4 } })
    const result = await service.getMemberRoleChallenges('reviewer', 'reviewer')
    expect(result.total).to.equal(4)
    expect(result.challenges.map(challenge => challenge.id)).to.deep.equal(['active', 'cancelled', 'completed', 'draft'])
    expect(result).not.to.have.property('fulfillment')
    expect(result).not.to.have.property('trackCounts')
  })
})

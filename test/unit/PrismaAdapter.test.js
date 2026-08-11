/**
 * Unit tests for Prisma 7 PostgreSQL adapter configuration compatibility.
 */

const { expect } = require('chai')
const {
  createPostgresAdapter,
  getPostgresSchema
} = require('../../src/common/prismaAdapter')

describe('Prisma PostgreSQL adapter', () => {
  it('preserves the schema query parameter from existing database URLs', () => {
    expect(getPostgresSchema(
      'postgresql://user:password@localhost:5432/member?schema=members'
    )).to.equal('members')
  })

  it('treats URLs without a valid schema parameter as using the driver default', () => {
    expect(getPostgresSchema('postgresql://user:password@localhost:5432/member')).to.equal(undefined)
    expect(getPostgresSchema('not a URL')).to.equal(undefined)
  })

  it('reports the existing environment variable name when a URL is missing', () => {
    expect(() => createPostgresAdapter(undefined, 'DATABASE_URL'))
      .to.throw('DATABASE_URL is not configured')
  })

  it('forwards explicit pool options without changing the connection URL', () => {
    const connectionString = 'postgresql://user:password@localhost:5432/member'
    const adapter = createPostgresAdapter(connectionString, 'DATABASE_URL', { min: 1 })

    expect(adapter.config).to.deep.equal({ connectionString, min: 1 })
  })
})

/*
 * Focused unit tests for member SendGrid email activity.
 */

require('../../app-bootstrap')
const chai = require('chai')
const config = require('config')
const axios = require('axios')

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.DATABASE_URL = process.env.DATABASE_URL || placeholderDbUrl
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl
process.env.CHALLENGES_DB_URL = process.env.CHALLENGES_DB_URL || placeholderDbUrl
process.env.ACADEMY_DB_URL = process.env.ACADEMY_DB_URL || placeholderDbUrl
process.env.RESOURCES_DB_URL = process.env.RESOURCES_DB_URL || placeholderDbUrl
process.env.ENGAGEMENTS_DB_URL = process.env.ENGAGEMENTS_DB_URL || placeholderDbUrl

const helper = require('../../src/common/helper')
const service = require('../../src/services/MemberService')

const should = chai.should()

describe('member sendgrid email activity unit tests', () => {
  let originalHasAdminRole
  let originalGetMemberByHandle
  let originalAxiosGet
  let originalSendgridApiKey

  before(() => {
    originalHasAdminRole = helper.hasAdminRole
    originalGetMemberByHandle = helper.getMemberByHandle
    originalAxiosGet = axios.get
    originalSendgridApiKey = config.SENDGRID_API_KEY
  })

  afterEach(() => {
    helper.hasAdminRole = originalHasAdminRole
    helper.getMemberByHandle = originalGetMemberByHandle
    axios.get = originalAxiosGet
    config.SENDGRID_API_KEY = originalSendgridApiKey
  })

  after(() => {
    helper.hasAdminRole = originalHasAdminRole
    helper.getMemberByHandle = originalGetMemberByHandle
    axios.get = originalAxiosGet
    config.SENDGRID_API_KEY = originalSendgridApiKey
  })

  it('returns only the 20 most recent sendgrid emails without pagination', async () => {
    config.SENDGRID_API_KEY = 'test-sendgrid-api-key'
    helper.hasAdminRole = () => true
    helper.getMemberByHandle = async () => ({ email: 'member@example.com' })

    const calls = []
    axios.get = async (url, options) => {
      calls.push({ url, options })
      return {
        data: {
          messages: Array.from({ length: 25 }, (_, index) => ({ messageId: `m${index + 1}` })),
          _metadata: {
            next: '/v3/messages?query=cursor123'
          }
        }
      }
    }

    const result = await service.getMemberSendgridEmails({ roles: ['admin'] }, 'member')

    should.equal(result.length, 20)
    should.equal(calls.length, 1)
    should.equal(calls[0].url, 'https://api.sendgrid.com/v3/messages')
    should.equal(calls[0].options.headers.Authorization, 'Bearer test-sendgrid-api-key')
    should.equal(calls[0].options.params.limit, 20)
    should.equal(calls[0].options.params.query.indexOf('to_email = "member@example.com"') >= 0, true)
    should.equal(result[0].messageId, 'm1')
    should.equal(result[19].messageId, 'm20')
  })

  it('returns an empty array when the member has no email', async () => {
    config.SENDGRID_API_KEY = 'test-sendgrid-api-key'
    helper.hasAdminRole = () => true
    helper.getMemberByHandle = async () => ({})
    axios.get = async () => {
      throw new Error('axios.get should not be called')
    }

    const result = await service.getMemberSendgridEmails({ roles: ['admin'] }, 'member')

    should.equal(Array.isArray(result), true)
    should.equal(result.length, 0)
  })
})

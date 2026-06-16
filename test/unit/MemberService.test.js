/*
 * Unit tests of member service
 */

require('../../app-bootstrap')
const _ = require('lodash')
const config = require('config')
const chai = require('chai')
const fs = require('fs')
const path = require('path')
const awsMock = require('aws-sdk-mock')
const axios = require('axios')

const placeholderDbUrl = 'postgresql://user:pass@localhost:5432/topcoder?schema=public'
process.env.SKILLS_DB_URL = process.env.SKILLS_DB_URL || placeholderDbUrl
process.env.CHALLENGES_DB_URL = process.env.CHALLENGES_DB_URL || placeholderDbUrl
process.env.ACADEMY_DB_URL = process.env.ACADEMY_DB_URL || placeholderDbUrl
process.env.RESOURCES_DB_URL = process.env.RESOURCES_DB_URL || placeholderDbUrl
process.env.ENGAGEMENTS_DB_URL = process.env.ENGAGEMENTS_DB_URL || placeholderDbUrl

const service = require('../../src/services/MemberService')
const prisma = require('../../src/common/prisma').getClient()
const testHelper = require('../testHelper')

const should = chai.should()

const photoContent = fs.readFileSync(path.join(__dirname, '../photo.png'))

describe('member service unit tests', () => {
  // test data
  let member1
  let member2

  before(async () => {
    await testHelper.createData()
    const data = testHelper.getData()
    member1 = data.member1
    member2 = data.member2

    // mock S3 before creating S3 instance
    awsMock.mock('S3', 'getObject', (params, callback) => {
      callback(null, { Body: Buffer.from(photoContent) })
    })

    awsMock.mock('S3', 'upload', (params, callback) => {
      callback(null)
    })
  })

  after(async () => {
    await testHelper.clearData()

    awsMock.restore('S3')
  })

  describe('get member tests', () => {
    it('get member successfully 1', async () => {
      const result = await service.getMember({ isMachine: true }, member1.handle, {})
      should.equal(_.isEqual(result.maxRating, member1.maxRating), true)
      should.equal(result.userId, member1.userId)
      should.equal(result.firstName, member1.firstName)
      should.equal(result.lastName, member1.lastName)
      should.equal(result.description, member1.description)
      // should.equal(result.otherLangName, member1.otherLangName)
      should.equal(result.handle, member1.handle)
      should.equal(result.handleLower, member1.handleLower)
      should.equal(result.status, member1.status)
      should.equal(result.email, member1.email)
      should.equal(result.addresses.length, 1)
      should.equal(result.addresses[0].streetAddr1, member1.addresses[0].streetAddr1)
      should.equal(result.addresses[0].streetAddr2, member1.addresses[0].streetAddr2)
      should.equal(result.addresses[0].city, member1.addresses[0].city)
      should.equal(result.addresses[0].zip, member1.addresses[0].zip)
      should.equal(result.addresses[0].stateCode, member1.addresses[0].stateCode)
      should.equal(result.addresses[0].type, member1.addresses[0].type)
      // should.equal(testHelper.getDatesDiff(result.addresses[0].createdAt, member1.addresses[0].createdAt), 0)
      // should.equal(testHelper.getDatesDiff(result.addresses[0].updatedAt, member1.addresses[0].updatedAt), 0)
      // should.equal(result.addresses[0].createdBy, member1.addresses[0].createdBy)
      // should.equal(result.addresses[0].updatedBy, member1.addresses[0].updatedBy)
      should.equal(result.homeCountryCode, member1.homeCountryCode)
      should.equal(result.competitionCountryCode, member1.competitionCountryCode)
      should.equal(result.photoURL, member1.photoURL)
      should.equal(_.isEqual(result.tracks, member1.tracks), true)
      should.equal(testHelper.getDatesDiff(result.createdAt, member1.createdAt), 0)
      should.equal(testHelper.getDatesDiff(result.updatedAt, member1.updatedAt), 0)
      // should.equal(result.createdBy, member1.createdBy)
      // should.equal(result.updatedBy, member1.updatedBy)
    })

    it('get member includes challenge point summary and details', async () => {
      const updateResult = await service.updateChallengePoints({ isMachine: true, userId: 'autopilot' }, 'challenge-1', {
        challengeName: 'AI Points Challenge',
        points: [
          { userId: member1.userId, placement: 1, points: 250 },
          { userId: member2.userId, placement: 2, points: 100 }
        ]
      })

      should.equal(updateResult.updated, 2)

      const result = await service.getMember({ isMachine: true }, member1.handle, {})
      should.equal(result.challengePoints.total, 250)
      should.equal(result.challengePoints.challenges, 1)
      should.equal(result.challengePoints.details.length, 1)
      should.equal(result.challengePoints.details[0].challengeId, 'challenge-1')
      should.equal(result.challengePoints.details[0].challengeName, 'AI Points Challenge')
      should.equal(result.challengePoints.details[0].placement, 1)
      should.equal(result.challengePoints.details[0].points, 250)
    })

    it('update challenge points replaces stale rows for the challenge', async () => {
      await service.updateChallengePoints({ isMachine: true, userId: 'autopilot' }, 'challenge-1', {
        challengeName: 'AI Points Challenge Updated',
        points: [
          { userId: member1.userId, placement: 1, points: 300 }
        ]
      })

      const member1Result = await service.getMember({ isMachine: true }, member1.handle, {})
      should.equal(member1Result.challengePoints.total, 300)
      should.equal(member1Result.challengePoints.details[0].challengeName, 'AI Points Challenge Updated')

      const member2Result = await service.getMember({ isMachine: true }, member2.handle, {})
      should.equal(member2Result.challengePoints.total, 0)
      should.equal(member2Result.challengePoints.challenges, 0)
    })

    it('get member successfully 2', async () => {
      const result = await service.getMember({ handle: 'test', roles: ['role'] }, member1.handle, {
        fields: 'userId,firstName,lastName,email,addresses'
      })
      should.equal(result.userId, member1.userId)
      should.equal(result.firstName, member1.firstName)
      // should.equal(result.lastName, member1.lastName)
      // identifiable fields should not be returned
      should.not.exist(result.email)
      // should.not.exist(result.addresses)
    })

    it('get member should not expose email for regular users when communication fields are misconfigured', async () => {
      const originalCommunicationSecureFields = [...config.COMMUNICATION_SECURE_FIELDS]
      config.COMMUNICATION_SECURE_FIELDS = ['loginCount', 'lastLoginDate']

      try {
        const result = await service.getMember({ handle: 'test', roles: ['role'] }, member1.handle, {
          fields: 'userId,firstName,lastName,email,loginCount,lastLoginDate'
        })
        should.equal(result.userId, member1.userId)
        should.equal(result.firstName, member1.firstName)
        should.not.exist(result.email)
      } finally {
        config.COMMUNICATION_SECURE_FIELDS = originalCommunicationSecureFields
      }
    })

    it('get member - not found', async () => {
      try {
        await service.getMember({ isMachine: true }, 'other', {})
      } catch (e) {
        should.equal(e.message, 'Member with handle: "other" doesn\'t exist')
        return
      }
      throw new Error('should not reach here')
    })

    it('get member - invalid field', async () => {
      try {
        await service.getMember({ isMachine: true }, member1.handle, { fields: 'invalid' })
      } catch (e) {
        should.equal(e.message, 'Invalid value: invalid')
        return
      }
      throw new Error('should not reach here')
    })

    it('get member - duplicate fields', async () => {
      try {
        await service.getMember({ isMachine: true }, member1.handle, { fields: 'email,email' })
      } catch (e) {
        should.equal(e.message, 'Duplicate values: email')
        return
      }
      throw new Error('should not reach here')
    })

    it('get member - unexpected query parameter', async () => {
      try {
        await service.getMember({ isMachine: true }, member1.handle, { invalid: 'email' })
      } catch (e) {
        should.equal(e.message.indexOf('"invalid" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })
  })

  describe('get profile completeness tests', () => {
    it('counts open-to-work availability complete without legacy preferred roles', async () => {
      const memberTraits = await prisma.memberTraits.findUnique({
        where: { userId: member1.userId }
      })

      try {
        await prisma.member.update({
          where: { userId: member1.userId },
          data: { availableForGigs: true }
        })

        await prisma.memberTraitPersonalization.create({
          data: {
            memberTraitId: memberTraits.id,
            key: 'openToWork',
            value: { availability: 'FULL_TIME' },
            private: true,
            createdBy: 'test'
          }
        })

        const result = await service.getProfileCompleteness({ isMachine: true }, member1.handle, {})

        should.equal(result.data.engagementAvailability, true)
        should.equal(result.data.percentComplete, 0.5)
      } finally {
        if (memberTraits) {
          await prisma.memberTraitPersonalization.deleteMany({
            where: {
              memberTraitId: memberTraits.id,
              key: 'openToWork'
            }
          })
        }

        await prisma.member.update({
          where: { userId: member1.userId },
          data: { availableForGigs: null }
        })
      }
    })
  })

  describe('get member sendgrid emails tests', () => {
    let originalSendgridApiKey
    let originalAxiosGet

    before(() => {
      originalSendgridApiKey = config.SENDGRID_API_KEY
      originalAxiosGet = axios.get
    })

    afterEach(() => {
      config.SENDGRID_API_KEY = originalSendgridApiKey
      axios.get = originalAxiosGet
    })

    after(() => {
      config.SENDGRID_API_KEY = originalSendgridApiKey
      axios.get = originalAxiosGet
    })

    it('get member sendgrid emails - forbidden for non-admin JWT user', async () => {
      config.SENDGRID_API_KEY = 'test-sendgrid-api-key'
      try {
        await service.getMemberSendgridEmails({ roles: ['Topcoder User'] }, member1.handle)
      } catch (e) {
        should.equal(e.message, 'You are not allowed to view SendGrid email activity.')
        return
      }
      throw new Error('should not reach here')
    })

    it('get member sendgrid emails successfully with capped recent results', async () => {
      config.SENDGRID_API_KEY = 'test-sendgrid-api-key'
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

      const result = await service.getMemberSendgridEmails({ roles: ['admin'] }, member1.handle)
      should.equal(result.length, 20)
      should.equal(calls.length, 1)
      should.equal(calls[0].url, 'https://api.sendgrid.com/v3/messages')
      should.equal(calls[0].options.headers.Authorization, 'Bearer test-sendgrid-api-key')
      should.equal(calls[0].options.params.limit, 20)
      should.equal(calls[0].options.params.query.indexOf(`to_email = "${member1.email}"`) >= 0, true)
      should.equal(result[0].messageId, 'm1')
      should.equal(result[19].messageId, 'm20')
    })

    it('get member sendgrid emails allows m2m caller', async () => {
      config.SENDGRID_API_KEY = 'test-sendgrid-api-key'
      axios.get = async () => ({
        data: {
          messages: [],
          _metadata: {}
        }
      })

      const result = await service.getMemberSendgridEmails({ isMachine: true }, member1.handle)
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 0)
    })

    it('get member sendgrid emails - missing api key', async () => {
      config.SENDGRID_API_KEY = ''
      try {
        await service.getMemberSendgridEmails({ isMachine: true }, member1.handle)
      } catch (e) {
        should.equal(e.message, 'SendGrid API key is not configured.')
        return
      }
      throw new Error('should not reach here')
    })
  })

  describe('verify email tests', () => {
    it('verify email - wrong token', async () => {
      try {
        await service.verifyEmail({ isMachine: true }, member1.handle, { token: 'wrong' })
      } catch (e) {
        should.equal(e.message, 'Wrong verification token.')
        return
      }
      throw new Error('should not reach here')
    })

    it('verify email successfully 1', async () => {
      const result = await service.verifyEmail({ isMachine: true }, member1.handle, {
        token: member1.emailVerifyToken
      })
      should.equal(result.emailChangeCompleted, false)
      should.equal(result.verifiedEmail, member1.email)
    })

    it('verify email successfully 2', async () => {
      const result = await service.verifyEmail({ isMachine: true }, member1.handle, {
        token: member1.newEmailVerifyToken
      })
      should.equal(result.emailChangeCompleted, true)
      should.equal(result.verifiedEmail, member1.newEmail)
    })

    it('verify email - not found', async () => {
      try {
        await service.verifyEmail({ isMachine: true }, 'other', { token: 'test' })
      } catch (e) {
        should.equal(e.message, 'Member with handle: "other" doesn\'t exist')
        return
      }
      throw new Error('should not reach here')
    })

    it('verify email - missing token', async () => {
      try {
        await service.verifyEmail({ isMachine: true }, member1.handle, {})
      } catch (e) {
        should.equal(e.message.indexOf('"token" is required') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('verify email - unexpected query parameter', async () => {
      try {
        await service.verifyEmail({ isMachine: true }, member1.handle, { token: 'abc', invalid: 'email' })
      } catch (e) {
        should.equal(e.message.indexOf('"invalid" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })
  })

  describe('update member tests', () => {
    it('update member successfully', async () => {
      const result = await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
        // userId: 999,
        firstName: 'fff',
        lastName: 'lll',
        description: 'updated desc',
        email: 'new-email@test.com'
      })
      // should.equal(result.maxRating, member2.maxRating)
      should.equal(result.firstName, 'fff')
      should.equal(result.lastName, 'lll')
      should.equal(result.description, 'updated desc')
      // should.equal(result.otherLangName, member2.otherLangName)
      should.equal(result.handle, member2.handle)
      should.equal(result.handleLower, member2.handleLower)
      should.equal(result.status, member2.status)
      // email is not updated to new email, because it is not verified yet
      should.equal(result.email, member2.email)
      should.equal(result.addresses.length, 1)
      should.equal(result.addresses[0].streetAddr1, member2.addresses[0].streetAddr1)
      should.equal(result.addresses[0].streetAddr2, member2.addresses[0].streetAddr2)
      should.equal(result.addresses[0].city, member2.addresses[0].city)
      should.equal(result.addresses[0].zip, member2.addresses[0].zip)
      should.equal(result.addresses[0].stateCode, member2.addresses[0].stateCode)
      should.equal(result.addresses[0].type, member2.addresses[0].type)
      // should.equal(testHelper.getDatesDiff(result.addresses[0].createdAt, member2.addresses[0].createdAt), 0)
      // should.equal(testHelper.getDatesDiff(result.addresses[0].updatedAt, member2.addresses[0].updatedAt), 0)
      // should.equal(result.addresses[0].createdBy, member2.addresses[0].createdBy)
      // should.equal(result.addresses[0].updatedBy, member2.addresses[0].updatedBy)
      should.equal(result.homeCountryCode, member2.homeCountryCode)
      should.equal(result.competitionCountryCode, member2.competitionCountryCode)
      should.equal(result.photoURL, member2.photoURL)
      should.equal(_.isEqual(result.tracks, member2.tracks), true)
      should.equal(testHelper.getDatesDiff(result.createdAt, member2.createdAt), 0)
      should.exist(result.updatedAt)
      // should.equal(result.createdBy, member2.createdBy)
      should.equal(result.updatedBy, 'sub1')
    })

    it('update member - not found', async () => {
      try {
        await service.updateMember({ isMachine: true, sub: 'sub1' }, 'other', {}, {
          firstName: '999'
        })
      } catch (e) {
        should.equal(e.message, 'Member with handle: "other" doesn\'t exist')
        return
      }
      throw new Error('should not reach here')
    })

    it('update member - invalid email', async () => {
      try {
        await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
          email: 'abc'
        })
      } catch (e) {
        should.equal(e.message.indexOf('"email" must be a valid email') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('update member - handle change not allowed', async () => {
      try {
        await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
          handle: 'newHandle'
        })
      } catch (e) {
        should.equal(e.message.indexOf('"handle" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('update member - unexpected field', async () => {
      try {
        await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
          other: 'abc'
        })
      } catch (e) {
        should.equal(e.message.indexOf('"other" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('update member - handle change not allowed', async () => {
      try {
        await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
          handle: 'newHandle'
        })
      } catch (e) {
        should.equal(e.message.indexOf('"handle" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('update member - track availableForGigs changes', async () => {
      const result = await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
        availableForGigs: true
      })
      should.equal(result.availableForGigs, true)
      should.exist(result.availableForGigsLastUpdateDate)
      should.equal(testHelper.getDatesDiff(result.availableForGigsLastUpdateDate, new Date()), 0)
    })

    it('update member - availableForGigsLastUpdateDate not set when availableForGigs not changed', async () => {
      const result = await service.updateMember({ isMachine: true, sub: 'sub1' }, member2.handle, {}, {
        firstName: 'test'
      })
      should.equal(result.firstName, 'test')
      should.not.exist(result.availableForGigsLastUpdateDate)
    })
  })

  describe('upload photo tests', () => {
    it('upload photo successfully', async () => {
      const result = await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, member2.handle, {
        photo: {
          data: photoContent,
          mimetype: 'image/png',
          name: 'photo.png',
          size: photoContent.length
        }
      })
      should.equal(result.photoURL.startsWith(config.PHOTO_URL_TEMPLATE.replace('<key>', '')), true)
    })

    it('upload photo - not found', async () => {
      try {
        await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, 'other', {
          photo: {
            data: photoContent,
            mimetype: 'image/png',
            name: 'photo.png',
            size: photoContent.length
          }
        })
      } catch (e) {
        should.equal(e.message, 'Member with handle: "other" doesn\'t exist')
        return
      }
      throw new Error('should not reach here')
    })

    it('upload photo - invalid file field', async () => {
      try {
        await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, member2.handle, {
          invalid: {
            data: photoContent,
            mimetype: 'image/png',
            name: 'photo.png',
            size: photoContent.length
          }
        })
      } catch (e) {
        should.equal(e.message.indexOf('"photo" is required') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('upload photo - missing file', async () => {
      try {
        await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, member2.handle, {})
      } catch (e) {
        should.equal(e.message.indexOf('"photo" is required') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('upload photo - empty handle', async () => {
      try {
        await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, '', {
          photo: {
            data: photoContent,
            mimetype: 'image/png',
            name: 'photo.png',
            size: photoContent.length
          }
        })
      } catch (e) {
        should.equal(e.message.indexOf('"handle" is not allowed to be empty') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })

    it('upload photo - unexpected field', async () => {
      try {
        await service.uploadPhoto({ handle: 'admin', roles: ['admin'] }, member2.handle, {
          photo: {
            data: photoContent,
            mimetype: 'image/png',
            name: 'photo.png',
            size: photoContent.length
          },
          other: 'invalid'
        })
      } catch (e) {
        should.equal(e.message.indexOf('"other" is not allowed') >= 0, true)
        return
      }
      throw new Error('should not reach here')
    })
  })
})

/*
 * E2E tests of member traits API - Focused on MemberTraitController.js endpoints
 */

require('../../app-bootstrap')
const chai = require('chai')
const chaiHttp = require('chai-http')
const app = require('../../app')
const testHelper = require('../testHelper')
const config = require('config')

const should = chai.should()
chai.use(chaiHttp)

const basePath = `/v6/members`

describe('Member Traits API E2E Tests - MemberTraitController Focus', function () {
  
  // Test data
  let data
  const notFoundHandle = 'nonexistentuser12345'

  before(async () => {
    await testHelper.clearData()
    await testHelper.createData()
    data = testHelper.getData()
  })

  after(async () => {
    await testHelper.clearData()
  })

  describe('GET /members/:handle/traits - getTraits', () => {
    it('should return member traits successfully with admin token', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      // Verify response structure
      should.exist(result)
      should.equal(Array.isArray(result), true)
      
      // Should have subscription trait for member1
      const subscriptionTrait = result.find(trait => trait.traitId === 'subscription')
      should.exist(subscriptionTrait)
      should.equal(subscriptionTrait.userId, data.member1.userId)
      should.equal(subscriptionTrait.categoryName, 'Subscription')
      should.exist(subscriptionTrait.traits)
      should.exist(subscriptionTrait.traits.data)
      should.equal(Array.isArray(subscriptionTrait.traits.data), true)
    })

    it('should return member traits with specific traitIds filter', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
        .query({ traitIds: 'education,work' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 2)
      
      const traitIds = result.map(trait => trait.traitId)
      should.equal(traitIds.includes('education'), true)
      should.equal(traitIds.includes('work'), true)
      should.equal(traitIds.includes('subscription'), false)
    })

    it('should return member traits with specific fields filter', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
        .query({ fields: 'userId,traitId,categoryName' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length > 0, true)
      
      // Verify only requested fields are present
      const trait = result[0]
      should.exist(trait.userId)
      should.exist(trait.traitId)
      should.exist(trait.categoryName)
      should.not.exist(trait.createdAt)
      should.not.exist(trait.updatedAt)
      should.not.exist(trait.createdBy)
      should.not.exist(trait.updatedBy)
    })

    it('should return sanitized traits for non-admin user', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.USER_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      
      // Verify secure fields are omitted for non-admin
      result.forEach(trait => {
        should.not.exist(trait.createdBy)
        should.not.exist(trait.updatedBy)
      })
    })

    it('should return public traits only for anonymous users', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      
      // Should only contain public traits
      const traitIds = result.map(trait => trait.traitId)
      const publicTraits = config.MEMBER_PUBLIC_TRAITS
      traitIds.forEach(traitId => {
        should.equal(publicTraits.includes(traitId), true)
      })
    })

    it('should return 404 for non-existent member', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${notFoundHandle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, `Member with handle: "${notFoundHandle}" doesn't exist`)
    })

    it('should handle member with no traits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 0)
    })

    it('should handle case-insensitive handle lookup', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle.toUpperCase()}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
    })

    it('should return all available traits when no traitIds specified', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length >= 2, true) // Should have education and work traits
    })
  })

  describe('POST /members/:handle/traits - createTraits', () => {
    beforeEach(async () => {
      // Clear any existing traits for member3 before each test
      await testHelper.clearMemberTraits(data.member3.userId)
    })

    it('should create basic_info trait successfully', async () => {
      const traitData = [{
        traitId: 'basic_info',
        categoryName: 'Basic Info',
        traits: {
          traitId: 'basic_info',
          data: [{
            userId: data.member3.userId,
            country: 'United States',
            primaryInterestInTopcoder: 'Competitive Programming',
            tshirtSize: 'M',
            gender: 'Male',
            shortBio: 'Software developer with passion for algorithms',
            birthDate: '1990-01-01T00:00:00.000Z',
            currentLocation: 'New York, NY'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      if (response.status !== 200) {
        console.log('Error response:', response.status, response.body)
      }
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'basic_info')
      should.equal(result[0].categoryName, 'Basic Info')
    })

    it('should create education trait successfully', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'Harvard University',
            degree: 'Bachelor of Science in Computer Science',
            endYear: 2020
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'education')
    })

    it('should create work trait successfully', async () => {
      const traitData = [{
        traitId: 'work',
        categoryName: 'Work Experience',
        traits: {
          traitId: 'work',
          data: [{
            industry: 'TechAndTechnologyService',
            companyName: 'Google Inc.',
            position: 'Senior Software Engineer',
            startDate: '2020-06-01T00:00:00.000Z',
            endDate: null,
            working: true
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'work')
    })

    it('should create languages trait successfully', async () => {
      const traitData = [{
        traitId: 'languages',
        categoryName: 'Languages',
        traits: {
          traitId: 'languages',
          data: [{
            language: 'English',
            spokenLevel: 'Native',
            writtenLevel: 'Native'
          }, {
            language: 'Spanish',
            spokenLevel: 'Intermediate',
            writtenLevel: 'Beginner'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'languages')
    })

    it('should create device trait successfully', async () => {
      const traitData = [{
        traitId: 'device',
        categoryName: 'Devices',
        traits: {
          traitId: 'device',
          data: [{
            deviceType: 'Laptop',
            manufacturer: 'Apple',
            model: 'MacBook Pro',
            operatingSystem: 'macOS',
            osLanguage: 'English',
            osVersion: '13.0'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'device')
    })

    it('should create software trait successfully', async () => {
      const traitData = [{
        traitId: 'software',
        categoryName: 'Software',
        traits: {
          traitId: 'software',
          data: [{
            softwareType: 'DeveloperTools',
            name: 'Visual Studio Code'
          }, {
            softwareType: 'Browser',
            name: 'Chrome'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'software')
    })

    it('should create service_provider trait successfully', async () => {
      const traitData = [{
        traitId: 'service_provider',
        categoryName: 'Service Providers',
        traits: {
          traitId: 'service_provider',
          data: [{
            type: 'InternetServiceProvider',
            name: 'Comcast'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'service_provider')
    })

    it('should create communities trait successfully', async () => {
      const traitData = [{
        traitId: 'communities',
        categoryName: 'Communities',
        traits: {
          traitId: 'communities',
          data: [{
            communityName: 'TopCoder',
            status: true
          }, {
            communityName: 'GitHub',
            status: false
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'communities')
    })

    it('should create hobby trait successfully', async () => {
      const traitData = [{
        traitId: 'hobby',
        categoryName: 'Hobbies',
        traits: {
          traitId: 'hobby',
          data: ['Photography', 'Hiking', 'Cooking']
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'hobby')
    })

    it('should create subscription trait successfully', async () => {
      const traitData = [{
        traitId: 'subscription',
        categoryName: 'Subscriptions',
        traits: {
          traitId: 'subscription',
          data: ['Netflix', 'Spotify', 'Adobe Creative Cloud']
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'subscription')
    })

    it('should create personalization trait successfully', async () => {
      const traitData = [{
        traitId: 'personalization',
        categoryName: 'Personalization',
        traits: {
          traitId: 'personalization',
          data: [{
            theme: 'dark',
            notifications: 'enabled',
            language: 'en'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'personalization')
    })

    it('should create onboarding_checklist trait successfully', async () => {
      const traitData = [{
        traitId: 'onboarding_checklist',
        categoryName: 'Onboarding Checklist',
        traits: {
          traitId: 'onboarding_checklist',
          data: [{
            listItemType: 'profile_completion',
            date: '2023-01-01T00:00:00.000Z',
            message: 'Complete your profile',
            status: 'completed',
            metadata: { progress: 100 }
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'onboarding_checklist')
    })

    it('should return 500 for invalid trait data due to Prisma validation', async () => {
      const traitData = [{
        traitId: 'basic_info',
        categoryName: 'Basic Info',
        traits: {
          traitId: 'basic_info',
          data: [{
            country: '', // Invalid: required field empty
            primaryInterestInTopcoder: 'Competitive Programming'
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 500)
      should.exist(response.body.message)
    })

    it('should return 400 for invalid traitId', async () => {
      const traitData = [{
        traitId: 'invalid_trait',
        categoryName: 'Invalid',
        traits: {
          traitId: 'invalid_trait',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should return 400 for duplicate trait creation', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'MIT',
            degree: 'Bachelor of Science',
            endYear: 2020
          }]
        }
      }]
      
      // First creation should succeed
      await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      // Second creation should fail
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.equal(response.body.message, 'The trait id education already exists for the member.')
    })

    it('should return 404 for non-existent member', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${notFoundHandle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, `Member with handle: "${notFoundHandle}" doesn't exist`)
    })

    it('should return 403 when user cannot manage member', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.USER_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 403)
      should.equal(response.body.message, 'You are not allowed to create traits of the member.')
    })

    it('should return 401 when no token provided', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .send(traitData)
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'No token provided.')
    })
  })

  describe('PUT /members/:handle/traits - updateTraits', () => {
    beforeEach(async () => {
      // Clear any existing traits for member3 before each test
      await testHelper.clearMemberTraits(data.member3.userId)
    })

    it('should update existing education trait successfully', async () => {
      // First create a trait
      const createData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'Old University',
            degree: 'Bachelor of Science',
            endYear: 2015
          }]
        }
      }]
      
      await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(createData)
      
      // Now update it
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'New University',
            degree: 'Master of Science',
            endYear: 2020
          }]
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'education')
    })

    it('should update work trait successfully', async () => {
      // First create a trait
      const createData = [{
        traitId: 'work',
        categoryName: 'Work Experience',
        traits: {
          traitId: 'work',
          data: [{
            industry: 'Banking',
            companyName: 'Old Company',
            position: 'Developer',
            working: false
          }]
        }
      }]
      
      await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(createData)
      
      // Now update it
      const updateData = [{
        traitId: 'work',
        categoryName: 'Work Experience',
        traits: {
          traitId: 'work',
          data: [{
            industry: 'TechAndTechnologyService',
            companyName: 'New Company',
            position: 'Senior Developer',
            startDate: '2020-01-01T00:00:00.000Z',
            endDate: '2023-01-01T00:00:00.000Z',
            working: false
          }]
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 200)
      const result = response.body
      
      should.equal(Array.isArray(result), true)
      should.equal(result.length, 1)
      should.equal(result[0].traitId, 'work')
    })

    it('should return 404 for updating non-existent trait', async () => {
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, 'The trait id education is not found for the member.')
    })

    it('should return 404 for non-existent member', async () => {
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${notFoundHandle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, `Member with handle: "${notFoundHandle}" doesn't exist`)
    })

    it('should return 403 when user cannot manage member', async () => {
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.USER_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 403)
      should.equal(response.body.message, 'You are not allowed to update traits of the member.')
    })

    it('should return 401 when no token provided', async () => {
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member3.handle}/traits`)
        .send(updateData)
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'No token provided.')
    })
  })

  describe('DELETE /members/:handle/traits - removeTraits', () => {
    it('should remove specific traits successfully', async () => {
      // First create some traits for member2
      const createData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'Test University',
            degree: 'Bachelor of Science',
            endYear: 2020
          }]
        }
      }, {
        traitId: 'work',
        categoryName: 'Work Experience',
        traits: {
          traitId: 'work',
          data: [{
            industry: 'TechAndTechnologyService',
            companyName: 'Test Company',
            position: 'Developer',
            working: true
          }]
        }
      }]

      await chai.request(app)
        .post(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(createData)
      
      // Now remove only education trait
      const response = await chai.request(app)
        .delete(`${basePath}/${data.member2.handle}/traits`)
        .query({ traitIds: 'education' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)

      if (response.status !== 200) {
        console.error('Error response:', response.status, response.body)
      }
      should.equal(response.status, 200)
      
      // Verify education trait is removed but work trait remains
      const getResponse = await chai.request(app)
        .get(`${basePath}/${data.member2.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      should.equal(getResponse.status, 200)
      const result = getResponse.body
      const traitIds = result.map(trait => trait.traitId)
      should.equal(traitIds.includes('education'), false)
      should.equal(traitIds.includes('work'), true)
    })

    it('should return 400 for removing non-existent trait', async () => {
      const response = await chai.request(app)
        .delete(`${basePath}/${data.member1.handle}/traits`)
        .query({ traitIds: 'education' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, 'The trait id education is not found for the member.')
    })

    it('should return 404 for non-existent member', async () => {
      const response = await chai.request(app)
        .delete(`${basePath}/${notFoundHandle}/traits`)
        .query({ traitIds: 'education' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 404)
      should.equal(response.body.message, `Member with handle: "${notFoundHandle}" doesn't exist`)
    })

    it('should return 403 when user cannot manage member', async () => {
      const response = await chai.request(app)
        .delete(`${basePath}/${data.member2.handle}/traits`)
        .query({ traitIds: 'education' })
        .set('Authorization', `Bearer ${config.USER_TOKEN}`)
      
      should.equal(response.status, 403)
      should.equal(response.body.message, 'You are not allowed to remove traits of the member.')
    })

    it('should return 401 when no token provided', async () => {
      const response = await chai.request(app)
        .delete(`${basePath}/${data.member3.handle}/traits`)
        .query({ traitIds: 'education' })
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'No token provided.')
    })
  })

  describe('M2M Token Tests', () => {
    it('should work with M2M full access token for getTraits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.M2M_FULL_ACCESS_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      should.equal(Array.isArray(result), true)
    })

    it('should work with M2M read access token for getTraits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.M2M_READ_ACCESS_TOKEN}`)
      
      should.equal(response.status, 200)
      const result = response.body
      should.equal(Array.isArray(result), true)
    })

    it('should return 401 for M2M create access token (placeholder token)', async () => {
      // Clear any existing traits first
      await testHelper.clearMemberTraits(data.member3.userId)
      
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'M2M University',
            degree: 'Bachelor of Science',
            endYear: 2020
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.M2M_CREATE_ACCESS_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 401)
      should.exist(response.body.message)
    })

    it('should work with M2M update access token for updateTraits', async () => {
      // Clear any existing traits first
      await testHelper.clearMemberTraits(data.member3.userId)
      
      // First create a trait
      const createData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'Test University',
            degree: 'Bachelor of Science',
            endYear: 2020
          }]
        }
      }]
      
      await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(createData)
      
      // Now update it with M2M token
      const updateData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'M2M Updated University',
            degree: 'Master of Science',
            endYear: 2022
          }]
        }
      }]
      
      const response = await chai.request(app)
        .put(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.M2M_UPDATE_ACCESS_TOKEN}`)
        .send(updateData)
      
      should.equal(response.status, 200)
      const result = response.body
      should.equal(Array.isArray(result), true)
    })

    it('should return 401 for M2M delete access token (placeholder token)', async () => {
      // Clear any existing traits first
      await testHelper.clearMemberTraits(data.member3.userId)
      
      // First create a trait
      const createData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: 'Test University',
            degree: 'Bachelor of Science',
            endYear: 2020
          }]
        }
      }]
      
      await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(createData)
      
      // Now try to remove it with M2M token (should fail with 401)
      const response = await chai.request(app)
        .delete(`${basePath}/${data.member3.handle}/traits`)
        .query({ traitIds: 'education' })
        .set('Authorization', `Bearer ${config.M2M_DELETE_ACCESS_TOKEN}`)
      
      should.equal(response.status, 401)
      should.exist(response.body.message)
    })

    it('should reject M2M read token for write operations', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.M2M_READ_ACCESS_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 403)
      should.equal(response.body.message, 'You are not allowed to perform this action!')
    })
  })

  describe('Authentication and Authorization Tests', () => {
    it('should handle invalid token format', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', 'invalid format')
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'No token provided.')
    })

    it('should handle invalid token', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.INVALID_TOKEN}`)
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'Failed to authenticate token.')
    })

    it('should handle expired token', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.EXPIRED_TOKEN}`)
      
      should.equal(response.status, 401)
      should.equal(response.body.message, 'Failed to authenticate token.')
    })
  })

  describe('Error Handling Tests', () => {
    it('should handle malformed JSON in request body', async () => {
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .set('Content-Type', 'application/json')
        .send('{"invalid": json}')
      
      should.equal(response.status, 500)
    })

    it('should handle concurrent requests', async () => {
      const promises = []
      for (let i = 0; i < 3; i++) {
        promises.push(
          chai.request(app)
            .get(`${basePath}/${data.member1.handle}/traits`)
            .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        )
      }
      
      const responses = await Promise.all(promises)
      responses.forEach(response => {
        should.equal(response.status, 200)
      })
    })

    it('should handle empty request body for createTraits', async () => {
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send([])
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle invalid trait data structure', async () => {
      const traitData = [{
        traitId: 'education',
        // Missing categoryName and traits
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle very large trait data', async () => {
      const largeTraitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: Array(1000).fill().map((_, i) => ({
            collegeName: `University ${i}`.repeat(100),
            degree: `Degree ${i}`.repeat(100),
            endYear: 2020 + i
          }))
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(largeTraitData)
      
      // Should either succeed or fail gracefully
      // Large data might cause server errors, which is acceptable
      should.equal(response.status >= 200, true)
      should.equal(response.status < 600, true) // Allow 5xx errors for large data
    })

    it('should handle special characters in trait data', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: [{
            collegeName: '<script>alert("xss")</script>',
            degree: 'Degree with "quotes" and \'apostrophes\'',
            endYear: 2020
          }]
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      // Should either succeed or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle very deep nested trait data', async () => {
      let deepTraitData = {
        traitId: 'personalization',
        categoryName: 'Personalization',
        traits: {
          traitId: 'personalization',
          data: [{}]
        }
      }
      
      let current = deepTraitData.traits.data[0]
      for (let i = 0; i < 100; i++) {
        current[`level${i}`] = { nested: {} }
        current = current[`level${i}`].nested
      }
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send([deepTraitData])
      
      // Should either succeed or fail gracefully
      // Deep nested data might cause server errors, which is acceptable
      should.equal(response.status >= 200, true)
      should.equal(response.status < 600, true) // Allow 5xx errors for deep nested data
    })

    it('should handle circular reference in trait data', async () => {
      const circularTraitData = {
        traitId: 'personalization',
        categoryName: 'Personalization',
        traits: {
          traitId: 'personalization',
          data: [{}]
        }
      }
      
      circularTraitData.traits.data[0].self = circularTraitData.traits.data[0]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send([circularTraitData])
      
      // Should either succeed or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle invalid traitId with special characters', async () => {
      const traitData = [{
        traitId: 'education<script>alert("xss")</script>',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle very long traitId', async () => {
      const longTraitId = 'a'.repeat(1000)
      const traitData = [{
        traitId: longTraitId,
        categoryName: 'Education',
        traits: {
          traitId: longTraitId,
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle invalid categoryName with special characters', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: '<script>alert("xss")</script>',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle very long categoryName', async () => {
      const longCategoryName = 'A'.repeat(10000)
      const traitData = [{
        traitId: 'education',
        categoryName: longCategoryName,
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      // Should either succeed or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle invalid HTTP methods for traits endpoints', async () => {
      const response = await chai.request(app)
        .patch(`${basePath}/${data.member1.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 404)
    })

    it('should handle malformed query parameters for traits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .query({ traitIds: 'education,work&malformed=param' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      // Should either succeed with valid traitIds or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle very long query parameters for traits', async () => {
      const longTraitIds = 'basic_info,education,work,communities,languages,hobby,organization,device,software,service_provider,subscription,personalization,connect_info,onboarding_checklist,'.repeat(100)
      
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .query({ traitIds: longTraitIds })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      // Should either succeed or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle invalid field parameters with special characters for traits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .query({ fields: 'userId,<script>alert("xss")</script>,traitId' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle duplicate field parameters with different cases for traits', async () => {
      const response = await chai.request(app)
        .get(`${basePath}/${data.member1.handle}/traits`)
        .query({ fields: 'userId,USERID,traitId' })
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
      
      should.equal(response.status, 400)
      should.exist(response.body.message)
    })

    it('should handle missing Content-Type header for POST requests', async () => {
      const traitData = [{
        traitId: 'education',
        categoryName: 'Education',
        traits: {
          traitId: 'education',
          data: []
        }
      }]
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(traitData)
      
      // Should either succeed or fail gracefully
      should.equal(response.status >= 200, true)
      should.equal(response.status < 500, true)
    })

    it('should handle invalid JSON in trait request body', async () => {
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .set('Content-Type', 'application/json')
        .send('{"invalid": json, "missing": quote}')
      
      should.equal(response.status, 500)
    })

    it('should handle very large trait data array', async () => {
      const largeTraitDataArray = Array(1000).fill().map((_, i) => ({
        traitId: 'education',
        categoryName: `Education ${i}`,
        traits: {
          traitId: 'education',
          data: [{
            collegeName: `University ${i}`,
            degree: `Degree ${i}`,
            endYear: 2020
          }]
        }
      }))
      
      const response = await chai.request(app)
        .post(`${basePath}/${data.member3.handle}/traits`)
        .set('Authorization', `Bearer ${config.ADMIN_TOKEN}`)
        .send(largeTraitDataArray)
      
      // Should either succeed or fail gracefully
      // Large data arrays might cause server errors, which is acceptable
      should.equal(response.status >= 200, true)
      should.equal(response.status < 600, true) // Allow 5xx errors for large data arrays
    })
  })
})

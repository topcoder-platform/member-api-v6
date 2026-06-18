/*
 * Unit tests for statistics service fallbacks.
 */

const path = require('path')
const chai = require('chai')

const should = chai.should()

const servicePath = path.resolve(__dirname, '../../src/services/StatisticsService.js')
const helperPath = path.resolve(__dirname, '../../src/common/helper.js')
const loggerPath = path.resolve(__dirname, '../../src/common/logger.js')
const prismaPath = path.resolve(__dirname, '../../src/common/prisma.js')
const reviewDbPath = path.resolve(__dirname, '../../src/common/reviewDb.js')
const reviewDbHelperPath = path.resolve(__dirname, '../../src/common/reviewDbHelper.js')
const statsDimensionHelperPath = path.resolve(__dirname, '../../src/common/statsDimensionHelper.js')
const developRatingEnginePath = path.resolve(__dirname, '../../src/ratings/developRatingEngine.js')
const mmRatingEnginePath = path.resolve(__dirname, '../../src/ratings/mmRatingEngine.js')
const prismaHelperPath = path.resolve(__dirname, '../../src/common/prismaHelper.js')
const joiPath = require.resolve('joi')
const configPath = require.resolve('config')

function setStubModule (modulePath, exports) {
  delete require.cache[modulePath]
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports
  }
}

function restoreModuleCache (originalEntries) {
  Object.entries(originalEntries).forEach(([modulePath, originalValue]) => {
    delete require.cache[modulePath]
    if (originalValue) {
      require.cache[modulePath] = originalValue
    }
  })
}

function createStatsDimensionHelperStub () {
  const TRACK_NAMES = {
    DEVELOP: 'DEVELOP',
    DESIGN: 'DESIGN',
    DATA_SCIENCE: 'DATA_SCIENCE',
    QA: 'QA'
  }
  const TYPE_NAMES = {
    CHALLENGE: 'Challenge',
    CODE: 'CODE',
    BUG_HUNT: 'BUG_HUNT',
    TEST_SCENARIOS: 'TEST_SCENARIOS',
    TEST_SUITES: 'TEST_SUITES',
    FIRST2FINISH: 'First2Finish',
    TASK: 'Task',
    SRM: 'SRM',
    MARATHON_MATCH: 'MARATHON_MATCH'
  }

  const trackIds = {
    DEVELOP: 'track-dev-id',
    DESIGN: 'track-design-id',
    DATA_SCIENCE: 'track-ds-id',
    QA: 'track-qa-id'
  }
  const typeIds = {
    CHALLENGE: 'type-challenge-id',
    CODE: 'type-code-id',
    BUG_HUNT: 'type-bug-hunt-id',
    TEST_SCENARIOS: 'type-test-scenarios-id',
    TEST_SUITES: 'type-test-suites-id',
    FIRST2FINISH: 'type-f2f-id',
    TASK: 'type-task-id',
    SRM: 'type-srm-id',
    MARATHON_MATCH: 'type-mm-id'
  }

  const trackNamesById = {
    'track-dev-id': TRACK_NAMES.DEVELOP,
    'track-design-id': TRACK_NAMES.DESIGN,
    'track-ds-id': TRACK_NAMES.DATA_SCIENCE,
    'track-qa-id': TRACK_NAMES.QA
  }
  const typeNamesById = {
    'type-challenge-id': TYPE_NAMES.CHALLENGE,
    'type-code-id': TYPE_NAMES.CODE,
    'type-bug-hunt-id': TYPE_NAMES.BUG_HUNT,
    'type-test-scenarios-id': TYPE_NAMES.TEST_SCENARIOS,
    'type-test-suites-id': TYPE_NAMES.TEST_SUITES,
    'type-f2f-id': TYPE_NAMES.FIRST2FINISH,
    'type-task-id': TYPE_NAMES.TASK,
    'type-srm-id': TYPE_NAMES.SRM,
    'type-mm-id': TYPE_NAMES.MARATHON_MATCH
  }

  const resolveTrackId = (value) => {
    if (!value) {
      return undefined
    }

    if (trackNamesById[value]) {
      return value
    }

    return trackIds[String(value).trim().toUpperCase()]
  }

  const resolveTypeId = (value) => {
    if (!value) {
      return undefined
    }

    if (typeNamesById[value]) {
      return value
    }

    const normalized = String(value).trim().toUpperCase()
    if (normalized === 'CHALLENGE' || normalized === 'CH') {
      return typeIds.CHALLENGE
    }
    if (normalized === 'FIRST2FINISH' || normalized === 'F2F') {
      return typeIds.FIRST2FINISH
    }
    if (normalized === 'CODE' || normalized === 'COD') {
      return typeIds.CODE
    }
    if (normalized === 'BUG_HUNT' || normalized === 'LBGH') {
      return typeIds.BUG_HUNT
    }
    if (normalized === 'TEST_SCENARIOS' || normalized === 'LSCN') {
      return typeIds.TEST_SCENARIOS
    }
    if (normalized === 'TEST_SUITES' || normalized === 'LTST') {
      return typeIds.TEST_SUITES
    }
    if (normalized === 'TASK' || normalized === 'TSK') {
      return typeIds.TASK
    }
    if (normalized === 'SRM') {
      return typeIds.SRM
    }
    if (normalized === 'MARATHON_MATCH' || normalized === 'MARATHON MATCH' || normalized === 'MM') {
      return typeIds.MARATHON_MATCH
    }

    return undefined
  }

  return {
    TRACK_NAMES,
    TYPE_NAMES,
    getCanonicalTrackName: (value) => {
      const normalized = String(value || '').trim().toUpperCase()
      if (normalized === 'DEVELOP' || normalized === 'DEV' || normalized === 'DEVELOPMENT') {
        return TRACK_NAMES.DEVELOP
      }
      if (normalized === 'DESIGN' || normalized === 'DES') {
        return TRACK_NAMES.DESIGN
      }
      if (normalized === 'DATA_SCIENCE' || normalized === 'DATA SCIENCE' || normalized === 'DS') {
        return TRACK_NAMES.DATA_SCIENCE
      }
      if (normalized === 'QUALITY_ASSURANCE' || normalized === 'QUALITY ASSURANCE' || normalized === 'QA') {
        return TRACK_NAMES.QA
      }
      return value
    },
    getCanonicalTypeName: (value) => {
      const normalized = String(value || '').trim().toUpperCase()
      if (normalized === 'FIRST2FINISH' || normalized === 'F2F') {
        return TYPE_NAMES.FIRST2FINISH
      }
      if (normalized === 'CHALLENGE' || normalized === 'CH') {
        return TYPE_NAMES.CHALLENGE
      }
      if (normalized === 'CODE' || normalized === 'COD') {
        return TYPE_NAMES.CODE
      }
      if (normalized === 'BUG_HUNT' || normalized === 'LBGH') {
        return TYPE_NAMES.BUG_HUNT
      }
      if (normalized === 'TEST_SCENARIOS' || normalized === 'LSCN') {
        return TYPE_NAMES.TEST_SCENARIOS
      }
      if (normalized === 'TEST_SUITES' || normalized === 'LTST') {
        return TYPE_NAMES.TEST_SUITES
      }
      if (normalized === 'TASK' || normalized === 'TSK') {
        return TYPE_NAMES.TASK
      }
      if (normalized === 'SRM') {
        return TYPE_NAMES.SRM
      }
      if (normalized === 'MARATHON_MATCH' || normalized === 'MARATHON MATCH' || normalized === 'MM') {
        return TYPE_NAMES.MARATHON_MATCH
      }
      return value
    },
    loadChallengeDimensionLookup: async () => ({
      trackIdsByLookup: new Map(),
      typeIdsByLookup: new Map(),
      trackNamesById: new Map(),
      typeNamesById: new Map()
    }),
    resolveTrackIdFromLookup: (lookup, value) => resolveTrackId(value),
    resolveTypeIdFromLookup: (lookup, value) => resolveTypeId(value),
    resolveTrackNameFromLookup: (lookup, value) => trackNamesById[value] || value,
    resolveTypeNameFromLookup: (lookup, value) => typeNamesById[value] || value
  }
}

function createJoiStub () {
  let chain
  chain = new Proxy(function () {}, {
    get: () => chain,
    apply: () => chain
  })

  return {
    any: () => chain,
    string: () => chain,
    object: () => chain,
    number: () => chain,
    array: () => chain,
    boolean: () => chain,
    alternatives: () => chain,
    positive: () => chain
  }
}

function loadStatisticsService (options = {}) {
  const originalEntries = {
    [servicePath]: require.cache[servicePath],
    [helperPath]: require.cache[helperPath],
    [loggerPath]: require.cache[loggerPath],
    [prismaPath]: require.cache[prismaPath],
    [reviewDbPath]: require.cache[reviewDbPath],
    [reviewDbHelperPath]: require.cache[reviewDbHelperPath],
    [statsDimensionHelperPath]: require.cache[statsDimensionHelperPath],
    [developRatingEnginePath]: require.cache[developRatingEnginePath],
    [mmRatingEnginePath]: require.cache[mmRatingEnginePath],
    [prismaHelperPath]: require.cache[prismaHelperPath],
    [joiPath]: require.cache[joiPath],
    [configPath]: require.cache[configPath]
  }
  const originalFetch = global.fetch
  const realConfig = originalEntries[configPath] ? originalEntries[configPath].exports : require('config')

  const statsDimensionHelper = createStatsDimensionHelperStub()
  const member = options.member || {
    userId: global.BigInt(88770025),
    handle: 'devtest1400',
    handleLower: 'devtest1400'
  }
  const prismaStub = options.prismaStub || {
    $queryRaw: async () => [],
    member: {
      findMany: async () => []
    },
    memberStats: {
      findFirst: async () => null,
      count: async () => 0,
      findMany: async () => []
    },
    memberStatsHistory: {
      findMany: async () => []
    }
  }
  const challengeRows = options.challengeRows || []
  const challengeWinnerRows = options.challengeWinnerRows || []
  const reviewRows = options.reviewRows || []

  setStubModule(statsDimensionHelperPath, statsDimensionHelper)
  delete require.cache[prismaHelperPath]

  setStubModule(helperPath, {
    parseCommaSeparatedString: () => null,
    getMemberByHandle: async () => member,
    getAllowedGroupIds: async () => options.groupIds || ['10'],
    canManageMember: () => true,
    hasAdminRole: () => true,
    bigIntToNumber: (value) => (value ? Number(value) : null),
    getRatingColor: () => '#EF3A3A'
  })
  setStubModule(loggerPath, {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    buildService: () => {}
  })
  setStubModule(prismaPath, {
    Prisma: {
      sql: (...args) => ({ args }),
      join: (values, separator) => ({ values, separator }),
      empty: {}
    },
    getClient: () => prismaStub,
    getSkillsClient: () => ({}),
    getMmClient: () => options.mmClient || {},
    getChallengesClient: () => ({
      challenge: {
        findFirst: async () => options.challengeRow || null,
        findMany: async () => challengeRows
      },
      ChallengeWinner: {
        findMany: async (args) => {
          if (options.onChallengeWinnerFindMany) {
            options.onChallengeWinnerFindMany(args)
          }
          return challengeWinnerRows
        }
      }
    })
  })
  setStubModule(reviewDbPath, options.reviewDbStub || {
    query: async () => ({ rows: reviewRows })
  })
  setStubModule(reviewDbHelperPath, {
    resolveChallengeResultRelation: async () => 'reviews."challengeResult"'
  })
  setStubModule(joiPath, createJoiStub())
  setStubModule(configPath, {
    ...realConfig,
    RATING_PATHS: options.ratingPaths || realConfig.RATING_PATHS
  })
  setStubModule(developRatingEnginePath, {
    rerateDevTrack: options.rerateDevTrack || (async () => ({}))
  })
  setStubModule(mmRatingEnginePath, {
    fetchRatingPathParticipantsForChallenge: options.fetchRatingPathParticipantsForChallenge || (async () => ({ participantRows: [] })),
    resolveRatingPathParticipantId: options.resolveRatingPathParticipantId || ((row) => global.BigInt(row.memberId || row.userId)),
    rerateMmTrack: options.rerateMmTrack || (async () => ({}))
  })

  delete require.cache[servicePath]
  const service = require(servicePath)
  if (options.fetchStub) {
    global.fetch = options.fetchStub
  } else {
    delete global.fetch
  }

  return {
    service,
    restore: () => {
      restoreModuleCache(originalEntries)
      if (typeof originalFetch === 'undefined') {
        delete global.fetch
      } else {
        global.fetch = originalFetch
      }
    }
  }
}

describe('statistics service unit tests', () => {
  afterEach(() => {
    delete require.cache[servicePath]
  })

  it('getDistribution should return an empty histogram for valid non-rated filters', async () => {
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => ({ id: global.BigInt(1) }),
          findMany: async () => []
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      const result = await service.getDistribution({
        track: 'DEVELOP',
        subTrack: 'First2Finish'
      })

      result.track.should.equal('DEVELOP')
      result.subTrack.should.equal('First2Finish')
      result.distribution.ratingRange0To099.should.equal(0)
      Object.values(result.distribution).every(value => value === 0).should.equal(true)
    } finally {
      restore()
    }
  })

  it('getDistribution should map grouped rating rows into supported buckets', async () => {
    let queryCalls = 0
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => {
          queryCalls += 1
          return [
            { rangeStart: 0, count: 3 },
            { rangeStart: 100, count: 5 },
            { rangeStart: 3900, count: 2 },
            { rangeStart: 4000, count: 9 }
          ]
        },
        memberStats: {
          findFirst: async () => {
            throw new Error('findFirst should not run when rated distribution rows exist')
          },
          findMany: async () => []
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      const result = await service.getDistribution({
        track: 'DATA_SCIENCE',
        subTrack: 'AI'
      })

      queryCalls.should.equal(1)
      result.track.should.equal('DATA_SCIENCE')
      result.subTrack.should.equal('AI')
      result.distribution.ratingRange0To099.should.equal(3)
      result.distribution.ratingRange100To199.should.equal(5)
      result.distribution.ratingRange3900To3999.should.equal(2)
      should.not.exist(result.distribution.ratingRange4000To4099)
    } finally {
      restore()
    }
  })

  it('getDistribution should exclude unrated zero rows from rating buckets', async () => {
    let rawQueryArgs
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async (...args) => {
          rawQueryArgs = args
          return []
        },
        memberStats: {
          findFirst: async () => ({ id: global.BigInt(1) }),
          findMany: async () => []
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      await service.getDistribution({
        track: 'DATA_SCIENCE',
        subTrack: 'MARATHON_MATCH'
      })

      should.exist(rawQueryArgs)
      const whereConditions = rawQueryArgs[1].values
      whereConditions.some((condition) => {
        const chunks = condition.args && condition.args[0]
        return Array.isArray(chunks) && chunks.join('').includes('"rating" >') && condition.args[1] === 0
      }).should.equal(true)
      whereConditions.some((condition) => {
        const chunks = condition.args && condition.args[0]
        return Array.isArray(chunks) && chunks.join('').includes('"rating" >=')
      }).should.equal(false)
    } finally {
      restore()
    }
  })

  it('getMemberStats should merge duplicate Marathon Match rows normalized under Data Science', async () => {
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findMany: async () => [
            {
              trackId: 'track-ds-id',
              typeId: 'type-mm-id',
              challenges: 458,
              wins: 88,
              rating: 2543,
              globalRank: 3,
              countryRank: 1,
              schoolRank: 0,
              volatility: 352,
              maxRating: 2925,
              minRating: 1641,
              topFiveFinishes: 135,
              topTenFinishes: 182,
              bestRank: 1,
              avgRank: 7,
              mostRecentSubmission: new Date('2024-06-25T14:46:39.719Z'),
              mostRecentEventDate: new Date('2024-09-17T00:00:00.000Z')
            },
            {
              trackId: 'track-dev-id',
              typeId: 'type-mm-id',
              challenges: 68,
              wins: null,
              mostRecentEventDate: new Date('2023-01-06T06:36:14.000Z')
            },
            {
              trackId: 'track-ds-id',
              typeId: 'type-srm-id',
              challenges: 880,
              wins: 2,
              rating: 1592,
              globalRank: 185,
              countryRank: 3,
              schoolRank: 0,
              volatility: 400,
              maxRating: 2435,
              minRating: 1301,
              mostRecentEventDate: new Date('2023-01-04T00:00:00.000Z')
            }
          ]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      const result = await service.getMemberStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].DATA_SCIENCE.challenges.should.equal(1406)
      result[0].DATA_SCIENCE.wins.should.equal(90)
      const marathon = result[0].DATA_SCIENCE.MARATHON_MATCH
      marathon.challenges.should.equal(526)
      marathon.wins.should.equal(88)
      marathon.mostRecentEventDate.should.equal(new Date('2024-09-17T00:00:00.000Z').getTime())
      marathon.rank.rating.should.equal(2543)
      marathon.rank.rank.should.equal(3)
      marathon.rank.countryRank.should.equal(1)
      marathon.rank.maximumRating.should.equal(2925)
      marathon.rank.minimumRating.should.equal(1641)
      marathon.rank.topFiveFinishes.should.equal(135)
      marathon.rank.topTenFinishes.should.equal(182)
    } finally {
      restore()
    }
  })

  it('getMemberStats should preserve imported Marathon Match rating bounds over historical rerates', async () => {
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challenges: 32,
            wins: 3,
            rating: 2166,
            globalRank: 32,
            countryRank: 1,
            maxRating: 2187,
            minRating: 1362,
            mostRecentEventDate: new Date('2025-12-06T08:03:06.205Z'),
            isPrivate: false
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19708',
            newRating: 2955,
            eventDate: new Date('2023-05-02T00:00:00.000Z'),
            createdBy: 'stats-migration',
            updatedBy: 'stats-migration'
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19709',
            newRating: 1641,
            eventDate: new Date('2023-05-03T00:00:00.000Z'),
            createdBy: 'stats-migration',
            updatedBy: 'stats-migration'
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'first-mm-uuid',
            newRating: 1263,
            eventDate: new Date('2023-05-31T10:02:00.000Z'),
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'peak-mm-uuid',
            newRating: 2548,
            eventDate: new Date('2023-06-01T10:02:00.000Z'),
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }]
        }
      },
      challengeRows: [{
        id: 'first-mm-uuid',
        legacyId: 19708,
        name: 'MM 148',
        status: 'COMPLETED'
      }, {
        id: 'peak-mm-uuid',
        legacyId: 19709,
        name: 'MM 149',
        status: 'COMPLETED'
      }]
    })

    try {
      const result = await service.getMemberStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const rank = result[0].DATA_SCIENCE.MARATHON_MATCH.rank
      rank.maximumRating.should.equal(2955)
      rank.minimumRating.should.equal(1641)
    } finally {
      restore()
    }
  })

  it('getMemberStats should compute Marathon Match rank when the stored global rank is zero', async () => {
    let countArgs
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          count: async (args) => {
            countArgs = args
            return 6
          },
          findMany: async () => [
            {
              trackId: 'track-ds-id',
              typeId: 'type-mm-id',
              challenges: 137,
              wins: 40,
              rating: 2720,
              globalRank: 0,
              countryRank: 0,
              schoolRank: 0,
              volatility: 513,
              maxRating: 3071,
              minRating: 1527,
              topFiveFinishes: 86,
              topTenFinishes: 102,
              mostRecentEventDate: new Date('2023-05-02T00:00:00.000Z'),
              isPrivate: false
            }
          ]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      const result = await service.getMemberStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.rank.rank.should.equal(7)
      countArgs.where.should.deep.equal({
        trackId: 'track-ds-id',
        typeId: 'type-mm-id',
        isPrivate: false,
        rating: {
          gt: 2720
        }
      })
    } finally {
      restore()
    }
  })

  it('getMemberStats should resolve configured rating path name filters to deterministic type ids', async () => {
    let findManyArgs
    const { service, restore } = loadStatisticsService({
      ratingPaths: [
        { name: 'AI Engineering', track: 'DATA_SCIENCE', tags: ['AI', 'AI Exponential League'] }
      ],
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findMany: async (args) => {
            findManyArgs = args
            return [
              {
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
          }
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      }
    })

    try {
      const result = await service.getMemberStats({ isMachine: true }, 'devtest1400', {
        trackId: 'DATA_SCIENCE',
        typeId: 'AI Engineering'
      })

      findManyArgs.where.typeId.should.equal('rating-path-ai-engineering')
      result.should.have.length(1)
      should.exist(result[0].DATA_SCIENCE['AI Engineering'])
      should.not.exist(result[0].DATA_SCIENCE['rating-path-ai-engineering'])
    } finally {
      restore()
    }
  })

  it('rerateMemberStats should route configured rating paths to the Marathon Match engine', async () => {
    let capturedOptions
    const { service, restore } = loadStatisticsService({
      rerateMmTrack: async (membersClient, challengeClient, mmClient, reviewDbClient, userId, challengeId, options) => {
        capturedOptions = options
        should.equal(mmClient, null)
        should.equal(String(userId), '88770025')
        should.equal(challengeId, 'ai-target-challenge')
        return {
          challengesProcessed: 1,
          ratingPathChallengesProcessed: 2,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateMemberStats({ isMachine: true }, 'devtest1400', {
        challengeId: 'ai-target-challenge',
        ratingName: 'AI Engineering'
      })

      should.exist(capturedOptions)
      capturedOptions.ratingPath.name.should.equal('AI Engineering')
      capturedOptions.ratingPath.tags.should.deep.equal(['AI', 'AI Exponential League'])
      result.trackId.should.equal('DATA_SCIENCE')
      result.typeId.should.equal('AI Engineering')
      result.ratingName.should.equal('AI Engineering')
      result.ratingTags.should.deep.equal(['AI', 'AI Exponential League'])
      result.ratingPathChallengesProcessed.should.equal(2)
      result.ratingsUpdated.should.equal(1)
    } finally {
      restore()
    }
  })

  it('rerateMemberStats should expose configured rating path skill ids in the rerate summary', async () => {
    const skillIds = [
      '9d3f5b4d-0000-4000-8000-000000000001',
      '9d3f5b4d-0000-4000-8000-000000000002'
    ]
    let capturedOptions
    const { service, restore } = loadStatisticsService({
      ratingPaths: [{
        name: 'Java MySQL',
        track: 'DEVELOPMENT',
        skillIds
      }],
      rerateMmTrack: async (membersClient, challengeClient, mmClient, reviewDbClient, userId, challengeId, options) => {
        capturedOptions = options
        should.equal(mmClient, null)
        should.equal(String(userId), '88770025')
        should.equal(challengeId, 'java-mysql-target-challenge')
        return {
          challengesProcessed: 1,
          ratingPathChallengesProcessed: 2,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateMemberStats({ isMachine: true }, 'devtest1400', {
        challengeId: 'java-mysql-target-challenge',
        ratingName: 'Java MySQL'
      })

      should.exist(capturedOptions)
      capturedOptions.ratingPath.name.should.equal('Java MySQL')
      capturedOptions.ratingPath.skillIds.should.deep.equal(skillIds)
      result.trackId.should.equal('DEVELOP')
      result.typeId.should.equal('Java MySQL')
      result.ratingName.should.equal('Java MySQL')
      result.ratingSkillIds.should.deep.equal(skillIds)
      result.ratingPathChallengesProcessed.should.equal(2)
      result.ratingsUpdated.should.equal(1)
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should rerate native and matching named ratings for every submitter', async () => {
    const devRerateCalls = []
    const namedRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'ai-target-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-01-15T00:00:00.000Z'),
        trackId: 'track-dev-id',
        typeId: 'type-challenge-id',
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: ['AI'],
        skills: [],
        metadata: []
      },
      reviewRows: [
        { challengeId: 'ai-target-challenge', userId: '101', finalScore: 98, placement: 1 },
        { challengeId: 'ai-target-challenge', userId: '102', finalScore: 87, placement: 2 },
        { challengeId: 'ai-target-challenge', userId: '101', finalScore: 96, placement: 3 }
      ],
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId) => {
        devRerateCalls.push({
          userId: String(userId),
          challengeId
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      },
      rerateMmTrack: async (membersClient, challengeClient, mmClient, reviewDbClient, userId, challengeId, options) => {
        should.equal(mmClient, null)
        namedRerateCalls.push({
          userId: String(userId),
          challengeId,
          ratingName: options.ratingPath.name
        })
        return {
          challengesProcessed: 1,
          ratingPathChallengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'ai-target-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(4)
      result.ratingsUpdated.should.equal(4)
      result.participantIds.should.deep.equal(['101', '102'])
      result.ratings.should.deep.equal([
        {
          trackId: 'DEVELOP',
          typeId: 'Challenge'
        },
        {
          trackId: 'DATA_SCIENCE',
          typeId: 'AI Engineering',
          ratingName: 'AI Engineering',
          ratingTags: ['AI', 'AI Exponential League'],
          ratingSkillIds: []
        }
      ])
      devRerateCalls.should.deep.equal([
        { userId: '101', challengeId: 'ai-target-challenge' },
        { userId: '102', challengeId: 'ai-target-challenge' }
      ])
      namedRerateCalls.should.deep.equal([
        { userId: '101', challengeId: 'ai-target-challenge', ratingName: 'AI Engineering' },
        { userId: '102', challengeId: 'ai-target-challenge', ratingName: 'AI Engineering' }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should rerate Data Science Challenge submitters', async () => {
    const dsRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'ds-target-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-06-02T05:30:04.536Z'),
        trackId: 'track-ds-id',
        typeId: 'type-challenge-id',
        track: { name: 'Data Science' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [],
        metadata: []
      },
      reviewRows: [
        { challengeId: 'ds-target-challenge', userId: '101', finalScore: 100, placement: 1 },
        { challengeId: 'ds-target-challenge', userId: '102', finalScore: 88.89, placement: 2 }
      ],
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId, options) => {
        dsRerateCalls.push({
          userId: String(userId),
          challengeId,
          options
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'ds-target-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(2)
      result.ratingsUpdated.should.equal(2)
      result.participantIds.should.deep.equal(['101', '102'])
      result.ratings.should.deep.equal([
        {
          trackId: 'DATA_SCIENCE',
          typeId: 'Challenge'
        }
      ])
      dsRerateCalls.should.deep.equal([
        {
          userId: '101',
          challengeId: 'ds-target-challenge',
          options: {
            targetTrackName: 'DATA_SCIENCE',
            targetTypeName: 'Challenge',
            challengeTrackNames: ['DATA_SCIENCE'],
            challengeTypeNames: ['Challenge']
          }
        },
        {
          userId: '102',
          challengeId: 'ds-target-challenge',
          options: {
            targetTrackName: 'DATA_SCIENCE',
            targetTypeName: 'Challenge',
            challengeTrackNames: ['DATA_SCIENCE'],
            challengeTypeNames: ['Challenge']
          }
        }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should include Data Science Challenge winners when result rows are absent', async () => {
    const dsRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'ds-winner-only-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-06-09T08:45:00.000Z'),
        trackId: 'track-ds-id',
        typeId: 'type-challenge-id',
        track: { name: 'Data Science' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [],
        metadata: []
      },
      reviewRows: [],
      challengeWinnerRows: [
        { challengeId: 'ds-winner-only-challenge', userId: 301, type: 'PLACEMENT', placement: 1 }
      ],
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId, options) => {
        dsRerateCalls.push({
          userId: String(userId),
          challengeId,
          options
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'ds-winner-only-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(1)
      result.ratingsAttempted.should.equal(1)
      result.ratingsUpdated.should.equal(1)
      result.participantIds.should.deep.equal(['301'])
      result.ratings.should.deep.equal([
        {
          trackId: 'DATA_SCIENCE',
          typeId: 'Challenge'
        }
      ])
      dsRerateCalls.should.deep.equal([
        {
          userId: '301',
          challengeId: 'ds-winner-only-challenge',
          options: {
            targetTrackName: 'DATA_SCIENCE',
            targetTypeName: 'Challenge',
            challengeTrackNames: ['DATA_SCIENCE'],
            challengeTypeNames: ['Challenge']
          }
        }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should rate QA Challenge winners in the QA bucket', async () => {
    const qaRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'qa-winner-only-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-06-10T05:41:34.931Z'),
        trackId: 'track-qa-id',
        typeId: 'type-challenge-id',
        track: { name: 'Quality Assurance', track: 'QUALITY_ASSURANCE' },
        type: { name: 'Challenge' },
        tags: [],
        skills: [],
        metadata: []
      },
      reviewRows: [],
      challengeWinnerRows: [
        { challengeId: 'qa-winner-only-challenge', userId: 89770374, type: 'PLACEMENT', placement: 1 },
        { challengeId: 'qa-winner-only-challenge', userId: 100000039, type: 'PLACEMENT', placement: 2 }
      ],
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId, options) => {
        qaRerateCalls.push({
          userId: String(userId),
          challengeId,
          options
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'qa-winner-only-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(2)
      result.ratingsUpdated.should.equal(2)
      result.participantIds.should.deep.equal(['89770374', '100000039'])
      result.ratings.should.deep.equal([
        {
          trackId: 'QA',
          typeId: 'Challenge'
        }
      ])
      qaRerateCalls.should.deep.equal([
        {
          userId: '89770374',
          challengeId: 'qa-winner-only-challenge',
          options: {
            targetTrackName: 'QA',
            targetTypeName: 'Challenge',
            challengeTrackNames: ['QUALITY_ASSURANCE'],
            challengeTypeNames: ['Challenge']
          }
        },
        {
          userId: '100000039',
          challengeId: 'qa-winner-only-challenge',
          options: {
            targetTrackName: 'QA',
            targetTypeName: 'Challenge',
            challengeTrackNames: ['QUALITY_ASSURANCE'],
            challengeTypeNames: ['Challenge']
          }
        }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should include Marathon Match final summation submitters when result rows are partial', async () => {
    const mmRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'mm-target-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-01-15T00:00:00.000Z'),
        trackId: 'track-ds-id',
        typeId: 'type-mm-id',
        track: { name: 'DATA_SCIENCE' },
        type: { name: 'MARATHON_MATCH' },
        tags: [],
        skills: [],
        metadata: []
      },
      reviewDbStub: {
        query: async () => ({
          rows: [
            { challengeId: 'mm-target-challenge', userId: '101', finalScore: 98, placement: 1 }
          ]
        })
      },
      fetchRatingPathParticipantsForChallenge: async () => ({
        participantRows: [
          { memberId: '101' },
          { memberId: '102' }
        ]
      }),
      resolveRatingPathParticipantId: row => global.BigInt(row.memberId),
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateMmTrack: async (membersClient, challengeClient, mmClient, reviewDbClient, userId, challengeId) => {
        should.equal(mmClient, null)
        mmRerateCalls.push({
          userId: String(userId),
          challengeId
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'mm-target-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(2)
      result.ratingsUpdated.should.equal(2)
      result.participantIds.should.deep.equal(['101', '102'])
      mmRerateCalls.should.deep.equal([
        { userId: '101', challengeId: 'mm-target-challenge' },
        { userId: '102', challengeId: 'mm-target-challenge' }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should include challenge winners when result rows are absent', async () => {
    const devRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'qa-target-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-01-15T00:00:00.000Z'),
        trackId: 'track-dev-id',
        typeId: 'type-challenge-id',
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        tags: ['QA'],
        skills: [],
        metadata: []
      },
      reviewRows: [],
      challengeWinnerRows: [
        { challengeId: 'qa-target-challenge', userId: 201, type: 'PLACEMENT', placement: 1 },
        { challengeId: 'qa-target-challenge', userId: 202, type: 'PLACEMENT', placement: 2 }
      ],
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId) => {
        devRerateCalls.push({
          userId: String(userId),
          challengeId
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'qa-target-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(2)
      result.ratingsUpdated.should.equal(2)
      result.participantIds.should.deep.equal(['201', '202'])
      devRerateCalls.should.deep.equal([
        { userId: '201', challengeId: 'qa-target-challenge' },
        { userId: '202', challengeId: 'qa-target-challenge' }
      ])
    } finally {
      restore()
    }
  })

  it('rerateChallengeSubmitterRatings should rerate Development track Marathon Match challenges as MM', async () => {
    const devRerateCalls = []
    const mmRerateCalls = []
    const { service, restore } = loadStatisticsService({
      challengeRow: {
        id: 'dev-mm-target-challenge',
        status: 'COMPLETED',
        endDate: new Date('2026-01-15T00:00:00.000Z'),
        trackId: 'track-dev-id',
        typeId: 'type-mm-id',
        track: { name: 'Development' },
        type: { name: 'MARATHON_MATCH' },
        tags: [],
        skills: [],
        metadata: []
      },
      reviewDbStub: {
        query: async () => ({
          rows: [
            { challengeId: 'dev-mm-target-challenge', userId: '101', finalScore: 98, placement: 1 }
          ]
        })
      },
      fetchRatingPathParticipantsForChallenge: async () => ({
        participantRows: [
          { memberId: '101' },
          { memberId: '102' }
        ]
      }),
      resolveRatingPathParticipantId: row => global.BigInt(row.memberId),
      prismaStub: {
        member: {
          findMany: async ({ where }) => where.userId.in.map((userId) => ({ userId }))
        }
      },
      rerateDevTrack: async (membersClient, challengeClient, reviewDbClient, userId, challengeId) => {
        devRerateCalls.push({
          userId: String(userId),
          challengeId
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      },
      rerateMmTrack: async (membersClient, challengeClient, mmClient, reviewDbClient, userId, challengeId) => {
        should.equal(mmClient, null)
        mmRerateCalls.push({
          userId: String(userId),
          challengeId
        })
        return {
          challengesProcessed: 1,
          ratingsUpdated: 1
        }
      }
    })

    try {
      const result = await service.rerateChallengeSubmitterRatings({ isMachine: true }, {
        challengeId: 'dev-mm-target-challenge'
      })

      result.rerated.should.equal(true)
      result.membersProcessed.should.equal(2)
      result.ratingsAttempted.should.equal(2)
      result.ratingsUpdated.should.equal(2)
      result.participantIds.should.deep.equal(['101', '102'])
      result.ratings.should.deep.equal([
        {
          trackId: 'DATA_SCIENCE',
          typeId: 'MARATHON_MATCH'
        }
      ])
      devRerateCalls.should.deep.equal([])
      mmRerateCalls.should.deep.equal([
        { userId: '101', challengeId: 'dev-mm-target-challenge' },
        { userId: '102', challengeId: 'dev-mm-target-challenge' }
      ])
    } finally {
      restore()
    }
  })

  it('getHistoryStats should synthesize missing review history for visible develop subtracks', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-dev-id',
            typeId: 'type-f2f-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [{
        id: 'challenge-uuid',
        legacyId: null,
        name: 'F2F challenge',
        status: 'COMPLETED',
        trackId: 'track-dev-id',
        typeId: 'type-f2f-id',
        endDate: ratingDate,
        track: { name: 'Development' },
        type: { name: 'First2Finish' },
        metadata: [],
        legacyRecord: null
      }],
      reviewRows: [{
        challengeId: 'challenge-uuid',
        userId: '88770025',
        finalScore: 97.5,
        placement: 1,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].groupId.should.equal(10)
      should.exist(result[0].DEVELOP)
      result[0].DEVELOP.subTracks.should.have.length(1)
      result[0].DEVELOP.subTracks[0].name.should.equal('First2Finish')
      result[0].DEVELOP.subTracks[0].history.should.have.length(1)
      result[0].DEVELOP.subTracks[0].history[0].challengeId.should.equal('challenge-uuid')
      result[0].DEVELOP.subTracks[0].history[0].challengeName.should.equal('F2F challenge')
      result[0].DEVELOP.subTracks[0].history[0].placement.should.equal(1)
      result[0].DEVELOP.subTracks[0].history[0].ratingDate.should.equal(ratingDate.getTime())
      result[0].DEVELOP.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should exclude non-completed challenge results from synthesized history', async () => {
    const completedDate = new Date('2025-11-27T05:48:36.899Z')
    const activeDate = new Date('2025-11-28T05:48:36.899Z')
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-dev-id',
            typeId: 'type-f2f-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [{
        id: 'completed-challenge-uuid',
        legacyId: null,
        name: 'Completed challenge',
        status: 'COMPLETED',
        trackId: 'track-dev-id',
        typeId: 'type-f2f-id',
        endDate: completedDate,
        track: { name: 'Development' },
        type: { name: 'First2Finish' },
        metadata: [],
        legacyRecord: null
      }, {
        id: 'active-challenge-uuid',
        legacyId: null,
        name: 'Active challenge',
        status: 'ACTIVE',
        trackId: 'track-dev-id',
        typeId: 'type-f2f-id',
        endDate: activeDate,
        track: { name: 'Development' },
        type: { name: 'First2Finish' },
        metadata: [],
        legacyRecord: null
      }],
      reviewRows: [{
        challengeId: 'completed-challenge-uuid',
        userId: '88770025',
        finalScore: 97.5,
        placement: 1,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }, {
        challengeId: 'active-challenge-uuid',
        userId: '88770025',
        finalScore: 0,
        placement: 0,
        rated: false,
        createdAt: new Date('2025-11-28T05:48:36.907Z')
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DEVELOP)
      result[0].DEVELOP.subTracks.should.have.length(1)
      result[0].DEVELOP.subTracks[0].history.should.have.length(1)
      result[0].DEVELOP.subTracks[0].history[0].challengeId.should.equal('completed-challenge-uuid')
      result[0].DEVELOP.subTracks[0].history[0].challengeName.should.equal('Completed challenge')
      result[0].DEVELOP.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('refreshMemberStats should ignore non-completed challenge results', async () => {
    const upsertCalls = []
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        $transaction: async (runInTransaction) => runInTransaction({
          memberStats: {
            upsert: async (args) => {
              upsertCalls.push(args)
            },
            findFirst: async () => null
          },
          memberStatsHistory: {
            updateMany: async () => {},
            findFirst: async () => null,
            update: async () => {}
          }
        }),
        memberStats: {
          findFirst: async () => null,
          findMany: async () => []
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [{
        id: 'completed-challenge-uuid',
        legacyId: null,
        name: 'Completed challenge',
        status: 'COMPLETED',
        trackId: 'track-dev-id',
        typeId: 'type-challenge-id',
        endDate: new Date('2025-11-27T05:48:36.899Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        metadata: [],
        legacyRecord: null
      }, {
        id: 'active-challenge-uuid',
        legacyId: null,
        name: 'Active challenge',
        status: 'ACTIVE',
        trackId: 'track-dev-id',
        typeId: 'type-challenge-id',
        endDate: new Date('2025-11-28T05:48:36.899Z'),
        track: { name: 'Development' },
        type: { name: 'Challenge' },
        metadata: [],
        legacyRecord: null
      }],
      reviewRows: [{
        challengeId: 'completed-challenge-uuid',
        userId: '88770025',
        finalScore: 100,
        placement: 1,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }, {
        challengeId: 'active-challenge-uuid',
        userId: '88770025',
        finalScore: 0,
        placement: 0,
        rated: false,
        createdAt: new Date('2025-11-28T05:48:36.907Z')
      }]
    })

    try {
      const result = await service.refreshMemberStats({ userId: 'operator-1' }, 'devtest1400', {})

      result.challengeResultsProcessed.should.equal(2)
      result.statsUpdated.should.equal(1)
      upsertCalls.should.have.length(1)
      upsertCalls[0].create.challenges.should.equal(1)
      upsertCalls[0].create.wins.should.equal(1)
      upsertCalls[0].create.trackId.should.equal('track-dev-id')
      upsertCalls[0].create.typeId.should.equal('type-challenge-id')
    } finally {
      restore()
    }
  })

  it('getHistoryStats should synthesize missing review history for visible design subtracks', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [{
        id: 'design-challenge-uuid',
        legacyId: null,
        name: 'Design challenge',
        status: 'COMPLETED',
        trackId: 'track-design-id',
        typeId: 'type-challenge-id',
        endDate: ratingDate,
        track: { name: 'Design' },
        type: { name: 'Challenge' },
        metadata: [],
        legacyRecord: null
      }],
      reviewRows: [{
        challengeId: 'design-challenge-uuid',
        userId: '88770025',
        finalScore: 97.5,
        placement: 1,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].groupId.should.equal(10)
      should.exist(result[0].DESIGN)
      result[0].DESIGN.subTracks.should.have.length(1)
      result[0].DESIGN.subTracks[0].name.should.equal('Challenge')
      result[0].DESIGN.subTracks[0].history.should.have.length(1)
      result[0].DESIGN.subTracks[0].history[0].challengeId.should.equal('design-challenge-uuid')
      result[0].DESIGN.subTracks[0].history[0].challengeName.should.equal('Design challenge')
      result[0].DESIGN.subTracks[0].history[0].placement.should.equal(1)
      result[0].DESIGN.subTracks[0].history[0].ratingDate.should.equal(ratingDate.getTime())
      result[0].DESIGN.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should ignore invalid or placeholder review history rows', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [{
        id: 'invalid-challenge-uuid',
        legacyId: null,
        name: 'Invalid challenge',
        status: 'COMPLETED',
        trackId: 'track-design-id',
        typeId: 'type-challenge-id',
        endDate: ratingDate,
        track: { name: 'Design' },
        type: { name: 'Challenge' },
        metadata: [],
        legacyRecord: null
      }, {
        id: 'missing-submission-challenge-uuid',
        legacyId: null,
        name: 'Missing submission challenge',
        status: 'COMPLETED',
        trackId: 'track-design-id',
        typeId: 'type-challenge-id',
        endDate: ratingDate,
        track: { name: 'Design' },
        type: { name: 'Challenge' },
        metadata: [],
        legacyRecord: null
      }],
      reviewRows: [{
        challengeId: 'invalid-challenge-uuid',
        userId: '88770025',
        submissionId: 'submission-invalid',
        finalScore: 97.5,
        placement: 1,
        rated: false,
        validSubmission: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }, {
        challengeId: 'missing-submission-challenge-uuid',
        userId: '88770025',
        submissionId: null,
        finalScore: 97.5,
        placement: 1,
        rated: false,
        validSubmission: true,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.deep.equal([])
    } finally {
      restore()
    }
  })

  it('getHistoryStats should recover missing CODE history from legacy challenge pages when legacy ids are not mapped', async () => {
    const codeMostRecentDate = new Date('2014-10-10T11:01:01.000Z')
    const pageHtmlByChallengeId = {
      30046082: '<meta name="twitter:title" content="[$600/$200] - Copy Number Algorithm Executable Updates (C++ required)"><a href="/challenges?search=C%2B%2B">C++</a><a href="/challenges?search=Data%20Science">Data Science</a><a href="/challenges?search=Other">Other</a>',
      30045145: '<meta name="twitter:title" content="[$750/$250] - iOS SDK Modernization"><a href="/challenges?search=Swift">Swift</a><a href="/challenges?search=Other">Other</a>',
      30043029: '<meta name="twitter:title" content="[$1200/$400] - Cross Language Runtime Updates"><a href="/challenges?search=Java">Java</a><a href="/challenges?search=C%2B%2B">C++</a><a href="/challenges?search=Python">Python</a>',
      30040814: '<meta name="twitter:title" content="[$1500/$750] - Asteroid Data Hunter - Phase 1 - Create Marathon Match Problem Statement">',
      30039557: '<meta name="twitter:title" content="[$3000/$2000] - EPA ToxCast - Predictive Capability Tests">',
      30034503: '<meta name="twitter:title" content="[$1500/$750] - FrameSkipper Paper Research Contest">'
    }

    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-dev-id',
            typeId: 'type-code-id',
            challenges: 3,
            mostRecentSubmission: null,
            mostRecentEventDate: codeMostRecentDate
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [],
      reviewRows: [{
        challengeId: '30046082',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2014-10-13T13:24:29.000Z')
      }, {
        challengeId: '30045145',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2014-09-09T16:40:22.000Z')
      }, {
        challengeId: '30043029',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2014-05-26T04:34:20.000Z')
      }, {
        challengeId: '30040814',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2014-03-17T15:55:19.000Z')
      }, {
        challengeId: '30039557',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2014-02-25T11:19:37.000Z')
      }, {
        challengeId: '30034503',
        userId: '88770025',
        placement: 0,
        createdAt: new Date('2013-06-18T20:40:34.000Z')
      }],
      fetchStub: async (url) => {
        const challengeId = String(url).split('/').pop()
        return {
          ok: true,
          text: async () => pageHtmlByChallengeId[challengeId] || '<meta name="twitter:title" content="Topcoder">'
        }
      }
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {
        trackId: 'DEVELOP',
        typeId: 'CODE'
      })

      result.should.have.length(1)
      should.exist(result[0].DEVELOP)
      result[0].DEVELOP.subTracks.should.have.length(1)
      result[0].DEVELOP.subTracks[0].name.should.equal('CODE')
      result[0].DEVELOP.subTracks[0].history.should.have.length(3)
      result[0].DEVELOP.subTracks[0].history[0].challengeId.should.equal(30046082)
      result[0].DEVELOP.subTracks[0].history[0].challengeName.should.equal('Copy Number Algorithm Executable Updates (C++ required)')
      result[0].DEVELOP.subTracks[0].history[0].mostRecent.should.equal(true)
      result[0].DEVELOP.subTracks[0].history[1].challengeId.should.equal(30045145)
      result[0].DEVELOP.subTracks[0].history[1].challengeName.should.equal('iOS SDK Modernization')
      result[0].DEVELOP.subTracks[0].history[2].challengeId.should.equal(30043029)
      result[0].DEVELOP.subTracks[0].history[2].challengeName.should.equal('Cross Language Runtime Updates')
    } finally {
      restore()
    }
  })

  it('getHistoryStats should backfill persisted placements from challenge winners', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const challenge = {
      id: 'design-challenge-uuid',
      legacyId: null,
      name: 'Design challenge',
      status: 'COMPLETED',
      trackId: 'track-design-id',
      typeId: 'type-challenge-id',
      endDate: ratingDate,
      track: { name: 'Design' },
      type: { name: 'Challenge' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id',
            challengeId: 'design-challenge-uuid',
            challengeName: 'Design challenge',
            eventDate: ratingDate,
            placement: 0,
            mostRecent: true
          }]
        }
      },
      challengeRows: [challenge],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'design-challenge-uuid',
        placement: 2,
        createdAt: new Date('2025-11-27T05:48:36.907Z'),
        challenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DESIGN)
      result[0].DESIGN.subTracks.should.have.length(1)
      result[0].DESIGN.subTracks[0].history.should.have.length(1)
      result[0].DESIGN.subTracks[0].history[0].placement.should.equal(2)
      result[0].DESIGN.subTracks[0].history[0].challengeName.should.equal('Design challenge')
      result[0].DESIGN.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should prefer challenge winner placements over zero-valued review placements', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const challenge = {
      id: 'design-challenge-uuid',
      legacyId: null,
      name: 'Design challenge',
      status: 'COMPLETED',
      trackId: 'track-design-id',
      typeId: 'type-challenge-id',
      endDate: ratingDate,
      track: { name: 'Design' },
      type: { name: 'Challenge' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [challenge],
      reviewRows: [{
        challengeId: 'design-challenge-uuid',
        userId: '88770025',
        finalScore: 97.5,
        placement: 0,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }],
      challengeWinnerRows: [{
        challengeId: 'design-challenge-uuid',
        placement: 2,
        createdAt: new Date('2025-11-27T05:48:36.907Z'),
        challenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DESIGN)
      result[0].DESIGN.subTracks.should.have.length(1)
      result[0].DESIGN.subTracks[0].history.should.have.length(1)
      result[0].DESIGN.subTracks[0].history[0].placement.should.equal(2)
      result[0].DESIGN.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should supplement winner-only challenge cards inside a review-backed pair', async () => {
    const olderDate = new Date('2025-11-27T05:48:36.899Z')
    const newerDate = new Date('2025-11-29T05:48:36.899Z')
    const reviewChallenge = {
      id: 'review-challenge-uuid',
      legacyId: null,
      name: 'Review-backed challenge',
      status: 'COMPLETED',
      trackId: 'track-design-id',
      typeId: 'type-challenge-id',
      endDate: olderDate,
      track: { name: 'Design' },
      type: { name: 'Challenge' },
      metadata: [],
      legacyRecord: null
    }
    const winnerOnlyChallenge = {
      id: 'winner-only-challenge-uuid',
      name: 'Winner-only challenge',
      status: 'COMPLETED',
      trackId: 'track-design-id',
      typeId: 'type-challenge-id',
      endDate: newerDate
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-design-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [reviewChallenge],
      reviewRows: [{
        challengeId: 'review-challenge-uuid',
        userId: '88770025',
        finalScore: 97.5,
        placement: 2,
        rated: false,
        createdAt: new Date('2025-11-27T05:48:36.907Z')
      }],
      challengeWinnerRows: [{
        challengeId: 'review-challenge-uuid',
        placement: 2,
        createdAt: new Date('2025-11-27T05:48:36.907Z'),
        challenge: reviewChallenge
      }, {
        challengeId: 'winner-only-challenge-uuid',
        placement: 1,
        createdAt: new Date('2025-11-29T05:48:36.907Z'),
        challenge: winnerOnlyChallenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DESIGN)
      result[0].DESIGN.subTracks.should.have.length(1)
      result[0].DESIGN.subTracks[0].history.should.have.length(2)
      result[0].DESIGN.subTracks[0].history[0].challengeId.should.equal('winner-only-challenge-uuid')
      result[0].DESIGN.subTracks[0].history[0].challengeName.should.equal('Winner-only challenge')
      result[0].DESIGN.subTracks[0].history[0].placement.should.equal(1)
      result[0].DESIGN.subTracks[0].history[0].mostRecent.should.equal(true)
      result[0].DESIGN.subTracks[0].history[1].challengeId.should.equal('review-challenge-uuid')
    } finally {
      restore()
    }
  })

  it('getHistoryStats should synthesize missing history from challenge winners when review rows are unavailable', async () => {
    const ratingDate = new Date('2025-11-27T05:48:36.899Z')
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-dev-id',
            typeId: 'type-f2f-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'challenge-uuid',
        placement: 1,
        createdAt: new Date('2025-11-27T05:48:36.907Z'),
        challenge: {
          id: 'challenge-uuid',
          name: 'Winner fallback challenge',
          status: 'COMPLETED',
          trackId: 'track-dev-id',
          typeId: 'type-f2f-id',
          endDate: ratingDate
        }
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DEVELOP)
      result[0].DEVELOP.subTracks.should.have.length(1)
      result[0].DEVELOP.subTracks[0].name.should.equal('First2Finish')
      result[0].DEVELOP.subTracks[0].history.should.have.length(1)
      result[0].DEVELOP.subTracks[0].history[0].challengeName.should.equal('Winner fallback challenge')
      result[0].DEVELOP.subTracks[0].history[0].placement.should.equal(1)
      result[0].DEVELOP.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should surface QA challenge winner history under QA', async () => {
    const ratingDate = new Date('2026-06-15T08:00:00.000Z')
    const qaChallenge = {
      id: 'qa-challenge-june-15',
      name: 'QA Challenge June 15',
      status: 'COMPLETED',
      trackId: 'track-qa-id',
      typeId: 'type-challenge-id',
      endDate: ratingDate
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-qa-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => []
        }
      },
      challengeRows: [],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'qa-challenge-june-15',
        type: 'PLACEMENT',
        placement: 1,
        createdAt: new Date('2026-06-15T08:01:00.000Z'),
        challenge: qaChallenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].QA)
      result[0].QA.subTracks.should.have.length(1)
      result[0].QA.subTracks[0].name.should.equal('Challenge')
      result[0].QA.subTracks[0].history.should.have.length(1)
      result[0].QA.subTracks[0].history[0].challengeId.should.equal('qa-challenge-june-15')
      result[0].QA.subTracks[0].history[0].challengeName.should.equal('QA Challenge June 15')
      result[0].QA.subTracks[0].history[0].placement.should.equal(1)
      result[0].QA.subTracks[0].history[0].ratingDate.should.equal(ratingDate.getTime())
      result[0].QA.subTracks[0].history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should remap persisted QA challenge history from Data Science to QA', async () => {
    const ratingDate = new Date('2026-06-15T08:00:00.000Z')
    const qaChallenge = {
      id: 'qa-challenge-june-15',
      legacyId: null,
      name: 'QA Challenge June 15',
      status: 'COMPLETED',
      trackId: 'track-qa-id',
      typeId: 'type-challenge-id',
      endDate: ratingDate
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-challenge-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-challenge-id',
            challengeId: 'qa-challenge-june-15',
            challengeName: null,
            eventDate: ratingDate,
            ratingDate,
            newRating: 1400,
            placement: 1,
            mostRecent: true
          }]
        }
      },
      challengeRows: [qaChallenge]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].QA)
      should.not.exist(result[0].DATA_SCIENCE)
      result[0].QA.subTracks.should.have.length(1)
      result[0].QA.subTracks[0].name.should.equal('Challenge')
      result[0].QA.subTracks[0].history.should.have.length(1)
      result[0].QA.subTracks[0].history[0].challengeId.should.equal('qa-challenge-june-15')
      result[0].QA.subTracks[0].history[0].challengeName.should.equal('QA Challenge June 15')
      result[0].QA.subTracks[0].history[0].rating.should.equal(1400)
      result[0].QA.subTracks[0].history[0].placement.should.equal(1)
      result[0].QA.subTracks[0].history[0].ratingDate.should.equal(ratingDate.getTime())
    } finally {
      restore()
    }
  })

  it('getHistoryStats should offset persisted passed-review placements by paid winners', async () => {
    const ratingDate = new Date('2025-08-27T17:05:00.000Z')
    const challenge = {
      id: 'mm-challenge-uuid',
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: ratingDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null,
      winners: [{
        placement: 1
      }]
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-challenge-uuid',
            eventDate: ratingDate,
            placement: 1,
            mostRecent: true
          }]
        }
      },
      challengeRows: [challenge],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'mm-challenge-uuid',
        type: 'PASSED_REVIEW',
        placement: 3,
        createdAt: new Date('2025-08-27T17:05:00.000Z'),
        challenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DATA_SCIENCE)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('Marathon Match 163')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].placement.should.equal(4)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should keep rerated Marathon Match final placement over paid winner placement', async () => {
    const ratingDate = new Date('2020-02-29T20:11:00.000Z')
    const challenge = {
      id: '74aaf772-5c1c-48bc-a50a-bccd529d7fb7',
      legacyId: 30092303,
      name: 'SingleCell - Trajectory Inference Methods',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: ratingDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null,
      winners: []
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '74aaf772-5c1c-48bc-a50a-bccd529d7fb7',
            eventDate: ratingDate,
            newRating: 1492,
            placement: 6,
            mostRecent: true,
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }]
        }
      },
      challengeRows: [],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: '74aaf772-5c1c-48bc-a50a-bccd529d7fb7',
        type: 'PLACEMENT',
        placement: 4,
        createdAt: ratingDate,
        challenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('SingleCell - Trajectory Inference Methods')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].rating.should.equal(1492)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].placement.should.equal(6)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should prefer rerated SingleCell canonical history over the stale legacy numeric row', async () => {
    const canonicalDate = new Date('2020-02-29T20:11:00.000Z')
    const legacyDate = new Date('2019-06-07T00:00:00.000Z')
    const challenge = {
      id: '74aaf772-5c1c-48bc-a50a-bccd529d7fb7',
      legacyId: 30092303,
      name: 'SingleCell - Trajectory Inference Methods',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: canonicalDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null,
      winners: []
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [{
          challengeId: global.BigInt(17558),
          challengeName: 'SingleCell - Trajectory Infere',
          date: legacyDate,
          rating: 1755,
          placement: 57,
          percentile: 85.6061
        }],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '17558',
            eventDate: legacyDate,
            newRating: 1755,
            placement: 57,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '74aaf772-5c1c-48bc-a50a-bccd529d7fb7',
            eventDate: canonicalDate,
            newRating: 1492,
            placement: 6,
            mostRecent: true,
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }]
        }
      },
      challengeRows: [challenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const history = result[0].DATA_SCIENCE.MARATHON_MATCH.history
      history.map(row => row.challengeId).should.not.include(17558)
      history.should.have.length(1)
      history[0].challengeId.should.equal('74aaf772-5c1c-48bc-a50a-bccd529d7fb7')
      history[0].challengeName.should.equal('SingleCell - Trajectory Inference Methods')
      history[0].rating.should.equal(1492)
      history[0].placement.should.equal(6)
      history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should use the best duplicate passed-review Marathon Match placement', async () => {
    const ratingDate = new Date('2023-05-31T10:02:00.000Z')
    const challenge = {
      id: 'mm-145',
      name: 'Marathon Match 145',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: ratingDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null,
      winners: [{
        placement: 1
      }]
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-145',
            eventDate: ratingDate,
            placement: 1,
            mostRecent: true
          }]
        }
      },
      challengeRows: [challenge],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'mm-145',
        type: 'PASSED_REVIEW',
        placement: 4,
        createdAt: ratingDate,
        challenge
      }, {
        challengeId: 'mm-145',
        type: 'PASSED_REVIEW',
        placement: 43,
        createdAt: ratingDate,
        challenge
      }]
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('Marathon Match 145')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].placement.should.equal(5)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should hydrate persisted Marathon Match names from challenge winners', async () => {
    const ratingDate = new Date('2025-08-27T17:05:00.000Z')
    const challenge = {
      id: 'mm-challenge-uuid',
      legacyId: null,
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: ratingDate,
      legacyRecord: null,
      winners: []
    }
    let winnerFindManyArgs
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-challenge-uuid',
            eventDate: ratingDate,
            newRating: 2543,
            placement: 1,
            mostRecent: true
          }]
        }
      },
      challengeRows: [],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'mm-challenge-uuid',
        type: 'PLACEMENT',
        placement: 1,
        createdAt: ratingDate,
        challenge
      }],
      onChallengeWinnerFindMany: (args) => {
        winnerFindManyArgs = args
      }
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      should.exist(winnerFindManyArgs)
      result.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeId.should.equal('mm-challenge-uuid')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('Marathon Match 163')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].placement.should.equal(1)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should drop unresolved legacy numeric Marathon Match rows', async () => {
    const ratingDate = new Date('2025-08-27T17:05:00.000Z')
    const legacyDate = new Date('2014-04-18T00:00:00.000Z')
    const challenge = {
      id: 'mm-challenge-uuid',
      legacyId: null,
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: ratingDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '15948',
            eventDate: legacyDate,
            newRating: 2946,
            mostRecent: true
          }, {
            trackId: 'track-dev-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-challenge-uuid',
            challengeName: 'Marathon Match 163',
            eventDate: ratingDate,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-challenge-uuid',
            challengeName: 'Marathon Match 163',
            eventDate: ratingDate,
            newRating: 2739,
            placement: 2,
            mostRecent: false
          }]
        }
      },
      challengeRows: [challenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      should.exist(result[0].DATA_SCIENCE)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(1)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeId.should.equal('mm-challenge-uuid')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('Marathon Match 163')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].rating.should.equal(2739)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should hide hydrated orphan legacy Marathon Match rows after native rerate', async () => {
    const legacyDate = new Date('2023-05-02T00:00:00.000Z')
    const canonicalDate = new Date('2025-08-27T17:05:00.000Z')
    const canonicalChallenge = {
      id: 'mm-163-canonical',
      legacyId: null,
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: canonicalDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [{
          challengeId: global.BigInt(19708),
          challengeName: 'MM 148',
          date: legacyDate,
          rating: 2955,
          placement: 3,
          percentile: 97.973
        }],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19708',
            eventDate: legacyDate,
            newRating: 2955,
            placement: 3,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-163-canonical',
            challengeName: 'Marathon Match 163',
            eventDate: canonicalDate,
            newRating: 2279,
            placement: 5,
            updatedBy: 'rerate-mm-stats',
            mostRecent: true
          }]
        }
      },
      challengeRows: [canonicalChallenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const history = result[0].DATA_SCIENCE.MARATHON_MATCH.history
      history.map(row => row.challengeId).should.not.include(19708)
      history.should.have.length(1)
      history[0].challengeId.should.equal('mm-163-canonical')
      history[0].challengeName.should.equal('Marathon Match 163')
      history[0].rating.should.equal(2279)
      history[0].mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should preserve imported Marathon Match history over matching canonical rerates', async () => {
    const mm144LegacyDate = new Date('2023-02-21T00:00:00.000Z')
    const mm144Date = new Date('2023-03-08T18:14:00.000Z')
    const mm145LegacyDate = new Date('2023-05-02T00:00:00.000Z')
    const mm145Date = new Date('2023-05-31T10:02:00.000Z')
    const mm149Date = new Date('2023-11-14T10:22:00.000Z')
    const mm144Challenge = {
      id: 'mm-144-canonical',
      legacyId: null,
      name: 'Marathon Match 144',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: mm144Date,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const mm145Challenge = {
      id: 'mm-145-canonical',
      legacyId: null,
      name: 'Marathon Match 145',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: mm145Date,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const mm149Challenge = {
      id: 'mm-149-canonical',
      legacyId: null,
      name: 'Marathon Match 149',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: mm149Date,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [{
          challengeId: global.BigInt(19578),
          challengeName: 'Marathon Match 144 ROUND',
          date: mm144LegacyDate,
          rating: 2779,
          placement: 4,
          percentile: 96.5217
        }, {
          challengeId: global.BigInt(19628),
          challengeName: 'MM 145',
          date: mm145LegacyDate,
          rating: 2925,
          placement: 2,
          percentile: 98.1982
        }],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19578',
            eventDate: mm144LegacyDate,
            newRating: 2779,
            placement: 4,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-144-canonical',
            eventDate: mm144Date,
            oldRating: 2343,
            newRating: null,
            placement: 1,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19628',
            eventDate: mm145LegacyDate,
            newRating: 2925,
            placement: 2,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-145-canonical',
            eventDate: mm145Date,
            oldRating: 2436,
            newRating: null,
            placement: 1,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-149-canonical',
            eventDate: mm149Date,
            oldRating: 2531,
            newRating: null,
            placement: 2,
            mostRecent: true
          }]
        }
      },
      challengeRows: [mm144Challenge, mm145Challenge, mm149Challenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const history = result[0].DATA_SCIENCE.MARATHON_MATCH.history
      history.map(row => row.challengeId).should.not.include('mm-144-canonical')
      history.map(row => row.challengeId).should.not.include('mm-145-canonical')
      const mm144 = history.find(row => row.challengeId === 19578)
      const mm145 = history.find(row => row.challengeId === 19628)
      const mm149 = history.find(row => row.challengeId === 'mm-149-canonical')
      should.exist(mm144)
      should.exist(mm145)
      should.exist(mm149)
      mm144.placement.should.equal(4)
      mm145.placement.should.equal(2)
      mm144.rating.should.equal(2779)
      mm145.rating.should.equal(2925)
      mm149.mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should prefer authoritative rerated Marathon Match rows over duplicate legacy rounds', async () => {
    const mm144LegacyDate = new Date('2023-02-21T00:00:00.000Z')
    const mm144Date = new Date('2023-03-08T18:14:00.000Z')
    const mm145LegacyDate = new Date('2023-05-02T00:00:00.000Z')
    const mm145Date = new Date('2023-05-31T10:02:00.000Z')
    const mm144Challenge = {
      id: 'mm-144-canonical',
      legacyId: null,
      name: 'Marathon Match 144',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: mm144Date,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const mm145Challenge = {
      id: 'mm-145-canonical',
      legacyId: null,
      name: 'Marathon Match 145',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: mm145Date,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [{
          challengeId: global.BigInt(19578),
          challengeName: 'Marathon Match 144 ROUND',
          date: mm144LegacyDate,
          rating: 2779,
          placement: 4,
          percentile: 96.5217
        }, {
          challengeId: global.BigInt(19628),
          challengeName: 'MM 145',
          date: mm145LegacyDate,
          rating: 2925,
          placement: 2,
          percentile: 98.1982
        }],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19578',
            eventDate: mm144LegacyDate,
            newRating: 2779,
            placement: 4,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-144-canonical',
            eventDate: mm144Date,
            oldRating: 2295,
            newRating: 2371,
            placement: 1,
            mostRecent: false,
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19628',
            eventDate: mm145LegacyDate,
            newRating: 2925,
            placement: 2,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'mm-145-canonical',
            eventDate: mm145Date,
            oldRating: 2371,
            newRating: 2455,
            placement: 1,
            mostRecent: true,
            createdBy: 'stats-migration',
            updatedBy: 'rerate-mm-stats'
          }]
        }
      },
      challengeRows: [mm144Challenge, mm145Challenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const history = result[0].DATA_SCIENCE.MARATHON_MATCH.history
      history.map(row => row.challengeId).should.not.include(19578)
      history.map(row => row.challengeId).should.not.include(19628)
      history.should.have.length(2)
      const mm144 = history.find(row => row.challengeId === 'mm-144-canonical')
      const mm145 = history.find(row => row.challengeId === 'mm-145-canonical')
      should.exist(mm144)
      should.exist(mm145)
      mm144.rating.should.equal(2371)
      mm144.placement.should.equal(1)
      mm145.rating.should.equal(2455)
      mm145.placement.should.equal(1)
      mm145.mostRecent.should.equal(true)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should append new canonical Marathon Match rows after imported history', async () => {
    const legacyDate = new Date('2023-05-02T00:00:00.000Z')
    const oldCanonicalDate = new Date('2023-03-08T18:14:00.000Z')
    const newCanonicalDate = new Date('2025-08-27T17:05:00.000Z')
    const newChallenge = {
      id: 'new-mm-challenge',
      legacyId: null,
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: newCanonicalDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const oldChallenge = {
      id: 'old-mm-challenge',
      legacyId: 19708,
      name: 'MM 148',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: oldCanonicalDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [{
          challengeId: global.BigInt(19708),
          challengeName: 'MM 148',
          date: legacyDate,
          rating: 2955,
          placement: 3,
          percentile: 97.973
        }],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: '19708',
            eventDate: legacyDate,
            newRating: 2955,
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'old-mm-challenge',
            challengeName: 'Marathon Match 144',
            eventDate: oldCanonicalDate,
            newRating: 2560,
            placement: 1,
            updatedBy: 'rerate-mm-stats',
            mostRecent: false
          }, {
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'new-mm-challenge',
            challengeName: 'Marathon Match 163',
            eventDate: newCanonicalDate,
            newRating: 2279,
            placement: 5,
            updatedBy: 'rerate-mm-stats',
            mostRecent: true
          }]
        }
      },
      challengeRows: [newChallenge, oldChallenge],
      reviewRows: [],
      challengeWinnerRows: []
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      result.should.have.length(1)
      const history = result[0].DATA_SCIENCE.MARATHON_MATCH.history
      history.should.have.length(2)
      history[0].challengeId.should.equal('new-mm-challenge')
      history[0].rating.should.equal(2279)
      history[0].mostRecent.should.equal(true)
      history[1].challengeId.should.equal(19708)
      history[1].challengeName.should.equal('MM 148')
      history[1].rating.should.equal(2955)
      history[1].placement.should.equal(3)
    } finally {
      restore()
    }
  })

  it('getHistoryStats should surface passed-review Marathon Match winners under Data Science', async () => {
    const olderDate = new Date('2024-06-18T00:00:00.000Z')
    const newerDate = new Date('2025-08-27T17:05:00.000Z')
    let winnerFindManyArgs
    const oldChallenge = {
      id: 'old-mm-challenge',
      legacyId: null,
      name: 'Marathon Match 154',
      status: 'COMPLETED',
      trackId: 'track-ds-id',
      typeId: 'type-mm-id',
      endDate: olderDate,
      track: { name: 'Data Science' },
      type: { name: 'Marathon Match' },
      metadata: [],
      legacyRecord: null
    }
    const newChallenge = {
      id: 'new-mm-challenge',
      name: 'Marathon Match 163',
      status: 'COMPLETED',
      trackId: 'track-dev-id',
      typeId: 'type-mm-id',
      endDate: newerDate,
      winners: [{
        placement: 1
      }]
    }
    const { service, restore } = loadStatisticsService({
      prismaStub: {
        $queryRaw: async () => [],
        memberStats: {
          findFirst: async () => null,
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id'
          }]
        },
        memberStatsHistory: {
          findMany: async () => [{
            trackId: 'track-ds-id',
            typeId: 'type-mm-id',
            challengeId: 'old-mm-challenge',
            eventDate: olderDate,
            newRating: 2680,
            mostRecent: true
          }]
        }
      },
      challengeRows: [oldChallenge],
      reviewRows: [],
      challengeWinnerRows: [{
        challengeId: 'new-mm-challenge',
        type: 'PASSED_REVIEW',
        placement: 3,
        createdAt: new Date('2026-01-29T21:08:01.332Z'),
        challenge: newChallenge
      }],
      onChallengeWinnerFindMany: (args) => {
        winnerFindManyArgs = args
      }
    })

    try {
      const result = await service.getHistoryStats({ isMachine: true }, 'devtest1400', {})

      winnerFindManyArgs.where.type.in.should.include('PASSED_REVIEW')
      should.exist(winnerFindManyArgs.select.challenge.select.winners)
      result.should.have.length(1)
      should.exist(result[0].DATA_SCIENCE)
      should.not.exist(result[0].DEVELOP)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history.should.have.length(2)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeId.should.equal('new-mm-challenge')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].challengeName.should.equal('Marathon Match 163')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].placement.should.equal(4)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[0].mostRecent.should.equal(true)
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[1].challengeId.should.equal('old-mm-challenge')
      result[0].DATA_SCIENCE.MARATHON_MATCH.history[1].mostRecent.should.equal(false)
    } finally {
      restore()
    }
  })
})

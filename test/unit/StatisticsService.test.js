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
    DATA_SCIENCE: 'DATA_SCIENCE'
  }
  const TYPE_NAMES = {
    CHALLENGE: 'Challenge',
    FIRST2FINISH: 'First2Finish',
    TASK: 'Task',
    SRM: 'SRM',
    MARATHON_MATCH: 'MARATHON_MATCH'
  }

  const trackIds = {
    DEVELOP: 'track-dev-id',
    DATA_SCIENCE: 'track-ds-id'
  }
  const typeIds = {
    CHALLENGE: 'type-challenge-id',
    FIRST2FINISH: 'type-f2f-id',
    TASK: 'type-task-id',
    SRM: 'type-srm-id',
    MARATHON_MATCH: 'type-mm-id'
  }

  const trackNamesById = {
    'track-dev-id': TRACK_NAMES.DEVELOP,
    'track-ds-id': TRACK_NAMES.DATA_SCIENCE
  }
  const typeNamesById = {
    'type-challenge-id': TYPE_NAMES.CHALLENGE,
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
      if (normalized === 'DATA_SCIENCE' || normalized === 'DATA SCIENCE' || normalized === 'DS') {
        return TRACK_NAMES.DATA_SCIENCE
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
    [joiPath]: require.cache[joiPath]
  }

  const statsDimensionHelper = createStatsDimensionHelperStub()
  const member = options.member || {
    userId: global.BigInt(88770025),
    handle: 'devtest1400',
    handleLower: 'devtest1400'
  }
  const prismaStub = options.prismaStub || {
    $queryRaw: async () => [],
    memberStats: {
      findFirst: async () => null,
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
    bigIntToNumber: (value) => (value ? Number(value) : null)
  })
  setStubModule(loggerPath, {
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
    getChallengesClient: () => ({
      challenge: {
        findMany: async () => challengeRows
      },
      ChallengeWinner: {
        findMany: async () => challengeWinnerRows
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
  setStubModule(developRatingEnginePath, {
    rerateDevTrack: async () => ({})
  })
  setStubModule(mmRatingEnginePath, {
    rerateMmTrack: async () => ({})
  })

  delete require.cache[servicePath]
  const service = require(servicePath)

  return {
    service,
    restore: () => restoreModuleCache(originalEntries)
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
})

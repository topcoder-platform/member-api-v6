/*
 * Unit tests for shared stats track/type dimension helpers.
 */

const chai = require('chai')

const prismaHelper = require('../../src/common/prismaHelper')
const {
  TRACK_NAMES,
  TYPE_NAMES,
  buildChallengeDimensionLookup,
  resolveTrackIdFromLookup,
  resolveTypeIdFromLookup,
  resolveTrackNameFromLookup,
  resolveTypeNameFromLookup
} = require('../../src/common/statsDimensionHelper')

const should = chai.should()

describe('stats dimension helper unit tests', () => {
  it('should resolve UUID ids and API labels through one lookup', () => {
    const lookup = buildChallengeDimensionLookup(
      [
        { id: 'track-dev-id', name: 'Development', abbreviation: 'DEV', legacyId: null },
        { id: 'track-design-id', name: 'Design', abbreviation: 'DES', legacyId: null },
        { id: 'track-ds-id', name: 'Data Science', abbreviation: 'DS', legacyId: null }
      ],
      [
        { id: 'type-ch-id', name: 'Challenge', abbreviation: 'CH', legacyId: null, isTask: false },
        { id: 'type-task-id', name: 'Task', abbreviation: 'TSK', legacyId: null, isTask: true },
        { id: 'type-hidden-id', name: 'ARCHITECTURE', abbreviation: 'LARC', legacyId: 118, isTask: false },
        { id: 'type-mm-id', name: 'Marathon Match', abbreviation: 'MM', legacyId: null, isTask: false }
      ]
    )

    resolveTrackIdFromLookup(lookup, 'DEVELOP').should.equal('track-dev-id')
    resolveTrackIdFromLookup(lookup, 'track-design-id').should.equal('track-design-id')
    resolveTypeIdFromLookup(lookup, 'CH').should.equal('type-ch-id')
    resolveTypeIdFromLookup(lookup, TYPE_NAMES.TASK).should.equal('type-task-id')
    resolveTypeIdFromLookup(lookup, 118).should.equal('type-hidden-id')
    resolveTrackNameFromLookup(lookup, 'track-ds-id').should.equal(TRACK_NAMES.DATA_SCIENCE)
    resolveTypeNameFromLookup(lookup, 'type-hidden-id').should.equal('ARCHITECTURE')
    resolveTypeNameFromLookup(lookup, 'type-mm-id').should.equal(TYPE_NAMES.MARATHON_MATCH)
  })

  it('buildUnifiedStatsResponse should use resolved names and ignore unknown rows', () => {
    const result = prismaHelper.buildUnifiedStatsResponse(
      {
        userId: global.BigInt(151743),
        handle: 'Ghostar',
        handleLower: 'ghostar',
        maxRating: null
      },
      [
        {
          trackId: 'track-dev-id',
          typeId: 'type-arch-id',
          trackName: TRACK_NAMES.DEVELOP,
          typeName: 'ARCHITECTURE',
          challenges: 120,
          wins: 27,
          mostRecentEventDate: null,
          mostRecentSubmission: null
        },
        {
          trackId: 'unknown-track-id',
          typeId: 'unknown-type-id',
          challenges: 999,
          wins: 999
        }
      ]
    )

    result.challenges.should.equal(120)
    result.wins.should.equal(27)
    should.exist(result.DEVELOP)
    result.DEVELOP.subTracks.should.have.length(1)
    result.DEVELOP.subTracks[0].name.should.equal('ARCHITECTURE')
    should.not.exist(result.DESIGN)
    should.not.exist(result.DATA_SCIENCE)
  })

  it('buildUnifiedStatsResponse should expose derived unified develop submission counts', () => {
    const result = prismaHelper.buildUnifiedStatsResponse(
      {
        userId: global.BigInt(15391415),
        handle: 'winterflame',
        handleLower: 'winterflame',
        maxRating: null
      },
      [
        {
          groupId: global.BigInt(1),
          trackId: 'track-dev-id',
          typeId: 'type-arch-id',
          trackName: TRACK_NAMES.DEVELOP,
          typeName: 'ARCHITECTURE',
          challenges: 62,
          wins: null,
          mostRecentSubmission: null,
          mostRecentEventDate: null
        }
      ]
    )

    result.groupId.should.equal(1)
    should.exist(result.DEVELOP)
    result.DEVELOP.subTracks.should.have.length(1)
    result.DEVELOP.subTracks[0].submissions.should.deep.equal({ submissions: 62 })
    result.DEVELOP.subTracks[0].rank.should.deep.equal({})
  })

  it('buildUnifiedStatsResponse should expose custom data science rating paths', () => {
    const result = prismaHelper.buildUnifiedStatsResponse(
      {
        userId: global.BigInt(15391415),
        handle: 'winterflame',
        handleLower: 'winterflame',
        maxRating: null
      },
      [
        {
          groupId: global.BigInt(1),
          trackId: 'track-ds-id',
          typeId: 'AI',
          trackName: TRACK_NAMES.DATA_SCIENCE,
          typeName: 'AI',
          challenges: 3,
          wins: null,
          rating: 1422,
          volatility: 331,
          mostRecentSubmission: null,
          mostRecentEventDate: new Date('2024-06-01T00:00:00.000Z')
        }
      ]
    )

    should.exist(result.DATA_SCIENCE)
    should.exist(result.DATA_SCIENCE.AI)
    result.DATA_SCIENCE.AI.challenges.should.equal(3)
    result.DATA_SCIENCE.AI.rank.should.deep.equal({
      rating: 1422,
      volatility: 331
    })
    result.maxRating.should.deep.equal({
      rating: 1422,
      track: TRACK_NAMES.DATA_SCIENCE,
      subTrack: 'AI',
      ratingColor: '#616BD5'
    })
  })

  it('buildUnifiedStatsResponse should display configured rating path names from deterministic type ids', () => {
    const result = prismaHelper.buildUnifiedStatsResponse(
      {
        userId: global.BigInt(15391415),
        handle: 'winterflame',
        handleLower: 'winterflame',
        maxRating: null
      },
      [
        {
          groupId: global.BigInt(1),
          trackId: 'track-ds-id',
          typeId: 'rating-path-ai-engineering',
          trackName: TRACK_NAMES.DATA_SCIENCE,
          challenges: 3,
          wins: null,
          rating: 1517,
          volatility: 331,
          mostRecentSubmission: null,
          mostRecentEventDate: new Date('2024-06-01T00:00:00.000Z')
        }
      ]
    )

    should.exist(result.DATA_SCIENCE)
    should.exist(result.DATA_SCIENCE['AI Engineering'])
    should.not.exist(result.DATA_SCIENCE['rating-path-ai-engineering'])
    result.DATA_SCIENCE['AI Engineering'].rank.should.deep.equal({
      rating: 1517,
      volatility: 331
    })
    result.maxRating.subTrack.should.equal('AI Engineering')
  })

  it('buildUnifiedStatsResponse should backfill an empty matching subtrack rank from maxRating', () => {
    const result = prismaHelper.buildUnifiedStatsResponse(
      {
        userId: global.BigInt(88770025),
        handle: 'devtest1400',
        handleLower: 'devtest1400',
        maxRating: {
          rating: 1301,
          track: TRACK_NAMES.DATA_SCIENCE,
          subTrack: TYPE_NAMES.MARATHON_MATCH,
          ratingColor: '#616BD5'
        }
      },
      [
        {
          groupId: global.BigInt(10),
          trackId: 'track-dev-id',
          typeId: 'type-mm-id',
          trackName: TRACK_NAMES.DATA_SCIENCE,
          typeName: TYPE_NAMES.MARATHON_MATCH,
          challenges: 1,
          wins: 1,
          mostRecentSubmission: new Date('2025-10-30T19:11:01.774Z'),
          mostRecentEventDate: new Date('2025-10-30T19:11:01.774Z')
        }
      ],
      ['DATA_SCIENCE']
    )

    result.DATA_SCIENCE.MARATHON_MATCH.rank.should.deep.equal({
      rating: 1301
    })
  })

  it('buildUnifiedStatsHistoryResponse should preserve canonical challenge ids and names', () => {
    const ratingDate = new Date('2024-01-01T00:00:00.000Z')
    const result = prismaHelper.buildUnifiedStatsHistoryResponse(
      {
        userId: global.BigInt(15391415),
        handle: 'winterflame',
        handleLower: 'winterflame'
      },
      [
        {
          groupId: global.BigInt(1),
          trackId: 'track-dev-id',
          typeId: 'type-spec-id',
          trackName: TRACK_NAMES.DEVELOP,
          typeName: 'SPECIFICATION',
          challengeId: '11111111-1111-1111-1111-111111111111',
          challengeName: 'Specification Challenge',
          newRating: 1321,
          oldRating: 1299,
          eventDate: ratingDate,
          mostRecent: true
        }
      ]
    )

    result.groupId.should.equal(1)
    should.exist(result.DEVELOP)
    result.DEVELOP.subTracks.should.have.length(1)
    result.DEVELOP.subTracks[0].history.should.have.length(1)
    result.DEVELOP.subTracks[0].history[0].challengeId.should.equal('11111111-1111-1111-1111-111111111111')
    result.DEVELOP.subTracks[0].history[0].challengeName.should.equal('Specification Challenge')
    result.DEVELOP.subTracks[0].history[0].ratingDate.should.equal(ratingDate.getTime())
  })
})

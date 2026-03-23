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
})

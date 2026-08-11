/*
 * Unit tests for Marathon Match rating source selection.
 */

const chai = require('chai')

const {
  fetchRatingPathParticipantsForChallenge
} = require('../../src/ratings/mmRatingEngine')

const should = chai.should()

/**
 * Build a review database test double that resolves review-api relations and
 * returns the supplied participant rows for the final standings query.
 * @param {Array<Object>} participantRows rows returned by the participant query
 * @returns {Object} review DB test double with captured query calls
 */
function createReviewDbClient (participantRows) {
  const queries = []

  return {
    queries,
    query: async (sql, params) => {
      const queryText = String(sql)
      queries.push({ sql: queryText, params })

      if (queryText.includes('pg_catalog.pg_class')) {
        return { rows: [{ schemaName: 'reviews' }] }
      }

      return { rows: participantRows }
    }
  }
}

describe('mm rating source selection unit tests', () => {
  it('should prefer an active placed MM submission over a later deleted summation', async () => {
    const reviewDbClient = createReviewDbClient([
      {
        submissionId: 'deleted-later',
        memberId: '40823122',
        challengeId: 'mm-145',
        placement: null,
        submissionStatus: 'DELETED',
        aggregateScore: 1,
        reviewedDate: new Date('2023-07-21T10:51:07.771Z'),
        createdAt: new Date('2023-07-21T10:51:07.771Z'),
        submissionCreatedAt: new Date('2023-05-22T11:57:41.000Z')
      },
      {
        submissionId: 'active-placed',
        memberId: '40823122',
        challengeId: 'mm-145',
        placement: 5,
        submissionStatus: 'ACTIVE',
        aggregateScore: 95.21712617834879,
        reviewedDate: new Date('2023-07-19T13:35:36.219Z'),
        createdAt: new Date('2023-07-19T13:35:36.219Z'),
        submissionCreatedAt: new Date('2023-05-21T09:44:47.000Z')
      },
      {
        submissionId: 'active-winner',
        memberId: '40562752',
        challengeId: 'mm-145',
        placement: 1,
        submissionStatus: 'ACTIVE',
        aggregateScore: 98.38958267394484,
        reviewedDate: new Date('2023-07-19T13:35:35.718Z'),
        createdAt: new Date('2023-07-19T13:35:35.718Z'),
        submissionCreatedAt: new Date('2023-05-22T10:15:46.000Z')
      }
    ])

    const result = await fetchRatingPathParticipantsForChallenge(reviewDbClient, {
      challengeId: 'mm-145',
      source: 'MARATHON_MATCH'
    })

    const xilledanxRows = result.participantRows.filter(row => row.memberId === '40823122')
    xilledanxRows.should.have.length(1)
    xilledanxRows[0].submissionId.should.equal('active-placed')
    xilledanxRows[0].placement.should.equal(5)

    const participantQuery = reviewDbClient.queries.find(query => query.sql.includes('"latestSubmissionSummation"'))
    should.exist(participantQuery)
    participantQuery.sql.should.include('s."status"::text = ANY($2::text[])')
    participantQuery.params[1].should.deep.equal(['ACTIVE', 'COMPLETED_WITHOUT_WIN', 'FAILED_REVIEW'])
  })
})

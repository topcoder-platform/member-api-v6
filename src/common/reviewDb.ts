/**
 * Shared review-api PostgreSQL client used for challengeResult reads.
 * The pool is created lazily from REVIEW_DB_URL and can be reused by
 * service handlers and maintenance scripts in this package.
 */

'use strict'

const config = require('config')
const { Pool } = require('pg')

const reviewDb = config.REVIEW_DB_URL
  ? new Pool({
    connectionString: config.REVIEW_DB_URL
  })
  : null

module.exports = reviewDb

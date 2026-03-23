/**
 * Resolve review-api relations used by the stats refresh and rerate flows.
 * REVIEW_DB_URL must point to the review-api database that contains
 * challengeResult. The helper caches the discovered schema-qualified relation
 * so repeated lookups do not re-query PostgreSQL system catalogs.
 */

'use strict'

const challengeResultRelationCache = new WeakMap()

function quotePgIdentifier (value) {
  return `"${String(value).replace(/"/g, '""')}"`
}

/**
 * Resolve the schema-qualified challengeResult relation name.
 * @param {Object} reviewDbClient raw pg pool/client
 * @returns {Promise<string>} quoted schema-qualified relation name
 * @throws {Error} if the relation cannot be found in the configured review DB
 */
async function resolveChallengeResultRelation (reviewDbClient) {
  if (challengeResultRelationCache.has(reviewDbClient)) {
    return challengeResultRelationCache.get(reviewDbClient)
  }

  const result = await reviewDbClient.query(`
    SELECT n.nspname AS "schemaName"
    FROM pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_namespace n
      ON n.oid = c.relnamespace
    WHERE c.relname = 'challengeResult'
      AND c.relkind IN ('r', 'p', 'v', 'm')
    ORDER BY
      CASE
        WHEN n.nspname = current_schema() THEN 0
        WHEN n.nspname = 'public' THEN 1
        ELSE 2
      END,
      n.nspname ASC
    LIMIT 1
  `)

  const schemaName = result.rows[0] && result.rows[0].schemaName
  if (!schemaName) {
    throw new Error('REVIEW_DB_URL does not expose challengeResult. Verify REVIEW_DB_URL points to the review-api database and that review-api-v6 migrations have been deployed.')
  }

  const relation = `${quotePgIdentifier(schemaName)}."challengeResult"`
  challengeResultRelationCache.set(reviewDbClient, relation)
  return relation
}

/**
 * Fail fast when the configured review DB cannot serve challengeResult queries.
 * @param {Object} reviewDbClient raw pg pool/client
 * @returns {Promise<void>}
 */
async function assertChallengeResultRelation (reviewDbClient) {
  await resolveChallengeResultRelation(reviewDbClient)
}

module.exports = {
  resolveChallengeResultRelation,
  assertChallengeResultRelation
}

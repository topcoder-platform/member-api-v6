'use strict'

/**
 * Recalculate current rating ranks for one unified stats track/type scope.
 */

/**
 * Recompute persisted global and country ranks from current memberStats ratings.
 * Global rank uses SQL RANK semantics across all rows in the same track/type and
 * privacy scope. Country rank uses the member competition country first, then
 * home country, then legacy country text when available.
 * @param {Object} client Prisma client or transaction client
 * @param {Object} dimensionIds unified stats dimension identifiers
 * @param {string} dimensionIds.trackId unified ChallengeTrack id
 * @param {string} dimensionIds.typeId unified ChallengeType id
 * @param {Object} [options] optional recalculation controls
 * @param {boolean} [options.isPrivate=false] rank private stats when true
 * @param {string} [options.updatedBy='rating-rank-recalc'] audit actor
 * @returns {Promise<number>} number of updated memberStats rows
 * @throws {Error} when required dimension ids are missing or the raw update fails
 */
async function recalculateRatingRanks (client, dimensionIds, options: any = {}) {
  if (!client || typeof client.$executeRawUnsafe !== 'function') {
    throw new Error('A Prisma client with $executeRawUnsafe is required to recalculate rating ranks')
  }
  if (!dimensionIds || !dimensionIds.trackId || !dimensionIds.typeId) {
    throw new Error('trackId and typeId are required to recalculate rating ranks')
  }

  const isPrivate = options.isPrivate === true
  const updatedBy = options.updatedBy || 'rating-rank-recalc'

  return client.$executeRawUnsafe(
    `
    WITH scoped AS (
      SELECT
        ms."id",
        ms."rating",
        COALESCE(
          NULLIF(TRIM(m."competitionCountryCode"), ''),
          NULLIF(TRIM(m."homeCountryCode"), ''),
          NULLIF(TRIM(m."country"), '')
        ) AS "countryKey"
      FROM "members"."memberStats" ms
      INNER JOIN "members"."member" m
        ON m."userId" = ms."userId"
      WHERE ms."trackId" = $1
        AND ms."typeId" = $2
        AND ms."isPrivate" = $3
    ),
    ranked AS (
      SELECT
        scoped."id",
        RANK() OVER (ORDER BY scoped."rating" DESC)::int AS "globalRank",
        CASE
          WHEN scoped."countryKey" IS NULL THEN NULL
          ELSE RANK() OVER (
            PARTITION BY scoped."countryKey"
            ORDER BY scoped."rating" DESC
          )::int
        END AS "countryRank"
      FROM scoped
      WHERE scoped."rating" IS NOT NULL
    )
    UPDATE "members"."memberStats" ms
    SET
      "globalRank" = ranked."globalRank",
      "countryRank" = ranked."countryRank",
      "updatedBy" = $4,
      "updatedAt" = CURRENT_TIMESTAMP
    FROM scoped
    LEFT JOIN ranked
      ON ranked."id" = scoped."id"
    WHERE ms."id" = scoped."id"
      AND (
        ms."globalRank" IS DISTINCT FROM ranked."globalRank"
        OR ms."countryRank" IS DISTINCT FROM ranked."countryRank"
      )
    `,
    dimensionIds.trackId,
    dimensionIds.typeId,
    isPrivate,
    updatedBy
  )
}

module.exports = {
  recalculateRatingRanks
}

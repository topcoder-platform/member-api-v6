#!/usr/bin/env node
'use strict'

/**
 * Backfill missing review-api challengeResult rows from review submissions,
 * final review summations, review scores, and challenge winner placements.
 *
 * Required environment variables:
 * - REVIEW_DB_URL, DATABASE_URL, or MEMBER_DB_URL pointing at the database that
 *   contains the reviews and challenges schemas.
 *
 * Usage examples:
 * - Preview all missing Development Challenge/CODE challengeResult rows:
 *   node src/scripts/backfillChallengeResults.js
 * - Preview one challenge:
 *   node src/scripts/backfillChallengeResults.js --challenge-id 21d5ea84-dfa4-43ad-afd8-483942aff152
 * - Apply all missing rows:
 *   node src/scripts/backfillChallengeResults.js --apply
 * - Apply only recently completed challenges:
 *   node src/scripts/backfillChallengeResults.js --apply --from-date 2026-03-01
 *
 * Notes:
 * - The script inserts one row per challenge/member, choosing the best valid
 *   submission first and then highest final score.
 * - By default only challenges with no existing challengeResult rows are
 *   considered. Use --include-existing-challenges to fill missing member rows
 *   in partially populated challenges.
 * - Existing rows are never updated; inserts use ON CONFLICT DO NOTHING.
 */

require('dotenv').config({ quiet: true })

const config = require('config')
const { Pool } = require('pg')

const DEFAULT_TRACK_NAME = 'Development'
const DEFAULT_TYPE_NAMES = ['Challenge', 'CODE']
const DEFAULT_VALID_STATUSES = ['ACTIVE', 'COMPLETED_WITHOUT_WIN', 'FAILED_REVIEW']
const DEFAULT_CREATED_BY = 'challenge-result-backfill'
const DEFAULT_PREVIEW_LIMIT = 20

function logInfo (message) {
  console.log(`[INFO] ${new Date().toISOString()} ${message}`)
}

function logError (message, error) {
  if (error) {
    console.error(`[ERROR] ${new Date().toISOString()} ${message}`, error)
    return
  }

  console.error(`[ERROR] ${new Date().toISOString()} ${message}`)
}

function parseCsv (value) {
  return String(value || '')
    .split(',')
    .map(item => item.trim())
    .filter(Boolean)
}

function parsePositiveInteger (value, optionName) {
  const numberValue = Number(value)
  if (!Number.isInteger(numberValue) || numberValue <= 0) {
    throw new Error(`${optionName} must be a positive integer`)
  }

  return numberValue
}

function parseDateOption (value, optionName) {
  if (!value) {
    return null
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    throw new Error(`${optionName} must be a valid date`)
  }

  return value
}

function parseArgs (argv) {
  const options = {
    apply: false,
    challengeIds: [],
    fromDate: null,
    toDate: null,
    trackName: DEFAULT_TRACK_NAME,
    typeNames: DEFAULT_TYPE_NAMES.slice(),
    validStatuses: DEFAULT_VALID_STATUSES.slice(),
    onlyMissingChallenges: true,
    createdBy: DEFAULT_CREATED_BY,
    previewLimit: DEFAULT_PREVIEW_LIMIT,
    help: false
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    const next = () => {
      index += 1
      if (index >= argv.length) {
        throw new Error(`${arg} requires a value`)
      }
      return argv[index]
    }

    if (arg === '--apply') {
      options.apply = true
    } else if (arg === '--dry-run') {
      options.apply = false
    } else if (arg === '--challenge-id') {
      options.challengeIds.push(next())
    } else if (arg === '--challenge-ids') {
      options.challengeIds.push(...parseCsv(next()))
    } else if (arg === '--from-date') {
      options.fromDate = parseDateOption(next(), arg)
    } else if (arg === '--to-date') {
      options.toDate = parseDateOption(next(), arg)
    } else if (arg === '--track-name') {
      options.trackName = next()
    } else if (arg === '--type-names') {
      options.typeNames = parseCsv(next())
    } else if (arg === '--valid-statuses') {
      options.validStatuses = parseCsv(next())
    } else if (arg === '--include-existing-challenges') {
      options.onlyMissingChallenges = false
    } else if (arg === '--created-by') {
      options.createdBy = next()
    } else if (arg === '--preview-limit') {
      options.previewLimit = parsePositiveInteger(next(), arg)
    } else if (arg === '--help' || arg === '-h') {
      options.help = true
    } else {
      throw new Error(`Unknown argument: ${arg}`)
    }
  }

  options.challengeIds = Array.from(new Set(options.challengeIds.map(String)))
  options.typeNames = Array.from(new Set(options.typeNames.map(String))).filter(Boolean)
  options.validStatuses = Array.from(new Set(options.validStatuses.map(String))).filter(Boolean)

  if (options.typeNames.length === 0) {
    throw new Error('--type-names must include at least one type')
  }

  if (options.validStatuses.length === 0) {
    throw new Error('--valid-statuses must include at least one submission status')
  }

  return options
}

function printUsage () {
  console.log(`Usage:
  node src/scripts/backfillChallengeResults.js [options]

Options:
  --apply                         Write missing challengeResult rows. Default is dry-run.
  --dry-run                       Preview rows without writing. This is the default.
  --challenge-id <id>             Restrict to one challenge id. Can be repeated.
  --challenge-ids <id,id>         Restrict to a comma-separated challenge id list.
  --from-date <date>              Include challenges ending on or after this date.
  --to-date <date>                Include challenges ending before this date.
  --track-name <name>             Challenge track name. Default: Development.
  --type-names <name,name>        Challenge type names. Default: Challenge,CODE.
  --valid-statuses <s,s>          Submission statuses treated as valid.
                                  Default: ACTIVE,COMPLETED_WITHOUT_WIN,FAILED_REVIEW.
  --include-existing-challenges   Also fill missing member rows for partially populated challenges.
  --created-by <value>            Audit user for inserted rows. Default: challenge-result-backfill.
  --preview-limit <number>        Challenge preview row limit. Default: 20.
  --help                          Show this help.
`)
}

function getDatabaseUrl () {
  return process.env.REVIEW_DB_URL ||
    process.env.DATABASE_URL ||
    process.env.MEMBER_DB_URL ||
    config.REVIEW_DB_URL ||
    config.DATABASE_URL ||
    config.MEMBER_DB_URL
}

function buildQueryParameters (options) {
  return [
    options.challengeIds.length > 0 ? options.challengeIds : null,
    options.fromDate,
    options.toDate,
    options.onlyMissingChallenges,
    options.trackName,
    options.typeNames,
    options.validStatuses,
    options.createdBy,
    options.previewLimit
  ]
}

function buildBackfillCteSql () {
  return `
    WITH filtered_challenges AS (
      SELECT c.id, c."endDate"
      FROM challenges."Challenge" c
      JOIN challenges."ChallengeTrack" track ON track.id = c."trackId"
      JOIN challenges."ChallengeType" challenge_type ON challenge_type.id = c."typeId"
      WHERE c.status::text = 'COMPLETED'
        AND track.name = $5
        AND challenge_type.name = ANY($6::text[])
        AND ($1::text[] IS NULL OR c.id = ANY($1::text[]))
        AND ($2::timestamp IS NULL OR c."endDate" >= $2::timestamp)
        AND ($3::timestamp IS NULL OR c."endDate" < $3::timestamp)
        AND (
          $4::boolean = false OR
          NOT EXISTS (
            SELECT 1
            FROM reviews."challengeResult" existing_challenge_result
            WHERE existing_challenge_result."challengeId" = c.id
          )
        )
    ),
    candidate_submissions AS (
      SELECT s.id,
             s."challengeId",
             s."memberId",
             s.status::text AS submission_status,
             s."createdAt",
             s."updatedAt",
             filtered_challenges."endDate"
      FROM reviews."submission" s
      JOIN filtered_challenges ON filtered_challenges.id = s."challengeId"
      WHERE s."memberId" IS NOT NULL
        AND btrim(s."memberId") <> ''
    ),
    final_summations AS (
      SELECT DISTINCT ON (review_summation."submissionId")
             review_summation."submissionId",
             review_summation."aggregateScore",
             review_summation."isPassing",
             review_summation."reviewedDate",
             review_summation."updatedAt"
      FROM reviews."reviewSummation" review_summation
      JOIN candidate_submissions ON candidate_submissions.id = review_summation."submissionId"
      WHERE review_summation."isFinal" = true
      ORDER BY review_summation."submissionId",
               review_summation."reviewedDate" DESC NULLS LAST,
               review_summation."updatedAt" DESC NULLS LAST,
               review_summation.id DESC
    ),
    review_scores AS (
      SELECT review."submissionId",
             avg(review."initialScore") FILTER (WHERE review."initialScore" IS NOT NULL) AS initial_score,
             avg(review."finalScore") FILTER (WHERE review."finalScore" IS NOT NULL) AS final_score,
             count(*) FILTER (WHERE review.committed = true) AS committed_reviews
      FROM reviews."review" review
      JOIN candidate_submissions ON candidate_submissions.id = review."submissionId"
      WHERE review.committed = true
      GROUP BY review."submissionId"
    ),
    placement_winners AS (
      SELECT "challengeId",
             "userId"::text AS "userId",
             min(placement)::integer AS placement
      FROM challenges."ChallengeWinner"
      WHERE type::text = 'PLACEMENT'
      GROUP BY "challengeId", "userId"::text
    ),
    enriched_submissions AS (
      SELECT candidate_submissions."challengeId",
             candidate_submissions."memberId" AS "userId",
             candidate_submissions.id AS "submissionId",
             COALESCE(review_scores.initial_score, review_scores.final_score, final_summations."aggregateScore", 0)::double precision AS "initialScore",
             COALESCE(review_scores.final_score, final_summations."aggregateScore", review_scores.initial_score, 0)::double precision AS "finalScore",
             COALESCE(placement_winners.placement, 0)::integer AS placement,
             false AS rated,
             COALESCE(final_summations."isPassing", false) AS "passedReview",
             (
               candidate_submissions.submission_status = ANY($7::text[]) OR
               (
                 candidate_submissions.submission_status = 'AI_FAILED_REVIEW' AND
                 COALESCE(final_summations."isPassing", false) = true
               )
             ) AS "validSubmission",
             COALESCE(
               final_summations."reviewedDate",
               final_summations."updatedAt",
               candidate_submissions."updatedAt",
               candidate_submissions."endDate",
               now()::timestamp
             ) AS result_time,
             candidate_submissions.submission_status,
             review_scores.committed_reviews,
             final_summations."submissionId" IS NOT NULL AS has_final_summation,
             placement_winners."userId" IS NOT NULL AS has_placement
      FROM candidate_submissions
      LEFT JOIN final_summations ON final_summations."submissionId" = candidate_submissions.id
      LEFT JOIN review_scores ON review_scores."submissionId" = candidate_submissions.id
      LEFT JOIN placement_winners
        ON placement_winners."challengeId" = candidate_submissions."challengeId"
       AND placement_winners."userId" = candidate_submissions."memberId"
      WHERE final_summations."submissionId" IS NOT NULL
         OR review_scores."submissionId" IS NOT NULL
         OR placement_winners."userId" IS NOT NULL
    ),
    ranked_submissions AS (
      SELECT enriched_submissions.*,
             row_number() OVER (
               PARTITION BY enriched_submissions."challengeId", enriched_submissions."userId"
               ORDER BY enriched_submissions."validSubmission" DESC,
                        enriched_submissions."finalScore" DESC,
                        enriched_submissions."passedReview" DESC,
                        enriched_submissions.has_final_summation DESC,
                        enriched_submissions.committed_reviews DESC NULLS LAST,
                        enriched_submissions.result_time ASC,
                        enriched_submissions."submissionId" ASC
             ) AS row_rank
      FROM enriched_submissions
    ),
    rows_to_insert AS (
      SELECT ranked_submissions."challengeId",
             ranked_submissions."userId",
             ranked_submissions."submissionId",
             ranked_submissions."initialScore",
             ranked_submissions."finalScore",
             ranked_submissions.placement,
             ranked_submissions.rated,
             ranked_submissions."passedReview",
             ranked_submissions."validSubmission",
             ranked_submissions.result_time,
             ranked_submissions.submission_status
      FROM ranked_submissions
      LEFT JOIN reviews."challengeResult" existing_challenge_result
        ON existing_challenge_result."challengeId" = ranked_submissions."challengeId"
       AND existing_challenge_result."userId" = ranked_submissions."userId"
      WHERE ranked_submissions.row_rank = 1
        AND existing_challenge_result."challengeId" IS NULL
    )
  `
}

function buildDryRunSql () {
  return `
    ${buildBackfillCteSql()},
    challenge_preview AS (
      SELECT rows_to_insert."challengeId",
             count(*) AS rows,
             count(*) FILTER (WHERE rows_to_insert.placement > 0) AS placement_rows,
             count(*) FILTER (WHERE rows_to_insert."passedReview") AS passed_rows,
             count(*) FILTER (WHERE rows_to_insert."validSubmission") AS valid_rows,
             max(rows_to_insert.result_time) AS latest_result_time
      FROM rows_to_insert
      GROUP BY rows_to_insert."challengeId"
      ORDER BY latest_result_time DESC NULLS LAST, rows_to_insert."challengeId" ASC
      LIMIT $9::integer
    )
    SELECT json_build_object(
             'eligibleChallenges', (SELECT count(*) FROM filtered_challenges),
             'rowsToInsert', (SELECT count(*) FROM rows_to_insert),
             'challengeResultsToCreate', (SELECT count(DISTINCT "challengeId") FROM rows_to_insert),
             'placementRows', (SELECT count(*) FROM rows_to_insert WHERE placement > 0),
             'passedReviewRows', (SELECT count(*) FROM rows_to_insert WHERE "passedReview"),
             'validSubmissionRows', (SELECT count(*) FROM rows_to_insert WHERE "validSubmission"),
             'invalidSubmissionRows', (SELECT count(*) FROM rows_to_insert WHERE NOT "validSubmission"),
             'createdBy', $8::text,
             'preview', COALESCE((SELECT json_agg(challenge_preview) FROM challenge_preview), '[]'::json)
           ) AS summary
  `
}

function buildApplySql () {
  return `
    ${buildBackfillCteSql()},
    inserted AS (
      INSERT INTO reviews."challengeResult" (
        "challengeId",
        "userId",
        "paymentId",
        "submissionId",
        "oldRating",
        "newRating",
        "initialScore",
        "finalScore",
        placement,
        rated,
        "passedReview",
        "validSubmission",
        "pointAdjustment",
        "ratingOrder",
        "createdAt",
        "createdBy",
        "updatedAt",
        "updatedBy"
      )
      SELECT rows_to_insert."challengeId",
             rows_to_insert."userId",
             NULL,
             rows_to_insert."submissionId",
             NULL,
             NULL,
             rows_to_insert."initialScore",
             rows_to_insert."finalScore",
             rows_to_insert.placement,
             rows_to_insert.rated,
             rows_to_insert."passedReview",
             rows_to_insert."validSubmission",
             NULL,
             NULL,
             rows_to_insert.result_time,
             $8,
             rows_to_insert.result_time,
             $8
      FROM rows_to_insert
      ON CONFLICT ("challengeId", "userId") DO NOTHING
      RETURNING "challengeId", "userId", placement, "passedReview", "validSubmission"
    )
    SELECT json_build_object(
             'insertedRows', (SELECT count(*) FROM inserted),
             'challengesUpdated', (SELECT count(DISTINCT "challengeId") FROM inserted),
             'placementRows', (SELECT count(*) FROM inserted WHERE placement > 0),
             'passedReviewRows', (SELECT count(*) FROM inserted WHERE "passedReview"),
             'validSubmissionRows', (SELECT count(*) FROM inserted WHERE "validSubmission"),
             'invalidSubmissionRows', (SELECT count(*) FROM inserted WHERE NOT "validSubmission"),
             'previewLimit', $9::integer
           ) AS summary
  `
}

async function runBackfill (options) {
  const databaseUrl = getDatabaseUrl()
  if (!databaseUrl) {
    throw new Error('REVIEW_DB_URL, DATABASE_URL, or MEMBER_DB_URL must be set')
  }

  const pool = new Pool({ connectionString: databaseUrl })
  const params = buildQueryParameters(options)
  const sql = options.apply ? buildApplySql() : buildDryRunSql()

  try {
    logInfo(`${options.apply ? 'Applying' : 'Previewing'} challengeResult backfill`)
    logInfo(`track=${options.trackName} types=${options.typeNames.join(',')} onlyMissingChallenges=${options.onlyMissingChallenges}`)
    if (options.challengeIds.length > 0) {
      logInfo(`challengeIds=${options.challengeIds.join(',')}`)
    }
    if (options.fromDate || options.toDate) {
      logInfo(`dateRange=${options.fromDate || ''}..${options.toDate || ''}`)
    }

    const result = await pool.query(sql, params)
    return result.rows[0] ? result.rows[0].summary : {}
  } finally {
    await pool.end()
  }
}

async function main () {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) {
    printUsage()
    return
  }

  const summary = await runBackfill(options)
  logInfo(`${options.apply ? 'Backfill applied' : 'Dry-run complete'}: ${JSON.stringify(summary, null, 2)}`)
  if (!options.apply) {
    logInfo('No rows were written. Re-run with --apply to insert the missing challengeResult rows.')
  }
}

if (require.main === module) {
  main().catch((error) => {
    logError('Failed to backfill challengeResult rows', error)
    process.exitCode = 1
  })
}

module.exports = {
  parseArgs,
  buildBackfillCteSql,
  buildDryRunSql,
  buildApplySql,
  runBackfill
}

/*
 * Backfill member profile challenge points from tc-finance-api point winnings.
 *
 * Usage:
 *   psql "$DATABASE_URL" \
 *     -f src/scripts/backfill-member-challenge-points-from-finance.sql
 *
 * Assumptions:
 * - Run this against a database connection that can see:
 *   - tc-finance-api tables in `public`: `winnings`, `payment`
 *   - member-api-v6 tables in `members`: `member`, `memberChallengePoints`
 * - If challenge-api tables `public."Challenge"` and
 *   `public."ChallengeWinner"` are visible, the script uses them for exact
 *   challenge names and placements.
 * - If challenge winner tables are not visible, placement falls back to:
 *   1. parsing finance title/description strings like `Challenge - 1st Place`
 *   2. ranking paid point totals within the challenge by points descending
 *
 * Safety:
 * - Only latest installment-1 payment versions are considered.
 * - Only finance rows with `winnings.type = POINTS`, `category = POINTS_AWARD`,
 *   `payment.currency = POINT`, and status `CREDITED` or `PAID` are copied.
 * - Existing member-api rows are updated in place by `(challengeId, userId)`.
 * - Rows for missing members are reported and skipped to preserve the FK.
 */

\set ON_ERROR_STOP on

BEGIN;

CREATE TEMP TABLE _finance_member_challenge_points_backfill ON COMMIT DROP AS
WITH latest_payment_version AS (
  SELECT
    p.winnings_id,
    MAX(p.version) AS max_version
  FROM public.payment p
  WHERE COALESCE(p.installment_number, 1) = 1
  GROUP BY p.winnings_id
),
paid_point_winnings AS (
  SELECT
    TRIM(w.external_id) AS challenge_id,
    COALESCE(NULLIF(TRIM(w.title), ''), NULLIF(TRIM(w.description), ''), TRIM(w.external_id)) AS challenge_name,
    w.description,
    w.winner_id::bigint AS user_id,
    p.total_amount::numeric AS points_amount,
    COALESCE(
      substring(w.description from ' - (?:Checkpoint )?([0-9]+)(?:st|nd|rd|th) Place$'),
      substring(w.title from ' - (?:Checkpoint )?([0-9]+)(?:st|nd|rd|th) Place$')
    )::integer AS parsed_placement
  FROM public.winnings w
  INNER JOIN latest_payment_version lpv
    ON lpv.winnings_id = w.winning_id
  INNER JOIN public.payment p
    ON p.winnings_id = w.winning_id
   AND p.version = lpv.max_version
   AND COALESCE(p.installment_number, 1) = 1
  WHERE w.type::text = 'POINTS'
    AND w.category::text = 'POINTS_AWARD'
    AND p.payment_status::text IN ('CREDITED', 'PAID')
    AND UPPER(COALESCE(p.currency, '')) = 'POINT'
    AND w.external_id IS NOT NULL
    AND TRIM(w.external_id) <> ''
    AND w.winner_id ~ '^[0-9]+$'
    AND COALESCE(p.total_amount, 0) > 0
)
SELECT
  challenge_id,
  MAX(challenge_name) AS challenge_name,
  user_id,
  MIN(parsed_placement) AS placement,
  GREATEST(0, ROUND(SUM(points_amount)))::integer AS points,
  CASE
    WHEN MIN(parsed_placement) IS NULL THEN NULL
    ELSE 'finance_description'
  END AS placement_source
FROM paid_point_winnings
GROUP BY challenge_id, user_id;

DO $$
BEGIN
  IF to_regclass('public."ChallengeWinner"') IS NOT NULL THEN
    IF to_regclass('public."Challenge"') IS NOT NULL THEN
      EXECUTE $sql$
        WITH ranked_winners AS (
          SELECT DISTINCT ON (cw."challengeId", cw."userId")
            cw."challengeId" AS challenge_id,
            cw."userId"::bigint AS user_id,
            cw."placement" AS placement,
            c."name" AS challenge_name
          FROM public."ChallengeWinner" cw
          LEFT JOIN public."Challenge" c
            ON c."id" = cw."challengeId"
          WHERE cw."type"::text IN ('PLACEMENT', 'CHECKPOINT')
          ORDER BY
            cw."challengeId",
            cw."userId",
            CASE WHEN cw."type"::text = 'PLACEMENT' THEN 0 ELSE 1 END,
            cw."placement"
        )
        UPDATE _finance_member_challenge_points_backfill target
        SET
          placement = ranked_winners.placement,
          challenge_name = COALESCE(NULLIF(TRIM(ranked_winners.challenge_name), ''), target.challenge_name),
          placement_source = 'challenge_winner'
        FROM ranked_winners
        WHERE ranked_winners.challenge_id = target.challenge_id
          AND ranked_winners.user_id = target.user_id
      $sql$;
    ELSE
      EXECUTE $sql$
        WITH ranked_winners AS (
          SELECT DISTINCT ON (cw."challengeId", cw."userId")
            cw."challengeId" AS challenge_id,
            cw."userId"::bigint AS user_id,
            cw."placement" AS placement
          FROM public."ChallengeWinner" cw
          WHERE cw."type"::text IN ('PLACEMENT', 'CHECKPOINT')
          ORDER BY
            cw."challengeId",
            cw."userId",
            CASE WHEN cw."type"::text = 'PLACEMENT' THEN 0 ELSE 1 END,
            cw."placement"
        )
        UPDATE _finance_member_challenge_points_backfill target
        SET
          placement = ranked_winners.placement,
          placement_source = 'challenge_winner'
        FROM ranked_winners
        WHERE ranked_winners.challenge_id = target.challenge_id
          AND ranked_winners.user_id = target.user_id
      $sql$;
    END IF;
  END IF;
END $$;

WITH inferred_placements AS (
  SELECT
    challenge_id,
    user_id,
    ROW_NUMBER() OVER (
      PARTITION BY challenge_id
      ORDER BY points DESC, user_id ASC
    )::integer AS inferred_placement
  FROM _finance_member_challenge_points_backfill
  WHERE placement IS NULL
)
UPDATE _finance_member_challenge_points_backfill target
SET
  placement = inferred_placements.inferred_placement,
  placement_source = 'points_rank'
FROM inferred_placements
WHERE inferred_placements.challenge_id = target.challenge_id
  AND inferred_placements.user_id = target.user_id;

\echo Backfill source summary
SELECT
  COUNT(*) AS source_rows,
  COUNT(*) FILTER (WHERE placement_source = 'challenge_winner') AS challenge_winner_rows,
  COUNT(*) FILTER (WHERE placement_source = 'finance_description') AS finance_description_rows,
  COUNT(*) FILTER (WHERE placement_source = 'points_rank') AS points_rank_rows,
  SUM(points) AS total_points
FROM _finance_member_challenge_points_backfill;

\echo Rows skipped because the member does not exist in members.member
SELECT
  source.challenge_id AS "challengeId",
  source.challenge_name AS "challengeName",
  source.user_id AS "userId",
  source.placement,
  source.points
FROM _finance_member_challenge_points_backfill source
LEFT JOIN members."member" member_row
  ON member_row."userId" = source.user_id
WHERE member_row."userId" IS NULL
ORDER BY source.challenge_id, source.user_id
LIMIT 50;

\echo Rows using inferred placement from point totals. Review these after the run.
SELECT
  challenge_id AS "challengeId",
  challenge_name AS "challengeName",
  user_id AS "userId",
  placement,
  points
FROM _finance_member_challenge_points_backfill
WHERE placement_source = 'points_rank'
ORDER BY challenge_id, placement
LIMIT 50;

INSERT INTO members."memberChallengePoints" (
  "challengeId",
  "challengeName",
  "userId",
  "placement",
  "points",
  "createdAt",
  "createdBy",
  "updatedAt",
  "updatedBy"
)
SELECT
  source.challenge_id,
  source.challenge_name,
  source.user_id,
  source.placement,
  source.points,
  NOW(),
  'finance-points-backfill',
  NOW(),
  'finance-points-backfill'
FROM _finance_member_challenge_points_backfill source
INNER JOIN members."member" member_row
  ON member_row."userId" = source.user_id
WHERE source.points > 0
  AND source.placement IS NOT NULL
ON CONFLICT ("challengeId", "userId") DO UPDATE
SET
  "challengeName" = EXCLUDED."challengeName",
  "placement" = EXCLUDED."placement",
  "points" = EXCLUDED."points",
  "updatedAt" = NOW(),
  "updatedBy" = 'finance-points-backfill';

\echo Backfilled member challenge point rows
SELECT
  COUNT(*) AS rows_in_member_api,
  SUM("points") AS points_in_member_api
FROM members."memberChallengePoints"
WHERE "updatedBy" = 'finance-points-backfill';

COMMIT;

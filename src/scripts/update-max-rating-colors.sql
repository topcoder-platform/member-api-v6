-- Recalculate maxRating.ratingColor using the legacy member-api thresholds.
-- Legacy mapping:
--   rating <  900  -> #9D9FA0
--   rating < 1200  -> #69C329
--   rating < 1500  -> #616BD5
--   rating < 2200  -> #FCD617
--   rating >= 2200 -> #EF3A3A

BEGIN;

UPDATE "members"."memberMaxRating" AS mmr
SET "ratingColor" = CASE
  WHEN mmr."rating" < 900 THEN '#9D9FA0'
  WHEN mmr."rating" < 1200 THEN '#69C329'
  WHEN mmr."rating" < 1500 THEN '#616BD5'
  WHEN mmr."rating" < 2200 THEN '#FCD617'
  ELSE '#EF3A3A'
END
WHERE mmr."ratingColor" IS DISTINCT FROM CASE
  WHEN mmr."rating" < 900 THEN '#9D9FA0'
  WHEN mmr."rating" < 1200 THEN '#69C329'
  WHEN mmr."rating" < 1500 THEN '#616BD5'
  WHEN mmr."rating" < 2200 THEN '#FCD617'
  ELSE '#EF3A3A'
END;

COMMIT;

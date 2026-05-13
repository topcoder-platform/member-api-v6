-- Partial B-tree index for homeCountryCode lookups (only indexes non-NULL rows)
CREATE INDEX "idx_member_home_country_code"
  ON members."member" ("homeCountryCode")
  WHERE "homeCountryCode" IS NOT NULL;

-- Partial B-tree index for competitionCountryCode lookups (only indexes non-NULL rows)
CREATE INDEX "idx_member_competition_country_code"
  ON members."member" ("competitionCountryCode")
  WHERE "competitionCountryCode" IS NOT NULL;

-- Functional index for case-insensitive country matching via UPPER(country)
-- Not representable in Prisma schema; managed as a raw migration.
CREATE INDEX "idx_member_country_upper"
  ON members."member" (UPPER("country"))
  WHERE "country" IS NOT NULL;

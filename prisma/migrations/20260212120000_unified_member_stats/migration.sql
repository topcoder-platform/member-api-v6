-- DropForeignKey
ALTER TABLE "members"."memberStats" DROP CONSTRAINT "memberStats_memberRatingId_fkey";

-- DropIndex
DROP INDEX "members"."memberStats_userId_groupId_idx";

-- AlterTable
ALTER TABLE "members"."memberStats" DROP COLUMN "groupId",
DROP COLUMN "memberRatingId",
ADD COLUMN     "avgNumSubmissions" INTEGER,
ADD COLUMN     "avgRank" DOUBLE PRECISION,
ADD COLUMN     "bestRank" INTEGER,
ADD COLUMN     "countryRank" INTEGER,
ADD COLUMN     "globalRank" INTEGER,
ADD COLUMN     "maxRating" INTEGER,
ADD COLUMN     "minRating" INTEGER,
ADD COLUMN     "mostRecentEventDate" TIMESTAMP(3),
ADD COLUMN     "mostRecentSubmission" TIMESTAMP(3),
ADD COLUMN     "rating" INTEGER,
ADD COLUMN     "schoolRank" INTEGER,
ADD COLUMN     "topFiveFinishes" INTEGER,
ADD COLUMN     "topTenFinishes" INTEGER,
ADD COLUMN     "trackId" TEXT,
ADD COLUMN     "typeId" TEXT,
ADD COLUMN     "volatility" INTEGER;

-- ValidateBackfillPrerequisites
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "members"."memberStats") THEN
    IF NOT EXISTS (SELECT 1 FROM "challenges"."ChallengeTrack") THEN
      RAISE EXCEPTION 'Cannot backfill members.memberStats.trackId because challenges.ChallengeTrack has no rows.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM "challenges"."ChallengeType") THEN
      RAISE EXCEPTION 'Cannot backfill members.memberStats.typeId because challenges.ChallengeType has no rows.';
    END IF;
  END IF;
END $$;

-- BackfillExistingMemberStatsTrackAndType
WITH defaults AS (
  SELECT
    COALESCE(
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."name") = 'development' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."abbreviation") = 'dev' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct ORDER BY ct."createdAt" ASC LIMIT 1)
    ) AS develop_track_id,
    COALESCE(
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."name") = 'design' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."abbreviation") = 'des' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct ORDER BY ct."createdAt" ASC LIMIT 1)
    ) AS design_track_id,
    COALESCE(
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."name") = 'data science' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct WHERE LOWER(ct."abbreviation") = 'ds' ORDER BY ct."createdAt" ASC LIMIT 1),
      (SELECT ct."id" FROM "challenges"."ChallengeTrack" ct ORDER BY ct."createdAt" ASC LIMIT 1)
    ) AS data_science_track_id,
    COALESCE(
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty WHERE LOWER(cty."name") = 'challenge' ORDER BY cty."createdAt" ASC LIMIT 1),
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty WHERE LOWER(cty."abbreviation") = 'ch' ORDER BY cty."createdAt" ASC LIMIT 1),
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty ORDER BY cty."createdAt" ASC LIMIT 1)
    ) AS challenge_type_id,
    COALESCE(
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty WHERE LOWER(cty."name") = 'marathon match' ORDER BY cty."createdAt" ASC LIMIT 1),
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty WHERE LOWER(cty."abbreviation") = 'mm' ORDER BY cty."createdAt" ASC LIMIT 1),
      (SELECT cty."id" FROM "challenges"."ChallengeType" cty ORDER BY cty."createdAt" ASC LIMIT 1)
    ) AS marathon_type_id
)
UPDATE "members"."memberStats" ms
SET
  "trackId" = COALESCE(
    ms."trackId",
    CASE
      WHEN EXISTS (SELECT 1 FROM "members"."memberDesignStats" mdes WHERE mdes."memberStatsId" = ms."id")
        THEN d.design_track_id
      WHEN EXISTS (SELECT 1 FROM "members"."memberDataScienceStats" mds WHERE mds."memberStatsId" = ms."id")
        THEN d.data_science_track_id
      WHEN EXISTS (SELECT 1 FROM "members"."memberCopilotStats" mcs WHERE mcs."memberStatsId" = ms."id")
        THEN d.develop_track_id
      WHEN EXISTS (SELECT 1 FROM "members"."memberDevelopStats" mdev WHERE mdev."memberStatsId" = ms."id")
        THEN d.develop_track_id
      ELSE d.develop_track_id
    END
  ),
  "typeId" = COALESCE(
    ms."typeId",
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM "members"."memberDataScienceStats" mds
        INNER JOIN "members"."memberMarathonStats" mm ON mm."dataScienceStatsId" = mds."id"
        WHERE mds."memberStatsId" = ms."id"
      ) THEN d.marathon_type_id
      ELSE d.challenge_type_id
    END
  )
FROM defaults d
WHERE ms."trackId" IS NULL OR ms."typeId" IS NULL;

-- AlterTable
ALTER TABLE "members"."memberStats"
ALTER COLUMN "trackId" SET NOT NULL,
ALTER COLUMN "typeId" SET NOT NULL;

-- CreateTable
CREATE TABLE "members"."memberStatsHistory" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "trackId" TEXT NOT NULL,
    "typeId" TEXT NOT NULL,
    "challengeId" TEXT NOT NULL,
    "oldRating" INTEGER,
    "newRating" INTEGER,
    "oldGlobalRank" INTEGER,
    "newGlobalRank" INTEGER,
    "oldCountryRank" INTEGER,
    "newCountryRank" INTEGER,
    "oldSchoolRank" INTEGER,
    "newSchoolRank" INTEGER,
    "eventDate" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberStatsHistory_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "memberStatsHistory_userId_idx" ON "members"."memberStatsHistory"("userId");

-- CreateIndex
CREATE INDEX "memberStatsHistory_userId_trackId_typeId_idx" ON "members"."memberStatsHistory"("userId", "trackId", "typeId");

-- CreateIndex
CREATE INDEX "memberStatsHistory_challengeId_idx" ON "members"."memberStatsHistory"("challengeId");

-- CreateIndex
CREATE INDEX "memberStatsHistory_eventDate_idx" ON "members"."memberStatsHistory"("eventDate");

-- CreateIndex
CREATE INDEX "memberStats_trackId_idx" ON "members"."memberStats"("trackId");

-- CreateIndex
CREATE INDEX "memberStats_typeId_idx" ON "members"."memberStats"("typeId");

-- CreateIndex
CREATE INDEX "memberStats_userId_trackId_typeId_idx" ON "members"."memberStats"("userId", "trackId", "typeId");

-- CreateIndex
CREATE INDEX "memberStats_globalRank_idx" ON "members"."memberStats"("globalRank");

-- CreateIndex
CREATE INDEX "memberStats_countryRank_idx" ON "members"."memberStats"("countryRank");

-- CreateIndex
CREATE INDEX "memberStats_schoolRank_idx" ON "members"."memberStats"("schoolRank");

-- CreateIndex
CREATE UNIQUE INDEX "memberStats_userId_trackId_typeId_key" ON "members"."memberStats"("userId", "trackId", "typeId");

-- AddForeignKey
ALTER TABLE "members"."memberStatsHistory" ADD CONSTRAINT "memberStatsHistory_userId_fkey" FOREIGN KEY ("userId") REFERENCES "members"."member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "members"."memberStats" ADD CONSTRAINT "memberStats_trackId_fkey" FOREIGN KEY ("trackId") REFERENCES "challenges"."ChallengeTrack"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "members"."memberStats" ADD CONSTRAINT "memberStats_typeId_fkey" FOREIGN KEY ("typeId") REFERENCES "challenges"."ChallengeType"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

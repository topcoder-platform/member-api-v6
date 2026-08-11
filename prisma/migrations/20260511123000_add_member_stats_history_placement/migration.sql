-- AlterTable
ALTER TABLE "members"."memberStatsHistory"
ADD COLUMN     "placement" INTEGER,
ADD COLUMN     "percentile" DOUBLE PRECISION;

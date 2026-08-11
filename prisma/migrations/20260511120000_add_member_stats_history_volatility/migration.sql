-- AlterTable
ALTER TABLE "members"."memberStatsHistory"
ADD COLUMN     "oldVolatility" INTEGER,
ADD COLUMN     "newVolatility" INTEGER;

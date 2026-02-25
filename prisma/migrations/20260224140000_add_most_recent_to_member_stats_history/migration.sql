-- AlterTable
ALTER TABLE "members"."memberStatsHistory"
ADD COLUMN     "mostRecent" BOOLEAN NOT NULL DEFAULT false;

-- CreateIndex
CREATE INDEX "memberStatsHistory_userId_trackId_typeId_mostRecent_idx"
ON "members"."memberStatsHistory"("userId", "trackId", "typeId", "mostRecent");

-- CreateIndex
CREATE UNIQUE INDEX "memberStatsHistory_userId_trackId_typeId_mostRecent_true_key"
ON "members"."memberStatsHistory"("userId", "trackId", "typeId")
WHERE "mostRecent" = true;

-- CreateIndex
CREATE INDEX "memberStats_trackId_typeId_rating_idx" ON "members"."memberStats"("trackId", "typeId", "rating");

-- Generated via prisma migrate diff because local database access is unavailable.
CREATE INDEX "member_status_handleLower_idx" ON "members"."member"("status", "handleLower");
CREATE INDEX "member_lastLoginDate_idx" ON "members"."member"("lastLoginDate");
CREATE INDEX "member_availableForGigs_idx" ON "members"."member"("availableForGigs");
CREATE INDEX "memberStats_userId_groupId_idx" ON "members"."memberStats"("userId", "groupId");
CREATE INDEX "memberHistoryStats_userId_isPrivate_idx" ON "members"."memberHistoryStats"("userId", "isPrivate");
CREATE INDEX "memberAddress_userId_type_idx" ON "members"."memberAddress"("userId", "type");

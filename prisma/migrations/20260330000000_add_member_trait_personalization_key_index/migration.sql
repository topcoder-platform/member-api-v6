-- CreateIndex: composite index on (memberTraitId, key) to speed up
-- EXISTS lookups that filter by userId (via memberTraits join) and key value.
CREATE INDEX "memberTraitPersonalization_memberTraitId_key_idx"
ON "members"."memberTraitPersonalization"("memberTraitId", "key");

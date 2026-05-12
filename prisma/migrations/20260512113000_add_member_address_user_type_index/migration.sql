-- CreateIndex
CREATE INDEX idx_member_address_user_type ON members."memberAddress"("userId", type, id DESC);

-- CreateIndex
CREATE INDEX "member_status_availableForGigs_idx" ON members."member"("status", "availableForGigs");

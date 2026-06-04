CREATE TABLE "members"."memberChallengePoints" (
  "id" BIGSERIAL NOT NULL,
  "challengeId" TEXT NOT NULL,
  "challengeName" TEXT NOT NULL,
  "userId" BIGINT NOT NULL,
  "placement" INTEGER NOT NULL,
  "points" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "createdBy" TEXT NOT NULL,
  "updatedAt" TIMESTAMP(3),
  "updatedBy" TEXT,

  CONSTRAINT "memberChallengePoints_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "memberChallengePoints_challengeId_userId_key"
  ON "members"."memberChallengePoints"("challengeId", "userId");

CREATE INDEX "memberChallengePoints_userId_idx"
  ON "members"."memberChallengePoints"("userId");

CREATE INDEX "memberChallengePoints_challengeId_idx"
  ON "members"."memberChallengePoints"("challengeId");

ALTER TABLE "members"."memberChallengePoints"
  ADD CONSTRAINT "memberChallengePoints_userId_fkey"
  FOREIGN KEY ("userId") REFERENCES "members"."member"("userId")
  ON DELETE CASCADE ON UPDATE CASCADE;

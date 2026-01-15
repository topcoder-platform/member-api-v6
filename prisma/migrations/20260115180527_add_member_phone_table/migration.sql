-- CreateTable
CREATE TABLE "members"."memberPhone" (
    "id" TEXT NOT NULL,
    "userId" BIGINT NOT NULL,
    "type" TEXT NOT NULL,
    "number" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberPhone_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "memberPhone_userId_idx" ON "members"."memberPhone"("userId");

-- AddForeignKey
ALTER TABLE "members"."memberPhone" ADD CONSTRAINT "memberPhone_userId_fkey" FOREIGN KEY ("userId") REFERENCES "members"."member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

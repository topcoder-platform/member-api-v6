-- AlterEnum
ALTER TYPE "members"."WorkIndustryType" ADD VALUE 'Other';

-- AlterTable
ALTER TABLE "members"."memberTraitWork" ADD COLUMN "otherIndustry" TEXT;

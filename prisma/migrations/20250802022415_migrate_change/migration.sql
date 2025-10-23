-- AlterEnum
ALTER TYPE "DeviceType" ADD VALUE 'Other';

-- AlterTable
ALTER TABLE "member" ADD COLUMN     "country" TEXT;

-- AlterTable
ALTER TABLE "memberTraitDevice" ADD COLUMN     "osLanguage" TEXT,
ADD COLUMN     "osVersion" TEXT;

-- AlterTable
ALTER TABLE "memberTraitOnboardChecklist" ADD COLUMN     "skip" BOOLEAN;

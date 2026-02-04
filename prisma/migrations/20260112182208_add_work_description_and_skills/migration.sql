/*
  Warnings:

  - You are about to drop the `displayMode` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `memberSkill` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `memberSkillLevel` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `skill` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `skillCategory` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `skillLevel` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "members"."memberSkill" DROP CONSTRAINT "memberSkill_displayModeId_fkey";

-- DropForeignKey
ALTER TABLE "members"."memberSkill" DROP CONSTRAINT "memberSkill_skillId_fkey";

-- DropForeignKey
ALTER TABLE "members"."memberSkill" DROP CONSTRAINT "memberSkill_userId_fkey";

-- DropForeignKey
ALTER TABLE "members"."memberSkillLevel" DROP CONSTRAINT "memberSkillLevel_memberSkillId_fkey";

-- DropForeignKey
ALTER TABLE "members"."memberSkillLevel" DROP CONSTRAINT "memberSkillLevel_skillLevelId_fkey";

-- DropForeignKey
ALTER TABLE "members"."skill" DROP CONSTRAINT "skill_categoryId_fkey";

-- AlterTable
ALTER TABLE "members"."memberTraitWork" ADD COLUMN     "associatedSkills" TEXT[],
ADD COLUMN     "description" TEXT;

-- DropTable
DROP TABLE "members"."displayMode";

-- DropTable
DROP TABLE "members"."memberSkill";

-- DropTable
DROP TABLE "members"."memberSkillLevel";

-- DropTable
DROP TABLE "members"."skill";

-- DropTable
DROP TABLE "members"."skillCategory";

-- DropTable
DROP TABLE "members"."skillLevel";

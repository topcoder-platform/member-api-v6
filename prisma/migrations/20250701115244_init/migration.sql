-- CreateEnum
CREATE TYPE "MemberStatus" AS ENUM ('UNVERIFIED', 'ACTIVE', 'INACTIVE_USER_REQUEST', 'INACTIVE_DUPLICATE_ACCOUNT', 'INACTIVE_IRREGULAR_ACCOUNT', 'UNKNOWN');

-- CreateEnum
CREATE TYPE "FinancialStatus" AS ENUM ('PENDING', 'PAID', 'FAILED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "DeviceType" AS ENUM ('Console', 'Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Wearable');

-- CreateEnum
CREATE TYPE "SoftwareType" AS ENUM ('DeveloperTools', 'Browser', 'Productivity', 'GraphAndDesign', 'Utilities');

-- CreateEnum
CREATE TYPE "ServiceProviderType" AS ENUM ('InternetServiceProvider', 'MobileCarrier', 'Television', 'FinancialInstitution', 'Other');

-- CreateEnum
CREATE TYPE "WorkIndustryType" AS ENUM ('Banking', 'ConsumerGoods', 'Energy', 'Entertainment', 'HealthCare', 'Pharma', 'PublicSector', 'TechAndTechnologyService', 'Telecoms', 'TravelAndHospitality');

-- CreateTable
CREATE TABLE "member" (
    "userId" BIGINT NOT NULL,
    "handle" TEXT NOT NULL,
    "handleLower" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "verified" BOOLEAN,
    "skillScore" DOUBLE PRECISION,
    "memberRatingId" BIGINT,
    "firstName" TEXT,
    "lastName" TEXT,
    "description" TEXT,
    "otherLangName" TEXT,
    "status" "MemberStatus",
    "newEmail" TEXT,
    "emailVerifyToken" TEXT,
    "emailVerifyTokenDate" TIMESTAMP(3),
    "newEmailVerifyToken" TEXT,
    "newEmailVerifyTokenDate" TIMESTAMP(3),
    "homeCountryCode" TEXT,
    "competitionCountryCode" TEXT,
    "photoURL" TEXT,
    "tracks" TEXT[],
    "loginCount" INTEGER,
    "lastLoginDate" TIMESTAMP(3),
    "availableForGigs" BOOLEAN,
    "skillScoreDeduction" DOUBLE PRECISION,
    "namesAndHandleAppearance" TEXT,
    "aggregatedSkills" JSONB,
    "enteredSkills" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "member_pkey" PRIMARY KEY ("userId")
);

-- CreateTable
CREATE TABLE "memberAddress" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "streetAddr1" TEXT,
    "streetAddr2" TEXT,
    "city" TEXT,
    "zip" TEXT,
    "stateCode" TEXT,
    "type" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberAddress_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberMaxRating" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "rating" INTEGER NOT NULL,
    "track" TEXT NOT NULL,
    "subTrack" TEXT NOT NULL,
    "ratingColor" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberMaxRating_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "distributionStats" (
    "id" BIGSERIAL NOT NULL,
    "track" TEXT NOT NULL,
    "subTrack" TEXT NOT NULL,
    "ratingRange0To099" INTEGER NOT NULL,
    "ratingRange100To199" INTEGER NOT NULL,
    "ratingRange200To299" INTEGER NOT NULL,
    "ratingRange300To399" INTEGER NOT NULL,
    "ratingRange400To499" INTEGER NOT NULL,
    "ratingRange500To599" INTEGER NOT NULL,
    "ratingRange600To699" INTEGER NOT NULL,
    "ratingRange700To799" INTEGER NOT NULL,
    "ratingRange800To899" INTEGER NOT NULL,
    "ratingRange900To999" INTEGER NOT NULL,
    "ratingRange1000To1099" INTEGER NOT NULL,
    "ratingRange1100To1199" INTEGER NOT NULL,
    "ratingRange1200To1299" INTEGER NOT NULL,
    "ratingRange1300To1399" INTEGER NOT NULL,
    "ratingRange1400To1499" INTEGER NOT NULL,
    "ratingRange1500To1599" INTEGER NOT NULL,
    "ratingRange1600To1699" INTEGER NOT NULL,
    "ratingRange1700To1799" INTEGER NOT NULL,
    "ratingRange1800To1899" INTEGER NOT NULL,
    "ratingRange1900To1999" INTEGER NOT NULL,
    "ratingRange2000To2099" INTEGER NOT NULL,
    "ratingRange2100To2199" INTEGER NOT NULL,
    "ratingRange2200To2299" INTEGER NOT NULL,
    "ratingRange2300To2399" INTEGER NOT NULL,
    "ratingRange2400To2499" INTEGER NOT NULL,
    "ratingRange2500To2599" INTEGER NOT NULL,
    "ratingRange2600To2699" INTEGER NOT NULL,
    "ratingRange2700To2799" INTEGER NOT NULL,
    "ratingRange2800To2899" INTEGER NOT NULL,
    "ratingRange2900To2999" INTEGER NOT NULL,
    "ratingRange3000To3099" INTEGER NOT NULL,
    "ratingRange3100To3199" INTEGER NOT NULL,
    "ratingRange3200To3299" INTEGER NOT NULL,
    "ratingRange3300To3399" INTEGER NOT NULL,
    "ratingRange3400To3499" INTEGER NOT NULL,
    "ratingRange3500To3599" INTEGER NOT NULL,
    "ratingRange3600To3699" INTEGER NOT NULL,
    "ratingRange3700To3799" INTEGER NOT NULL,
    "ratingRange3800To3899" INTEGER NOT NULL,
    "ratingRange3900To3999" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "distributionStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberFinancial" (
    "userId" BIGINT NOT NULL,
    "amount" DOUBLE PRECISION NOT NULL,
    "status" "FinancialStatus" NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberFinancial_pkey" PRIMARY KEY ("userId")
);

-- CreateTable
CREATE TABLE "memberHistoryStats" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "groupId" BIGINT,
    "isPrivate" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberHistoryStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDevelopHistoryStats" (
    "id" BIGSERIAL NOT NULL,
    "historyStatsId" BIGINT NOT NULL,
    "challengeId" BIGINT NOT NULL,
    "challengeName" TEXT NOT NULL,
    "ratingDate" TIMESTAMP(3) NOT NULL,
    "newRating" INTEGER NOT NULL,
    "subTrack" TEXT NOT NULL,
    "subTrackId" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDevelopHistoryStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDataScienceHistoryStats" (
    "id" BIGSERIAL NOT NULL,
    "historyStatsId" BIGINT NOT NULL,
    "challengeId" BIGINT NOT NULL,
    "challengeName" TEXT NOT NULL,
    "date" TIMESTAMP(3) NOT NULL,
    "rating" INTEGER NOT NULL,
    "placement" INTEGER NOT NULL,
    "percentile" DOUBLE PRECISION NOT NULL,
    "subTrack" TEXT NOT NULL,
    "subTrackId" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDataScienceHistoryStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberStats" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "memberRatingId" BIGINT,
    "challenges" INTEGER,
    "wins" INTEGER,
    "groupId" BIGINT,
    "isPrivate" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberCopilotStats" (
    "id" BIGSERIAL NOT NULL,
    "memberStatsId" BIGINT NOT NULL,
    "contests" INTEGER NOT NULL,
    "projects" INTEGER NOT NULL,
    "failures" INTEGER NOT NULL,
    "reposts" INTEGER NOT NULL,
    "activeContests" INTEGER NOT NULL,
    "activeProjects" INTEGER NOT NULL,
    "fulfillment" DOUBLE PRECISION NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberCopilotStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDevelopStats" (
    "id" BIGSERIAL NOT NULL,
    "memberStatsId" BIGINT NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDevelopStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDevelopStatsItem" (
    "id" BIGSERIAL NOT NULL,
    "developStatsId" BIGINT NOT NULL,
    "name" TEXT NOT NULL,
    "subTrackId" INTEGER NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "numInquiries" BIGINT,
    "submissions" BIGINT,
    "passedScreening" BIGINT,
    "passedReview" BIGINT,
    "appeals" BIGINT,
    "submissionRate" DOUBLE PRECISION,
    "screeningSuccessRate" DOUBLE PRECISION,
    "reviewSuccessRate" DOUBLE PRECISION,
    "appealSuccessRate" DOUBLE PRECISION,
    "minScore" DOUBLE PRECISION,
    "maxScore" DOUBLE PRECISION,
    "avgScore" DOUBLE PRECISION,
    "avgPlacement" DOUBLE PRECISION,
    "winPercent" DOUBLE PRECISION,
    "rating" INTEGER,
    "minRating" INTEGER,
    "maxRating" INTEGER,
    "volatility" INTEGER,
    "reliability" DOUBLE PRECISION,
    "overallRank" INTEGER,
    "overallSchoolRank" INTEGER,
    "overallCountryRank" INTEGER,
    "overallPercentile" DOUBLE PRECISION,
    "activeRank" INTEGER,
    "activeSchoolRank" INTEGER,
    "activeCountryRank" INTEGER,
    "activePercentile" DOUBLE PRECISION,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDevelopStatsItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDesignStats" (
    "id" BIGSERIAL NOT NULL,
    "memberStatsId" BIGINT NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDesignStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDesignStatsItem" (
    "id" BIGSERIAL NOT NULL,
    "designStatsId" BIGINT NOT NULL,
    "name" TEXT NOT NULL,
    "subTrackId" INTEGER NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "numInquiries" INTEGER NOT NULL,
    "submissions" INTEGER NOT NULL,
    "passedScreening" INTEGER NOT NULL,
    "avgPlacement" DOUBLE PRECISION NOT NULL,
    "screeningSuccessRate" DOUBLE PRECISION NOT NULL,
    "submissionRate" DOUBLE PRECISION NOT NULL,
    "winPercent" DOUBLE PRECISION NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDesignStatsItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberDataScienceStats" (
    "id" BIGSERIAL NOT NULL,
    "memberStatsId" BIGINT NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "mostRecentEventName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberDataScienceStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberSrmStats" (
    "id" BIGSERIAL NOT NULL,
    "dataScienceStatsId" BIGINT NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "mostRecentEventName" TEXT,
    "rating" INTEGER NOT NULL,
    "percentile" DOUBLE PRECISION NOT NULL,
    "rank" INTEGER NOT NULL,
    "countryRank" INTEGER NOT NULL,
    "schoolRank" INTEGER NOT NULL,
    "volatility" INTEGER NOT NULL,
    "maximumRating" INTEGER NOT NULL,
    "minimumRating" INTEGER NOT NULL,
    "defaultLanguage" TEXT NOT NULL,
    "competitions" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberSrmStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberSrmChallengeDetail" (
    "id" BIGSERIAL NOT NULL,
    "srmStatsId" BIGINT NOT NULL,
    "challenges" INTEGER NOT NULL,
    "failedChallenges" INTEGER NOT NULL,
    "levelName" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberSrmChallengeDetail_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberSrmDivisionDetail" (
    "id" BIGSERIAL NOT NULL,
    "srmStatsId" BIGINT NOT NULL,
    "problemsSubmitted" INTEGER NOT NULL,
    "problemsSysByTest" INTEGER NOT NULL,
    "problemsFailed" INTEGER NOT NULL,
    "levelName" TEXT NOT NULL,
    "divisionName" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberSrmDivisionDetail_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberMarathonStats" (
    "id" BIGSERIAL NOT NULL,
    "dataScienceStatsId" BIGINT NOT NULL,
    "challenges" BIGINT,
    "wins" BIGINT,
    "mostRecentSubmission" TIMESTAMP(3),
    "mostRecentEventDate" TIMESTAMP(3),
    "mostRecentEventName" TEXT,
    "rating" INTEGER NOT NULL,
    "percentile" DOUBLE PRECISION NOT NULL,
    "rank" INTEGER NOT NULL,
    "avgRank" DOUBLE PRECISION NOT NULL,
    "avgNumSubmissions" INTEGER NOT NULL,
    "bestRank" INTEGER NOT NULL,
    "countryRank" INTEGER NOT NULL,
    "schoolRank" INTEGER NOT NULL,
    "volatility" INTEGER NOT NULL,
    "maximumRating" INTEGER NOT NULL,
    "minimumRating" INTEGER NOT NULL,
    "defaultLanguage" TEXT NOT NULL,
    "competitions" INTEGER NOT NULL,
    "topFiveFinishes" INTEGER NOT NULL,
    "topTenFinishes" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberMarathonStats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraits" (
    "id" BIGSERIAL NOT NULL,
    "userId" BIGINT NOT NULL,
    "subscriptions" TEXT[],
    "hobby" TEXT[],
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraits_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitDevice" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "deviceType" "DeviceType" NOT NULL,
    "manufacturer" TEXT NOT NULL,
    "model" TEXT NOT NULL,
    "operatingSystem" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitDevice_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitSoftware" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "softwareType" "SoftwareType" NOT NULL,
    "name" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitSoftware_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitServiceProvider" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "type" "ServiceProviderType" NOT NULL,
    "name" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitServiceProvider_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitWork" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "industry" "WorkIndustryType",
    "companyName" TEXT NOT NULL,
    "position" TEXT NOT NULL,
    "startDate" TIMESTAMP(3),
    "endDate" TIMESTAMP(3),
    "working" BOOLEAN,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitWork_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitEducation" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "collegeName" TEXT NOT NULL,
    "degree" TEXT NOT NULL,
    "endYear" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitEducation_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitBasicInfo" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "userId" BIGINT NOT NULL,
    "country" TEXT NOT NULL,
    "primaryInterestInTopcoder" TEXT NOT NULL,
    "tshirtSize" TEXT,
    "gender" TEXT,
    "shortBio" TEXT NOT NULL,
    "birthDate" TIMESTAMP(3),
    "currentLocation" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitBasicInfo_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitLanguage" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "language" TEXT NOT NULL,
    "spokenLevel" TEXT,
    "writtenLevel" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitLanguage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitOnboardChecklist" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "listItemType" TEXT NOT NULL,
    "date" TIMESTAMP(3) NOT NULL,
    "message" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "metadata" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitOnboardChecklist_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitPersonalization" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "key" TEXT,
    "value" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitPersonalization_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberTraitCommunity" (
    "id" BIGSERIAL NOT NULL,
    "memberTraitId" BIGINT NOT NULL,
    "communityName" TEXT NOT NULL,
    "status" BOOLEAN NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberTraitCommunity_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "skillCategory" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "skillCategory_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "skill" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "categoryId" UUID NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "skill_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "displayMode" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "displayMode_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "skillLevel" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "skillLevel_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberSkill" (
    "id" UUID NOT NULL,
    "userId" BIGINT NOT NULL,
    "skillId" UUID NOT NULL,
    "displayModeId" UUID,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberSkill_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "memberSkillLevel" (
    "memberSkillId" UUID NOT NULL,
    "skillLevelId" UUID NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdBy" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3),
    "updatedBy" TEXT,

    CONSTRAINT "memberSkillLevel_pkey" PRIMARY KEY ("memberSkillId","skillLevelId")
);

-- CreateIndex
CREATE UNIQUE INDEX "member_handleLower_key" ON "member"("handleLower");

-- CreateIndex
CREATE UNIQUE INDEX "member_email_key" ON "member"("email");

-- CreateIndex
CREATE INDEX "member_handleLower_idx" ON "member"("handleLower");

-- CreateIndex
CREATE INDEX "member_email_idx" ON "member"("email");

-- CreateIndex
CREATE INDEX "memberAddress_userId_idx" ON "memberAddress"("userId");

-- CreateIndex
CREATE INDEX "memberMaxRating_userId_idx" ON "memberMaxRating"("userId");

-- CreateIndex
CREATE UNIQUE INDEX "memberMaxRating_userId_key" ON "memberMaxRating"("userId");

-- CreateIndex
CREATE UNIQUE INDEX "distributionStats_track_subTrack_key" ON "distributionStats"("track", "subTrack");

-- CreateIndex
CREATE INDEX "memberHistoryStats_userId_idx" ON "memberHistoryStats"("userId");

-- CreateIndex
CREATE INDEX "memberHistoryStats_groupId_idx" ON "memberHistoryStats"("groupId");

-- CreateIndex
CREATE INDEX "memberDevelopHistoryStats_historyStatsId_idx" ON "memberDevelopHistoryStats"("historyStatsId");

-- CreateIndex
CREATE INDEX "memberDataScienceHistoryStats_historyStatsId_idx" ON "memberDataScienceHistoryStats"("historyStatsId");

-- CreateIndex
CREATE INDEX "memberStats_userId_idx" ON "memberStats"("userId");

-- CreateIndex
CREATE INDEX "memberCopilotStats_memberStatsId_idx" ON "memberCopilotStats"("memberStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberCopilotStats_memberStatsId_key" ON "memberCopilotStats"("memberStatsId");

-- CreateIndex
CREATE INDEX "memberDevelopStats_memberStatsId_idx" ON "memberDevelopStats"("memberStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberDevelopStats_memberStatsId_key" ON "memberDevelopStats"("memberStatsId");

-- CreateIndex
CREATE INDEX "memberDevelopStatsItem_developStatsId_idx" ON "memberDevelopStatsItem"("developStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberDevelopStatsItem_developStatsId_name_key" ON "memberDevelopStatsItem"("developStatsId", "name");

-- CreateIndex
CREATE INDEX "memberDesignStats_memberStatsId_idx" ON "memberDesignStats"("memberStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberDesignStats_memberStatsId_key" ON "memberDesignStats"("memberStatsId");

-- CreateIndex
CREATE INDEX "memberDesignStatsItem_designStatsId_idx" ON "memberDesignStatsItem"("designStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberDesignStatsItem_designStatsId_name_key" ON "memberDesignStatsItem"("designStatsId", "name");

-- CreateIndex
CREATE UNIQUE INDEX "memberDataScienceStats_memberStatsId_key" ON "memberDataScienceStats"("memberStatsId");

-- CreateIndex
CREATE INDEX "memberDataScienceStats_memberStatsId_idx" ON "memberDataScienceStats"("memberStatsId");

-- CreateIndex
CREATE INDEX "memberSrmStats_dataScienceStatsId_idx" ON "memberSrmStats"("dataScienceStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberSrmStats_dataScienceStatsId_key" ON "memberSrmStats"("dataScienceStatsId");

-- CreateIndex
CREATE INDEX "memberSrmChallengeDetail_srmStatsId_idx" ON "memberSrmChallengeDetail"("srmStatsId");

-- CreateIndex
CREATE INDEX "memberSrmDivisionDetail_srmStatsId_idx" ON "memberSrmDivisionDetail"("srmStatsId");

-- CreateIndex
CREATE INDEX "memberMarathonStats_dataScienceStatsId_idx" ON "memberMarathonStats"("dataScienceStatsId");

-- CreateIndex
CREATE UNIQUE INDEX "memberMarathonStats_dataScienceStatsId_key" ON "memberMarathonStats"("dataScienceStatsId");

-- CreateIndex
CREATE INDEX "memberTraits_userId_idx" ON "memberTraits"("userId");

-- CreateIndex
CREATE UNIQUE INDEX "memberTraits_userId_key" ON "memberTraits"("userId");

-- CreateIndex
CREATE INDEX "memberTraitDevice_memberTraitId_idx" ON "memberTraitDevice"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitSoftware_memberTraitId_idx" ON "memberTraitSoftware"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitServiceProvider_memberTraitId_idx" ON "memberTraitServiceProvider"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitWork_memberTraitId_idx" ON "memberTraitWork"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitEducation_memberTraitId_idx" ON "memberTraitEducation"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitBasicInfo_memberTraitId_idx" ON "memberTraitBasicInfo"("memberTraitId");

-- CreateIndex
CREATE UNIQUE INDEX "memberTraitBasicInfo_memberTraitId_key" ON "memberTraitBasicInfo"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitLanguage_memberTraitId_idx" ON "memberTraitLanguage"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitOnboardChecklist_memberTraitId_idx" ON "memberTraitOnboardChecklist"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitPersonalization_memberTraitId_idx" ON "memberTraitPersonalization"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberTraitCommunity_memberTraitId_idx" ON "memberTraitCommunity"("memberTraitId");

-- CreateIndex
CREATE INDEX "memberSkill_userId_idx" ON "memberSkill"("userId");

-- CreateIndex
CREATE INDEX "memberSkill_skillId_idx" ON "memberSkill"("skillId");

-- CreateIndex
CREATE UNIQUE INDEX "memberSkill_userId_skillId_key" ON "memberSkill"("userId", "skillId");

-- AddForeignKey
ALTER TABLE "memberAddress" ADD CONSTRAINT "memberAddress_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberMaxRating" ADD CONSTRAINT "memberMaxRating_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberFinancial" ADD CONSTRAINT "memberFinancial_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberHistoryStats" ADD CONSTRAINT "memberHistoryStats_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDevelopHistoryStats" ADD CONSTRAINT "memberDevelopHistoryStats_historyStatsId_fkey" FOREIGN KEY ("historyStatsId") REFERENCES "memberHistoryStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDataScienceHistoryStats" ADD CONSTRAINT "memberDataScienceHistoryStats_historyStatsId_fkey" FOREIGN KEY ("historyStatsId") REFERENCES "memberHistoryStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberStats" ADD CONSTRAINT "memberStats_memberRatingId_fkey" FOREIGN KEY ("memberRatingId") REFERENCES "memberMaxRating"("id") ON DELETE NO ACTION ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberStats" ADD CONSTRAINT "memberStats_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberCopilotStats" ADD CONSTRAINT "memberCopilotStats_memberStatsId_fkey" FOREIGN KEY ("memberStatsId") REFERENCES "memberStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDevelopStats" ADD CONSTRAINT "memberDevelopStats_memberStatsId_fkey" FOREIGN KEY ("memberStatsId") REFERENCES "memberStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDevelopStatsItem" ADD CONSTRAINT "memberDevelopStatsItem_developStatsId_fkey" FOREIGN KEY ("developStatsId") REFERENCES "memberDevelopStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDesignStats" ADD CONSTRAINT "memberDesignStats_memberStatsId_fkey" FOREIGN KEY ("memberStatsId") REFERENCES "memberStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDesignStatsItem" ADD CONSTRAINT "memberDesignStatsItem_designStatsId_fkey" FOREIGN KEY ("designStatsId") REFERENCES "memberDesignStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberDataScienceStats" ADD CONSTRAINT "memberDataScienceStats_memberStatsId_fkey" FOREIGN KEY ("memberStatsId") REFERENCES "memberStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSrmStats" ADD CONSTRAINT "memberSrmStats_dataScienceStatsId_fkey" FOREIGN KEY ("dataScienceStatsId") REFERENCES "memberDataScienceStats"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSrmChallengeDetail" ADD CONSTRAINT "memberSrmChallengeDetail_srmStatsId_fkey" FOREIGN KEY ("srmStatsId") REFERENCES "memberSrmStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSrmDivisionDetail" ADD CONSTRAINT "memberSrmDivisionDetail_srmStatsId_fkey" FOREIGN KEY ("srmStatsId") REFERENCES "memberSrmStats"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberMarathonStats" ADD CONSTRAINT "memberMarathonStats_dataScienceStatsId_fkey" FOREIGN KEY ("dataScienceStatsId") REFERENCES "memberDataScienceStats"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraits" ADD CONSTRAINT "memberTraits_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitDevice" ADD CONSTRAINT "memberTraitDevice_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitSoftware" ADD CONSTRAINT "memberTraitSoftware_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitServiceProvider" ADD CONSTRAINT "memberTraitServiceProvider_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitWork" ADD CONSTRAINT "memberTraitWork_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitEducation" ADD CONSTRAINT "memberTraitEducation_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitBasicInfo" ADD CONSTRAINT "memberTraitBasicInfo_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitLanguage" ADD CONSTRAINT "memberTraitLanguage_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitOnboardChecklist" ADD CONSTRAINT "memberTraitOnboardChecklist_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitPersonalization" ADD CONSTRAINT "memberTraitPersonalization_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberTraitCommunity" ADD CONSTRAINT "memberTraitCommunity_memberTraitId_fkey" FOREIGN KEY ("memberTraitId") REFERENCES "memberTraits"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "skill" ADD CONSTRAINT "skill_categoryId_fkey" FOREIGN KEY ("categoryId") REFERENCES "skillCategory"("id") ON DELETE NO ACTION ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSkill" ADD CONSTRAINT "memberSkill_displayModeId_fkey" FOREIGN KEY ("displayModeId") REFERENCES "displayMode"("id") ON DELETE NO ACTION ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSkill" ADD CONSTRAINT "memberSkill_skillId_fkey" FOREIGN KEY ("skillId") REFERENCES "skill"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSkill" ADD CONSTRAINT "memberSkill_userId_fkey" FOREIGN KEY ("userId") REFERENCES "member"("userId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSkillLevel" ADD CONSTRAINT "memberSkillLevel_memberSkillId_fkey" FOREIGN KEY ("memberSkillId") REFERENCES "memberSkill"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "memberSkillLevel" ADD CONSTRAINT "memberSkillLevel_skillLevelId_fkey" FOREIGN KEY ("skillLevelId") REFERENCES "skillLevel"("id") ON DELETE NO ACTION ON UPDATE CASCADE;

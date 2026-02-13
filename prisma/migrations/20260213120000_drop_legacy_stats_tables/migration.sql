-- Drop legacy member stats/history/distribution tables after unified stats migration.
DROP TABLE IF EXISTS "members"."distributionStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberDataScienceHistoryStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberDevelopHistoryStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberHistoryStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberCopilotStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberMarathonStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberSrmDivisionDetail" CASCADE;
DROP TABLE IF EXISTS "members"."memberSrmChallengeDetail" CASCADE;
DROP TABLE IF EXISTS "members"."memberSrmStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberDataScienceStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberDesignStatsItem" CASCADE;
DROP TABLE IF EXISTS "members"."memberDesignStats" CASCADE;
DROP TABLE IF EXISTS "members"."memberDevelopStatsItem" CASCADE;
DROP TABLE IF EXISTS "members"."memberDevelopStats" CASCADE;

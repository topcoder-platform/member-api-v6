/*
  Warnings:

  - You are about to drop the column `subTrackId` on the `memberDataScienceHistoryStats` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "memberDataScienceHistoryStats" DROP COLUMN "subTrackId";

-- AlterTable
ALTER TABLE "memberMaxRating" ALTER COLUMN "track" DROP NOT NULL,
ALTER COLUMN "subTrack" DROP NOT NULL;

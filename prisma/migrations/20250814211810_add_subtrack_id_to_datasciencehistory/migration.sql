/*
  Warnings:

  - Added the required column `subTrackId` to the `memberDataScienceHistoryStats` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "memberDataScienceHistoryStats" ADD COLUMN     "subTrackId" INTEGER NOT NULL;

const prismaManager = require('../common/prisma')
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()

async function main () {
  console.log('Clearing address and financial data')
  // delete address and financial data
  await prisma.memberAddress.deleteMany()
  await prisma.memberFinancial.deleteMany()
  // delete stats
  console.log('Clearing member stats data')
  await prisma.memberCopilotStats.deleteMany()
  await prisma.memberMarathonStats.deleteMany()
  await prisma.memberDesignStatsItem.deleteMany()
  await prisma.memberDesignStats.deleteMany()
  await prisma.memberDevelopStatsItem.deleteMany()
  await prisma.memberDevelopStats.deleteMany()
  await prisma.memberSrmChallengeDetail.deleteMany()
  await prisma.memberSrmDivisionDetail.deleteMany()
  await prisma.memberSrmStats.deleteMany()
  await prisma.memberStats.deleteMany()
  await prisma.memberDataScienceStats.deleteMany()
  // delete stats history
  console.log('Clearing member stats history data')
  await prisma.memberDataScienceHistoryStats.deleteMany()
  await prisma.memberDevelopHistoryStats.deleteMany()
  await prisma.memberHistoryStats.deleteMany()
  // delete traits
  console.log('Clearing member traits data')
  await prisma.memberTraitBasicInfo.deleteMany()
  await prisma.memberTraitCommunity.deleteMany()
  await prisma.memberTraitDevice.deleteMany()
  await prisma.memberTraitEducation.deleteMany()
  await prisma.memberTraitLanguage.deleteMany()
  await prisma.memberTraitOnboardChecklist.deleteMany()
  await prisma.memberTraitPersonalization.deleteMany()
  await prisma.memberTraitServiceProvider.deleteMany()
  await prisma.memberTraitSoftware.deleteMany()
  await prisma.memberTraitWork.deleteMany()
  await prisma.memberTraits.deleteMany()
  // delete member skills
  console.log('Clearing member skills data')
  await prisma.memberSkillLevel.deleteMany()
  await prisma.memberSkill.deleteMany()
  // delete member
  console.log('Clearing maxRating and member data')
  await prisma.memberMaxRating.deleteMany()
  await prisma.member.deleteMany()

  // delete skills
  console.log('Clearing skill data')
  await skillsPrisma.skillLevel.deleteMany()
  await skillsPrisma.skill.deleteMany()
  await skillsPrisma.skillCategory.deleteMany()
  await prisma.displayMode.deleteMany()

  // delete distribution
  console.log('Clearing rating distribution data')
  await prisma.distributionStats.deleteMany()

  console.log('All done')
}

main()

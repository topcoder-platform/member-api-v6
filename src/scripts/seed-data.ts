const path = require('path')
const fs = require('fs')
const _ = require('lodash')
const { v4: uuidv4 } = require('uuid')
const config = require('./config')
const prismaManager = require('../common/prisma')
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()

const OUTPUT_DIR = config.fileLocation
const handleList = config.handleList

const statsHistoryDir = path.join(OUTPUT_DIR, 'statsHistory')

const memberBasicData = [
  'userId',
  'handle',
  'handleLower',
  'email',
  'verified',
  'skillScore',
  'firstName',
  'lastName',
  'description',
  'otherLangName',
  'newEmail',
  'emailVerifyToken',
  'emailVerifyTokenDate',
  'newEmailVerifyToken',
  'newEmailVerifyTokenDate',
  'homeCountryCode',
  'competitionCountryCode',
  'photoURL',
  'tracks',
  'loginCount',
  'lastLoginDate',
  'availableForGigs',
  'skillScoreDeduction',
  'namesAndHandleAppearance'
]

const createdBy = 'migration'

function readDate (milliseconds) {
  return milliseconds ? new Date(milliseconds) : null
}

function buildMemberData (memberData, prismaData) {
  // pick basic data
  _.assign(prismaData, _.pick(memberData, memberBasicData))
  // set status
  prismaData.status = memberData.status
  // set mock emails
  prismaData.email = `${memberData.handle}@topcoder.com`
  // set createdAt, updatedAt
  prismaData.createdAt = readDate(memberData.createdAt)
  prismaData.updatedAt = readDate(memberData.updatedAt)
  prismaData.createdBy = memberData.createdBy || createdBy
  // set max rating
  const maxRatingData = {
    ...memberData.maxRating,
    createdBy: createdBy
  }
  maxRatingData.track = maxRatingData.track || ''
  maxRatingData.subTrack = maxRatingData.subTrack || ''
  prismaData.maxRating = { create: maxRatingData }
  const addressList = _.map(_.get(memberData, 'addresses', []), t => ({
    ...t,
    type: t.type || '',
    createdAt: prismaData.createdAt,
    createdBy
  }))
  if (addressList.length > 0) {
    prismaData.addresses = {
      create: addressList
    }
  }
}

async function createSkillData (memberData) {
  // use upsert to create skill, skillLevel, displayMode, skillCategory
  if (!memberData.skills || memberData.skills.length === 0) {
    return
  }
  for (let skillData of memberData.skills) {
    await skillsPrisma.skillCategory.upsert({
      create: { id: skillData.category.id, name: skillData.category.name, createdBy },
      update: { name: skillData.category.name },
      where: { id: skillData.category.id }
    })
    if (_.get(skillData, 'displayMode.id')) {
      await prisma.displayMode.upsert({
        create: { id: skillData.displayMode.id, name: skillData.displayMode.name, createdBy },
        update: { name: skillData.displayMode.name },
        where: { id: skillData.displayMode.id }
      })
    }
    for (let level of skillData.levels) {
      await skillsPrisma.skillLevel.upsert({
        create: { id: level.id, name: level.name, description: level.description, createdBy },
        update: { name: level.name, description: level.description },
        where: { id: level.id }
      })
    }
    await skillsPrisma.skill.upsert({
      create: {
        id: skillData.id,
        name: skillData.name,
        createdBy,
        category: { connect: { id: skillData.category.id } }
      },
      update: { name: skillData.name },
      where: { id: skillData.id }
    })
  }
}

function buildDevelopStatsData (jsonData) {
  const ret: any = {
    challenges: jsonData.challenges,
    wins: jsonData.wins,
    mostRecentSubmission: readDate(jsonData.mostRecentSubmission),
    mostRecentEventDate: readDate(jsonData.mostRecentEventDate),
    createdBy
  }
  const itemData = jsonData.subTracks
  const items = _.map(itemData, t => ({
    name: t.name,
    subTrackId: t.id,
    challenges: t.challenges,
    wins: t.wins,
    mostRecentSubmission: readDate(t.mostRecentSubmission),
    mostRecentEventDate: readDate(t.mostRecentEventDate),
    ...t.submissions,
    ...t.rank,
    createdBy
  }))
  if (items.length > 0) {
    ret.items = { create: items }
  }
  return ret
}

const designStatsItemFields = [
  'name', 'challenges', 'wins', 'numInquiries', 'submissions', 'passedScreening',
  'avgPlacement', 'screeningSuccessRate', 'submissionRate', 'winPercent'
]

function buildDesignStatsData (jsonData) {
  const ret: any = {
    challenges: jsonData.challenges,
    wins: jsonData.wins,
    mostRecentSubmission: readDate(jsonData.mostRecentSubmission),
    mostRecentEventDate: readDate(jsonData.mostRecentEventDate),
    createdBy
  }
  const itemData = jsonData.subTracks
  const items = _.map(itemData, t => ({
    subTrackId: t.id,
    mostRecentSubmission: readDate(t.mostRecentSubmission),
    mostRecentEventDate: readDate(t.mostRecentEventDate),
    ..._.pick(t, designStatsItemFields),
    createdBy
  }))
  if (items.length > 0) {
    ret.items = { create: items }
  }
  return ret
}

function buildSrmData (jsonData) {
  // missing 'mostRecentEventName'
  const prismaData = {
    ..._.pick(jsonData, ['challenges', 'wins', 'mostRecentEventName']),
    mostRecentSubmission: readDate(jsonData.mostRecentSubmission),
    mostRecentEventDate: readDate(jsonData.mostRecentEventDate),
    ...jsonData.rank,
    createdBy
  }
  if (jsonData.challengeDetails && jsonData.challengeDetails.length > 0) {
    const items = _.map(jsonData.challengeDetails, t => ({
      ...t,
      createdBy
    }))
    prismaData.challengeDetails = { create: items }
  }
  // check division data
  if (jsonData.division2 && jsonData.division2.length > 0) {
    let items = _.map(jsonData.division2, t => ({
      ...t,
      divisionName: 'division2',
      createdBy
    }))
    if (jsonData.division1 && jsonData.division1.length > 0) {
      const newItems = _.map(jsonData.division1, t => ({
        ...t,
        divisionName: 'division1',
        createdBy
      }))
      items = _.concat(items, newItems)
    }
    prismaData.divisions = { create: items }
  }
  return prismaData
}

function buildMarathonData (jsonData) {
  // missing 'mostRecentEventName'
  return {
    ..._.pick(jsonData, ['challenges', 'wins', 'mostRecentEventName']),
    mostRecentSubmission: readDate(jsonData.mostRecentSubmission),
    mostRecentEventDate: readDate(jsonData.mostRecentEventDate),
    ...jsonData.rank,
    createdBy
  }
}

async function createSkills (memberData) {
  // set skills
  const memberSkillData = []
  const memberSkillLevels = []
  if (!memberData.skills || memberData.skills.length === 0) {
    return
  }
  for (let skillData of memberData.skills) {
    const memberSkillId = uuidv4()
    const memberSkill: any = {
      id: memberSkillId,
      userId: memberData.userId,
      skillId: skillData.id,
      createdBy
    }
    if (skillData.displayMode) {
      memberSkill.displayModeId = skillData.displayMode.id
    }
    memberSkillData.push(memberSkill)
    for (let level of skillData.levels) {
      memberSkillLevels.push({
        memberSkillId,
        skillLevelId: level.id,
        createdBy
      })
    }
  }

  await prisma.memberSkill.createMany({
    data: memberSkillData
  })

  await prisma.memberSkillLevel.createMany({
    data: memberSkillLevels
  })
}

async function createStats (memberData, maxRatingId) {
  let statsData: any = {}
  if (memberData.stats && memberData.stats.length > 0) {
    statsData = memberData.stats[0]
  }
  if (!statsData.userId) {
    return
  }
  const prismaData: any = {
    member: { connect: { userId: memberData.userId } },
    maxRating: { connect: { id: maxRatingId } },
    challenges: statsData.challenges,
    wins: statsData.wins,
    createdBy
  }
  if (_.get(statsData, 'COPILOT.contests')) {
    prismaData.copilot = {
      create: { ...statsData.COPILOT, createdBy }
    }
  }
  if (_.get(statsData, 'DEVELOP.challenges')) {
    const developData = buildDevelopStatsData(statsData.DEVELOP)
    prismaData.develop = { create: developData }
  }
  if (_.get(statsData, 'DESIGN.challenges')) {
    const designData = buildDesignStatsData(statsData.DESIGN)
    prismaData.design = { create: designData }
  }
  if (_.get(statsData, 'DATA_SCIENCE.challenges')) {
    const dataScienceData: any = {
      challenges: statsData.DATA_SCIENCE.challenges,
      wins: statsData.DATA_SCIENCE.wins,
      // mostRecentEventName: statsData.DATA_SCIENCE.mostRecentEventName,
      mostRecentSubmission: readDate(statsData.DATA_SCIENCE.mostRecentSubmission),
      mostRecentEventDate: readDate(statsData.DATA_SCIENCE.mostRecentEventDate),
      createdBy
    }
    if (_.get(statsData, 'DATA_SCIENCE.SRM.challenges')) {
      const jsonData = statsData.DATA_SCIENCE.SRM
      dataScienceData.srm = { create: buildSrmData(jsonData) }
    }
    if (_.get(statsData, 'DATA_SCIENCE.MARATHON_MATCH.challenges')) {
      const jsonData = statsData.DATA_SCIENCE.MARATHON_MATCH
      dataScienceData.marathon = { create: buildMarathonData(jsonData) }
    }
    prismaData.dataScience = { create: dataScienceData }
  }

  await prisma.memberStats.create({
    data: prismaData
  })
}

async function importMember (handle) {
  console.log(`Import member data for ${handle}`)
  const filename = path.join(OUTPUT_DIR, `${handle}.json`)
  const rawData = fs.readFileSync(filename, 'utf8')
  const dataList = JSON.parse(rawData)
  const memberData = _.find(dataList, t => t.handle === handle)
  if (!memberData) {
    console.log(`Can't find member data for user ${handle}`)
    return
  }
  // get skill data and create them
  await createSkillData(memberData)
  // build prisma data structure for this member
  const prismaData = {}
  buildMemberData(memberData, prismaData)

  const member = await prisma.member.create({
    data: prismaData,
    include: {
      maxRating: true
    }
  })

  if (handle !== 'iamtong' && handle !== 'jiangliwu') {
    await createStats(memberData, member.maxRating.id)
  }
  await createSkills(memberData)
  console.log(`Import member data complete for ${handle}`)
}

async function importStatsHistory () {
  for (let handle of handleList) {
    if (handle === 'iamtong' || handle === 'jiangliwu') {
      continue
    }
    console.log(`Import stats history for member ${handle}`)
    const filename = path.join(statsHistoryDir, `${handle}.json`)
    const rawData = fs.readFileSync(filename, 'utf8')
    const data = JSON.parse(rawData)
    if (!data || data.length === 0) {
      continue
    }
    const statsData = data[0]
    const historyRecords = []

    // handle develop stats history
    if (statsData.DEVELOP && statsData.DEVELOP.subTracks &&
      statsData.DEVELOP.subTracks.length > 0) {
      _.forEach(statsData.DEVELOP.subTracks, t => {
        const typeId = `${t.name || t.id || 'Challenge'}`
        _.forEach(t.history, h => {
          historyRecords.push({
            userId: statsData.userId,
            trackId: 'DEVELOP',
            typeId,
            challengeId: `${h.challengeId}`,
            oldRating: h.oldRating,
            newRating: h.newRating,
            oldGlobalRank: h.oldGlobalRank,
            newGlobalRank: h.newGlobalRank,
            oldCountryRank: h.oldCountryRank,
            newCountryRank: h.newCountryRank,
            oldSchoolRank: h.oldSchoolRank,
            newSchoolRank: h.newSchoolRank,
            eventDate: new Date(h.ratingDate),
            createdBy,
            updatedBy: createdBy
          })
        })
      })
    }

    // handle data science stats history
    const srmHistory = _.get(statsData, 'DATA_SCIENCE.SRM.history', [])
    const marathonHistory = _.get(statsData, 'DATA_SCIENCE.MARATHON_MATCH.history', [])
    if (srmHistory.length > 0) {
      _.forEach(srmHistory, t => {
        historyRecords.push({
          userId: statsData.userId,
          trackId: 'DATA_SCIENCE',
          typeId: 'SRM',
          challengeId: `${t.challengeId}`,
          oldRating: t.oldRating,
          newRating: t.newRating || t.rating,
          oldGlobalRank: t.oldGlobalRank,
          newGlobalRank: t.newGlobalRank || t.placement,
          oldCountryRank: t.oldCountryRank,
          newCountryRank: t.newCountryRank,
          oldSchoolRank: t.oldSchoolRank,
          newSchoolRank: t.newSchoolRank,
          eventDate: new Date(t.date),
          createdBy,
          updatedBy: createdBy
        })
      })
    }
    if (marathonHistory.length > 0) {
      _.forEach(marathonHistory, t => {
        historyRecords.push({
          userId: statsData.userId,
          trackId: 'DATA_SCIENCE',
          typeId: 'MARATHON_MATCH',
          challengeId: `${t.challengeId}`,
          oldRating: t.oldRating,
          newRating: t.newRating || t.rating,
          oldGlobalRank: t.oldGlobalRank,
          newGlobalRank: t.newGlobalRank || t.placement,
          oldCountryRank: t.oldCountryRank,
          newCountryRank: t.newCountryRank,
          oldSchoolRank: t.oldSchoolRank,
          newSchoolRank: t.newSchoolRank,
          eventDate: new Date(t.date),
          createdBy,
          updatedBy: createdBy
        })
      })
    }

    if (historyRecords.length > 0) {
      await prisma.memberStatsHistory.createMany({
        data: historyRecords
      })
    }
  }
  console.log('Importing stats history complete')
}

/**
 * This function will create mock data for member stats history.
 */
async function mockPrivateStatsHistory () {
  console.log('Creating mock stats history data for ACRush')
  await prisma.memberStatsHistory.createMany({
    data: [{
      userId: 19849563,
      trackId: 'DEVELOP',
      typeId: 'SECRET_TRACK',
      challengeId: '99999',
      newRating: 3000,
      eventDate: new Date(),
      createdBy,
      updatedBy: createdBy
    }, {
      userId: 19849563,
      trackId: 'DATA_SCIENCE',
      typeId: 'SRM',
      challengeId: '99998',
      newRating: 2999,
      newGlobalRank: 1,
      eventDate: new Date(),
      createdBy,
      updatedBy: createdBy
    }, {
      userId: 19849563,
      trackId: 'DATA_SCIENCE',
      typeId: 'MARATHON_MATCH',
      challengeId: '99997',
      newRating: 2998,
      newGlobalRank: 1,
      eventDate: new Date(),
      createdBy,
      updatedBy: createdBy
    }]
  })
}

async function mockPrivateStats () {
  console.log('Creating mock stats data for ACRush')
  await prisma.memberStats.createMany({
    data: [{
      userId: 19849563,
      trackId: 'DEVELOP',
      typeId: 'Challenge',
      challenges: 999,
      wins: 999,
      isPrivate: true,
      createdBy,
      updatedBy: createdBy
    }, {
      userId: 19849563,
      trackId: 'DATA_SCIENCE',
      typeId: 'SRM',
      challenges: 999,
      wins: 999,
      isPrivate: true,
      createdBy,
      updatedBy: createdBy
    }, {
      userId: 19849563,
      trackId: 'COPILOT',
      typeId: 'Challenge',
      challenges: 100,
      wins: 100,
      isPrivate: true,
      createdBy,
      updatedBy: createdBy
    }]
  })
}

async function main () {
  for (let handle of handleList) {
    await importMember(handle)
  }
  await importStatsHistory()
  // create mock data for private stats history
  await mockPrivateStatsHistory()
  // create mock data for private stats
  await mockPrivateStats()
  console.log('All done')
}

main()

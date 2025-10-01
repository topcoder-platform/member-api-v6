const path = require('path')
const fs = require('fs')
const readline = require('readline')
const { concat, isArray, isBoolean, isEmpty, isEqual, isInteger, find, omit, pick, isNumber, forEach, map, uniqBy, isString, cloneDeep } = require('lodash')
const { v4: uuidv4 } = require('uuid')
const config = require('./config')
const prisma = require('../common/prisma').getClient()

const CREATED_BY = 'migrate'
const MIGRATE_DIR = config.migrateLocation
const BATCH_SIZE = 1000
const DEFAULT_RATING_COLOR = '#EF3A3A'
const DEFAULT_SRM_ID = 101
const DEFAULT_MARATHON_MATCH_ID = 102

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName', 'tracks', 'status',
  'addresses', 'description', 'email', 'country', 'homeCountryCode', 'competitionCountryCode', 'photoURL', 'verified', 'maxRating',
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'loginCount', 'lastLoginDate', 'skills', 'availableForGigs',
  'skillScoreDeduction', 'namesAndHandleAppearance']

const MEMBER_STATUS = ['UNVERIFIED', 'ACTIVE', 'INACTIVE_USER_REQUEST', 'INACTIVE_DUPLICATE_ACCOUNT', 'INACTIVE_IRREGULAR_ACCOUNT', 'UNKNOWN']

const DEVICE_TYPE = ['Console', 'Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Wearable', 'Other']

const MAX_RATING_FIELDS = ['rating', 'track', 'subTrack', 'ratingColor']

const ADDRESS_FIELDS = ['streetAddr1', 'streetAddr2', 'city', 'zip', 'stateCode', 'type']

const TRAIT_BASIC_INFO = ['userId', 'country', 'primaryInterestInTopcoder', 'tshirtSize', 'gender', 'shortBio', 'birthDate', 'currentLocation']
const TRAIT_LANGUAGE = ['language', 'spokenLevel', 'writtenLevel']
const TRAIT_SERVICE_PROVIDER = ['serviceProviderType', 'name']
const TRAIT_DEVICE = ['deviceType', 'manufacturer', 'model', 'operatingSystem', 'osVersion', 'osLanguage']

/**
 * Clear All DB.
 */
async function clearDB () {
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
  await prisma.skillLevel.deleteMany()
  await prisma.skill.deleteMany()
  await prisma.skillCategory.deleteMany()
  await prisma.displayMode.deleteMany()

  // delete distribution
  console.log('Clearing rating distribution data')
  await prisma.distributionStats.deleteMany()

  console.log('All done')
}

/**
 * Convert date number into Date
 * @param {Number} dateNum the date number
 * @returns the date instance
 */
function _convert2Date (dateValue) {
  if (dateValue === null || dateValue === undefined) {
    return undefined
  }

  if (dateValue instanceof Date) {
    return dateValue
  }

  if (isNumber(dateValue) && dateValue >= 0) {
    return new Date(dateValue)
  }

  if (isString(dateValue) && dateValue.length > 0) {
    const parsed = new Date(dateValue)
    if (!Number.isNaN(parsed.getTime())) {
      return parsed
    }
    const numericValue = Number(dateValue)
    if (!Number.isNaN(numericValue) && numericValue >= 0) {
      return new Date(numericValue)
    }
  }

  return undefined
}

/**
 * Import the Dynamo members from file
 * @param {String} filename filename
 */
async function importDynamoMember (filename) {
  const memberDynamoFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberDynamoFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberDynamoFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  // count the json items
  let count = 0
  // count the insert items
  let total = 0
  // store the temp json object string
  let stringObject = ''
  // store batch items
  let batchItems = []
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items`)
    }

    // paste line string data, and combine to member data
    const trimmedLine = line.trimEnd()
    if (trimmedLine === '    },') {
      stringObject += '}'
      if (stringObject.length <= 2) {
        continue
      }
      count += 1
      const dataItem = JSON.parse(stringObject)

      const dataObj = await fixMemberData(dataItem, batchItems)
      if (dataObj) {
        batchItems.push(dataObj)
      }
      stringObject = ''

      if (count % BATCH_SIZE === 0) {
        // create member
        await createMembers(batchItems)
        total += batchItems.length
        batchItems = []
      }
    } else if (trimmedLine === '    {') {
      stringObject = '{'
    } else if (trimmedLine === '[' || trimmedLine === ']') {
      continue
    } else if (stringObject.length > 0) {
      stringObject += line.trim()
    }

    // count += 1

    // if (count >= 10000) {
    //   break
    // }
  }

  // batchItems still contains some data, input them into DB
  if (batchItems.length > 0) {
    await createMembers(batchItems)
    total += batchItems.length
  }
  console.log(`\nIt has inserted ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Count the file lines
 * @param {String} dataFilePath the data file path
 * @returns the file lines count
 */
async function countFileLines (dataFilePath) {
  let lineCount = 0
  // Count lines
  const rlCount = readline.createInterface({
    input: fs.createReadStream(dataFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  console.log(`Counting file size...`)
  // eslint-disable-next-line
  for await (const line of rlCount) {
    lineCount++
  }

  return lineCount
}

/**
 * Fix the member data structure and ignore invalid data
 * @param {Object} memberItem member item
 * @param {Array} batchItems batch items
 * @returns the fixed member data
 */
async function fixMemberData (memberItem, batchItems) {
  // fix member data
  if (memberItem.email === 'email@domain.com.z') {
    const emailId = uuidv4()
    memberItem.email = `email${emailId}@domain.com.z`
  }

  if (memberItem.status === 'INACTIVE') {
    memberItem.status = 'INACTIVE_USER_REQUEST'
  } else if (!MEMBER_STATUS.find(status => status === memberItem.status)) {
    memberItem.status = 'UNKNOWN'
  }

  if (memberItem.addresses) {
    let addressArr = JSON.parse(memberItem.addresses)
    if (isArray(addressArr) && addressArr.length > 0) {
      addressArr = addressArr.map(addressItem => ({
        ...addressItem,
        zip: addressItem.zip ? '' + addressItem.zip : undefined,
        type: addressItem.type ? addressItem.type : 'HOME',
        createdAt: _convert2Date(addressItem.createdAt),
        createdBy: CREATED_BY,
        updatedAt: _convert2Date(addressItem.updatedAt),
        updatedBy: addressItem.updatedBy ? addressItem.updatedBy : undefined
      }))
      memberItem.addresses = {
        create: addressArr
      }
    } else {
      memberItem.addresses = undefined
    }
  }

  if (memberItem.maxRating) {
    let maxRatingObj = JSON.parse(memberItem.maxRating)
    maxRatingObj = pick(maxRatingObj, MAX_RATING_FIELDS)
    maxRatingObj.track = maxRatingObj.track ? maxRatingObj.track : 'DEV'
    maxRatingObj.subTrack = maxRatingObj.subTrack ? maxRatingObj.subTrack : 'CODE'
    maxRatingObj.ratingColor = maxRatingObj.ratingColor ? maxRatingObj.ratingColor : DEFAULT_RATING_COLOR
    maxRatingObj.createdBy = CREATED_BY
    if (isInteger(maxRatingObj.rating) && maxRatingObj.rating > 0) {
      memberItem.maxRating = {
        create: maxRatingObj
      }
    } else {
      memberItem.maxRating = undefined
    }
  }

  // check duplicate fields: handleLower, email
  let found = batchItems.find(item => item.email === memberItem.email)
  let foundInDb = await prisma.member.findFirst({
    where: {
      email: memberItem.email
    }
  })
  if (found || foundInDb) {
    const emailId = uuidv4()
    memberItem.email = `email${emailId}@test.com`
  }

  found = batchItems.find(item => item.handleLower === memberItem.handleLower)
  foundInDb = await prisma.member.findFirst({
    where: {
      handleLower: memberItem.handleLower
    }
  })
  if (found || foundInDb) {
    const handleId = uuidv4()
    memberItem.handleLower = memberItem.handleLower + handleId.substring(0, 8)
  }

  const memberItemDB = {
    ...pick(memberItem, MEMBER_FIELDS),
    lastLoginDate: _convert2Date(memberItem.lastLoginDate),
    createdAt: _convert2Date(memberItem.createdAt),
    createdBy: memberItem.createdBy || CREATED_BY,
    updatedAt: _convert2Date(memberItem.updatedAt),
    updatedBy: memberItem.updatedBy ? memberItem.updatedBy : undefined
  }

  if (memberItemDB.userId && memberItemDB.handle && memberItemDB.handleLower && memberItemDB.email) {
    return memberItemDB
  }

  return null
}

/**
 * Crate member items in DB
 * @param {Array} memberItems member items
 */
async function createMembers (memberItems) {
  const memberWithAddress = memberItems.filter(item => item.addresses || item.maxRating)
  const memberWithoutAddress = memberItems.filter(item => !(item.addresses || item.maxRating))
  return prisma.$transaction(async (tx) => {
    // create address without address
    await tx.member.createMany({
      data: memberWithoutAddress
    })

    for (const memberItem of memberWithAddress) {
      await tx.member.create({
        data: memberItem
      })
    }
  })
}

/**
 * Import the Dynamo member stats from file
 * @param {String} filename filename
 */
async function importDynamoMemberStat (filename) {
  const memberStatDynamoFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberStatDynamoFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberStatDynamoFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  // count the json items
  let count = 0
  // count the insert items
  let total = 0
  // store the temp json object string
  let stringObject = ''

  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items`)
    }

    // paste line string data, and combine to member data
    const trimmedLine = line.trimEnd()
    if (trimmedLine === '    },') {
      stringObject += '}'
      if (stringObject.length <= 2) {
        continue
      }
      count += 1
      const dataItem = JSON.parse(stringObject)

      // make sure the member is exist
      const member = await prisma.member.findFirst({
        where: {
          userId: dataItem.userId
        },
        include: {
          maxRating: true
        }
      })

      if (member) {
        const memberStat = fixDynamoMemberStatData(dataItem)

        const memberStateToCreate = {
          challenges: memberStat.challenges,
          wins: memberStat.wins,
          // isPrivate: memberStat.isPrivate,
          createdBy: CREATED_BY,
          userId: member.userId,
          develop: {
            create: memberStat.develop
          },
          design: {
            create: memberStat.design
          },
          dataScience: {
            create: memberStat.dataScience
          },
          copilot: {
            create: memberStat.copilot
          }
        }

        if (member.maxRating) {
          memberStateToCreate.memberRatingId = member.maxRating.id
        }

        await prisma.memberStats.create({
          data: memberStateToCreate
        })

        total += 1
      }

      stringObject = ''
    } else if (trimmedLine === '    {') {
      stringObject = '{'
    } else if (trimmedLine === '[' || trimmedLine === ']') {
      continue
    } else if (stringObject.length > 0) {
      stringObject += line.trim()
    }
  }

  console.log(`\nIt has inserted ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Fix Dynamo member stat data
 * @param {Object} dataItem item to be fixed
 * @returns member stat data
 */
function fixDynamoMemberStatData (dataItem) {
  const memberStat = {
    challenges: dataItem.challenges,
    wins: dataItem.wins,
    groupId: dataItem.groupId
  }

  if (dataItem.DEVELOP) {
    const developData = JSON.parse(dataItem.DEVELOP)
    memberStat.develop = {
      challenges: developData.challenges,
      wins: developData.wins,
      mostRecentSubmission: _convert2Date(developData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(developData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (developData.subTracks && developData.subTracks.length > 0) {
      let developItems = developData.subTracks.map(item => ({
        ...(item.submissions ? item.submissions : {}),
        ...(item.rank ? item.rank : {}),
        subTrackId: item.id,
        name: item.name,
        challenges: item.challenges,
        wins: item.wins,
        mostRecentSubmission: _convert2Date(item.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(item.mostRecentEventDate),
        createdBy: CREATED_BY
      }))

      developItems = developItems.filter(item => isString(item.name) && isInteger(item.subTrackId)
      )

      if (developItems.length > 0) {
        memberStat.develop.items = {
          create: developItems
        }
      }
    }
  }

  if (dataItem.DESIGN) {
    const designData = JSON.parse(dataItem.DESIGN)
    memberStat.design = {
      challenges: designData.challenges,
      wins: designData.wins,
      mostRecentSubmission: _convert2Date(designData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(designData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (designData.subTracks) {
      let designItems = designData.subTracks.map(item => ({
        ...(omit(item, ['id', 'mostRecentSubmission', 'mostRecentEventDate'])),
        subTrackId: item.id,
        mostRecentSubmission: _convert2Date(item.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(item.mostRecentEventDate),
        createdBy: CREATED_BY
      }))

      designItems = designItems.filter(item => isString(item.name) && isInteger(item.subTrackId) &&
        isInteger(item.numInquiries) && isInteger(item.submissions) &&
        isInteger(item.passedScreening) && isNumber(item.avgPlacement) &&
        isNumber(item.screeningSuccessRate) && isNumber(item.submissionRate) &&
        isNumber(item.winPercent)
      )

      if (designItems.length > 0) {
        memberStat.design.items = {
          create: designItems
        }
      }
    }
  }

  if (dataItem.DATA_SCIENCE) {
    const scienceData = JSON.parse(dataItem.DATA_SCIENCE)
    memberStat.dataScience = {
      challenges: scienceData.challenges,
      wins: scienceData.wins,
      mostRecentEventName: scienceData.mostRecentEventName,
      mostRecentSubmission: _convert2Date(scienceData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(scienceData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (scienceData.SRM) {
      const dataScienceSrmData = {
        ...(scienceData.SRM.rank),
        challenges: scienceData.SRM.challenges,
        wins: scienceData.SRM.wins,
        mostRecentEventName: scienceData.SRM.mostRecentEventName,
        mostRecentSubmission: _convert2Date(scienceData.SRM.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(scienceData.SRM.mostRecentEventDate),
        createdBy: CREATED_BY
      }

      if (isInteger(dataScienceSrmData.rating) && isNumber(dataScienceSrmData.percentile) &&
        isInteger(dataScienceSrmData.rank) && isInteger(dataScienceSrmData.countryRank) &&
        isInteger(dataScienceSrmData.schoolRank) && isInteger(dataScienceSrmData.volatility) &&
        isInteger(dataScienceSrmData.maximumRating) && isInteger(dataScienceSrmData.rating) &&
        isInteger(dataScienceSrmData.rating) && isInteger(dataScienceSrmData.minimumRating) &&
        isString(dataScienceSrmData.defaultLanguage) && isInteger(dataScienceSrmData.competitions)) {
        memberStat.dataScience.srm = {
          create: dataScienceSrmData
        }

        if (scienceData.SRM.challengeDetails) {
          const srmChallengeDetailData = scienceData.SRM.challengeDetails.map(item => ({
            ...item,
            createdBy: CREATED_BY
          }))

          if (isInteger(srmChallengeDetailData.challenges) && isString(srmChallengeDetailData.levelName) &&
            isInteger(srmChallengeDetailData.failedChallenges)) {
            memberStat.dataScience.srm.create.challengeDetails = {
              create: srmChallengeDetailData
            }
          }
        }

        if (scienceData.SRM.division1 || scienceData.SRM.division2) {
          const srmDivision1Data = (scienceData.SRM.division1 || []).map(item => ({
            ...item,
            divisionName: 'division1',
            createdBy: CREATED_BY
          }))

          const srmDivision2Data = (scienceData.SRM.division2 || []).map(item => ({
            ...item,
            divisionName: 'division2',
            createdBy: CREATED_BY
          }))

          let divisionArr = concat(srmDivision1Data, srmDivision2Data)

          divisionArr = divisionArr.filter(item => isString(item.levelName) && isInteger(item.problemsSubmitted) &&
            isInteger(item.problemsSysByTest) && isInteger(item.problemsFailed)
          )

          if (divisionArr.length > 0) {
            memberStat.dataScience.srm.create.divisions = {
              create: divisionArr
            }
          }
        }
      }
    }

    if (scienceData.MARATHON_MATCH) {
      const dataScienceMarathonData = {
        ...(scienceData.MARATHON_MATCH.rank),
        challenges: scienceData.MARATHON_MATCH.challenges,
        wins: scienceData.MARATHON_MATCH.wins,
        mostRecentEventName: scienceData.MARATHON_MATCH.mostRecentEventName,
        mostRecentSubmission: _convert2Date(scienceData.MARATHON_MATCH.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(scienceData.MARATHON_MATCH.mostRecentEventDate),
        createdBy: CREATED_BY
      }

      if (isInteger(dataScienceMarathonData.rating) && isInteger(dataScienceMarathonData.competitions) &&
        isNumber(dataScienceMarathonData.avgRank) && isInteger(dataScienceMarathonData.avgNumSubmissions) &&
        isInteger(dataScienceMarathonData.bestRank) && isInteger(dataScienceMarathonData.topFiveFinishes) &&
        isInteger(dataScienceMarathonData.topTenFinishes) && isInteger(dataScienceMarathonData.rank) &&
        isNumber(dataScienceMarathonData.percentile) && isInteger(dataScienceMarathonData.volatility) &&
        isInteger(dataScienceMarathonData.minimumRating) && isInteger(dataScienceMarathonData.maximumRating) &&
        isInteger(dataScienceMarathonData.countryRank) && isInteger(dataScienceMarathonData.schoolRank) &&
        isString(dataScienceMarathonData.defaultLanguage)) {
        memberStat.dataScience.marathon = {
          create: dataScienceMarathonData
        }
      }
    }
  }

  if (dataItem.COPILOT) {
    const copilotData = JSON.parse(dataItem.COPILOT)

    if (isInteger(copilotData.contests) && isInteger(copilotData.projects) &&
      isInteger(copilotData.failures) && isInteger(copilotData.reposts) &&
      isInteger(copilotData.activeContests) && isInteger(copilotData.activeProjects) &&
      isNumber(copilotData.fulfillment)) {
      memberStat.copilot = {
        ...copilotData,
        createdBy: CREATED_BY
      }
    }
  }
  return memberStat
}

/**
 * Import the Dynamo member stat history from file
 * @param {String} filename filename
 */
async function importDynamoMemberStatHistory (filename) {
  const memberStatHistoryDynamoFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberStatHistoryDynamoFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberStatHistoryDynamoFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  // count the json items
  let count = 0
  // count the insert items
  let total = 0
  // store the temp json object string
  let stringObject = ''

  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items`)
    }

    // paste line string data, and combine to member data
    const trimmedLine = line.trimEnd()
    if (trimmedLine === '    },') {
      stringObject += '}'
      if (stringObject.length <= 2) {
        continue
      }
      count += 1

      const dataItem = JSON.parse(stringObject)
      // make sure the member is exist
      const member = await prisma.member.findFirst({
        where: {
          userId: dataItem.userId
        }
      })

      if (member) {
        const statHistory = fixDynamoMemberStatHistoryData(dataItem)

        if (!isEmpty(statHistory)) {
          await prisma.memberHistoryStats.create({
            data: {
              groupId: dataItem.groupId,
              createdBy: CREATED_BY,
              userId: member.userId,
              develop: {
                create: statHistory.develop
              },
              dataScience: {
                create: statHistory.dataScience
              }
            },
            include: { develop: true, dataScience: true }
          })
          total += 1
        }
      }

      stringObject = ''
    } else if (trimmedLine === '    {') {
      stringObject = '{'
    } else if (trimmedLine === '[' || trimmedLine === ']') {
      continue
    } else if (stringObject.length > 0) {
      stringObject += line.trim()
    }
  }

  console.log(`\nIt has inserted ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Import the Dynamo member stat history from file
 * @param {String} filename filename
 */
async function importDynamoMemberStatHistoryPrivate (filename) {
  const memberStatHistoryDynamoFilePath = path.join(MIGRATE_DIR, filename)

  let lineCount = 0
  let stringObject = ''
  let total = 0
  const rlCount = readline.createInterface({
    input: fs.createReadStream(memberStatHistoryDynamoFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  console.log(`Counting ${filename} size...`)
  for await (const line of rlCount) {
    lineCount++
    stringObject += line
  }

  console.log(`${filename} has ${lineCount} lines in total`)

  const dataItems = JSON.parse(stringObject)

  for (let idx = 0; idx < dataItems.length; idx++) {
    const dataItem = dataItems[idx]
    const member = await prisma.member.findFirst({
      where: {
        userId: dataItem.userId
      }
    })

    if (member) {
      const statHistory = fixDynamoMemberStatHistoryData(dataItem)

      await prisma.memberHistoryStats.create({
        data: {
          groupId: dataItem.groupId,
          isPrivate: true,
          createdBy: CREATED_BY,
          userId: member.userId,
          develop: {
            create: statHistory.develop
          },
          dataScience: {
            create: statHistory.dataScience
          }
        },
        include: { develop: true, dataScience: true }
      })

      total += 1
    }
  }

  console.log(`\nIt has inserted ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Fix Dynamo member stat history data
 * @param {Object} dataItem item to be fixed
 * @returns member stat history data
 */
function fixDynamoMemberStatHistoryData (dataItem) {
  const statHistory = {}

  if (dataItem.DEVELOP) {
    const developData = JSON.parse(dataItem.DEVELOP)
    if (developData.subTracks && developData.subTracks.length > 0) {
      statHistory.develop = []
      developData.subTracks.forEach(item => {
        if (item.history && item.history.length > 0) {
          let historyItems = item.history.map(item2 => ({
            ...item2,
            ratingDate: _convert2Date(item2.ratingDate),
            subTrackId: item.id || DEFAULT_SRM_ID,
            subTrack: item.name,
            createdBy: CREATED_BY
          }))

          historyItems = historyItems.filter(item => isInteger(item.challengeId) && isString(item.challengeName) &&
            item.ratingDate && isInteger(item.newRating)
          )

          if (historyItems.length > 0) {
            statHistory.develop = statHistory.develop.concat(historyItems)
          }
        }
      })

      if (isEmpty(statHistory.develop)) {
        delete statHistory.develop
      }
    }
  }

  if (dataItem.DATA_SCIENCE) {
    const scienceData = JSON.parse(dataItem.DATA_SCIENCE)
    statHistory.dataScience = []
    if (scienceData.SRM && scienceData.SRM.history && scienceData.SRM.history.length > 0) {
      let historyItems = scienceData.SRM.history.map(item => ({
        ...item,
        date: _convert2Date(item.date),
        subTrack: 'SRM',
        subTrackId: item.id || DEFAULT_SRM_ID,
        createdBy: CREATED_BY
      }))

      statHistory.dataScience = historyItems
    }
    if (scienceData.MARATHON_MATCH && scienceData.MARATHON_MATCH.history && scienceData.MARATHON_MATCH.history.length > 0) {
      let historyItems = scienceData.MARATHON_MATCH.history.map(item => ({
        ...item,
        date: _convert2Date(item.date),
        subTrack: 'MARATHON_MATCH',
        subTrackId: item.id || DEFAULT_MARATHON_MATCH_ID,
        createdBy: CREATED_BY
      }))

      statHistory.dataScience = statHistory.dataScience.concat(historyItems)
    }

    if (statHistory.dataScience.length > 0) {
      statHistory.dataScience = statHistory.dataScience.filter(item => isInteger(item.challengeId) && isString(item.challengeName) &&
        item.date && isInteger(item.rating) &&
        isInteger(item.placement) && isNumber(item.percentile)
      )
    }

    if (isEmpty(statHistory.dataScience)) {
      delete statHistory.dataScience
    }
  }
  return statHistory
}

/**
 * Update the ElasticSearch members from file
 * @param {String} filename filename
 */
async function importElasticSearchMember (filename) {
  const memberElasticFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberElasticFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberElasticFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  // count the json items
  let count = 0
  // count the insert items
  let total = 0
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items`)
    }

    count += 1
    const dataItem = JSON.parse(line.trim())

    const dbItem = await prisma.member.findFirst({
      where: {
        userId: dataItem._source.userId
      },
      include: {
        addresses: true
      }
    })

    const dataObj = await fixMemberUpdateData(dataItem._source, dbItem || {})

    await updateMembersWithTraitsAndSkills(dataObj)
  }

  console.log(`\nIt has updated ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Fix the member data structure for updating
 * @param {Object} memberItem member item
 * @param {Object} dbItem the member in DB
 * @returns the fixed member data
 */
async function fixMemberUpdateData (memberItem, dbItem) {
  // fix member update data
  const omitFields = ['email', 'handle', 'handleLower', 'lastLoginDate']

  let memberItemUpdate = {
    ...pick(memberItem, MEMBER_FIELDS),
    createdAt: _convert2Date(memberItem.createdAt),
    createdBy: memberItem.createdBy || CREATED_BY,
    updatedAt: _convert2Date(memberItem.updatedAt),
    updatedBy: memberItem.updatedBy ? memberItem.updatedBy : undefined
  }

  memberItemUpdate = omit(memberItemUpdate, omitFields)

  if (memberItemUpdate.addresses) {
    const updateAddr = pick(memberItemUpdate.addresses['0'], ADDRESS_FIELDS)
    let addressItem = {}
    if (dbItem.addresses && dbItem.addresses.length > 0) {
      addressItem = dbItem.addresses[0]
    }
    const dbAddr = pick(addressItem, ADDRESS_FIELDS)

    if (isEqual(updateAddr, dbAddr)) {
      memberItemUpdate = omit(memberItemUpdate, ['addresses'])
    } else if (updateAddr.type) {
      memberItemUpdate.addresses = [{
        ...updateAddr,
        zip: '' + updateAddr.zip
      }]
    } else {
      delete memberItemUpdate.addresses
    }
  }

  if (memberItem.traits && dbItem.userId) {
    memberItemUpdate.memberTraits = {}
    if (memberItem.traits.traitId === 'onboarding_checklist') {
      const checkKeys = Object.keys(memberItem.traits.data[0])
      memberItemUpdate.memberTraits.checklist = []
      for (const checkKey of checkKeys) {
        const traitData = memberItem.traits.data[0][checkKey]
        if (traitData.date && traitData.status) {
          memberItemUpdate.memberTraits.checklist.push({
            listItemType: checkKey,
            date: _convert2Date(traitData.date) || new Date(),
            message: traitData.message || '',
            status: traitData.status,
            metadata: traitData.metadata,
            skip: traitData.skip
          })
        }
      }
      if (isEmpty(memberItemUpdate.memberTraits.checklist)) {
        memberItemUpdate.memberTraits = omit(memberItemUpdate.memberTraits, ['checklist'])
      }
    } else if (memberItem.traits.traitId === 'communities') {
      const communityKeys = Object.keys(memberItem.traits.data[0])
      memberItemUpdate.memberTraits.community = []
      for (const communityKey of communityKeys) {
        const traitData = memberItem.traits.data[0][communityKey]
        if (isBoolean(traitData)) {
          memberItemUpdate.memberTraits.community.push({
            communityName: communityKey,
            status: traitData
          })
        }
      }
    } else if (memberItem.traits.traitId === 'basic_info') {
      const traitData = pick(memberItem.traits.data[0], TRAIT_BASIC_INFO)
      if (traitData.userId && traitData.country && traitData.primaryInterestInTopcoder && traitData.shortBio) {
        memberItemUpdate.memberTraits.basicInfo = [{
          ...traitData,
          birthDate: _convert2Date(traitData.birthDate)
        }]
      }
    } else if (memberItem.traits.traitId === 'languages') {
      const traitData = pick(memberItem.traits.data[0], TRAIT_LANGUAGE)
      if (traitData.language) {
        memberItemUpdate.memberTraits.language = [{
          ...traitData
        }]
      }
    } else if (memberItem.traits.traitId === 'education') {
      const educationTraits = []
      forEach(memberItem.traits.data, traitData => {
        if (traitData && traitData.schoolCollegeName && traitData.major) {
          let endYear
          const normalizedEndDate = _convert2Date(traitData.timePeriodTo)
          if (normalizedEndDate) {
            endYear = normalizedEndDate.getFullYear()
          } else if (isNumber(traitData.endYear)) {
            endYear = traitData.endYear
          }
          educationTraits.push({
            collegeName: traitData.schoolCollegeName,
            degree: traitData.major,
            endYear
          })
        }
      })
      if (!isEmpty(educationTraits)) {
        memberItemUpdate.memberTraits.education = educationTraits
      }
    } else if (memberItem.traits.traitId === 'service_provider') {
      const traitData = pick(memberItem.traits.data[0], TRAIT_SERVICE_PROVIDER)
      if (traitData.serviceProviderType && traitData.name) {
        let providerType = traitData.serviceProviderType
        if (providerType === 'Internet Service Provider') {
          providerType = 'InternetServiceProvider'
        } else if (providerType === 'Mobile Carrier') {
          providerType = 'MobileCarrier'
        } else if (providerType === 'Financial Institution') {
          providerType = 'FinancialInstitution'
        }
        memberItemUpdate.memberTraits.serviceProvider = [{
          type: providerType,
          name: traitData.name
        }]
      }
    } else if (memberItem.traits.traitId === 'hobby') {
      memberItemUpdate.memberTraits.hobby = []
      forEach(memberItem.traits.data, traitData => {
        memberItemUpdate.memberTraits.hobby.push(traitData.hobby)
      })
    } else if (memberItem.traits.traitId === 'subscription') {
      memberItemUpdate.memberTraits.subscription = []
      forEach(memberItem.traits.data, traitData => {
        memberItemUpdate.memberTraits.subscription.push(traitData.name)
      })
    } else if (memberItem.traits.traitId === 'device') {
      memberItemUpdate.memberTraits.device = []
      forEach(memberItem.traits.data, traitData => {
        const deviceData = pick(traitData, TRAIT_DEVICE)
        if (!DEVICE_TYPE.find(type => type === deviceData.deviceType)) {
          deviceData.deviceType = 'Other'
        }
        if (deviceData.deviceType && deviceData.manufacturer && deviceData.model && deviceData.operatingSystem) {
          memberItemUpdate.memberTraits.device.push(deviceData)
        }
      })
    } else if (memberItem.traits.traitId === 'software') {
      memberItemUpdate.memberTraits.software = []
      forEach(memberItem.traits.data, traitData => {
        if (traitData.softwareType && traitData.name) {
          let softwareType = traitData.softwareType
          if (softwareType === 'Developer Tools') {
            softwareType = 'DeveloperTools'
          } else if (softwareType === 'Graphics & Design') {
            softwareType = 'GraphAndDesign'
          }
          memberItemUpdate.memberTraits.software.push({
            softwareType,
            name: traitData.name
          })
        }
      })
    } else if (memberItem.traits.traitId === 'organization') {
      memberItemUpdate.memberTraits.organization = []
      forEach(memberItem.traits.data, traitData => {
        if (traitData.softwareType && traitData.name) {
          memberItemUpdate.memberTraits.organization.push({
            softwareType: traitData.softwareType,
            name: traitData.name
          })
        }
      })
    } else if (memberItem.traits.traitId === 'work') {
      const workTraits = []
      forEach(memberItem.traits.data, traitData => {
        if (!traitData) {
          return
        }
        const companyName = traitData.companyName || traitData.company
        if (!(companyName && traitData.position)) {
          return
        }
        const workTrait = {
          companyName,
          position: traitData.position
        }
        if (traitData.industry) {
          workTrait.industry = traitData.industry
        }
        if (isBoolean(traitData.working)) {
          workTrait.working = traitData.working
        }
        const normalizedStartDate = _convert2Date(traitData.startDate || traitData.timePeriodFrom)
        if (normalizedStartDate) {
          workTrait.startDate = normalizedStartDate
        }
        const normalizedEndDate = _convert2Date(traitData.endDate || traitData.timePeriodTo)
        if (normalizedEndDate) {
          workTrait.endDate = normalizedEndDate
        }
        workTraits.push(workTrait)
      })
      if (!isEmpty(workTraits)) {
        memberItemUpdate.memberTraits.work = workTraits
      }
    } else if (memberItem.traits.traitId === 'skill') {
      // Ignore, we do not have skill traits, and there are only 3 data in files
    } else if (memberItem.traits.traitId === 'personalization' || memberItem.traits.traitId === 'connect_info' || memberItem.traits.traitId === 'organization') {
      memberItemUpdate.memberTraits.personalization = [{
        key: memberItem.traits.traitId,
        value: memberItem.traits.data
      }]
    }

    if (isEmpty(memberItemUpdate.memberTraits)) {
      memberItemUpdate = omit(memberItemUpdate, ['memberTraits'])
    }
  }

  if (memberItem.skills && dbItem.userId) {
    memberItemUpdate.memberSkills = []
    forEach(memberItem.skills, skillData => {
      if (skillData.id && skillData.name) {
        const skillObj = {
          id: skillData.id,
          name: skillData.name
        }
        if (skillData.category && skillData.category.id && skillData.category.name) {
          skillObj.category = {
            id: skillData.category.id,
            name: skillData.category.name
          }
        }
        if (skillData.levels && skillData.levels.length > 0) {
          skillObj.levels = skillData.levels
        }
        if (skillData.displayMode) {
          skillObj.displayMode = skillData.displayMode
        }
        memberItemUpdate.memberSkills.push(skillObj)
      }
    })
  }

  const pureMemberObj = omit(memberItemUpdate, ['lastLoginDate', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy'])

  if (Object.keys(pureMemberObj).length > 0 && (dbItem.userId || (memberItemUpdate.userId && memberItemUpdate.handle && memberItemUpdate.handleLower && memberItemUpdate.email))) {
    return memberItemUpdate
  }

  return null
}

/**
 * Update the member data with traits and skills
 * @param {Object} memberObj member item
 */
async function updateMembersWithTraitsAndSkills (memberObj) {
  if (!isEmpty(omit(memberObj, ['userId', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'lastLoginDate']))) {
    await prisma.$transaction(async (tx) => {
      const onlyMemberObj = omit(memberObj, ['userId', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'lastLoginDate', 'skills', 'maxRating', 'addresses', 'memberTraits', 'memberSkills'])

      if (onlyMemberObj.status === 'INACTIVE') {
        onlyMemberObj.status = 'INACTIVE_USER_REQUEST'
      } else if (!MEMBER_STATUS.find(status => status === onlyMemberObj.status)) {
        onlyMemberObj.status = 'UNKNOWN'
      }

      if (!isEmpty(onlyMemberObj)) {
        await tx.member.update({
          where: {
            userId: memberObj.userId
          },
          data: onlyMemberObj
        })
      }

      if (memberObj.maxRating && memberObj.maxRating.rating && memberObj.maxRating.track && memberObj.maxRating.subTrack) {
        const existMaxRating = await tx.memberMaxRating.findFirst({
          where: { userId: memberObj.userId }
        })

        if (existMaxRating) {
          // update current maxRating
          await tx.memberMaxRating.update({
            where: {
              id: existMaxRating.id
            },
            data: {
              ...memberObj.maxRating,
              userId: memberObj.userId,
              updatedBy: CREATED_BY
            }
          })
        } else {
          // create new maxRating
          await tx.memberMaxRating.create({
            data: {
              ...memberObj.maxRating,
              ratingColor: memberObj.maxRating.ratingColor || DEFAULT_RATING_COLOR,
              userId: memberObj.userId,
              createdBy: CREATED_BY
            }
          })
        }
      }

      if (memberObj.addresses && memberObj.addresses.length > 0) {
        // clear current addresses
        await tx.memberAddress.deleteMany({
          where: { userId: memberObj.userId }
        })
        // create new addresses
        await tx.memberAddress.createMany({
          data: map(memberObj.addresses, t => ({
            ...t,
            userId: memberObj.userId,
            createdBy: CREATED_BY
          }))
        })
      }

      if (memberObj.memberTraits) {
        let memberTraitsDB = await tx.memberTraits.findFirst({
          where: {
            userId: memberObj.userId
          }
        })

        if (!memberTraitsDB) {
          memberTraitsDB = await tx.memberTraits.create({
            data: {
              userId: memberObj.userId,
              createdBy: CREATED_BY
            }
          })
        }

        if ((memberObj.memberTraits.subscriptions && memberObj.memberTraits.subscriptions.length > 0) || (memberObj.memberTraits.hobby && memberObj.memberTraits.hobby.length > 0)) {
          const toUpdateObj = {}
          if (memberObj.memberTraits.subscriptions && memberObj.memberTraits.subscriptions.length > 0) {
            toUpdateObj.subscriptions = memberObj.memberTraits.subscriptions
          }
          if (memberObj.memberTraits.hobby && memberObj.memberTraits.hobby.length > 0) {
            toUpdateObj.hobby = memberObj.memberTraits.hobby
          }
          await tx.memberTraits.update({
            where: {
              id: memberTraitsDB.id
            },
            data: toUpdateObj
          })
        }

        await updateTraitElement(memberObj.memberTraits.device, tx.memberTraitDevice, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.software, tx.memberTraitSoftware, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.serviceProvider, tx.memberTraitServiceProvider, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.work, tx.memberTraitWork, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.education, tx.memberTraitEducation, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.basicInfo, tx.memberTraitBasicInfo, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.language, tx.memberTraitLanguage, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.checklist, tx.memberTraitOnboardChecklist, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.personalization, tx.memberTraitPersonalization, memberTraitsDB.id, CREATED_BY)
        await updateTraitElement(memberObj.memberTraits.community, tx.memberTraitCommunity, memberTraitsDB.id, CREATED_BY)
      }

      if (memberObj.memberSkills) {
        await tx.memberSkill.deleteMany({
          where: {
            userId: memberObj.userId
          }
        })

        let allCategories = await tx.skillCategory.findMany()
        let allDisplayModes = await tx.displayMode.findMany()
        let allSkillLevels = await tx.skillLevel.findMany()
        let allSkills = await tx.skill.findMany()
        let newCategories = []
        let newDisplayModes = []
        let newSkillLevels = []
        let newSkills = []
        memberObj.memberSkills.forEach(elemItem => {
          if (elemItem.category && !find(allCategories, category => category.id === elemItem.category.id)) {
            newCategories.push({
              ...elemItem.category,
              createdBy: CREATED_BY
            })
          }
          if (elemItem.displayMode && !find(allDisplayModes, displayMode => displayMode.id === elemItem.displayMode.id)) {
            newDisplayModes.push({
              ...elemItem.displayMode,
              createdBy: CREATED_BY
            })
          }
          if (elemItem.levels && elemItem.levels.length > 0) {
            elemItem.levels.forEach(levelItem => {
              if (!find(allSkillLevels, level => levelItem.id === level.id)) {
                newSkillLevels.push({
                  ...levelItem,
                  createdBy: CREATED_BY
                })
              }
            })
          }
          if (!find(allSkills, skill => skill.id === elemItem.id)) {
            newSkills.push({
              id: elemItem.id,
              name: elemItem.name,
              categoryId: elemItem.category.id,
              createdBy: CREATED_BY
            })
          }
        })

        newCategories = uniqBy(newCategories, elemItem => elemItem.id)
        newDisplayModes = uniqBy(newDisplayModes, elemItem => elemItem.id)
        newSkillLevels = uniqBy(newSkillLevels, elemItem => elemItem.id)
        newSkills = uniqBy(newSkills, elemItem => elemItem.id)
        if (newCategories.length > 0) {
          await tx.skillCategory.createMany({
            data: newCategories
          })
        }
        if (newDisplayModes.length > 0) {
          await tx.displayMode.createMany({
            data: newDisplayModes
          })
        }
        if (newSkillLevels.length > 0) {
          await tx.skillLevel.createMany({
            data: newSkillLevels
          })
        }
        if (newSkills.length > 0) {
          await tx.skill.createMany({
            data: newSkills
          })
        }

        const toCreateArr = memberObj.memberSkills.map(elemItem => {
          const skillObj = {
            id: uuidv4(),
            skill: {
              connect: {
                id: elemItem.id
              }
            },
            member: {
              connect: {
                userId: memberObj.userId
              }
            },
            createdBy: CREATED_BY
          }
          if (elemItem.levels) {
            skillObj.levels = {
              create: elemItem.levels.map(level => ({
                skillLevel: {
                  connect: {
                    id: level.id
                  }
                },
                createdBy: CREATED_BY
              }))
            }
          }
          if (elemItem.displayMode) {
            skillObj.displayMode = {
              connect: {
                id: elemItem.displayMode.id
              }
            }
          }
          return skillObj
        })

        for (const skillObj of toCreateArr) {
          const exist = await tx.memberSkill.findFirst({
            where: {
              userId: memberObj.userId,
              skillId: skillObj.skill.connect.id
            }
          })
          if (!exist) {
            await tx.memberSkill.create({
              data: skillObj
            })
          }
        }
      }
    })
  }
}

/**
 * Update the trait element
 * @param {Array} objArr trait items
 * @param {Object} txObject prisma tx instance
 * @param {String} memberTraitId the memberTraitId
 * @param {String} createdBy the createdBy
 */
async function updateTraitElement (objArr, txObject, memberTraitId, createdBy) {
  if (objArr && objArr.length > 0) {
    const toUpdateArr = objArr.map(elemItem => ({
      ...elemItem,
      memberTraitId,
      createdBy
    }))

    await txObject.deleteMany({
      where: {
        memberTraitId
      }
    })
    await txObject.createMany({
      data: toUpdateArr
    })
  }
}

/**
 * Update the ElasticSearch member stat from file
 * @param {String} filename filename
 */
async function importElasticSearchMemberStat (filename) {
  const memberStatElasticFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberStatElasticFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberStatElasticFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  // count the json items
  let count = 0
  // count the insert items
  let total = 0
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items`)
    }

    count += 1

    let dataItem = JSON.parse(line.trim())
    dataItem = dataItem._source

    const memberDB = await prisma.member.findFirst({
      where: {
        userId: dataItem.userId
      },
      include: {
        maxRating: true
      }
    })

    if (memberDB) {
      const memberStat = fixElasticSearchMemberStatData(dataItem)

      // update the member stat
      await updateMemberStat(memberStat, memberDB, CREATED_BY)

      total += 1
    }
  }

  console.log(`\nIt has updated ${total} items totally`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Fix elastic search member stat data
 * @param {Object} dataItem item to be fixed
 * @returns member stat data
 */
function fixElasticSearchMemberStatData (dataItem) {
  const memberStat = {
    challenges: dataItem.challenges,
    wins: dataItem.wins,
    groupId: dataItem.groupId
  }

  if (dataItem.maxRating && isInteger(dataItem.maxRating.rating) && isString(dataItem.maxRating.track) && isString(dataItem.maxRating.subTrack)) {
    memberStat.maxRating = {
      rating: dataItem.maxRating.rating,
      track: dataItem.maxRating.track,
      subTrack: dataItem.maxRating.subTrack,
      ratingColor: dataItem.maxRating.ratingColor || DEFAULT_RATING_COLOR
    }
  }

  if (dataItem.DEVELOP) {
    const developData = dataItem.DEVELOP
    memberStat.develop = {
      challenges: developData.challenges,
      wins: developData.wins,
      mostRecentSubmission: _convert2Date(developData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(developData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (developData.subTracks && developData.subTracks.length > 0) {
      let developItems = developData.subTracks.map(item => ({
        ...(item.submissions ? item.submissions : {}),
        ...(item.rank ? item.rank : {}),
        subTrackId: item.id,
        name: item.name,
        challenges: item.challenges,
        wins: item.wins,
        mostRecentSubmission: _convert2Date(item.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(item.mostRecentEventDate),
        createdBy: CREATED_BY
      }))

      developItems = developItems.filter(item => isString(item.name) && isInteger(item.subTrackId)
      )

      if (developItems.length > 0) {
        memberStat.develop.items = developItems
      }
    }
  }

  if (dataItem.DESIGN) {
    const designData = dataItem.DESIGN
    memberStat.design = {
      challenges: designData.challenges,
      wins: designData.wins,
      mostRecentSubmission: _convert2Date(designData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(designData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (designData.subTracks) {
      let designItems = designData.subTracks.map(item => ({
        ...(omit(item, ['id', 'mostRecentSubmission', 'mostRecentEventDate'])),
        subTrackId: item.id,
        mostRecentSubmission: _convert2Date(item.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(item.mostRecentEventDate),
        createdBy: CREATED_BY
      }))

      designItems = designItems.filter(item => isString(item.name) && isInteger(item.subTrackId) &&
        isInteger(item.numInquiries) && isInteger(item.submissions) &&
        isInteger(item.passedScreening) && isNumber(item.avgPlacement) &&
        isNumber(item.screeningSuccessRate) && isNumber(item.submissionRate) &&
        isNumber(item.winPercent)
      )

      if (designItems.length > 0) {
        memberStat.design.items = designItems
      }
    }
  }

  if (dataItem.DATA_SCIENCE) {
    const scienceData = dataItem.DATA_SCIENCE
    memberStat.dataScience = {
      challenges: scienceData.challenges,
      wins: scienceData.wins,
      mostRecentEventName: scienceData.mostRecentEventName,
      mostRecentSubmission: _convert2Date(scienceData.mostRecentSubmission),
      mostRecentEventDate: _convert2Date(scienceData.mostRecentEventDate),
      createdBy: CREATED_BY
    }

    if (scienceData.SRM) {
      const dataScienceSrmData = {
        ...(scienceData.SRM.rank),
        challenges: scienceData.SRM.challenges,
        wins: scienceData.SRM.wins,
        mostRecentEventName: scienceData.SRM.mostRecentEventName,
        mostRecentSubmission: _convert2Date(scienceData.SRM.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(scienceData.SRM.mostRecentEventDate),
        createdBy: CREATED_BY
      }

      if (isInteger(dataScienceSrmData.rating) && isNumber(dataScienceSrmData.percentile) &&
        isInteger(dataScienceSrmData.rank) && isInteger(dataScienceSrmData.countryRank) &&
        isInteger(dataScienceSrmData.schoolRank) && isInteger(dataScienceSrmData.volatility) &&
        isInteger(dataScienceSrmData.maximumRating) && isInteger(dataScienceSrmData.rating) &&
        isInteger(dataScienceSrmData.rating) && isInteger(dataScienceSrmData.minimumRating) &&
        isString(dataScienceSrmData.defaultLanguage) && isInteger(dataScienceSrmData.competitions)) {
        memberStat.dataScience.srm = dataScienceSrmData

        if (scienceData.SRM.challengeDetails) {
          const srmChallengeDetailData = scienceData.SRM.challengeDetails.map(item => ({
            ...item,
            createdBy: CREATED_BY
          }))

          if (isInteger(srmChallengeDetailData.challenges) && isString(srmChallengeDetailData.levelName) &&
            isInteger(srmChallengeDetailData.failedChallenges)) {
            memberStat.dataScience.srm.challengeDetails = srmChallengeDetailData
          }
        }

        if (scienceData.SRM.division1 || scienceData.SRM.division2) {
          const srmDivision1Data = (scienceData.SRM.division1 || []).map(item => ({
            ...item,
            divisionName: 'division1',
            createdBy: CREATED_BY
          }))

          const srmDivision2Data = (scienceData.SRM.division2 || []).map(item => ({
            ...item,
            divisionName: 'division2',
            createdBy: CREATED_BY
          }))

          // let divisionArr = concat(srmDivision1Data, srmDivision2Data)
          const division1Arr = srmDivision1Data.filter(item => isString(item.levelName) && isInteger(item.problemsSubmitted) &&
            isInteger(item.problemsSysByTest) && isInteger(item.problemsFailed)
          )

          if (division1Arr.length > 0) {
            memberStat.dataScience.srm.division1 = division1Arr
          }

          const division2Arr = srmDivision2Data.filter(item => isString(item.levelName) && isInteger(item.problemsSubmitted) &&
            isInteger(item.problemsSysByTest) && isInteger(item.problemsFailed)
          )

          if (division2Arr.length > 0) {
            memberStat.dataScience.srm.division2 = division2Arr
          }
        }
      }
    }

    if (scienceData.MARATHON_MATCH) {
      const dataScienceMarathonData = {
        ...(scienceData.MARATHON_MATCH.rank),
        challenges: scienceData.MARATHON_MATCH.challenges,
        wins: scienceData.MARATHON_MATCH.wins,
        mostRecentEventName: scienceData.MARATHON_MATCH.mostRecentEventName,
        mostRecentSubmission: _convert2Date(scienceData.MARATHON_MATCH.mostRecentSubmission),
        mostRecentEventDate: _convert2Date(scienceData.MARATHON_MATCH.mostRecentEventDate),
        createdBy: CREATED_BY
      }

      if (isInteger(dataScienceMarathonData.rating) && isInteger(dataScienceMarathonData.competitions) &&
        isNumber(dataScienceMarathonData.avgRank) && isInteger(dataScienceMarathonData.avgNumSubmissions) &&
        isInteger(dataScienceMarathonData.bestRank) && isInteger(dataScienceMarathonData.topFiveFinishes) &&
        isInteger(dataScienceMarathonData.topTenFinishes) && isInteger(dataScienceMarathonData.rank) &&
        isNumber(dataScienceMarathonData.percentile) && isInteger(dataScienceMarathonData.volatility) &&
        isInteger(dataScienceMarathonData.minimumRating) && isInteger(dataScienceMarathonData.maximumRating) &&
        isInteger(dataScienceMarathonData.countryRank) && isInteger(dataScienceMarathonData.schoolRank) &&
        isString(dataScienceMarathonData.defaultLanguage)) {
        memberStat.dataScience.marathon = dataScienceMarathonData
      }
    }
  }

  if (dataItem.COPILOT) {
    const copilotData = dataItem.COPILOT

    if (isInteger(copilotData.contests) && isInteger(copilotData.projects) &&
      isInteger(copilotData.failures) && isInteger(copilotData.reposts) &&
      isInteger(copilotData.activeContests) && isInteger(copilotData.activeProjects) &&
      isNumber(copilotData.fulfillment)) {
      memberStat.copilot = {
        ...copilotData,
        createdBy: CREATED_BY
      }
    }
  }
  return memberStat
}

/**
 * Update or Create item.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 * @returns new created item data or undefined
 */
async function updateOrCreateModel (itemData, existingData, txModel, parentId, operatorId) {
  if (existingData) {
    await txModel.update({
      where: {
        id: existingData.id
      },
      data: {
        ...itemData,
        updatedBy: operatorId
      }
    })
  } else {
    const newItemData = await txModel.create({
      data: {
        ...itemData,
        ...parentId,
        createdBy: operatorId
      }
    })
    return newItemData
  }
}

/**
 * Update array items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.subTrackId === item.subTrackId)
    if (foundItem) {
      item.id = foundItem.id
      toUpdate.push(item)
    } else {
      toCreate.push(item)
    }
  })
  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ...omit(elem, ['id', 'subTrackId', 'name']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update array division items.
 * @param {Array} updateD1Items division1 items to be updated
 * @param {Array} updateD2Items division2 items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayDivisionItems (updateD1Items, updateD2Items, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if ((!updateD1Items || updateD1Items.length === 0) && (!updateD2Items || updateD2Items.length === 0)) {
    return
  }

  if (updateD1Items) {
    updateD1Items.forEach(item => {
      const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName && eItem.divisionName === 'division1')
      if (foundItem) {
        item.id = foundItem.id
        toUpdate.push(item)
      } else {
        item.divisionName = 'division1'
        toCreate.push(item)
      }
    })
  }

  if (updateD2Items) {
    updateD2Items.forEach(item => {
      const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName && eItem.divisionName === 'division2')
      if (foundItem) {
        item.id = foundItem.id
        toUpdate.push(item)
      } else {
        item.divisionName = 'division2'
        toCreate.push(item)
      }
    })
  }

  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ...omit(elem, ['id']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update array level items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayLevelItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []
  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.levelName === item.levelName)
    if (foundItem) {
      item.id = foundItem.id
      toUpdate.push(item)
    } else {
      toCreate.push(item)
    }
  })
  const toDeleteIds = []
  existingItems.forEach(item => {
    const found = toUpdate.find(item2 => item2.id === item.id)
    if (!found) {
      toDeleteIds.push(item.id)
    }
  })

  for (let i = 0; i < toUpdate.length; i++) {
    const elem = toUpdate[i]
    await txModel.update({
      where: {
        id: elem.id
      },
      data: {
        ...omit(elem, ['id']),
        updatedBy: operatorId
      }
    })
  }

  await txModel.createMany({
    data: toCreate.map(item => ({
      ...item,
      ...parentId,
      createdBy: operatorId
    }))
  })

  await txModel.deleteMany({
    where: {
      id: {
        in: toDeleteIds
      }
    }
  })
}

/**
 * Update member stats
 * @param {Object} data the member stats data
 * @param {Object} member the member
 * @param {String} operatorId the operator Id
 */
async function updateMemberStat (data, member, operatorId) {
  return prisma.$transaction(async (tx) => {
    // update model memberStats
    let memberStatDB = await prisma.memberStats.findFirst({
      where: {
        userId: member.id
      },
      include: {
        develop: { include: { items: true } },
        design: { include: { items: true } },
        dataScience: { include: {
          srm: { include: { challengeDetails: true, divisions: true } },
          marathon: true
        } },
        copilot: true
      }
    })

    if (memberStatDB) {
      await prisma.memberStats.update({
        where: {
          id: memberStatDB.id
        },
        data: {
          challenges: data.challenges,
          wins: data.wins,
          groupId: data.groupId,
          updatedBy: operatorId
        }
      })
    } else {
      memberStatDB = await prisma.memberStats.create({
        data: {
          challenges: data.challenges,
          wins: data.wins,
          groupId: data.groupId,
          createdBy: operatorId
        }
      })
    }

    // update maxRating
    if (data.maxRating) {
      await updateOrCreateModel(data.maxRating, member.maxRating, tx.memberMaxRating, { userId: member.userId }, operatorId)
    }

    // update DEVELOP
    if (data.develop) {
      const developData = pick(data.DEVELOP, ['challenges', 'wins', 'mostRecentSubmission', 'mostRecentEventDate'])
      const newDevelop = await updateOrCreateModel(developData, memberStatDB.develop, tx.memberDevelopStats, { memberStatsId: memberStatDB.id }, operatorId)
      if (newDevelop) {
        memberStatDB.develop = newDevelop
      }

      // update develop subTracks
      if (data.develop.items) {
        const developStatsId = memberStatDB.develop.id
        const existingItems = memberStatDB.develop.items || []

        await updateArrayItems(data.develop.items, existingItems, tx.memberDevelopStatsItem, { developStatsId }, operatorId)
      }
    }

    // update DESIGN
    if (data.design) {
      const designData = pick(data.design, ['challenges', 'wins', 'mostRecentSubmission', 'mostRecentEventDate'])
      const newDesign = await updateOrCreateModel(designData, memberStatDB.design, tx.memberDesignStats, { memberStatsId: memberStatDB.id }, operatorId)
      if (newDesign) {
        memberStatDB.design = newDesign
      }

      // update design subTracks
      if (data.design.items) {
        const designStatsId = memberStatDB.design.id
        const existingItems = memberStatDB.design.items || []

        await updateArrayItems(data.design.items, existingItems, tx.memberDesignStatsItem, { designStatsId }, operatorId)
      }
    }

    // update DATA_SCIENCE
    if (data.dataScience) {
      const dataScienceData = pick(data.dataScience, ['challenges', 'wins', 'mostRecentEventName', 'mostRecentSubmission', 'mostRecentEventDate'])
      const newDataScience = await updateOrCreateModel(dataScienceData, memberStatDB.dataScience, tx.memberDataScienceStats, { memberStatsId: memberStatDB.id }, operatorId)
      if (newDataScience) {
        memberStatDB.dataScience = newDataScience
      }

      // update data science srm
      if (data.dataScience.srm) {
        const dataScienceSrmData = omit(data.dataScience.srm, ['challengeDetails', 'division1', 'division2'])
        const newDataScienceSrm = await updateOrCreateModel(dataScienceSrmData, memberStatDB.dataScience.srm, tx.memberSrmStats, { dataScienceStatsId: memberStatDB.dataScience.id }, operatorId)
        if (newDataScienceSrm) {
          memberStatDB.dataScience.srm = newDataScienceSrm
        }

        const srmStatsId = memberStatDB.dataScience.srm.id
        if (data.dataScience.srm.challengeDetails) {
          const existingItems = memberStatDB.dataScience.srm.challengeDetails || []

          await updateArrayLevelItems(data.dataScience.srm.challengeDetails, existingItems, tx.memberSrmChallengeDetail, { srmStatsId }, operatorId)
        }

        if (data.dataScience.srm.division1 || data.dataScience.srm.division2) {
          const existingItems = memberStatDB.dataScience.srm.divisions || []

          await updateArrayDivisionItems(data.dataScience.srm.division1, data.dataScience.srm.division2, existingItems, tx.memberSrmDivisionDetail, { srmStatsId }, operatorId)
        }
      }

      // update data science marathon
      if (data.dataScience.marathon) {
        await updateOrCreateModel(data.dataScience.marathon, memberStatDB.dataScience.marathon, tx.memberMarathonStats, { dataScienceStatsId: memberStatDB.dataScience.id }, operatorId)
      }
    }

    // update COPILOT
    if (data.copilot) {
      await updateOrCreateModel(data.copilot, memberStatDB.copilot, tx.memberCopilotStats, { memberStatsId: memberStatDB.id }, operatorId)
    }
  })
}

/**
 * import distribution stats.
 */
async function importDistributionStats () {
  return prisma.$transaction(async (tx) => {
    const total = await tx.memberMaxRating.count({})
    console.log(`There are ${total} maxRating records`)

    let current = 0
    const uniqueMap = new Map()
    const distributionStat = {
      ratingRange0To099: 0,
      ratingRange100To199: 0,
      ratingRange200To299: 0,
      ratingRange300To399: 0,
      ratingRange400To499: 0,
      ratingRange500To599: 0,
      ratingRange600To699: 0,
      ratingRange700To799: 0,
      ratingRange800To899: 0,
      ratingRange900To999: 0,
      ratingRange1000To1099: 0,
      ratingRange1100To1199: 0,
      ratingRange1200To1299: 0,
      ratingRange1300To1399: 0,
      ratingRange1400To1499: 0,
      ratingRange1500To1599: 0,
      ratingRange1600To1699: 0,
      ratingRange1700To1799: 0,
      ratingRange1800To1899: 0,
      ratingRange1900To1999: 0,
      ratingRange2000To2099: 0,
      ratingRange2100To2199: 0,
      ratingRange2200To2299: 0,
      ratingRange2300To2399: 0,
      ratingRange2400To2499: 0,
      ratingRange2500To2599: 0,
      ratingRange2600To2699: 0,
      ratingRange2700To2799: 0,
      ratingRange2800To2899: 0,
      ratingRange2900To2999: 0,
      ratingRange3000To3099: 0,
      ratingRange3100To3199: 0,
      ratingRange3200To3299: 0,
      ratingRange3300To3399: 0,
      ratingRange3400To3499: 0,
      ratingRange3500To3599: 0,
      ratingRange3600To3699: 0,
      ratingRange3700To3799: 0,
      ratingRange3800To3899: 0,
      ratingRange3900To3999: 0
    }

    while (current <= total) {
      const records = await tx.memberMaxRating.findMany({
        where: {},
        orderBy: { id: 'asc' },
        take: BATCH_SIZE,
        skip: current
      })
      console.log(`Counting ${current} maxRating record`)

      records.forEach(record => {
        const mapKey = record.track.toUpperCase() + '-' + record.subTrack.toUpperCase()
        let distributionValue
        if (uniqueMap.has(mapKey)) {
          distributionValue = uniqueMap.get(mapKey)
        } else {
          distributionValue = cloneDeep(distributionStat)
        }

        const idxVal = Math.floor(record.rating / 100)
        const ratingKey = idxVal === 0 ? 'ratingRange0To099' : `ratingRange${idxVal}00To${idxVal}99`
        distributionValue[ratingKey] += 1
        uniqueMap.set(mapKey, distributionValue)
      })

      current += BATCH_SIZE
    }

    if (uniqueMap.size > 0) {
      const dataArray = []
      uniqueMap.forEach((value, key) => {
        const tracks = key.split('-')
        const data = {
          ...value,
          track: tracks[0],
          subTrack: tracks[1],
          createdBy: CREATED_BY
        }
        dataArray.push(data)
      })
      await tx.distributionStats.createMany({
        data: dataArray
      })
    }

    console.log(`Finished counted ${uniqueMap.size} distributionStats records\n`)
  })
}

async function main () {
  console.log('This script is migrating data into DB')
  console.log('The data number is huge, about 5,000,000 ~ 10,000,000 lines for each file')
  console.log('Each steps will run very long time (about 1.5h ~ 6h+)')
  console.log('So please run scripts one by one')
  console.log('')
  console.log('0. Clear all DB data')
  console.log('1. Import Dynamo Member')
  console.log('2. Update ElasticSearch Member with traits and skills')
  console.log('3. Import Dynamo MemberStat')
  console.log('4. Update ElasticSearch MemberStat')
  console.log('5. Import Dynamo MemberStatHistory')
  console.log('6. Import Distribution Stats')
  console.log('')

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  })

  rl.question('Please select your step to run (0-6): ', async (step) => {
    if (step !== '0' && step !== '1' && step !== '2' && step !== '3' && step !== '4' && step !== '5' && step !== '6') {
      rl.close()
    } else {
      console.log(`Running step ${step} ...`)
      rl.question('Please confirm (yes/no): ', async (answer) => {
        const answerLower = answer.toLowerCase()
        if (answerLower === 'yes' || answerLower === 'y') {
          console.log('')
          if (step === '0') {
            console.log('Clearing all DB data...')
            await clearDB()
          } else if (step === '1') {
            console.log('Clearing member data...')
            await prisma.memberAddress.deleteMany()
            await prisma.memberMaxRating.deleteMany()
            await prisma.member.deleteMany()

            const memberDynamoFilename = 'MemberProfile_dynamo_data.json'
            await importDynamoMember(memberDynamoFilename)
          } else if (step === '2') {
            console.log('Clearing member trait and skill data...')
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

            await prisma.memberSkillLevel.deleteMany()
            await prisma.memberSkill.deleteMany()
            await prisma.skillLevel.deleteMany()
            await prisma.skill.deleteMany()
            await prisma.skillCategory.deleteMany()
            await prisma.displayMode.deleteMany()

            const memberElasticsearchFilename = 'members-2020-01.json'
            await importElasticSearchMember(memberElasticsearchFilename)
          } else if (step === '3') {
            console.log('Clearing member stats data...')
            await prisma.memberCopilotStats.deleteMany()
            await prisma.memberMarathonStats.deleteMany()
            await prisma.memberDesignStatsItem.deleteMany()
            await prisma.memberDesignStats.deleteMany()
            await prisma.memberDevelopStatsItem.deleteMany()
            await prisma.memberDevelopStats.deleteMany()
            await prisma.memberSrmChallengeDetail.deleteMany()
            await prisma.memberSrmDivisionDetail.deleteMany()
            await prisma.memberSrmStats.deleteMany()
            await prisma.memberDataScienceStats.deleteMany()
            await prisma.memberStats.deleteMany()

            const memberStateDynamoFilename = 'MemberStats_dynamo_data.json'
            await importDynamoMemberStat(memberStateDynamoFilename)
          } else if (step === '4') {
            const memberStatElasticsearchFilename = 'memberstats-2020-01.json'
            await importElasticSearchMemberStat(memberStatElasticsearchFilename)
          } else if (step === '5') {
            console.log('Clearing member stats history data...')
            await prisma.memberDataScienceHistoryStats.deleteMany()
            await prisma.memberDevelopHistoryStats.deleteMany()
            await prisma.memberHistoryStats.deleteMany()

            const memberStateHistoryDynamoFilename = 'MemberStatsHistory_dynamo_data.json'
            await importDynamoMemberStatHistory(memberStateHistoryDynamoFilename)

            const memberStatePrivateDynamoFilename = 'MemberStatsHistory_Private_dynamo_data.json'
            await importDynamoMemberStatHistoryPrivate(memberStatePrivateDynamoFilename)
          } else if (step === '6') {
            console.log('Clearing distribution stats data...')
            await prisma.distributionStats.deleteMany()

            await importDistributionStats()
          }
        }

        console.log('Script is finished.')
        rl.close()
      })
    }
  })
}

main()

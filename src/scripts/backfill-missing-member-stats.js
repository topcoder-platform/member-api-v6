const path = require('path')
const fs = require('fs')
const readline = require('readline')

const config = require('./config')
const prismaManager = require('../common/prisma')

const prisma = prismaManager.getClient()

const {
  fixDynamoMemberStatData,
  parseDateFilter,
  shouldProcessRecord
} = require('./migrate-dynamo-data')

const MIGRATE_DIR = config.migrateLocation
const DEFAULT_STATS_FILE = 'MemberStats.json'
const UPDATED_BY = 'member-stats-backfill'
const CREATED_BY = UPDATED_BY

const TOP_LEVEL_FIELDS = ['challenges', 'wins', 'groupId']
const DEVELOP_FIELDS = ['challenges', 'wins', 'mostRecentSubmission', 'mostRecentEventDate']
const DEVELOP_ITEM_FIELDS = [
  'subTrackId',
  'name',
  'challenges',
  'wins',
  'mostRecentSubmission',
  'mostRecentEventDate',
  'numInquiries',
  'submissions',
  'passedScreening',
  'passedReview',
  'appeals',
  'submissionRate',
  'screeningSuccessRate',
  'reviewSuccessRate',
  'appealSuccessRate',
  'minScore',
  'maxScore',
  'avgScore',
  'avgPlacement',
  'winPercent',
  'rating',
  'minRating',
  'maxRating',
  'volatility',
  'reliability',
  'overallRank',
  'overallSchoolRank',
  'overallCountryRank',
  'overallPercentile',
  'activeRank',
  'activeSchoolRank',
  'activeCountryRank',
  'activePercentile'
]
const DESIGN_FIELDS = ['challenges', 'wins', 'mostRecentSubmission', 'mostRecentEventDate']
const DESIGN_ITEM_FIELDS = [
  'subTrackId',
  'name',
  'challenges',
  'wins',
  'mostRecentSubmission',
  'mostRecentEventDate',
  'numInquiries',
  'submissions',
  'passedScreening',
  'avgPlacement',
  'screeningSuccessRate',
  'submissionRate',
  'winPercent'
]
const DATA_SCIENCE_FIELDS = ['challenges', 'wins', 'mostRecentSubmission', 'mostRecentEventDate', 'mostRecentEventName']
const SRM_FIELDS = [
  'challenges',
  'wins',
  'mostRecentSubmission',
  'mostRecentEventDate',
  'mostRecentEventName',
  'rating',
  'percentile',
  'rank',
  'countryRank',
  'schoolRank',
  'volatility',
  'maximumRating',
  'minimumRating',
  'defaultLanguage',
  'competitions'
]
const SRM_DETAIL_FIELDS = ['levelName', 'challenges', 'failedChallenges']
const SRM_DIVISION_FIELDS = ['levelName', 'divisionName', 'problemsSubmitted', 'problemsSysByTest', 'problemsFailed']
const MARATHON_FIELDS = [
  'challenges',
  'wins',
  'mostRecentSubmission',
  'mostRecentEventDate',
  'mostRecentEventName',
  'rating',
  'percentile',
  'rank',
  'avgRank',
  'avgNumSubmissions',
  'bestRank',
  'countryRank',
  'schoolRank',
  'volatility',
  'maximumRating',
  'minimumRating',
  'defaultLanguage',
  'competitions',
  'topFiveFinishes',
  'topTenFinishes'
]
const COPILOT_FIELDS = ['contests', 'projects', 'failures', 'reposts', 'activeContests', 'activeProjects', 'fulfillment']

const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key)
const isMissing = (value) => value === null || value === undefined

function parseCliArgs (argv) {
  const options = { positional: [] }

  argv.forEach((arg) => {
    if (!arg) {
      return
    }
    if (arg.startsWith('--')) {
      const [rawKey, rawValue] = arg.slice(2).split('=')
      const key = rawKey.trim()
      if (!key) {
        return
      }
      if (rawValue === undefined) {
        options[key] = true
      } else {
        options[key] = rawValue
      }
    } else {
      options.positional.push(arg)
    }
  })

  return options
}

function resolveStatsFilePath (requestedPath) {
  if (!requestedPath) {
    return path.join(MIGRATE_DIR, DEFAULT_STATS_FILE)
  }

  if (path.isAbsolute(requestedPath)) {
    return requestedPath
  }

  return path.join(MIGRATE_DIR, requestedPath)
}

function parseHandleFilter (optionValue) {
  if (!optionValue) {
    return null
  }

  const handles = optionValue
    .split(',')
    .map(entry => entry.trim().toLowerCase())
    .filter(entry => entry.length > 0)

  if (handles.length === 0) {
    return null
  }

  return new Set(handles)
}

async function * iterateMemberStatsFile (filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Member stats file not found at ${filePath}`)
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  })

  let buffer = ''
  let capturing = false
  let depth = 0
  let inString = false
  let escapeNext = false

  for await (const line of rl) {
    if (!line) {
      continue
    }

    for (let idx = 0; idx < line.length; idx += 1) {
      const char = line[idx]

      if (!capturing) {
        if (char === '{') {
          capturing = true
          buffer = '{'
          depth = 1
          inString = false
          escapeNext = false
        }
        continue
      }

      buffer += char

      if (escapeNext) {
        escapeNext = false
        continue
      }

      if (char === '\\') {
        escapeNext = true
        continue
      }

      if (char === '"') {
        inString = !inString
        continue
      }

      if (inString) {
        continue
      }

      if (char === '{') {
        depth += 1
      } else if (char === '}') {
        depth -= 1
        if (depth === 0) {
          try {
            yield JSON.parse(buffer)
          } catch (err) {
            console.warn(`Skipping malformed member stat entry: ${err.message}`)
          }
          buffer = ''
          capturing = false
          inString = false
          escapeNext = false
        }
      }
    }
  }

  if (buffer.trim().length > 0) {
    try {
      yield JSON.parse(buffer)
    } catch (err) {
      console.warn(`Trailing member stat payload could not be parsed: ${err.message}`)
    }
  }
}

function normalizeMemberStatRecord (record) {
  if (!record) {
    return null
  }

  const fixed = fixDynamoMemberStatData(record)
  if (!fixed) {
    return null
  }

  const normalized = {
    challenges: fixed.challenges,
    wins: fixed.wins,
    groupId: hasOwn(fixed, 'groupId') ? fixed.groupId : null,
    develop: null,
    design: null,
    dataScience: null,
    copilot: null
  }

  if (fixed.develop) {
    const { items, createdBy: _createdBy, ...rest } = fixed.develop
    normalized.develop = { ...rest }
    if (normalized.develop.createdBy) {
      delete normalized.develop.createdBy
    }
    normalized.develop.items = []
    if (items && Array.isArray(items.create)) {
      normalized.develop.items = items.create.map(item => {
        const { createdBy: _itemCreatedBy, ...itemRest } = item
        return { ...itemRest }
      })
    }
  }

  if (fixed.design) {
    const { items, createdBy: _createdBy, ...rest } = fixed.design
    normalized.design = { ...rest }
    if (normalized.design.createdBy) {
      delete normalized.design.createdBy
    }
    normalized.design.items = []
    if (items && Array.isArray(items.create)) {
      normalized.design.items = items.create.map(item => {
        const { createdBy: _itemCreatedBy, ...itemRest } = item
        return { ...itemRest }
      })
    }
  }

  if (fixed.dataScience) {
    const { srm, marathon, createdBy: _createdBy, ...rest } = fixed.dataScience
    normalized.dataScience = { ...rest }
    if (normalized.dataScience.createdBy) {
      delete normalized.dataScience.createdBy
    }

    if (srm && srm.create) {
      const { challengeDetails, divisions, createdBy: _srmCreatedBy, ...srmRest } = srm.create
      normalized.dataScience.srm = { ...srmRest }
      if (normalized.dataScience.srm.createdBy) {
        delete normalized.dataScience.srm.createdBy
      }
      normalized.dataScience.srm.challengeDetails = []
      normalized.dataScience.srm.divisions = []

      if (challengeDetails && Array.isArray(challengeDetails.create)) {
        normalized.dataScience.srm.challengeDetails = challengeDetails.create.map(detail => {
          const { createdBy: _detailCreatedBy, ...detailRest } = detail
          return { ...detailRest }
        })
      }

      if (divisions && Array.isArray(divisions.create)) {
        normalized.dataScience.srm.divisions = divisions.create.map(division => {
          const { createdBy: _divisionCreatedBy, ...divisionRest } = division
          return { ...divisionRest }
        })
      }
    }

    if (marathon && marathon.create) {
      const { createdBy: _marathonCreatedBy, ...marathonRest } = marathon.create
      normalized.dataScience.marathon = { ...marathonRest }
      if (normalized.dataScience.marathon.createdBy) {
        delete normalized.dataScience.marathon.createdBy
      }
    }
  }

  if (fixed.copilot) {
    const { createdBy: _createdBy, ...rest } = fixed.copilot
    normalized.copilot = { ...rest }
  }

  return normalized
}

function hasMeaningfulData (normalized) {
  if (!normalized) {
    return false
  }

  if (!isMissing(normalized.challenges) || !isMissing(normalized.wins)) {
    return true
  }

  if (normalized.develop || normalized.design || normalized.dataScience || normalized.copilot) {
    return true
  }

  return false
}

function pickPresent (source, fields) {
  const result = {}
  if (!source) {
    return result
  }

  fields.forEach((field) => {
    if (hasOwn(source, field) && source[field] !== undefined) {
      result[field] = source[field]
    }
  })

  return result
}

function extractMissingFields (current, source, fields) {
  const updates = {}
  if (!current || !source) {
    return updates
  }

  fields.forEach((field) => {
    if (!hasOwn(source, field)) {
      return
    }
    const nextValue = source[field]
    if (nextValue === undefined) {
      return
    }
    if (isMissing(current[field])) {
      updates[field] = nextValue
    }
  })

  return updates
}

async function executeWrite (dryRun, description, operation) {
  if (dryRun) {
    console.log(`[DRY-RUN] ${description}`)
    return null
  }

  try {
    return await operation()
  } catch (err) {
    console.error(`Failed to ${description}: ${err.message}`)
    throw err
  }
}

function buildMemberStatsCreateData (normalized, member) {
  if (!normalized) {
    return null
  }

  const data = {
    userId: member.userId,
    createdBy: CREATED_BY
  }

  const topLevel = pickPresent(normalized, TOP_LEVEL_FIELDS)
  Object.assign(data, topLevel)

  if (member.maxRating && member.maxRating.id) {
    data.memberRatingId = member.maxRating.id
  }

  if (normalized.develop) {
    const developData = pickPresent(normalized.develop, DEVELOP_FIELDS)
    if (Object.keys(developData).length > 0 || (normalized.develop.items && normalized.develop.items.length > 0)) {
      data.develop = {
        create: {
          ...developData,
          createdBy: CREATED_BY
        }
      }

      if (normalized.develop.items && normalized.develop.items.length > 0) {
        data.develop.create.items = {
          create: normalized.develop.items.map(item => ({
            ...pickPresent(item, DEVELOP_ITEM_FIELDS),
            createdBy: CREATED_BY
          }))
        }
      }
    }
  }

  if (normalized.design) {
    const designData = pickPresent(normalized.design, DESIGN_FIELDS)
    if (Object.keys(designData).length > 0 || (normalized.design.items && normalized.design.items.length > 0)) {
      data.design = {
        create: {
          ...designData,
          createdBy: CREATED_BY
        }
      }

      if (normalized.design.items && normalized.design.items.length > 0) {
        data.design.create.items = {
          create: normalized.design.items.map(item => ({
            ...pickPresent(item, DESIGN_ITEM_FIELDS),
            createdBy: CREATED_BY
          }))
        }
      }
    }
  }

  if (normalized.dataScience) {
    const dataScienceData = pickPresent(normalized.dataScience, DATA_SCIENCE_FIELDS)
    if (Object.keys(dataScienceData).length > 0 || normalized.dataScience.srm || normalized.dataScience.marathon) {
      data.dataScience = {
        create: {
          ...dataScienceData,
          createdBy: CREATED_BY
        }
      }

      if (normalized.dataScience.srm) {
        const srmData = pickPresent(normalized.dataScience.srm, SRM_FIELDS)
        data.dataScience.create.srm = {
          create: {
            ...srmData,
            createdBy: CREATED_BY
          }
        }

        if (normalized.dataScience.srm.challengeDetails && normalized.dataScience.srm.challengeDetails.length > 0) {
          data.dataScience.create.srm.create.challengeDetails = {
            create: normalized.dataScience.srm.challengeDetails.map(detail => ({
              ...pickPresent(detail, SRM_DETAIL_FIELDS),
              createdBy: CREATED_BY
            }))
          }
        }

        if (normalized.dataScience.srm.divisions && normalized.dataScience.srm.divisions.length > 0) {
          data.dataScience.create.srm.create.divisions = {
            create: normalized.dataScience.srm.divisions.map(division => ({
              ...pickPresent(division, SRM_DIVISION_FIELDS),
              createdBy: CREATED_BY
            }))
          }
        }
      }

      if (normalized.dataScience.marathon) {
        const marathonData = pickPresent(normalized.dataScience.marathon, MARATHON_FIELDS)
        data.dataScience.create.marathon = {
          create: {
            ...marathonData,
            createdBy: CREATED_BY
          }
        }
      }
    }
  }

  if (normalized.copilot) {
    const copilotData = pickPresent(normalized.copilot, COPILOT_FIELDS)
    if (Object.keys(copilotData).length > 0) {
      data.copilot = {
        create: {
          ...copilotData,
          createdBy: CREATED_BY
        }
      }
    }
  }

  return data
}

function selectMemberStatsRecord (existingStats, groupId) {
  if (!existingStats || existingStats.length === 0) {
    return null
  }

  const normalizedGroupId = groupId === undefined ? null : groupId
  const match = existingStats.find(stat => (stat.groupId ?? null) === (normalizedGroupId ?? null))
  if (match) {
    return match
  }

  if (normalizedGroupId === null && existingStats.length === 1) {
    return existingStats[0]
  }

  return null
}

async function backfillDevelopStats (memberStatsRecord, normalizedDevelop, dryRun) {
  if (!normalizedDevelop) {
    return { updated: false }
  }

  const developRecord = memberStatsRecord.develop
  if (!developRecord) {
    const createData = {
      memberStatsId: memberStatsRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedDevelop, DEVELOP_FIELDS)
    }

    if (normalizedDevelop.items && normalizedDevelop.items.length > 0) {
      createData.items = {
        create: normalizedDevelop.items.map(item => ({
          ...pickPresent(item, DEVELOP_ITEM_FIELDS),
          createdBy: CREATED_BY
        }))
      }
    }

    await executeWrite(dryRun, `create develop stats for memberStats ${memberStatsRecord.id}`, () => prisma.memberDevelopStats.create({ data: createData }))
    return { updated: true }
  }

  const updates = extractMissingFields(developRecord, normalizedDevelop, DEVELOP_FIELDS)
  let changed = false

  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update develop stats ${developRecord.id}`, () => prisma.memberDevelopStats.update({
      where: { id: developRecord.id },
      data: updates
    }))
    changed = true
  }

  if (normalizedDevelop.items && normalizedDevelop.items.length > 0) {
    const existingItems = new Map()
    for (const item of developRecord.items || []) {
      if (item && item.name) {
        existingItems.set(item.name.toLowerCase(), item)
      }
    }

    for (const incoming of normalizedDevelop.items) {
      if (!incoming || !incoming.name) {
        continue
      }
      const key = incoming.name.toLowerCase()
      const existing = existingItems.get(key)
      if (!existing) {
        const itemCreate = {
          developStatsId: developRecord.id,
          createdBy: CREATED_BY,
          ...pickPresent(incoming, DEVELOP_ITEM_FIELDS)
        }
        await executeWrite(dryRun, `create develop subTrack ${incoming.name} for develop stats ${developRecord.id}`, () => prisma.memberDevelopStatsItem.create({ data: itemCreate }))
        changed = true
        continue
      }

      const itemUpdates = extractMissingFields(existing, incoming, DEVELOP_ITEM_FIELDS)
      if (Object.keys(itemUpdates).length > 0) {
        itemUpdates.updatedBy = UPDATED_BY
        await executeWrite(dryRun, `update develop subTrack ${existing.id}`, () => prisma.memberDevelopStatsItem.update({
          where: { id: existing.id },
          data: itemUpdates
        }))
        changed = true
      }
    }
  }

  return { updated: changed }
}

async function backfillDesignStats (memberStatsRecord, normalizedDesign, dryRun) {
  if (!normalizedDesign) {
    return { updated: false }
  }

  const designRecord = memberStatsRecord.design
  if (!designRecord) {
    const createData = {
      memberStatsId: memberStatsRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedDesign, DESIGN_FIELDS)
    }

    if (normalizedDesign.items && normalizedDesign.items.length > 0) {
      createData.items = {
        create: normalizedDesign.items.map(item => ({
          ...pickPresent(item, DESIGN_ITEM_FIELDS),
          createdBy: CREATED_BY
        }))
      }
    }

    await executeWrite(dryRun, `create design stats for memberStats ${memberStatsRecord.id}`, () => prisma.memberDesignStats.create({ data: createData }))
    return { updated: true }
  }

  const updates = extractMissingFields(designRecord, normalizedDesign, DESIGN_FIELDS)
  let changed = false

  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update design stats ${designRecord.id}`, () => prisma.memberDesignStats.update({
      where: { id: designRecord.id },
      data: updates
    }))
    changed = true
  }

  if (normalizedDesign.items && normalizedDesign.items.length > 0) {
    const existingItems = new Map()
    for (const item of designRecord.items || []) {
      if (item && item.name) {
        existingItems.set(item.name.toLowerCase(), item)
      }
    }

    for (const incoming of normalizedDesign.items) {
      if (!incoming || !incoming.name) {
        continue
      }
      const key = incoming.name.toLowerCase()
      const existing = existingItems.get(key)
      if (!existing) {
        const itemCreate = {
          designStatsId: designRecord.id,
          createdBy: CREATED_BY,
          ...pickPresent(incoming, DESIGN_ITEM_FIELDS)
        }
        await executeWrite(dryRun, `create design subTrack ${incoming.name} for design stats ${designRecord.id}`, () => prisma.memberDesignStatsItem.create({ data: itemCreate }))
        changed = true
        continue
      }

      const itemUpdates = extractMissingFields(existing, incoming, DESIGN_ITEM_FIELDS)
      if (Object.keys(itemUpdates).length > 0) {
        itemUpdates.updatedBy = UPDATED_BY
        await executeWrite(dryRun, `update design subTrack ${existing.id}`, () => prisma.memberDesignStatsItem.update({
          where: { id: existing.id },
          data: itemUpdates
        }))
        changed = true
      }
    }
  }

  return { updated: changed }
}

async function backfillSrmStats (dataScienceRecord, normalizedSrm, dryRun) {
  if (!normalizedSrm) {
    return { updated: false }
  }

  const srmRecord = dataScienceRecord.srm
  if (!srmRecord) {
    const createData = {
      dataScienceStatsId: dataScienceRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedSrm, SRM_FIELDS)
    }

    if (normalizedSrm.challengeDetails && normalizedSrm.challengeDetails.length > 0) {
      createData.challengeDetails = {
        create: normalizedSrm.challengeDetails.map(detail => ({
          ...pickPresent(detail, SRM_DETAIL_FIELDS),
          createdBy: CREATED_BY
        }))
      }
    }

    if (normalizedSrm.divisions && normalizedSrm.divisions.length > 0) {
      createData.divisions = {
        create: normalizedSrm.divisions.map(division => ({
          ...pickPresent(division, SRM_DIVISION_FIELDS),
          createdBy: CREATED_BY
        }))
      }
    }

    await executeWrite(dryRun, `create SRM stats for dataScience ${dataScienceRecord.id}`, () => prisma.memberSrmStats.create({ data: createData }))
    return { updated: true }
  }

  let changed = false
  const updates = extractMissingFields(srmRecord, normalizedSrm, SRM_FIELDS)
  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update SRM stats ${srmRecord.id}`, () => prisma.memberSrmStats.update({
      where: { id: srmRecord.id },
      data: updates
    }))
    changed = true
  }

  if (normalizedSrm.challengeDetails && normalizedSrm.challengeDetails.length > 0) {
    const existingDetails = new Map()
    for (const detail of srmRecord.challengeDetails || []) {
      if (detail && detail.levelName) {
        existingDetails.set(detail.levelName.toLowerCase(), detail)
      }
    }

    for (const incoming of normalizedSrm.challengeDetails) {
      if (!incoming || !incoming.levelName) {
        continue
      }
      const key = incoming.levelName.toLowerCase()
      const existing = existingDetails.get(key)
      if (!existing) {
        const detailCreate = {
          srmStatsId: srmRecord.id,
          createdBy: CREATED_BY,
          ...pickPresent(incoming, SRM_DETAIL_FIELDS)
        }
        await executeWrite(dryRun, `create SRM challenge detail ${incoming.levelName} for SRM ${srmRecord.id}`, () => prisma.memberSrmChallengeDetail.create({ data: detailCreate }))
        changed = true
        continue
      }

      const detailUpdates = extractMissingFields(existing, incoming, SRM_DETAIL_FIELDS)
      if (Object.keys(detailUpdates).length > 0) {
        detailUpdates.updatedBy = UPDATED_BY
        await executeWrite(dryRun, `update SRM challenge detail ${existing.id}`, () => prisma.memberSrmChallengeDetail.update({
          where: { id: existing.id },
          data: detailUpdates
        }))
        changed = true
      }
    }
  }

  if (normalizedSrm.divisions && normalizedSrm.divisions.length > 0) {
    const existingDivisions = new Map()
    for (const division of srmRecord.divisions || []) {
      if (division && division.levelName && division.divisionName) {
        const key = `${division.levelName.toLowerCase()}::${division.divisionName.toLowerCase()}`
        existingDivisions.set(key, division)
      }
    }

    for (const incoming of normalizedSrm.divisions) {
      if (!incoming || !incoming.levelName || !incoming.divisionName) {
        continue
      }
      const key = `${incoming.levelName.toLowerCase()}::${incoming.divisionName.toLowerCase()}`
      const existing = existingDivisions.get(key)
      if (!existing) {
        const divisionCreate = {
          srmStatsId: srmRecord.id,
          createdBy: CREATED_BY,
          ...pickPresent(incoming, SRM_DIVISION_FIELDS)
        }
        await executeWrite(dryRun, `create SRM division detail ${incoming.levelName}/${incoming.divisionName} for SRM ${srmRecord.id}`, () => prisma.memberSrmDivisionDetail.create({ data: divisionCreate }))
        changed = true
        continue
      }

      const divisionUpdates = extractMissingFields(existing, incoming, SRM_DIVISION_FIELDS)
      if (Object.keys(divisionUpdates).length > 0) {
        divisionUpdates.updatedBy = UPDATED_BY
        await executeWrite(dryRun, `update SRM division detail ${existing.id}`, () => prisma.memberSrmDivisionDetail.update({
          where: { id: existing.id },
          data: divisionUpdates
        }))
        changed = true
      }
    }
  }

  return { updated: changed }
}

async function backfillMarathonStats (dataScienceRecord, normalizedMarathon, dryRun) {
  if (!normalizedMarathon) {
    return { updated: false }
  }

  const marathonRecord = dataScienceRecord.marathon
  if (!marathonRecord) {
    const createData = {
      dataScienceStatsId: dataScienceRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedMarathon, MARATHON_FIELDS)
    }
    await executeWrite(dryRun, `create Marathon stats for dataScience ${dataScienceRecord.id}`, () => prisma.memberMarathonStats.create({ data: createData }))
    return { updated: true }
  }

  const updates = extractMissingFields(marathonRecord, normalizedMarathon, MARATHON_FIELDS)
  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update Marathon stats ${marathonRecord.id}`, () => prisma.memberMarathonStats.update({
      where: { id: marathonRecord.id },
      data: updates
    }))
    return { updated: true }
  }

  return { updated: false }
}

async function backfillDataScienceStats (memberStatsRecord, normalizedDataScience, dryRun) {
  if (!normalizedDataScience) {
    return { updated: false }
  }

  const dataScienceRecord = memberStatsRecord.dataScience
  if (!dataScienceRecord) {
    const createData = {
      memberStatsId: memberStatsRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedDataScience, DATA_SCIENCE_FIELDS)
    }

    if (normalizedDataScience.srm) {
      const srmData = pickPresent(normalizedDataScience.srm, SRM_FIELDS)
      createData.srm = {
        create: {
          ...srmData,
          createdBy: CREATED_BY
        }
      }

      if (normalizedDataScience.srm.challengeDetails && normalizedDataScience.srm.challengeDetails.length > 0) {
        createData.srm.create.challengeDetails = {
          create: normalizedDataScience.srm.challengeDetails.map(detail => ({
            ...pickPresent(detail, SRM_DETAIL_FIELDS),
            createdBy: CREATED_BY
          }))
        }
      }

      if (normalizedDataScience.srm.divisions && normalizedDataScience.srm.divisions.length > 0) {
        createData.srm.create.divisions = {
          create: normalizedDataScience.srm.divisions.map(division => ({
            ...pickPresent(division, SRM_DIVISION_FIELDS),
            createdBy: CREATED_BY
          }))
        }
      }
    }

    if (normalizedDataScience.marathon) {
      const marathonData = pickPresent(normalizedDataScience.marathon, MARATHON_FIELDS)
      createData.marathon = {
        create: {
          ...marathonData,
          createdBy: CREATED_BY
        }
      }
    }

    await executeWrite(dryRun, `create data science stats for memberStats ${memberStatsRecord.id}`, () => prisma.memberDataScienceStats.create({ data: createData }))
    return { updated: true }
  }

  let changed = false
  const updates = extractMissingFields(dataScienceRecord, normalizedDataScience, DATA_SCIENCE_FIELDS)
  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update data science stats ${dataScienceRecord.id}`, () => prisma.memberDataScienceStats.update({
      where: { id: dataScienceRecord.id },
      data: updates
    }))
    changed = true
  }

  if (normalizedDataScience.srm) {
    const srmResult = await backfillSrmStats(dataScienceRecord, normalizedDataScience.srm, dryRun)
    if (srmResult.updated) {
      changed = true
    }
  }

  if (normalizedDataScience.marathon) {
    const marathonResult = await backfillMarathonStats(dataScienceRecord, normalizedDataScience.marathon, dryRun)
    if (marathonResult.updated) {
      changed = true
    }
  }

  return { updated: changed }
}

async function backfillCopilotStats (memberStatsRecord, normalizedCopilot, dryRun) {
  if (!normalizedCopilot) {
    return { updated: false }
  }

  const copilotRecord = memberStatsRecord.copilot
  if (!copilotRecord) {
    const createData = {
      memberStatsId: memberStatsRecord.id,
      createdBy: CREATED_BY,
      ...pickPresent(normalizedCopilot, COPILOT_FIELDS)
    }
    await executeWrite(dryRun, `create copilot stats for memberStats ${memberStatsRecord.id}`, () => prisma.memberCopilotStats.create({ data: createData }))
    return { updated: true }
  }

  const updates = extractMissingFields(copilotRecord, normalizedCopilot, COPILOT_FIELDS)
  if (Object.keys(updates).length > 0) {
    updates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update copilot stats ${copilotRecord.id}`, () => prisma.memberCopilotStats.update({
      where: { id: copilotRecord.id },
      data: updates
    }))
    return { updated: true }
  }

  return { updated: false }
}

async function backfillMemberStatRecord (member, memberStatsRecord, normalized, dryRun) {
  let updated = false

  const topLevelUpdates = extractMissingFields(memberStatsRecord, normalized, TOP_LEVEL_FIELDS)
  if (!memberStatsRecord.memberRatingId && member.maxRating && member.maxRating.id) {
    topLevelUpdates.memberRatingId = member.maxRating.id
  }

  if (Object.keys(topLevelUpdates).length > 0) {
    topLevelUpdates.updatedBy = UPDATED_BY
    await executeWrite(dryRun, `update memberStats ${memberStatsRecord.id}`, () => prisma.memberStats.update({
      where: { id: memberStatsRecord.id },
      data: topLevelUpdates
    }))
    updated = true
  }

  const developResult = await backfillDevelopStats(memberStatsRecord, normalized.develop, dryRun)
  if (developResult.updated) {
    updated = true
  }

  const designResult = await backfillDesignStats(memberStatsRecord, normalized.design, dryRun)
  if (designResult.updated) {
    updated = true
  }

  const dataScienceResult = await backfillDataScienceStats(memberStatsRecord, normalized.dataScience, dryRun)
  if (dataScienceResult.updated) {
    updated = true
  }

  const copilotResult = await backfillCopilotStats(memberStatsRecord, normalized.copilot, dryRun)
  if (copilotResult.updated) {
    updated = true
  }

  return { updated }
}

async function main () {
  const args = parseCliArgs(process.argv.slice(2))
  const statsFilePath = resolveStatsFilePath(args.positional[0] || args.file)
  const handlesFilter = parseHandleFilter(args.handles || args.handle)
  const dryRun = args['dry-run'] === true || args.dryRun === true

  let dateFilter = null
  if (args.date) {
    const parsed = parseDateFilter(args.date)
    if (parsed) {
      dateFilter = parsed
    } else {
      console.warn(`Ignoring invalid date filter value: ${args.date}`)
    }
  }

  console.log(`Reading member stats from ${statsFilePath}`)
  console.log(dryRun ? 'Running in DRY-RUN mode (no changes will be written).' : 'Writes are enabled.')

  let processed = 0
  let created = 0
  let updated = 0
  let skipped = 0
  let missingMembers = 0

  for await (const record of iterateMemberStatsFile(statsFilePath)) {
    processed += 1

    if (handlesFilter && handlesFilter.size > 0) {
      const handleLower = (record.handleLower || record.handle || '').toLowerCase()
      if (!handlesFilter.has(handleLower)) {
        continue
      }
    }

    if (!shouldProcessRecord(record, dateFilter)) {
      skipped += 1
      continue
    }

    const normalized = normalizeMemberStatRecord(record)
    if (!hasMeaningfulData(normalized)) {
      skipped += 1
      continue
    }

    const member = await prisma.member.findFirst({
      where: { userId: record.userId },
      include: { maxRating: true }
    })

    if (!member) {
      console.warn(`No member found for userId ${record.userId}; skipping.`)
      missingMembers += 1
      continue
    }

    const existingStats = await prisma.memberStats.findMany({
      where: { userId: member.userId },
      include: {
        develop: { include: { items: true } },
        design: { include: { items: true } },
        dataScience: {
          include: {
            srm: { include: { challengeDetails: true, divisions: true } },
            marathon: true
          }
        },
        copilot: true
      }
    })

    const targetStats = selectMemberStatsRecord(existingStats, normalized.groupId)

    if (!targetStats) {
      const createData = buildMemberStatsCreateData(normalized, member)
      if (!createData) {
        skipped += 1
        continue
      }

      await executeWrite(dryRun, `create memberStats for user ${member.userId}`, () => prisma.memberStats.create({ data: createData }))
      created += 1
      continue
    }

    const result = await backfillMemberStatRecord(member, targetStats, normalized, dryRun)
    if (result.updated) {
      updated += 1
    }

    if (processed % 100 === 0) {
      console.log(`Processed ${processed} records (created: ${created}, updated: ${updated}, skipped: ${skipped}, missing members: ${missingMembers})`)
    }
  }

  console.log('Backfill completed:')
  console.log(`  Processed records: ${processed}`)
  console.log(`  Created stats:     ${created}`)
  console.log(`  Updated stats:     ${updated}`)
  console.log(`  Skipped records:   ${skipped}`)
  console.log(`  Missing members:   ${missingMembers}`)
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`Backfill execution failed: ${err.message}`)
    process.exitCode = 1
  })
}

module.exports = {
  normalizeMemberStatRecord,
  buildMemberStatsCreateData,
  backfillMemberStatRecord
}

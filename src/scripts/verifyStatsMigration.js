const _ = require('lodash')
const prismaManager = require('../common/prisma')

const prisma = prismaManager.getClient()

const LEGACY_TABLES = [
  'memberDevelopStats',
  'memberDesignStats',
  'memberDataScienceStats',
  'memberDevelopHistoryStats',
  'memberDataScienceHistoryStats',
  'memberHistoryStats'
]

function parseArgs (argv) {
  const args = { samples: 15 }
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--samples' && argv[i + 1]) {
      args.samples = _.clamp(Number(argv[i + 1]), 1, 50)
      i += 1
    }
  }
  return args
}

function toNumber (value) {
  if (_.isNil(value)) {
    return 0
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : 0
}

function toIso (value) {
  if (!value) {
    return null
  }
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) {
    return null
  }
  return date.toISOString()
}

async function tableExists (tableName) {
  const rows = await prisma.$queryRaw`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'members'
      AND table_name = ${tableName}
    ) AS "exists"
  `
  return !!(rows[0] && rows[0].exists)
}

async function countTable (tableName) {
  const exists = await tableExists(tableName)
  if (!exists) {
    return null
  }
  const rows = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::bigint AS count FROM "members"."${tableName}"`)
  return toNumber(rows[0] && rows[0].count)
}

function maxIso (values) {
  let latest = null
  values.forEach(value => {
    const iso = toIso(value)
    if (!iso) {
      return
    }
    if (!latest || iso > latest) {
      latest = iso
    }
  })
  return latest
}

function summarizeRows (rows) {
  return {
    challenges: _.sumBy(rows, row => toNumber(row.challenges)),
    wins: _.sumBy(rows, row => toNumber(row.wins)),
    mostRecentSubmission: maxIso(_.map(rows, 'mostRecentSubmission')),
    mostRecentEventDate: maxIso(_.map(rows, 'mostRecentEventDate'))
  }
}

const TRACK_NAMES = {
  DEVELOP: 'DEVELOP',
  DESIGN: 'DESIGN',
  DATA_SCIENCE: 'DATA_SCIENCE',
  COPILOT: 'COPILOT'
}

function getUnifiedTrackName (trackId) {
  const normalized = String(trackId || '').toUpperCase().trim()
  if (TRACK_NAMES[normalized]) {
    return TRACK_NAMES[normalized]
  }
  if (normalized.includes('DATA') && normalized.includes('SCIENCE')) {
    return TRACK_NAMES.DATA_SCIENCE
  }
  if (normalized.includes('DEVELOP') || normalized === 'DEV') {
    return TRACK_NAMES.DEVELOP
  }
  if (normalized.includes('DESIGN') || normalized === 'DES') {
    return TRACK_NAMES.DESIGN
  }
  if (normalized.includes('COPILOT')) {
    return TRACK_NAMES.COPILOT
  }
  return normalized
}

function resolveUnifiedTrackName (row) {
  const candidates = [row.trackName, row.trackAbbreviation, row.trackId]
  for (const value of candidates) {
    const unifiedTrackName = getUnifiedTrackName(value)
    if (
      unifiedTrackName === TRACK_NAMES.DEVELOP ||
      unifiedTrackName === TRACK_NAMES.DESIGN ||
      unifiedTrackName === TRACK_NAMES.DATA_SCIENCE
    ) {
      return unifiedTrackName
    }
  }
  return null
}

function getRowsForTrack (rowsByTrackId, trackIdsByUnifiedTrack, trackName) {
  const trackIds = trackIdsByUnifiedTrack[trackName] || []
  return _.flatMap(trackIds, trackId => rowsByTrackId[trackId] || [])
}

function compareTrack (label, oldSummary, newSummary) {
  if (!oldSummary) {
    return null
  }

  const diff = {}
  if (toNumber(oldSummary.challenges) !== toNumber(newSummary.challenges)) {
    diff.challenges = { old: toNumber(oldSummary.challenges), new: toNumber(newSummary.challenges) }
  }
  if (toNumber(oldSummary.wins) !== toNumber(newSummary.wins)) {
    diff.wins = { old: toNumber(oldSummary.wins), new: toNumber(newSummary.wins) }
  }
  if (toIso(oldSummary.mostRecentSubmission) !== toIso(newSummary.mostRecentSubmission)) {
    diff.mostRecentSubmission = { old: toIso(oldSummary.mostRecentSubmission), new: toIso(newSummary.mostRecentSubmission) }
  }
  if (toIso(oldSummary.mostRecentEventDate) !== toIso(newSummary.mostRecentEventDate)) {
    diff.mostRecentEventDate = { old: toIso(oldSummary.mostRecentEventDate), new: toIso(newSummary.mostRecentEventDate) }
  }

  return _.isEmpty(diff) ? null : { track: label, diff }
}

async function compareUser (userId, availableTables) {
  const newRows = await prisma.$queryRaw`
    SELECT
      ms."trackId",
      ms."typeId",
      ms."challenges",
      ms."wins",
      ms."mostRecentSubmission",
      ms."mostRecentEventDate",
      ct."name" AS "trackName",
      ct."abbreviation" AS "trackAbbreviation"
    FROM "members"."memberStats" ms
    LEFT JOIN "challenges"."ChallengeTrack" ct
      ON ct."id" = ms."trackId"
    WHERE ms."userId" = ${userId}
  `

  const rowsByTrackId = _.groupBy(newRows, row => String(row.trackId || ''))
  const trackIdsByUnifiedTrack = _.transform(Object.keys(rowsByTrackId), (acc, trackId) => {
    const unifiedTrackName = resolveUnifiedTrackName(rowsByTrackId[trackId][0])
    if (!unifiedTrackName) {
      return
    }
    if (!acc[unifiedTrackName]) {
      acc[unifiedTrackName] = []
    }
    acc[unifiedTrackName].push(trackId)
  }, {})

  const newDevelop = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DEVELOP))
  const newDesign = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DESIGN))
  const newDataScience = summarizeRows(getRowsForTrack(rowsByTrackId, trackIdsByUnifiedTrack, TRACK_NAMES.DATA_SCIENCE))

  const legacyParentRows = await prisma.$queryRaw`
    SELECT id
    FROM "members"."memberStats"
    WHERE "userId" = ${userId}
  `
  const legacyIds = _.map(legacyParentRows, row => row.id)

  const mismatches = []
  if (legacyIds.length === 0) {
    return mismatches
  }

  if (availableTables.memberDevelopStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDevelopStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DEVELOP', summarizeRows(oldRows), newDevelop)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  if (availableTables.memberDesignStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDesignStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DESIGN', summarizeRows(oldRows), newDesign)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  if (availableTables.memberDataScienceStats) {
    const oldRows = await prisma.$queryRaw`
      SELECT "challenges", "wins", "mostRecentSubmission", "mostRecentEventDate"
      FROM "members"."memberDataScienceStats"
      WHERE "memberStatsId" = ANY(${legacyIds})
    `
    if (oldRows.length > 0) {
      const mismatch = compareTrack('DATA_SCIENCE', summarizeRows(oldRows), newDataScience)
      if (mismatch) {
        mismatches.push(mismatch)
      }
    }
  }

  return mismatches
}

async function main () {
  const args = parseArgs(process.argv.slice(2))

  console.log('Checking table availability...')
  const availableTables = {}
  for (const tableName of LEGACY_TABLES) {
    // eslint-disable-next-line no-await-in-loop
    availableTables[tableName] = await tableExists(tableName)
  }
  console.log('Legacy tables found:', availableTables)

  console.log('\nRunning count checks...')
  const unifiedCount = await countTable('memberStats')
  const unifiedHistoryCount = await countTable('memberStatsHistory')
  const developCount = await countTable('memberDevelopStats')
  const designCount = await countTable('memberDesignStats')
  const dataScienceCount = await countTable('memberDataScienceStats')

  const legacyStatsTotal = _.sumBy([developCount, designCount, dataScienceCount], value => toNumber(value))

  console.log('memberStats (unified):', unifiedCount)
  console.log('memberDevelopStats (legacy):', developCount)
  console.log('memberDesignStats (legacy):', designCount)
  console.log('memberDataScienceStats (legacy):', dataScienceCount)
  console.log('legacy stats total:', legacyStatsTotal)
  console.log('memberStatsHistory (unified):', unifiedHistoryCount)

  const legacyHistoryCount = _.sumBy([
    await countTable('memberDevelopHistoryStats'),
    await countTable('memberDataScienceHistoryStats')
  ], value => toNumber(value))
  console.log('legacy history rows (develop + data science):', legacyHistoryCount)

  const rankRows = await prisma.$queryRaw`
    SELECT COUNT(*)::int AS "count"
    FROM "members"."memberStats"
    WHERE "rating" IS NOT NULL
      OR "avgRank" IS NOT NULL
      OR "globalRank" IS NOT NULL
      OR "countryRank" IS NOT NULL
      OR "schoolRank" IS NOT NULL
  `
  console.log('non-null rating/rank rows in unified memberStats:', toNumber(rankRows[0] && rankRows[0].count))

  console.log('\nRunning sampled user comparisons...')
  const sampledUsers = await prisma.$queryRaw`
    SELECT DISTINCT "userId"
    FROM "members"."memberStats"
    ORDER BY random()
    LIMIT ${args.samples}
  `

  let mismatchCount = 0
  for (const row of sampledUsers) {
    // eslint-disable-next-line no-await-in-loop
    const mismatches = await compareUser(row.userId, availableTables)
    if (mismatches.length > 0) {
      mismatchCount += mismatches.length
      console.log(`userId=${row.userId} mismatches:`, JSON.stringify(mismatches))
    }
  }

  if (mismatchCount === 0) {
    console.log(`No sampled mismatches detected across ${sampledUsers.length} users.`)
  } else {
    console.log(`Detected ${mismatchCount} sampled mismatches across ${sampledUsers.length} users.`)
  }
}

main()
  .catch(err => {
    console.error('Verification failed:', err)
    process.exitCode = 1
  })
  .finally(async () => {
    await prisma.$disconnect()
  })

const path = require('path')
const fs = require('fs')
const readline = require('readline')
const config = require('./config')
const prismaManager = require('../common/prisma')

const prisma = prismaManager.getClient()

const {
  fixMemberUpdateData,
  updateMembersWithTraitsAndSkills,
  parseDateFilter,
  shouldProcessRecord
} = require('./migrate-dynamo-data')

const MIGRATE_DIR = config.migrateLocation
const DEFAULT_TRAIT_FILE = 'MemberProfileTrait.json'
const TRAIT_KEY_MAP = new Map([
  ['subscription', 'subscriptions'],
  ['service_provider', 'serviceProvider'],
  ['software', 'software'],
  ['languages', 'language']
])

function parseCliArgs (argv) {
  const options = {
    positional: []
  }

  argv.forEach((arg) => {
    if (arg.startsWith('--')) {
      const [rawKey, rawValue] = arg.slice(2).split('=')
      const key = rawKey.trim()
      if (!key) {
        return
      }
      options[key] = rawValue !== undefined ? rawValue : true
    } else if (arg) {
      options.positional.push(arg)
    }
  })

  return options
}

async function * iterateTraitFile (filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Trait file not found at ${filePath}`)
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  })

  let buffer = ''
  for await (const line of rl) {
    let trimmed = line.trim()
    if (!trimmed || trimmed === ',' || trimmed === '[' || trimmed === ']' || trimmed === '],') {
      continue
    }

    if (trimmed.startsWith('[')) {
      trimmed = trimmed.substring(1).trim()
      if (!trimmed) {
        continue
      }
    }

    if (trimmed.endsWith(']')) {
      trimmed = trimmed.substring(0, trimmed.length - 1).trim()
      if (!trimmed) {
        continue
      }
    }

    if (!buffer) {
      buffer = trimmed
    } else {
      buffer += trimmed
    }

    let candidate = buffer
    if (candidate.endsWith(',')) {
      candidate = candidate.slice(0, -1)
    }

    try {
      const parsed = JSON.parse(candidate)
      buffer = ''
      yield parsed
    } catch (err) {
      buffer += ' '
    }
  }

  if (buffer.trim()) {
    try {
      yield JSON.parse(buffer)
    } catch (err) {
      console.warn(`Trailing JSON fragment could not be parsed: ${err.message}`)
    }
  }
}

function resolveTraitFilePath (requestedPath) {
  if (!requestedPath) {
    return path.join(MIGRATE_DIR, DEFAULT_TRAIT_FILE)
  }
  if (path.isAbsolute(requestedPath)) {
    return requestedPath
  }
  return path.join(MIGRATE_DIR, requestedPath)
}

function resolveTraitSelection (selection) {
  if (!selection) {
    return new Set(TRAIT_KEY_MAP.keys())
  }

  const selectedTraits = selection.split(',')
    .map(entry => entry.trim())
    .filter(entry => entry.length > 0)

  if (selectedTraits.length === 0) {
    return new Set(TRAIT_KEY_MAP.keys())
  }

  const resolved = new Set()
  selectedTraits.forEach((traitId) => {
    if (TRAIT_KEY_MAP.has(traitId)) {
      resolved.add(traitId)
    } else {
      console.warn(`Ignoring unsupported trait "${traitId}"`)
    }
  })

  return resolved.size > 0 ? resolved : new Set(TRAIT_KEY_MAP.keys())
}

async function processTraitRecord (record, traitKey) {
  let traitsPayload = record.traits
  if (typeof traitsPayload === 'string') {
    try {
      traitsPayload = JSON.parse(traitsPayload)
    } catch (err) {
      console.warn(`Skipping trait ${record.traitId} for user ${record.userId}: invalid JSON payload (${err.message})`)
      return { updated: false, reason: 'invalid-json' }
    }
  }

  const dataItems = Array.isArray(traitsPayload && traitsPayload.data) ? traitsPayload.data : []
  if (dataItems.length === 0) {
    return { updated: false, reason: 'empty-trait-data' }
  }

  const memberFromDb = await prisma.member.findFirst({
    where: { userId: record.userId },
    include: { addresses: true }
  })

  if (!memberFromDb) {
    console.warn(`No existing member found for user ${record.userId}; skipping trait ${record.traitId}`)
    return { updated: false, reason: 'missing-member' }
  }

  const normalizedRecord = {
    ...record,
    traits: traitsPayload
  }

  const updatePayload = await fixMemberUpdateData(normalizedRecord, memberFromDb)
  if (!updatePayload || !updatePayload.memberTraits) {
    return { updated: false, reason: 'no-op' }
  }

  const traitValue = updatePayload.memberTraits[traitKey]
  if (!traitValue || (Array.isArray(traitValue) && traitValue.length === 0)) {
    return { updated: false, reason: 'no-op' }
  }

  const traitUpdate = {
    userId: updatePayload.userId,
    memberTraits: {
      [traitKey]: traitValue
    }
  }

  await updateMembersWithTraitsAndSkills(traitUpdate)
  return { updated: true }
}

async function main () {
  const args = parseCliArgs(process.argv.slice(2))
  const traitFilePath = resolveTraitFilePath(args.positional[0] || args.file)
  const selectedTraits = resolveTraitSelection(args.traits)

  let dateFilter = null
  if (args.date) {
    dateFilter = parseDateFilter(args.date)
    if (!dateFilter) {
      console.error('Invalid date filter provided. Expected format YYYY-MM-DD.')
      process.exit(1)
    }
  }

  console.log('Backfilling member traits using file:', traitFilePath)
  console.log('Target trait IDs:', Array.from(selectedTraits).join(', '))
  if (dateFilter) {
    console.log(`Date filter: ${dateFilter.toISOString()}`)
  }

  let totalCandidates = 0
  let filtered = 0
  let updated = 0
  let skipped = 0
  let errors = 0

  try {
    for await (const record of iterateTraitFile(traitFilePath)) {
      if (!record || !record.traitId) {
        continue
      }

      if (!selectedTraits.has(record.traitId)) {
        continue
      }

      totalCandidates += 1

      if (!shouldProcessRecord(record, dateFilter)) {
        filtered += 1
        continue
      }

      const traitKey = TRAIT_KEY_MAP.get(record.traitId)
      if (!traitKey) {
        skipped += 1
        continue
      }

      try {
        const result = await processTraitRecord(record, traitKey)
        if (result.updated) {
          updated += 1
        } else {
          skipped += 1
        }
      } catch (err) {
        errors += 1
        console.error(`Failed processing trait ${record.traitId} for user ${record.userId}: ${err.message}`)
      }
    }
  } finally {
    await prisma.$disconnect().catch(() => {})
  }

  console.log('Backfill complete')
  console.log(`Total targeted traits: ${totalCandidates}`)
  console.log(`Applied updates: ${updated}`)
  console.log(`Skipped: ${skipped}`)
  console.log(`Filtered out by date: ${filtered}`)
  console.log(`Errors: ${errors}`)
}

main().catch((err) => {
  console.error(`Trait backfill failed: ${err.message}`)
  process.exit(1)
})

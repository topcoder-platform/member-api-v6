const path = require('path')
const fs = require('fs')
const readline = require('readline')
const { isEmpty } = require('lodash')
const config = require('./config')
const prisma = require('../common/prisma').getClient()
const { fixMemberUpdateData, updateMembersWithTraitsAndSkills } = require('./migrate-dynamo-data')

const DEFAULT_FILE = 'members-2020-01.json'

async function updateTraitsFromFile (filename) {
  const sourceFile = path.isAbsolute(filename) ? filename : path.join(config.migrateLocation, filename)
  if (!fs.existsSync(sourceFile)) {
    throw new Error(`Cannot find traits file at ${sourceFile}`)
  }

  const readStream = fs.createReadStream(sourceFile)
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity
  })

  let processed = 0
  let updated = 0
  let skippedNoMember = 0
  let skippedNoTraits = 0

  for await (const line of rl) {
    const trimmed = line.trim()
    if (!trimmed) {
      continue
    }

    processed += 1

    let parsed
    try {
      parsed = JSON.parse(trimmed)
    } catch (err) {
      console.warn(`Skipping malformed JSON on line ${processed}: ${err.message}`)
      continue
    }

    const payload = parsed._source || parsed
    if (!payload.userId) {
      skippedNoMember += 1
      continue
    }

    const member = await prisma.member.findFirst({
      where: { userId: payload.userId }
    })

    if (!member) {
      skippedNoMember += 1
      continue
    }

    const updateData = await fixMemberUpdateData(payload, member)
    if (!updateData || !updateData.memberTraits || isEmpty(updateData.memberTraits)) {
      skippedNoTraits += 1
      continue
    }

    const traitUpdate = {
      userId: member.userId,
      memberTraits: updateData.memberTraits
    }

    await updateMembersWithTraitsAndSkills(traitUpdate)
    updated += 1

    if (processed % 100 === 0) {
      console.log(`Processed ${processed} lines, updated ${updated} members`)
    }
  }

  console.log('Trait update complete')
  console.log(`Processed lines: ${processed}`)
  console.log(`Members updated: ${updated}`)
  console.log(`Skipped (no matching member): ${skippedNoMember}`)
  console.log(`Skipped (no trait changes): ${skippedNoTraits}`)
}

async function main () {
  const filename = process.argv[2] || DEFAULT_FILE
  console.log(`Updating member traits from ${filename}`)
  try {
    await updateTraitsFromFile(filename)
  } catch (err) {
    console.error(err.message)
    process.exitCode = 1
  } finally {
    await prisma.$disconnect()
  }
}

if (require.main === module) {
  main()
}

module.exports = {
  updateTraitsFromFile
}

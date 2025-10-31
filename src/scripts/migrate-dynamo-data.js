const path = require('path')
const fs = require('fs')
const readline = require('readline')
const { concat, isArray, isBoolean, isEmpty, isEqual, isInteger, find, omit, pick, isNumber, forEach, map, uniqBy, isString, cloneDeep, flattenDeep } = require('lodash')
const { v4: uuidv4 } = require('uuid')
const config = require('./config')
const prismaManager = require('../common/prisma')
const prisma = prismaManager.getClient()
const skillsPrisma = prismaManager.getSkillsClient()

const CREATED_BY = 'migrate'
const MIGRATE_DIR = config.migrateLocation
const BATCH_SIZE = 1000
const TRANSACTION_TIMEOUT_MS = 60000
const TRANSACTION_MAX_RETRIES = 3
const TRANSACTION_RETRY_DELAY_MS = 1000
const DEFAULT_RATING_COLOR = '#EF3A3A'
const DEFAULT_SRM_ID = 101
const DEFAULT_MARATHON_MATCH_ID = 102
const DRY_RUN = process.env.MEMBER_MIGRATION_DRY_RUN === 'true'
const LOG_LEVELS = {
  INFO: 'INFO',
  WARN: 'WARN',
  ERROR: 'ERROR'
}

const MEMBER_FIELDS = ['userId', 'handle', 'handleLower', 'firstName', 'lastName', 'tracks', 'status',
  'addresses', 'description', 'email', 'country', 'homeCountryCode', 'competitionCountryCode', 'photoURL', 'verified', 'maxRating',
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'loginCount', 'lastLoginDate', 'skills', 'availableForGigs',
  'skillScoreDeduction', 'namesAndHandleAppearance']

const MEMBER_STRING_FIELDS = ['handle', 'handleLower', 'firstName', 'lastName', 'description', 'otherLangName', 'status',
  'newEmail', 'emailVerifyToken', 'newEmailVerifyToken', 'country', 'homeCountryCode', 'competitionCountryCode',
  'photoURL', 'createdBy', 'updatedBy', 'namesAndHandleAppearance']

const MEMBER_STRING_ARRAY_FIELDS = ['tracks']

const MEMBER_STATUS = ['UNVERIFIED', 'ACTIVE', 'INACTIVE_USER_REQUEST', 'INACTIVE_DUPLICATE_ACCOUNT', 'INACTIVE_IRREGULAR_ACCOUNT', 'UNKNOWN']

const DEVICE_TYPE = ['Console', 'Desktop', 'Laptop', 'Smartphone', 'Tablet', 'Wearable', 'Other']

const MAX_RATING_FIELDS = ['rating', 'track', 'subTrack', 'ratingColor']

const MAX_RATING_STRING_FIELDS = ['track', 'subTrack', 'ratingColor', 'createdBy', 'updatedBy']

const ADDRESS_FIELDS = ['streetAddr1', 'streetAddr2', 'city', 'zip', 'stateCode', 'type']

const TRAIT_BASIC_INFO = ['userId', 'country', 'primaryInterestInTopcoder', 'tshirtSize', 'gender', 'shortBio', 'birthDate', 'currentLocation']
const TRAIT_LANGUAGE = ['language', 'spokenLevel', 'writtenLevel']
const TRAIT_SERVICE_PROVIDER = ['serviceProviderType', 'name']
const TRAIT_DEVICE = ['deviceType', 'manufacturer', 'model', 'operatingSystem', 'osVersion', 'osLanguage']
const WORK_INDUSTRY_TYPES = ['Banking', 'ConsumerGoods', 'Energy', 'Entertainment', 'HealthCare', 'Pharma', 'PublicSector', 'TechAndTechnologyService', 'Telecoms', 'TravelAndHospitality']
const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/
const FALLBACK_RECORD_DATE_FIELDS = ['lastLoginDate', 'modified', 'modifiedAt', 'modified_on', 'modifiedOn', 'lastModified', 'lastModifiedAt', 'lastModifiedOn', 'timestamp', 'lastActivityDate']
const NULL_BYTE_REGEX = /\u0000/g

const SKILL_IMPORT_LOG_PATH = path.join(MIGRATE_DIR, 'skill-import.log')
let skillImportLogStream

const CHILD_HISTORY_LOG_PATH = path.join(MIGRATE_DIR, 'child-history.ndjson')
let childHistoryLogStream

const destructiveSteps = new Set(['0', '1', '2', '3', '5', '6'])
const destructiveFlagFromArgs = process.argv.slice(2).some(arg => arg === '--full-reset' || arg.startsWith('--full-reset='))
const allowDestructiveClears = destructiveFlagFromArgs || process.env.ALLOW_DESTRUCTIVE === 'true'
const ALLOW_STALE_DELETIONS = process.env.ALLOW_STALE_DELETIONS === 'true'
const migrationRuntimeState = {
  step: null,
  dateFilter: null
}
const destructiveApprovals = new Map()

function logWithLevel (level, message, context = null) {
  const timestamp = new Date().toISOString()
  const contextSuffix = context ? ` | ${JSON.stringify(context)}` : ''
  const output = `[${level}] ${timestamp} ${message}${contextSuffix}`
  if (level === LOG_LEVELS.ERROR) {
    console.error(output)
  } else if (level === LOG_LEVELS.WARN) {
    console.warn(output)
  } else {
    console.log(output)
  }
}

function logInfo (message, context) {
  logWithLevel(LOG_LEVELS.INFO, message, context)
}

function logWarn (message, context) {
  logWithLevel(LOG_LEVELS.WARN, message, context)
}

function logError (message, context) {
  logWithLevel(LOG_LEVELS.ERROR, message, context)
}

async function executeWrite (description, operation, context = {}) {
  if (DRY_RUN) {
    logInfo(`DRY_RUN active, skipping write: ${description}`, context)
    return null
  }
  try {
    return await operation()
  } catch (err) {
    logError(`Failed to execute write: ${description}`, { ...context, error: err?.message })
    throw err
  }
}

function compactObject (obj) {
  if (!obj) {
    return {}
  }
  const result = {}
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) {
      result[key] = value
    }
  }
  return result
}

function buildAddressKey (address) {
  if (!address) {
    return 'unknown'
  }
  const typePart = (address.type || 'unknown').trim().toLowerCase()
  const streetPart = (address.streetAddr1 || '').trim().toLowerCase()
  return `${typePart}::${streetPart}`
}

function createTraitHash (item) {
  if (!item) {
    return ''
  }
  const ignoreKeys = new Set(['id', 'memberTraitId', 'createdAt', 'updatedAt', 'createdBy', 'updatedBy'])
  const normalized = {}
  Object.keys(item)
    .filter(key => !ignoreKeys.has(key))
    .sort()
    .forEach(key => {
      normalized[key] = item[key]
    })
  return JSON.stringify(normalized)
}

const skillCaches = {
  categoriesById: new Map(),
  skillsById: new Map(),
  skillLevelsById: new Map(),
  skillLevelsByName: new Map(),
  displayModesById: new Map(),
  displayModesByName: new Map()
}

function resetSkillCaches () {
  skillCaches.categoriesById.clear()
  skillCaches.skillsById.clear()
  skillCaches.skillLevelsById.clear()
  skillCaches.skillLevelsByName.clear()
  skillCaches.displayModesById.clear()
  skillCaches.displayModesByName.clear()
}

function normalizeUserId (value) {
  if (value === null || value === undefined) {
    return null
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.trunc(value)
  }

  if (typeof value === 'bigint') {
    return Number(value)
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return Math.trunc(parsed)
    }
  }

  return null
}

function appendSkillImportLog (message) {
  if (!skillImportLogStream) {
    const logDir = path.dirname(SKILL_IMPORT_LOG_PATH)
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true })
    }
    skillImportLogStream = fs.createWriteStream(SKILL_IMPORT_LOG_PATH, { flags: 'a' })
  }
  skillImportLogStream.write(`${new Date().toISOString()} ${message}\n`)
}

function ensureChildHistoryStream () {
  if (!childHistoryLogStream) {
    const logDir = path.dirname(CHILD_HISTORY_LOG_PATH)
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true })
    }
    childHistoryLogStream = fs.createWriteStream(CHILD_HISTORY_LOG_PATH, { flags: 'a' })
  }
  return childHistoryLogStream
}

function toSerializable (value) {
  if (value === null || value === undefined) {
    return value
  }
  if (typeof value === 'bigint') {
    return value.toString()
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (Array.isArray(value)) {
    return value.map(entry => toSerializable(entry))
  }
  if (typeof value === 'object') {
    const normalized = {}
    Object.keys(value).forEach((key) => {
      normalized[key] = toSerializable(value[key])
    })
    return normalized
  }
  return value
}

function appendChildHistoryLog (entry) {
  const stream = ensureChildHistoryStream()
  stream.write(`${JSON.stringify(toSerializable(entry))}\n`)
}

function recordChildHistory (entityName, records, action, context = {}) {
  if (!Array.isArray(records) || records.length === 0) {
    return
  }
  const timestamp = new Date().toISOString()
  const baseContext = {
    ...context,
    step: migrationRuntimeState.step || null,
    dateFilter: migrationRuntimeState.dateFilter ? migrationRuntimeState.dateFilter.toISOString() : null
  }
  records.forEach((record) => {
    appendChildHistoryLog({
      timestamp,
      action,
      entityName,
      recordId: record?.id ?? null,
      userId: record?.userId ?? baseContext.userId ?? null,
      validFrom: record?.createdAt ? new Date(record.createdAt).toISOString() : null,
      validTo: timestamp,
      context: baseContext,
      snapshot: toSerializable(record)
    })
  })
}

async function handleStaleRecords (entityName, txModel, staleRecords, context = {}) {
  if (!Array.isArray(staleRecords) || staleRecords.length === 0) {
    return { action: 'none', count: 0 }
  }

  const logContext = {
    ...context,
    entityName,
    step: migrationRuntimeState.step || null,
    dateFilter: migrationRuntimeState.dateFilter ? migrationRuntimeState.dateFilter.toISOString() : null,
    staleIds: staleRecords.map(record => record.id)
  }

  const initialAction = ALLOW_STALE_DELETIONS ? 'pending-delete' : 'retained'
  recordChildHistory(entityName, staleRecords, initialAction, context)

  if (migrationRuntimeState.dateFilter) {
    logInfo('Incremental migration run detected; retaining stale records for review', logContext)
    return { action: 'skipped', count: staleRecords.length, reason: 'incremental-run' }
  }

  if (!ALLOW_STALE_DELETIONS) {
    logInfo('Destructive stale deletions disabled; logging retained records for review', logContext)
    return { action: 'skipped', count: staleRecords.length, reason: 'configuration-disabled' }
  }

  await executeWrite(`${entityName}.deleteMany`, () => txModel.deleteMany({
    where: {
      id: {
        in: staleRecords.map(record => record.id)
      }
    }
  }), logContext)

  logInfo('Deleted stale records', logContext)
  recordChildHistory(entityName, staleRecords, 'deleted', context)
  return { action: 'deleted', count: staleRecords.length }
}

function isIncrementalRunActive () {
  return Boolean(migrationRuntimeState.dateFilter)
}

function destructiveConfirmationToken (step) {
  return `ERASE STEP ${step}`
}

async function ensureDestructiveApproval ({ step, askQuestion, description }) {
  if (destructiveApprovals.has(step)) {
    return destructiveApprovals.get(step).confirmed === true
  }
  const token = destructiveConfirmationToken(step)
  const answer = (await askQuestion(`Destructive action "${description}" will clear existing data. Type "${token}" to continue, or press Enter to cancel: `)).trim()
  if (answer !== token) {
    logWarn('Destructive confirmation not granted; skipping destructive operation', { step, description })
    destructiveApprovals.set(step, { confirmed: false })
    return false
  }
  destructiveApprovals.set(step, { confirmed: true })
  return true
}

async function withDestructiveGuard ({ step, askQuestion, description }, operation) {
  const context = {
    step,
    description,
    dateFilter: migrationRuntimeState.dateFilter ? migrationRuntimeState.dateFilter.toISOString() : null
  }

  if (!destructiveSteps.has(step)) {
    await operation()
    return true
  }

  if (isIncrementalRunActive()) {
    logInfo('Incremental run detected; destructive clear skipped', context)
    return false
  }

  if (!allowDestructiveClears) {
    logWarn('Destructive clears disabled; rerun with --full-reset or ALLOW_DESTRUCTIVE=true to enable', context)
    return false
  }

  if (!askQuestion || typeof askQuestion !== 'function') {
    throw new Error('Destructive operations require an interactive confirmation handler')
  }

  const approved = await ensureDestructiveApproval({ step, askQuestion, description })
  if (!approved) {
    return false
  }

  await operation()
  return true
}

async function ensureSkillCategory (tx, category) {
  if (!category || !category.id || !category.name) {
    return null
  }

  if (skillCaches.categoriesById.has(category.id)) {
    return skillCaches.categoriesById.get(category.id)
  }

  let existing = await tx.skillCategory.findUnique({
    where: { id: category.id }
  })

  if (!existing) {
    existing = await tx.skillCategory.findFirst({
      where: { name: category.name }
    })
  }

  if (!existing) {
    existing = await tx.skillCategory.create({
      data: {
        id: category.id,
        name: category.name,
        description: category.description || null
      }
    })
  }

  skillCaches.categoriesById.set(existing.id, existing)
  return existing
}

async function ensureSkillDefinition (tx, skill, categoryId) {
  if (!skill || !skill.id || !skill.name) {
    return null
  }

  if (skillCaches.skillsById.has(skill.id)) {
    return skillCaches.skillsById.get(skill.id)
  }

  let existing = await tx.skill.findUnique({
    where: { id: skill.id }
  })

  if (!existing) {
    existing = await tx.skill.findFirst({
      where: { name: skill.name }
    })
  }

  if (!existing) {
    const data = {
      id: skill.id,
      name: skill.name,
      description: skill.description || null
    }

    if (categoryId) {
      data.categoryId = categoryId
    }

    existing = await tx.skill.create({ data })
  } else if (categoryId && existing.categoryId !== categoryId) {
    existing = await tx.skill.update({
      where: { id: existing.id },
      data: { categoryId }
    })
  }

  skillCaches.skillsById.set(existing.id, existing)
  return existing
}

async function ensureSkillLevel (tx, level) {
  if (!level || (!level.id && !level.name)) {
    return null
  }

  const idKey = level.id
  const nameKey = level.name

  if (idKey && skillCaches.skillLevelsById.has(idKey)) {
    return skillCaches.skillLevelsById.get(idKey)
  }

  if (nameKey && skillCaches.skillLevelsByName.has(nameKey)) {
    return skillCaches.skillLevelsByName.get(nameKey)
  }

  let existing = null

  if (idKey) {
    existing = await tx.userSkillLevel.findUnique({
      where: { id: idKey }
    })
  }

  if (!existing && nameKey) {
    existing = await tx.userSkillLevel.findFirst({
      where: { name: nameKey }
    })
  }

  if (!existing) {
    const data = {
      name: nameKey || String(idKey),
      description: level.description || null
    }

    if (idKey) {
      data.id = idKey
    }

    existing = await tx.userSkillLevel.create({ data })
  }

  if (existing.id) {
    skillCaches.skillLevelsById.set(existing.id, existing)
  }

  if (existing.name) {
    skillCaches.skillLevelsByName.set(existing.name, existing)
  }

  return existing
}

async function ensureSkillDisplayMode (tx, displayMode) {
  const fallbackName = 'principal'
  const payload = displayMode && (displayMode.id || displayMode.name) ? displayMode : { name: fallbackName }
  const idKey = payload.id
  const nameKey = payload.name || fallbackName

  if (idKey && skillCaches.displayModesById.has(idKey)) {
    return skillCaches.displayModesById.get(idKey)
  }

  if (nameKey && skillCaches.displayModesByName.has(nameKey)) {
    return skillCaches.displayModesByName.get(nameKey)
  }

  let existing = null

  if (idKey) {
    existing = await tx.userSkillDisplayMode.findUnique({
      where: { id: idKey }
    })
  }

  if (!existing && nameKey) {
    existing = await tx.userSkillDisplayMode.findFirst({
      where: { name: nameKey }
    })
  }

  if (!existing) {
    const data = {
      name: nameKey,
      description: payload.description || null
    }

    if (idKey) {
      data.id = idKey
    }

    existing = await tx.userSkillDisplayMode.create({ data })
  }

  if (existing.id) {
    skillCaches.displayModesById.set(existing.id, existing)
  }

  if (existing.name) {
    skillCaches.displayModesByName.set(existing.name, existing)
  }

  return existing
}

async function syncMemberSkills (userId, memberSkills, handle = null) {
  if (!Array.isArray(memberSkills) || memberSkills.length === 0) {
    return
  }

  const normalizedUserId = normalizeUserId(userId)
  if (normalizedUserId === null) {
    return
  }

  await skillsPrisma.$transaction(async (skillsTx) => {
    const existingUserSkills = await skillsTx.userSkill.findMany({
      where: { userId: normalizedUserId },
      select: {
        id: true,
        skillId: true,
        userSkillLevelId: true,
        userSkillDisplayModeId: true
      }
    })
    const existingMap = new Map()
    existingUserSkills.forEach(record => {
      const key = `${record.skillId}:${record.userSkillLevelId}`
      existingMap.set(key, record)
    })
    const processedSkillIds = new Set()

    for (const skill of memberSkills) {
      if (!skill || !skill.id || !skill.name || !skill.category) {
        continue
      }

      const categoryRecord = await ensureSkillCategory(skillsTx, skill.category)
      if (!categoryRecord) {
        continue
      }

      const skillRecord = await ensureSkillDefinition(skillsTx, skill, categoryRecord.id)
      if (!skillRecord) {
        continue
      }

      const displayModeRecord = await ensureSkillDisplayMode(skillsTx, skill.displayMode)
      if (!displayModeRecord) {
        continue
      }

      const levelCandidates = isArray(skill.levels) ? skill.levels : []
      const filteredLevels = levelCandidates.filter(level => level && (level.id || level.name))
      const uniqueLevels = uniqBy(filteredLevels, level => level.id || level.name)

      if (uniqueLevels.length === 0) {
        continue
      }

      let skillProcessed = false
      for (const level of uniqueLevels) {
        const levelRecord = await ensureSkillLevel(skillsTx, level)
        if (!levelRecord) {
          continue
        }

        const compositeKey = `${skillRecord.id}:${levelRecord.id}`
        const existingEntry = existingMap.get(compositeKey)

        if (existingEntry) {
          if (existingEntry.userSkillDisplayModeId !== displayModeRecord.id) {
            const updated = await skillsTx.userSkill.update({
              where: { id: existingEntry.id },
              data: { userSkillDisplayModeId: displayModeRecord.id }
            })
            existingMap.set(compositeKey, updated)
          }
        } else {
          const created = await skillsTx.userSkill.create({
            data: {
              userId: normalizedUserId,
              skillId: skillRecord.id,
              userSkillLevelId: levelRecord.id,
              userSkillDisplayModeId: displayModeRecord.id
            }
          })
          existingMap.set(compositeKey, created)
        }
        skillProcessed = true
      }
      if (skillProcessed) {
        processedSkillIds.add(skillRecord.id)
      }
    }

    if (processedSkillIds.size > 0) {
      const identifier = handle || `userId:${normalizedUserId}`
      appendSkillImportLog(`Imported ${processedSkillIds.size} skills for user ${identifier}`)
    }
  })
}

/**
 * Clear All DB.
 */
async function clearDB () {
  const context = { step: '0', operation: 'full-reset' }
  console.log('Clearing address and financial data')
  await executeWrite('memberAddress.deleteMany', () => prisma.memberAddress.deleteMany(), context)
  await executeWrite('memberFinancial.deleteMany', () => prisma.memberFinancial.deleteMany(), context)

  console.log('Clearing member stats data')
  await executeWrite('memberCopilotStats.deleteMany', () => prisma.memberCopilotStats.deleteMany(), context)
  await executeWrite('memberMarathonStats.deleteMany', () => prisma.memberMarathonStats.deleteMany(), context)
  await executeWrite('memberDesignStatsItem.deleteMany', () => prisma.memberDesignStatsItem.deleteMany(), context)
  await executeWrite('memberDesignStats.deleteMany', () => prisma.memberDesignStats.deleteMany(), context)
  await executeWrite('memberDevelopStatsItem.deleteMany', () => prisma.memberDevelopStatsItem.deleteMany(), context)
  await executeWrite('memberDevelopStats.deleteMany', () => prisma.memberDevelopStats.deleteMany(), context)
  await executeWrite('memberSrmChallengeDetail.deleteMany', () => prisma.memberSrmChallengeDetail.deleteMany(), context)
  await executeWrite('memberSrmDivisionDetail.deleteMany', () => prisma.memberSrmDivisionDetail.deleteMany(), context)
  await executeWrite('memberSrmStats.deleteMany', () => prisma.memberSrmStats.deleteMany(), context)
  await executeWrite('memberStats.deleteMany', () => prisma.memberStats.deleteMany(), context)
  await executeWrite('memberDataScienceStats.deleteMany', () => prisma.memberDataScienceStats.deleteMany(), context)

  console.log('Clearing member stats history data')
  await executeWrite('memberDataScienceHistoryStats.deleteMany', () => prisma.memberDataScienceHistoryStats.deleteMany(), context)
  await executeWrite('memberDevelopHistoryStats.deleteMany', () => prisma.memberDevelopHistoryStats.deleteMany(), context)
  await executeWrite('memberHistoryStats.deleteMany', () => prisma.memberHistoryStats.deleteMany(), context)

  console.log('Clearing member traits data')
  await executeWrite('memberTraitBasicInfo.deleteMany', () => prisma.memberTraitBasicInfo.deleteMany(), context)
  await executeWrite('memberTraitCommunity.deleteMany', () => prisma.memberTraitCommunity.deleteMany(), context)
  await executeWrite('memberTraitDevice.deleteMany', () => prisma.memberTraitDevice.deleteMany(), context)
  await executeWrite('memberTraitEducation.deleteMany', () => prisma.memberTraitEducation.deleteMany(), context)
  await executeWrite('memberTraitLanguage.deleteMany', () => prisma.memberTraitLanguage.deleteMany(), context)
  await executeWrite('memberTraitOnboardChecklist.deleteMany', () => prisma.memberTraitOnboardChecklist.deleteMany(), context)
  await executeWrite('memberTraitPersonalization.deleteMany', () => prisma.memberTraitPersonalization.deleteMany(), context)
  await executeWrite('memberTraitServiceProvider.deleteMany', () => prisma.memberTraitServiceProvider.deleteMany(), context)
  await executeWrite('memberTraitSoftware.deleteMany', () => prisma.memberTraitSoftware.deleteMany(), context)
  await executeWrite('memberTraitWork.deleteMany', () => prisma.memberTraitWork.deleteMany(), context)
  await executeWrite('memberTraits.deleteMany', () => prisma.memberTraits.deleteMany(), context)

  console.log('Clearing member skills data')
  await executeWrite('skills.userSkill.deleteMany', () => skillsPrisma.userSkill.deleteMany(), context)
  console.log('Clearing skill reference data')
  await executeWrite('skills.userSkillDisplayMode.deleteMany', () => skillsPrisma.userSkillDisplayMode.deleteMany(), context)
  await executeWrite('skills.userSkillLevel.deleteMany', () => skillsPrisma.userSkillLevel.deleteMany(), context)
  await executeWrite('skills.skill.deleteMany', () => skillsPrisma.skill.deleteMany(), context)
  await executeWrite('skills.skillCategory.deleteMany', () => skillsPrisma.skillCategory.deleteMany(), context)
  resetSkillCaches()

  console.log('Clearing maxRating and member data')
  await executeWrite('memberMaxRating.deleteMany', () => prisma.memberMaxRating.deleteMany(), context)
  await executeWrite('member.deleteMany', () => prisma.member.deleteMany(), context)

  console.log('Clearing rating distribution data')
  await executeWrite('distributionStats.deleteMany', () => prisma.distributionStats.deleteMany(), context)

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
 * Parse a YYYY-MM-DD date string into a Date representing the start of that day in UTC.
 * Rejects values that are not strictly formatted as YYYY-MM-DD.
 * @param {String} dateString the date filter string
 * @returns {Date|null} the parsed date at UTC midnight or null when not provided or invalid
 */
function parseDateFilter (dateString) {
  if (!dateString || !dateString.trim()) {
    return null
  }

  const trimmed = dateString.trim()
  if (!DATE_ONLY_REGEX.test(trimmed)) {
    return null
  }

  const parsed = new Date(`${trimmed}T00:00:00.000Z`)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }

  return parsed
}

/**
 * Determine if a record should be processed based on the optional date filter.
 * Falls back to additional timestamp fields (e.g. lastLoginDate, modified) when createdAt/updatedAt are absent.
 * Records without any recognized timestamp fields are skipped when a filter is provided.
 * @param {Object} record the data record to evaluate
 * @param {Date|null} filterDate the filter threshold
 * @returns {Boolean} true when the record passes the filter
 */
function shouldProcessRecord (record, filterDate) {
  if (!filterDate) {
    return true
  }

  if (!record) {
    return false
  }

  const createdAt = _convert2Date(record.createdAt)
  const updatedAt = _convert2Date(record.updatedAt)

  if (createdAt && createdAt >= filterDate) {
    return true
  }

  if (updatedAt && updatedAt >= filterDate) {
    return true
  }

  for (const field of FALLBACK_RECORD_DATE_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(record, field)) {
      continue
    }

    const fallbackValue = _convert2Date(record[field])
    if (fallbackValue && fallbackValue >= filterDate) {
      return true
    }
  }

  return false
}

/**
 * Coerce numeric address fields to strings while leaving other values untouched.
 * Prisma schemas expect string values for address fields, but legacy data may store numbers.
 * @param {Object} address the address object to normalize
 * @returns {Object} the normalized address object
 */
function normalizeAddressFieldStrings (address) {
  if (!address) {
    return address
  }

  const normalized = { ...address }

  for (const field of ADDRESS_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(normalized, field)) {
      continue
    }

    const value = normalized[field]
    if (isNumber(value)) {
      normalized[field] = stripNullBytes(`${value}`)
    } else if (isString(value)) {
      normalized[field] = stripNullBytes(value)
    }
  }

  return normalized
}

/**
 * Ensure the provided fields are stored as strings when numeric values are encountered.
 * Mutates the target object.
 * @param {Object} target object to normalize
 * @param {Array<String>} fieldNames field names expected to store string values
 */
function normalizeStringFields (target, fieldNames) {
  if (!target) {
    return
  }

  for (const field of fieldNames) {
    if (!Object.prototype.hasOwnProperty.call(target, field)) {
      continue
    }

    const value = target[field]
    if (isNumber(value)) {
      target[field] = stripNullBytes(`${value}`)
    } else if (isString(value)) {
      target[field] = stripNullBytes(value)
    }
  }
}

/**
 * Ensure array fields contain string elements when numeric values are encountered.
 * Mutates the target object.
 * @param {Object} target object to normalize
 * @param {Array<String>} fieldNames field names expected to store arrays of strings
 */
function normalizeStringArrayFields (target, fieldNames) {
  if (!target) {
    return
  }

  for (const field of fieldNames) {
    if (!Object.prototype.hasOwnProperty.call(target, field)) {
      continue
    }

    const value = target[field]
    if (isArray(value)) {
      target[field] = value.map(item => {
        if (isNumber(item)) {
          return stripNullBytes(`${item}`)
        }
        return isString(item) ? stripNullBytes(item) : item
      })
    }
  }
}

/**
 * Normalize string-based fields for member max rating payloads.
 * @param {Object} maxRating max rating object to normalize
 */
function normalizeMaxRatingStringFields (maxRating) {
  if (!maxRating) {
    return
  }

  normalizeStringFields(maxRating, MAX_RATING_STRING_FIELDS)
}

/**
 * Normalize all known string-based member fields (including nested structures).
 * Mutates the member object to coerce numeric values to strings where required.
 * @param {Object} member member object to normalize
 */
function normalizeMemberStringFields (member) {
  if (!member) {
    return member
  }

  normalizeStringFields(member, MEMBER_STRING_FIELDS)
  normalizeStringArrayFields(member, MEMBER_STRING_ARRAY_FIELDS)

  if (member.addresses) {
    if (isArray(member.addresses)) {
      member.addresses = member.addresses.map(normalizeAddressFieldStrings)
    } else if (member.addresses.create && isArray(member.addresses.create)) {
      member.addresses.create = member.addresses.create.map(normalizeAddressFieldStrings)
    } else if (member.addresses.update && isArray(member.addresses.update)) {
      member.addresses.update = member.addresses.update.map(normalizeAddressFieldStrings)
    }
  }

  if (member.maxRating) {
    if (member.maxRating.create) {
      normalizeMaxRatingStringFields(member.maxRating.create)
    } else if (member.maxRating.update) {
      normalizeMaxRatingStringFields(member.maxRating.update)
    } else {
      normalizeMaxRatingStringFields(member.maxRating)
    }
  }

  return member
}

function stripNullBytes (value) {
  if (typeof value !== 'string') {
    return value
  }
  return value.replace(NULL_BYTE_REGEX, '')
}

/**
 * Recursively remove null-byte characters from strings in the provided target.
 * Mutates the input so Prisma/Postgres never encounter invalid UTF-8 sequences.
 * @param {*} target any structure to sanitize
 * @returns {Boolean} true when at least one null byte was removed
 */
function sanitizeNullBytesDeep (target) {
  let removed = false

  function visit (value) {
    if (typeof value === 'string') {
      if (NULL_BYTE_REGEX.test(value)) {
        removed = true
        return value.replace(NULL_BYTE_REGEX, '')
      }
      return value
    }

    if (value === null || value === undefined) {
      return value
    }

    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        value[i] = visit(value[i])
      }
      return value
    }

    if (value instanceof Date || Buffer.isBuffer(value)) {
      return value
    }

    if (typeof value === 'object') {
      for (const key of Object.keys(value)) {
        value[key] = visit(value[key])
      }
    }

    return value
  }

  visit(target)
  return removed
}

function ensureDirectoryReadable (dirPath) {
  const errors = []
  try {
    const stats = fs.statSync(dirPath)
    if (!stats.isDirectory()) {
      errors.push('Path is not a directory')
    }
  } catch (err) {
    errors.push(err.message)
  }
  return errors
}

function validateMigrationConfiguration () {
  const errors = []
  const warnings = []

  if (!MIGRATE_DIR) {
    errors.push('MIGRATE_DIR is not configured')
  } else {
    const dirErrors = ensureDirectoryReadable(MIGRATE_DIR)
    if (dirErrors.length > 0) {
      errors.push(`MIGRATE_DIR is not accessible: ${dirErrors.join('; ')}`)
    }
  }

  const requiredEnvVars = ['DATABASE_URL']
  requiredEnvVars.forEach((envVar) => {
    if (!process.env[envVar]) {
      warnings.push(`Environment variable ${envVar} is not set`)
    }
  })

  const result = {
    isValid: errors.length === 0,
    errors,
    warnings
  }

  if (!result.isValid) {
    logError('Migration configuration validation failed', { errors })
  } else {
    logInfo('Migration configuration validated', { warnings })
  }

  return result
}

async function getMemberIntegritySnapshot () {
  const [memberCount, addressCount, traitCount] = await Promise.all([
    prisma.member.count(),
    prisma.memberAddress.count(),
    prisma.memberTraits.count()
  ])

  return {
    memberCount,
    addressCount,
    traitCount,
    capturedAt: new Date().toISOString()
  }
}

async function validateIncrementalMigrationData (sampleRecord = null, dateFilter = null) {
  const warnings = []
  const errors = []

  if (dateFilter && Number.isNaN(dateFilter.getTime())) {
    errors.push('Date filter is invalid or not a real date')
  }

  if (dateFilter && dateFilter > new Date()) {
    warnings.push('Date filter is in the future; no records are expected to match')
  }

  const requiredFields = ['userId', 'handle', 'handleLower', 'email']
  if (sampleRecord) {
    requiredFields.forEach((field) => {
      if (!sampleRecord[field]) {
        errors.push(`Missing required field ${field} in source record`)
      }
    })
  } else {
    warnings.push('No sample record provided for validation; required field checks were skipped')
  }

  try {
    await prisma.$queryRawUnsafe('SELECT 1')
  } catch (err) {
    errors.push(`Database connectivity check failed: ${err.message}`)
  }

  const result = {
    isValid: errors.length === 0,
    warnings,
    errors
  }

  if (!result.isValid) {
    logError('Incremental migration validation failed', { errors })
  } else {
    logInfo('Incremental migration validation completed', { warnings })
  }

  return result
}

async function verifyDataIntegrity (beforeSnapshot, afterSnapshot, context = {}) {
  const issues = []
  if (beforeSnapshot && afterSnapshot) {
    if (afterSnapshot.memberCount < beforeSnapshot.memberCount) {
      issues.push('Member count decreased after migration')
    }
    if (afterSnapshot.addressCount < beforeSnapshot.addressCount) {
      issues.push('Address count decreased after migration')
    }
    if (afterSnapshot.traitCount < beforeSnapshot.traitCount) {
      issues.push('Trait count decreased after migration')
    }
  }

  let orphanAddresses = 0
  let orphanTraits = 0

  try {
    orphanAddresses = await prisma.memberAddress.count({
      where: {
        member: {
          is: null
        }
      }
    })
  } catch (err) {
    logWarn('Failed to detect orphan member addresses', { error: err.message })
  }

  try {
    orphanTraits = await prisma.memberTraits.count({
      where: {
        member: {
          is: null
        }
      }
    })
  } catch (err) {
    logWarn('Failed to detect orphan member traits', { error: err.message })
  }

  const summary = {
    beforeSnapshot,
    afterSnapshot,
    orphanAddresses,
    orphanTraits,
    context
  }

  if (issues.length > 0 || orphanAddresses > 0 || orphanTraits > 0) {
    logWarn('Data integrity verification detected potential issues', { ...summary, issues })
  } else {
    logInfo('Data integrity verification passed', summary)
  }

  return {
    issues,
    orphanAddresses,
    orphanTraits
  }
}

/**
 * Import the Dynamo members from file
 * @param {String} filename filename
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importDynamoMember (filename, dateFilter = null) {
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
  // count skipped items due to date filter
  let skipped = 0
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
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}`)
    }

    // normalize line content so we can handle both pretty-printed JSON arrays
    // and files where each member appears on a single line.
    let trimmedLine = line.trim()
    if (!trimmedLine || trimmedLine === ',' || trimmedLine === '[' || trimmedLine === ']' || trimmedLine === '],') {
      continue
    }

    // strip leading/trailing array delimiters that might be attached to the object
    if (trimmedLine.startsWith('[')) {
      trimmedLine = trimmedLine.substring(1).trim()
    }
    if (trimmedLine.endsWith(']')) {
      trimmedLine = trimmedLine.substring(0, trimmedLine.length - 1).trim()
      if (!trimmedLine) {
        continue
      }
    }

    if (!stringObject) {
      stringObject = trimmedLine
    } else {
      stringObject += trimmedLine
    }

    let jsonCandidate = stringObject
    if (jsonCandidate.endsWith(',')) {
      jsonCandidate = jsonCandidate.slice(0, -1)
    }

    let dataItem
    try {
      dataItem = JSON.parse(jsonCandidate)
    } catch (err) {
      // keep collecting lines until we have a full JSON object
      continue
    }

    count += 1
    if (!shouldProcessRecord(dataItem, dateFilter)) {
      skipped += 1
      stringObject = ''
      continue
    }
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
  }

  // attempt to parse any remaining buffered JSON object
  if (stringObject) {
    let jsonCandidate = stringObject
    if (jsonCandidate.endsWith(',')) {
      jsonCandidate = jsonCandidate.slice(0, -1)
    }
    if (jsonCandidate) {
      try {
        const dataItem = JSON.parse(jsonCandidate)
        count += 1
        if (!shouldProcessRecord(dataItem, dateFilter)) {
          skipped += 1
        } else {
          const dataObj = await fixMemberData(dataItem, batchItems)
          if (dataObj) {
            batchItems.push(dataObj)
          }
        }
      } catch (err) {
        console.warn(`Skipping malformed member JSON object near line ${currentLine}`)
      }
    }
  }

  // batchItems still contains some data, input them into DB
  if (batchItems.length > 0) {
    await createMembers(batchItems)
    total += batchItems.length
  }
  logInfo('Dynamo member import completed', {
    filename,
    processed: count,
    inserted: total,
    skipped
  })
  console.log(`\nIt has inserted ${total} items totally, skipped ${skipped} items`)

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
      addressArr = addressArr.map(addressItem => {
        const normalized = normalizeAddressFieldStrings(addressItem)
        const zipValue = normalized.zip
        return {
          ...normalized,
          zip: zipValue !== null && zipValue !== undefined ? `${zipValue}` : undefined,
          type: normalized.type ? normalized.type : 'HOME',
          createdAt: _convert2Date(normalized.createdAt),
          createdBy: CREATED_BY,
          updatedAt: _convert2Date(normalized.updatedAt),
          updatedBy: normalized.updatedBy ? normalized.updatedBy : undefined
        }
      })
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

  normalizeMemberStringFields(memberItem)
  const removedNullBytes = sanitizeNullBytesDeep(memberItem)

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

  normalizeMemberStringFields(memberItemDB)
  const removedNullBytesDb = sanitizeNullBytesDeep(memberItemDB)

  if ((removedNullBytes || removedNullBytesDb) && memberItemDB.userId) {
    console.warn(`Sanitized null bytes for member ${memberItemDB.userId}`)
  }

  if (memberItemDB.userId && memberItemDB.handle && memberItemDB.handleLower && memberItemDB.email) {
    return memberItemDB
  }

  return null
}

function isTransactionTimeoutError (err) {
  return err?.code === 'P2028' || (err?.message && err.message.includes('Transaction already closed'))
}

function delay (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function executeWithTransactionRetry (operation, attempt = 1) {
  try {
    return await operation()
  } catch (err) {
    if (!isTransactionTimeoutError(err) || attempt >= TRANSACTION_MAX_RETRIES) {
      throw err
    }
    console.warn(`Transaction timed out (attempt ${attempt}/${TRANSACTION_MAX_RETRIES}). Retrying...`)
    await delay(attempt * TRANSACTION_RETRY_DELAY_MS)
    return executeWithTransactionRetry(operation, attempt + 1)
  }
}

function isInvalidUtf8Error (err) {
  if (!err) {
    return false
  }

  if (err.code === '22021') {
    return true
  }

  const message = err.message || ''
  return message.includes('invalid byte sequence for encoding "UTF8"') || message.includes('0x00')
}

function isUniqueConstraintError (err) {
  if (!err) {
    return false
  }
  if (err.code === 'P2002') {
    return true
  }
  const message = err.message || ''
  return message.includes('Unique constraint failed')
}

function logUniqueConstraintSkip (memberItem, err) {
  const identifier = compactObject({
    userId: memberItem?.userId,
    handle: memberItem?.handle,
    handleLower: memberItem?.handleLower
  })
  logWarn('Skipping member due to unique constraint violation', {
    ...identifier,
    target: err?.meta?.target
  })
}

async function createMembersIndividually (memberItems) {
  for (const memberItem of memberItems) {
    try {
      await executeWithTransactionRetry(() => prisma.$transaction(async (tx) => {
        await tx.member.create({
          data: memberItem
        })
      }, {
        timeout: TRANSACTION_TIMEOUT_MS
      }))
    } catch (err) {
      if (isUniqueConstraintError(err)) {
        logUniqueConstraintSkip(memberItem, err)
        continue
      }
      if (isInvalidUtf8Error(err)) {
        console.warn(`Skipping member ${memberItem.userId || memberItem.handleLower || 'unknown'} due to invalid UTF-8 data`)
        continue
      }
      throw err
    }
  }
}

/**
 * Crate member items in DB
 * @param {Array} memberItems member items
 */
async function createMembers (memberItems) {
  const memberWithAddress = memberItems.filter(item => item.addresses || item.maxRating)
  const memberWithoutAddress = memberItems.filter(item => !(item.addresses || item.maxRating))
  try {
    return await executeWithTransactionRetry(() => prisma.$transaction(async (tx) => {
      if (memberWithoutAddress.length > 0) {
        await tx.member.createMany({
          data: memberWithoutAddress
        })
      }

      for (const memberItem of memberWithAddress) {
        await tx.member.create({
          data: memberItem
        })
      }
    }, {
      timeout: TRANSACTION_TIMEOUT_MS
    }))
  } catch (err) {
    if (isInvalidUtf8Error(err)) {
      console.warn('Batch insert failed due to invalid UTF-8 data. Falling back to per-member inserts.')
      await createMembersIndividually(memberItems)
      return
    }
    if (isUniqueConstraintError(err)) {
      console.warn('Batch insert failed due to unique constraint violation. Falling back to per-member inserts.')
      await createMembersIndividually(memberItems)
      return
    }
    throw err
  }
}

/**
 * Import the Dynamo member stats from file
 * @param {String} filename filename
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importDynamoMemberStat (filename, dateFilter = null) {
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
  // count skipped items due to date filter
  let skipped = 0
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
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}`)
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

      if (!shouldProcessRecord(dataItem, dateFilter)) {
        skipped += 1
        stringObject = ''
        continue
      }

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

  console.log(`\nIt has inserted ${total} items totally, skipped ${skipped} items`)

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
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importDynamoMemberStatHistory (filename, dateFilter = null) {
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
  // count skipped items due to date filter
  let skipped = 0
  // store the temp json object string
  const processObjectString = async (objectString) => {
    let candidate = objectString.trim()
    if (!candidate) {
      return false
    }

    if (candidate.endsWith(',')) {
      candidate = candidate.slice(0, -1)
    }

    if (!candidate.startsWith('{') || !candidate.endsWith('}')) {
      return false
    }

    let dataItem
    try {
      dataItem = JSON.parse(candidate)
    } catch (err) {
      // The buffer does not yet contain a complete JSON object; keep accumulating.
      return false
    }

    count += 1

    if (!shouldProcessRecord(dataItem, dateFilter)) {
      skipped += 1
      return true
    }

    const member = await prisma.member.findFirst({
      where: {
        userId: dataItem.userId
      }
    })

    if (member) {
      const statHistory = fixDynamoMemberStatHistoryData(dataItem)

      if (!isEmpty(statHistory)) {
        const createData = {
          groupId: dataItem.groupId,
          createdBy: CREATED_BY,
          userId: member.userId
        }

        if (statHistory.develop) {
          createData.develop = {
            create: statHistory.develop
          }
        }

        if (statHistory.dataScience) {
          createData.dataScience = {
            create: statHistory.dataScience
          }
        }

        if (createData.develop || createData.dataScience) {
          await prisma.memberHistoryStats.create({
            data: createData,
            include: { develop: true, dataScience: true }
          })
          total += 1
        }
      }
    }

    return true
  }

  let buffer = ''
  let capturingObject = false
  let depth = 0
  let inString = false
  let escapeNext = false

  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}`)
    }
    for (let idx = 0; idx < line.length; idx += 1) {
      const char = line[idx]

      if (!capturingObject) {
        if (char === '{') {
          capturingObject = true
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
          const objectString = buffer
          // eslint-disable-next-line no-await-in-loop
          await processObjectString(objectString)
          buffer = ''
          capturingObject = false
          inString = false
          escapeNext = false
        }
      }
    }
  }

  // Process any remaining buffered object.
  if (buffer) {
    await processObjectString(buffer)
  }

  console.log(`\nIt has inserted ${total} items totally, skipped ${skipped} items`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Import the Dynamo member stat history from file
 * @param {String} filename filename
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importDynamoMemberStatHistoryPrivate (filename, dateFilter = null) {
  const memberStatHistoryDynamoFilePath = path.join(MIGRATE_DIR, filename)

  let lineCount = 0
  let stringObject = ''
  let total = 0
  let skipped = 0
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
    if (!shouldProcessRecord(dataItem, dateFilter)) {
      skipped += 1
      continue
    }
    const member = await prisma.member.findFirst({
      where: {
        userId: dataItem.userId
      }
    })

    if (member) {
      const statHistory = fixDynamoMemberStatHistoryData(dataItem)

      const createData = {
        groupId: dataItem.groupId,
        isPrivate: true,
        createdBy: CREATED_BY,
        userId: member.userId
      }

      if (statHistory.develop) {
        createData.develop = {
          create: statHistory.develop
        }
      }

      if (statHistory.dataScience) {
        createData.dataScience = {
          create: statHistory.dataScience
        }
      }

      if (createData.develop || createData.dataScience) {
        await prisma.memberHistoryStats.create({
          data: createData,
          include: { develop: true, dataScience: true }
        })

        total += 1
      }
    }
  }

  console.log(`\nIt has inserted ${total} items totally, skipped ${skipped} items`)

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
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importElasticSearchMember (filename, dateFilter = null) {
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
  // count skipped items due to date filter
  let skipped = 0
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}`)
    }

    count += 1
    const dataItem = JSON.parse(line.trim())

    if (!shouldProcessRecord(dataItem._source, dateFilter)) {
      skipped += 1
      continue
    }

    const dbItem = await prisma.member.findFirst({
      where: {
        userId: dataItem._source.userId
      },
      include: {
        addresses: true
      }
    })

    const dataObj = await fixMemberUpdateData(dataItem._source, dbItem || {})
    if (!dataObj) {
      continue
    }

    await updateMembersWithTraitsAndSkills(dataObj)
    total += 1
  }

  console.log(`\nIt has updated ${total} items totally, skipped ${skipped} items`)

  console.log(`Finished reading the file: ${filename}\n`)
}

/**
 * Update member status values from ElasticSearch snapshot
 * @param {String} filename filename
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function updateMemberStatusFromElasticSearch (filename, dateFilter = null) {
  const memberElasticFilePath = path.join(MIGRATE_DIR, filename)

  const lineCount = await countFileLines(memberElasticFilePath)
  console.log(`${filename} has ${lineCount} lines in total`)

  const rlRead = readline.createInterface({
    input: fs.createReadStream(memberElasticFilePath),
    crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
  })

  let currentLine = 0
  let count = 0
  let total = 0
  let skipped = 0
  let unchanged = 0
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      process.stdout.clearLine()
      process.stdout.cursorTo(0)
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}, unchanged ${unchanged}`)
    }

    count += 1
    const parsedLine = JSON.parse(line.trim())
    const dataItem = parsedLine._source || parsedLine

    if (!shouldProcessRecord(dataItem, dateFilter)) {
      skipped += 1
      continue
    }

    const normalizedUserId = normalizeUserId(dataItem.userId)
    const statusValue = dataItem.status
    if (!normalizedUserId || !isString(statusValue)) {
      skipped += 1
      continue
    }

    let normalizedStatus = statusValue
    if (normalizedStatus === 'INACTIVE') {
      normalizedStatus = 'INACTIVE_USER_REQUEST'
    } else if (!MEMBER_STATUS.includes(normalizedStatus)) {
      skipped += 1
      continue
    }

    const updateResult = await prisma.member.updateMany({
      where: {
        userId: normalizedUserId,
        NOT: {
          status: normalizedStatus
        }
      },
      data: {
        status: normalizedStatus
      }
    })

    if (updateResult.count > 0) {
      total += updateResult.count
    } else {
      unchanged += 1
    }
  }

  console.log(`\nIt has updated ${total} items totally, skipped ${skipped} items, unchanged ${unchanged} items`)

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
    const firstUpdateAddress = memberItemUpdate.addresses['0'] || {}
    const updateAddr = normalizeAddressFieldStrings(pick(firstUpdateAddress, ADDRESS_FIELDS))
    let addressItem = {}
    if (dbItem.addresses && dbItem.addresses.length > 0) {
      addressItem = dbItem.addresses[0]
    }
    const dbAddr = normalizeAddressFieldStrings(pick(addressItem, ADDRESS_FIELDS))

    if (isEqual(updateAddr, dbAddr)) {
      memberItemUpdate = omit(memberItemUpdate, ['addresses'])
    } else if (updateAddr.type) {
      const zipValue = updateAddr.zip
      memberItemUpdate.addresses = [{
        ...updateAddr,
        zip: zipValue !== null && zipValue !== undefined ? `${zipValue}` : undefined
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
      const hobbyValues = []
      forEach(memberItem.traits.data, traitData => {
        if (!traitData || traitData.hobby === undefined || traitData.hobby === null) {
          return
        }
        const rawValues = isArray(traitData.hobby) ? flattenDeep(traitData.hobby) : [traitData.hobby]
        forEach(rawValues, hobbyValue => {
          if (isString(hobbyValue) && hobbyValue.trim()) {
            hobbyValues.push(hobbyValue)
          } else if (hobbyValue !== undefined && hobbyValue !== null) {
            console.warn(`[WARN] Skipping invalid hobby value for user ${memberItem.userId}: ${JSON.stringify(hobbyValue)}`)
          }
        })
      })
      if (!isEmpty(hobbyValues)) {
        memberItemUpdate.memberTraits.hobby = hobbyValues
      }
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

  normalizeMemberStringFields(memberItemUpdate)

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
  const hasUpdatableFields = !isEmpty(omit(memberObj, ['userId', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'lastLoginDate']))
  const context = { userId: memberObj.userId }

  if (hasUpdatableFields) {
    try {
      await executeWithTransactionRetry(() => prisma.$transaction(async (tx) => {
        const onlyMemberObj = omit(memberObj, ['userId', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'lastLoginDate', 'skills', 'maxRating', 'addresses', 'memberTraits', 'memberSkills'])

        if (onlyMemberObj.status === 'INACTIVE') {
          onlyMemberObj.status = 'INACTIVE_USER_REQUEST'
        } else if (!MEMBER_STATUS.find(status => status === onlyMemberObj.status)) {
          onlyMemberObj.status = 'UNKNOWN'
        }

        const memberUpdateData = compactObject(onlyMemberObj)

        if (!isEmpty(memberUpdateData) || (memberObj.userId && memberObj.handle && memberObj.handleLower && memberObj.email)) {
          const createDefaults = compactObject({
            userId: memberObj.userId,
            handle: memberObj.handle,
            handleLower: memberObj.handleLower,
            email: memberObj.email,
            status: memberUpdateData.status || memberObj.status || 'UNKNOWN',
            firstName: memberObj.firstName,
            lastName: memberObj.lastName,
            country: memberObj.country,
            homeCountryCode: memberObj.homeCountryCode,
            competitionCountryCode: memberObj.competitionCountryCode,
            photoURL: memberObj.photoURL,
            tracks: memberObj.tracks,
            availableForGigs: memberObj.availableForGigs,
            namesAndHandleAppearance: memberObj.namesAndHandleAppearance,
            createdBy: memberObj.createdBy || CREATED_BY,
            updatedBy: memberObj.updatedBy || memberUpdateData.updatedBy || CREATED_BY
          })

          const requiredForCreate = ['userId', 'handle', 'handleLower', 'email']
          const missingRequiredForCreate = requiredForCreate.filter(field => !createDefaults[field])

          if (missingRequiredForCreate.length > 0) {
            logWarn('Skipping member upsert due to missing required fields', { ...context, missingFields: missingRequiredForCreate })
            if (!isEmpty(memberUpdateData)) {
              await executeWrite('member.update', () => tx.member.update({
                where: { userId: memberObj.userId },
                data: memberUpdateData
              }), context)
            }
          } else {
            const createData = { ...createDefaults, ...memberUpdateData }
            await executeWrite('member.upsert', () => tx.member.upsert({
              where: { userId: memberObj.userId },
              update: memberUpdateData,
              create: createData
            }), context)
          }
        }

        if (memberObj.maxRating && memberObj.maxRating.rating && memberObj.maxRating.track && memberObj.maxRating.subTrack) {
          const existingMaxRating = await tx.memberMaxRating.findFirst({
            where: { userId: memberObj.userId }
          })

          const maxRatingData = compactObject({
            ...memberObj.maxRating,
            ratingColor: memberObj.maxRating.ratingColor || DEFAULT_RATING_COLOR,
            userId: memberObj.userId
          })

          if (existingMaxRating) {
            const diff = {}
            MAX_RATING_FIELDS.forEach((field) => {
              if (maxRatingData[field] !== undefined && !isEqual(existingMaxRating[field], maxRatingData[field])) {
                diff[field] = maxRatingData[field]
              }
            })
            if (!isEmpty(diff)) {
              diff.updatedBy = CREATED_BY
              await executeWrite('memberMaxRating.update', () => tx.memberMaxRating.update({
                where: { id: existingMaxRating.id },
                data: diff
              }), context)
            }
          } else {
            await executeWrite('memberMaxRating.create', () => tx.memberMaxRating.create({
              data: {
                ...maxRatingData,
                createdBy: memberObj.maxRating.createdBy || CREATED_BY
              }
            }), context)
          }
        }

        if (memberObj.addresses && memberObj.addresses.length > 0) {
          await syncMemberAddresses(tx, memberObj.userId, memberObj.addresses)
        }

        if (memberObj.memberTraits) {
          let memberTraitsDB = await tx.memberTraits.findFirst({
            where: {
              userId: memberObj.userId
            }
          })

          if (!memberTraitsDB) {
            const createdMemberTraits = await executeWrite('memberTraits.create', () => tx.memberTraits.create({
              data: {
                userId: memberObj.userId,
                createdBy: CREATED_BY
              }
            }), context)
            memberTraitsDB = createdMemberTraits || memberTraitsDB
            if (!memberTraitsDB) {
              memberTraitsDB = { id: null }
            }
          }

          if (memberTraitsDB) {
            if ((memberObj.memberTraits.subscriptions && memberObj.memberTraits.subscriptions.length > 0) || (memberObj.memberTraits.hobby && memberObj.memberTraits.hobby.length > 0)) {
              const toUpdateObj = {}
              if (memberObj.memberTraits.subscriptions && memberObj.memberTraits.subscriptions.length > 0) {
                toUpdateObj.subscriptions = memberObj.memberTraits.subscriptions
              }
              if (memberObj.memberTraits.hobby && memberObj.memberTraits.hobby.length > 0) {
                const sanitizedHobbies = []
                const hobbySource = isArray(memberObj.memberTraits.hobby) ? flattenDeep(memberObj.memberTraits.hobby) : [memberObj.memberTraits.hobby]
                forEach(hobbySource, hobbyValue => {
                  if (isString(hobbyValue) && hobbyValue.trim()) {
                    sanitizedHobbies.push(hobbyValue)
                  }
                })
                if (isEmpty(sanitizedHobbies)) {
                  logWarn('Skipping hobby update due to invalid values', context)
                } else {
                  toUpdateObj.hobby = sanitizedHobbies
                }
              }
              if (!isEmpty(toUpdateObj)) {
                await executeWrite('memberTraits.update', () => tx.memberTraits.update({
                  where: {
                    id: memberTraitsDB.id
                  },
                  data: toUpdateObj
                }), context)
              }
            }

            await updateTraitElement(memberObj.memberTraits.device, tx.memberTraitDevice, memberTraitsDB.id, CREATED_BY, 'memberTraitDevice', context)
            await updateTraitElement(memberObj.memberTraits.software, tx.memberTraitSoftware, memberTraitsDB.id, CREATED_BY, 'memberTraitSoftware', context)
            await updateTraitElement(memberObj.memberTraits.serviceProvider, tx.memberTraitServiceProvider, memberTraitsDB.id, CREATED_BY, 'memberTraitServiceProvider', context)
            await updateTraitElement(memberObj.memberTraits.work, tx.memberTraitWork, memberTraitsDB.id, CREATED_BY, 'memberTraitWork', context)
            await updateTraitElement(memberObj.memberTraits.education, tx.memberTraitEducation, memberTraitsDB.id, CREATED_BY, 'memberTraitEducation', context)
            await updateTraitElement(memberObj.memberTraits.basicInfo, tx.memberTraitBasicInfo, memberTraitsDB.id, CREATED_BY, 'memberTraitBasicInfo', context)
            await updateTraitElement(memberObj.memberTraits.language, tx.memberTraitLanguage, memberTraitsDB.id, CREATED_BY, 'memberTraitLanguage', context)
            await updateTraitElement(memberObj.memberTraits.checklist, tx.memberTraitOnboardChecklist, memberTraitsDB.id, CREATED_BY, 'memberTraitOnboardChecklist', context)
            await updateTraitElement(memberObj.memberTraits.personalization, tx.memberTraitPersonalization, memberTraitsDB.id, CREATED_BY, 'memberTraitPersonalization', context)
            await updateTraitElement(memberObj.memberTraits.community, tx.memberTraitCommunity, memberTraitsDB.id, CREATED_BY, 'memberTraitCommunity', context)
          }
        }
      }, {
        timeout: TRANSACTION_TIMEOUT_MS
      }))
    } catch (err) {
      logError('Failed to update member with traits and skills', { ...context, error: err?.message })
      throw err
    }
  }

  if (memberObj.memberSkills && memberObj.memberSkills.length > 0) {
    try {
      await syncMemberSkills(memberObj.userId, memberObj.memberSkills, memberObj.handle)
    } catch (err) {
      logError('Failed to sync member skills', { ...context, error: err?.message })
      throw err
    }
  }
}

async function syncMemberAddresses (tx, userId, addresses = []) {
  if (!Array.isArray(addresses) || addresses.length === 0) {
    logInfo('No addresses provided for synchronization', { userId })
    return
  }

  const existingAddresses = await tx.memberAddress.findMany({
    where: { userId }
  })

  const processedIds = new Set()
  const normalizedIncoming = addresses.map((address) => {
    const clonedAddress = cloneDeep(address || {})
    sanitizeNullBytesDeep(clonedAddress)
    const normalized = normalizeAddressFieldStrings(clonedAddress)
    return {
      ...normalized,
      userId,
      createdBy: normalized.createdBy || CREATED_BY
    }
  })

  for (const incoming of normalizedIncoming) {
    const addressKey = buildAddressKey(incoming)
    const candidateById = incoming.id ? existingAddresses.find(existing => `${existing.id}` === `${incoming.id}`) : null
    let matchingAddress = candidateById || existingAddresses.find(existing => buildAddressKey(existing) === addressKey)

    const updatePayload = compactObject({
      streetAddr1: incoming.streetAddr1,
      streetAddr2: incoming.streetAddr2,
      city: incoming.city,
      zip: incoming.zip ? `${incoming.zip}` : incoming.zip,
      stateCode: incoming.stateCode,
      type: incoming.type,
      updatedBy: CREATED_BY
    })

    if (matchingAddress) {
      processedIds.add(matchingAddress.id)
      const diff = {}
      ADDRESS_FIELDS.forEach((field) => {
        const incomingValue = updatePayload[field] ?? null
        const existingValue = matchingAddress[field] ?? null
        if (!isEqual(existingValue, incomingValue)) {
          diff[field] = incomingValue
        }
      })

      if (!isEmpty(diff)) {
        diff.updatedBy = CREATED_BY
        await executeWrite('memberAddress.update', () => tx.memberAddress.update({
          where: { id: matchingAddress.id },
          data: diff
        }), { userId, addressKey })
      }
    } else {
      const createPayload = compactObject({
        ...updatePayload,
        userId,
        createdBy: incoming.createdBy || CREATED_BY
      })

      await executeWrite('memberAddress.create', () => tx.memberAddress.create({
        data: createPayload
      }), { userId, addressKey })
    }
  }

  const addressesToRemove = existingAddresses.filter(address => !processedIds.has(address.id))
  if (addressesToRemove.length > 0) {
    await handleStaleRecords('memberAddress', tx.memberAddress, addressesToRemove, { userId })
  }
}

/**
 * Update the trait element
 * @param {Array} objArr trait items
 * @param {Object} txObject prisma tx instance
 * @param {String} memberTraitId the memberTraitId
 * @param {String} createdBy the createdBy
 * @param {String} entityName name for logging context
 * @param {Object} context parent context
 */
async function updateTraitElement (objArr, txObject, memberTraitId, createdBy, entityName = 'traitElement', context = {}) {
  if (!objArr || objArr.length === 0) {
    return
  }

  const effectiveContext = { ...context, memberTraitId, entityName }

  if (DRY_RUN) {
    logInfo('DRY_RUN active; trait element synchronization skipped', effectiveContext)
    return
  }

  const existingElements = await txObject.findMany({
    where: { memberTraitId }
  })

  const processedIds = new Set()

  for (const elemItem of objArr) {
    const traitItem = cloneDeep(elemItem || {})
    sanitizeNullBytesDeep(traitItem)

    if (Object.prototype.hasOwnProperty.call(traitItem, 'industry')) {
      const { industry } = traitItem
      if (!WORK_INDUSTRY_TYPES.includes(industry)) {
        traitItem.industry = null
      }
    }

    const preparedItem = {
      ...traitItem,
      memberTraitId,
      createdBy: traitItem.createdBy || createdBy
    }

    const traitHash = createTraitHash(preparedItem)
    let matchingElement = null

    if (preparedItem.id) {
      matchingElement = existingElements.find(record => `${record.id}` === `${preparedItem.id}`)
    }
    if (!matchingElement) {
      matchingElement = existingElements.find(record => createTraitHash(record) === traitHash)
    }

    const upsertPayload = compactObject({
      ...preparedItem
    })
    delete upsertPayload.id
    delete upsertPayload.createdAt
    delete upsertPayload.updatedAt

    if (matchingElement) {
      processedIds.add(matchingElement.id)
      const diff = {}
      Object.keys(upsertPayload).forEach((key) => {
        if (key === 'memberTraitId' || key === 'createdBy') {
          return
        }
        const incomingValue = upsertPayload[key]
        const existingValue = matchingElement[key]
        if (!isEqual(existingValue, incomingValue)) {
          diff[key] = incomingValue
        }
      })
      if (!isEmpty(diff)) {
        diff.updatedBy = createdBy
        await executeWrite(`${entityName}.update`, () => txObject.update({
          where: { id: matchingElement.id },
          data: diff
        }), effectiveContext)
      }
    } else {
      await executeWrite(`${entityName}.create`, () => txObject.create({
        data: upsertPayload
      }), effectiveContext)
    }
  }

  const staleElements = existingElements.filter(record => !processedIds.has(record.id))
  if (staleElements.length > 0) {
    await handleStaleRecords(entityName, txObject, staleElements, effectiveContext)
  }
}

/**
 * Update the ElasticSearch member stat from file
 * @param {String} filename filename
 * @param {Date|null} [dateFilter=null] optional date filter threshold
 */
async function importElasticSearchMemberStat (filename, dateFilter = null) {
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
  // count skipped items due to date filter
  let skipped = 0
  for await (const line of rlRead) {
    currentLine += 1
    if (currentLine % 10 === 0) {
      const percentage = ((currentLine / lineCount) * 100).toFixed(2)
      // Clear the current line
      process.stdout.clearLine()
      // Move the cursor to the beginning of the line
      process.stdout.cursorTo(0)
      // Write the new percentage
      process.stdout.write(`Migrate Progress: ${percentage}%, read ${count} items, skipped ${skipped}`)
    }

    count += 1

    let dataItem = JSON.parse(line.trim())
    dataItem = dataItem._source

    if (!shouldProcessRecord(dataItem, dateFilter)) {
      skipped += 1
      continue
    }

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

  console.log(`\nIt has updated ${total} items totally, skipped ${skipped} items`)

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
async function updateArrayItems (updateItems, existingItems, txModel, parentId, operatorId, entityName) {
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

  const staleRecords = existingItems.filter(item => toDeleteIds.includes(item.id))
  if (staleRecords.length > 0) {
    await handleStaleRecords(entityName || 'memberStatsItems', txModel, staleRecords, { ...parentId, operatorId })
  }
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
async function updateArrayDivisionItems (updateD1Items, updateD2Items, existingItems, txModel, parentId, operatorId, entityName) {
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

  const staleRecords = existingItems.filter(item => toDeleteIds.includes(item.id))
  if (staleRecords.length > 0) {
    await handleStaleRecords(entityName || 'memberSrmDivisionDetail', txModel, staleRecords, { ...parentId, operatorId })
  }
}

/**
 * Update array level items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateArrayLevelItems (updateItems, existingItems, txModel, parentId, operatorId, entityName) {
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

  const staleRecords = existingItems.filter(item => toDeleteIds.includes(item.id))
  if (staleRecords.length > 0) {
    await handleStaleRecords(entityName || 'memberSrmChallengeDetail', txModel, staleRecords, { ...parentId, operatorId })
  }
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
        userId: member.userId
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
          userId: member.userId,
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

        await updateArrayItems(data.develop.items, existingItems, tx.memberDevelopStatsItem, { developStatsId }, operatorId, 'memberDevelopStatsItem')
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

        await updateArrayItems(data.design.items, existingItems, tx.memberDesignStatsItem, { designStatsId }, operatorId, 'memberDesignStatsItem')
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

          await updateArrayLevelItems(data.dataScience.srm.challengeDetails, existingItems, tx.memberSrmChallengeDetail, { srmStatsId }, operatorId, 'memberSrmChallengeDetail')
        }

        if (data.dataScience.srm.division1 || data.dataScience.srm.division2) {
          const existingItems = memberStatDB.dataScience.srm.divisions || []

          await updateArrayDivisionItems(data.dataScience.srm.division1, data.dataScience.srm.division2, existingItems, tx.memberSrmDivisionDetail, { srmStatsId }, operatorId, 'memberSrmDivisionDetail')
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
    const maxDistributionBucket = 39

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

        const idxValRaw = Math.floor(record.rating / 100)
        const idxVal = Math.max(0, Math.min(idxValRaw, maxDistributionBucket))
        const ratingKey = idxVal === 0 ? 'ratingRange0To099' : `ratingRange${idxVal}00To${idxVal}99`
        distributionValue[ratingKey] = (distributionValue[ratingKey] ?? 0) + 1
        uniqueMap.set(mapKey, distributionValue)
      })

      current += BATCH_SIZE
    }

    if (uniqueMap.size > 0) {
      for (const [key, value] of uniqueMap.entries()) {
        const [track, subTrack] = key.split('-')
        const createData = {
          ...value,
          track,
          subTrack,
          createdBy: CREATED_BY
        }
        const updateData = {
          ...value,
          updatedBy: CREATED_BY
        }

        await executeWrite('distributionStats.upsert', () => tx.distributionStats.upsert({
          where: {
            track_subTrack: {
              track,
              subTrack
            }
          },
          create: createData,
          update: updateData
        }), {
          track,
          subTrack,
          step: migrationRuntimeState.step
        })
      }
    }

    console.log(`Finished counted ${uniqueMap.size} distributionStats records\n`)
  })
}

async function runMigrationStep (step, dateFilter, askQuestion) {
  const logContext = { step, dateFilter: dateFilter ? dateFilter.toISOString() : null }
  migrationRuntimeState.step = step
  migrationRuntimeState.dateFilter = dateFilter
  destructiveApprovals.delete(step)

  const shouldRunIntegrityCheck = ['1', '2', '3', '4', '5', '7'].includes(step)

  try {
    if (shouldRunIntegrityCheck) {
      const validationResult = await validateIncrementalMigrationData(null, dateFilter)
      if (!validationResult.isValid) {
        throw new Error('Incremental migration validation failed. Resolve the reported issues before re-running.')
      }
    }

    let beforeSnapshot = null
    if (shouldRunIntegrityCheck) {
      beforeSnapshot = await getMemberIntegritySnapshot()
    }

    logInfo('Starting migration step', logContext)

    switch (step) {
      case '0': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Full database reset (step 0)' }, async () => {
          console.log('Clearing all DB data...')
          await clearDB()
        })
        if (!executed) {
          logInfo('Full reset skipped; existing data retained', logContext)
        }
        break
      }
      case '1': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Member profile cleanup (step 1)' }, async () => {
          console.log('Clearing member data...')
          const cleanupContext = { ...logContext, operation: 'pre-import-clear' }
          await executeWrite('memberAddress.deleteMany', () => prisma.memberAddress.deleteMany(), cleanupContext)
          await executeWrite('memberMaxRating.deleteMany', () => prisma.memberMaxRating.deleteMany(), cleanupContext)
          await executeWrite('member.deleteMany', () => prisma.member.deleteMany(), cleanupContext)
        })
        if (!executed) {
          logInfo('Retaining existing member records for step 1 import', logContext)
        }
        const memberDynamoFilename = 'MemberProfile.json'
        await importDynamoMember(memberDynamoFilename, dateFilter)
        break
      }
      case '2': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Member trait and skill cleanup (step 2)' }, async () => {
          console.log('Clearing member trait and skill data...')
          const cleanupContext = { ...logContext, operation: 'pre-import-clear' }
          await executeWrite('memberTraitBasicInfo.deleteMany', () => prisma.memberTraitBasicInfo.deleteMany(), cleanupContext)
          await executeWrite('memberTraitCommunity.deleteMany', () => prisma.memberTraitCommunity.deleteMany(), cleanupContext)
          await executeWrite('memberTraitDevice.deleteMany', () => prisma.memberTraitDevice.deleteMany(), cleanupContext)
          await executeWrite('memberTraitEducation.deleteMany', () => prisma.memberTraitEducation.deleteMany(), cleanupContext)
          await executeWrite('memberTraitLanguage.deleteMany', () => prisma.memberTraitLanguage.deleteMany(), cleanupContext)
          await executeWrite('memberTraitOnboardChecklist.deleteMany', () => prisma.memberTraitOnboardChecklist.deleteMany(), cleanupContext)
          await executeWrite('memberTraitPersonalization.deleteMany', () => prisma.memberTraitPersonalization.deleteMany(), cleanupContext)
          await executeWrite('memberTraitServiceProvider.deleteMany', () => prisma.memberTraitServiceProvider.deleteMany(), cleanupContext)
          await executeWrite('memberTraitSoftware.deleteMany', () => prisma.memberTraitSoftware.deleteMany(), cleanupContext)
          await executeWrite('memberTraitWork.deleteMany', () => prisma.memberTraitWork.deleteMany(), cleanupContext)
          await executeWrite('memberTraits.deleteMany', () => prisma.memberTraits.deleteMany(), cleanupContext)
        })
        if (!executed) {
          logInfo('Retaining existing trait data for step 2 import', logContext)
        }
        const memberElasticsearchFilename = 'members-2020-01.json'
        await importElasticSearchMember(memberElasticsearchFilename, dateFilter)
        break
      }
      case '3': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Member stats cleanup (step 3)' }, async () => {
          console.log('Clearing member stats data...')
          const cleanupContext = { ...logContext, operation: 'pre-import-clear' }
          await executeWrite('memberCopilotStats.deleteMany', () => prisma.memberCopilotStats.deleteMany(), cleanupContext)
          await executeWrite('memberMarathonStats.deleteMany', () => prisma.memberMarathonStats.deleteMany(), cleanupContext)
          await executeWrite('memberDesignStatsItem.deleteMany', () => prisma.memberDesignStatsItem.deleteMany(), cleanupContext)
          await executeWrite('memberDesignStats.deleteMany', () => prisma.memberDesignStats.deleteMany(), cleanupContext)
          await executeWrite('memberDevelopStatsItem.deleteMany', () => prisma.memberDevelopStatsItem.deleteMany(), cleanupContext)
          await executeWrite('memberDevelopStats.deleteMany', () => prisma.memberDevelopStats.deleteMany(), cleanupContext)
          await executeWrite('memberSrmChallengeDetail.deleteMany', () => prisma.memberSrmChallengeDetail.deleteMany(), cleanupContext)
          await executeWrite('memberSrmDivisionDetail.deleteMany', () => prisma.memberSrmDivisionDetail.deleteMany(), cleanupContext)
          await executeWrite('memberSrmStats.deleteMany', () => prisma.memberSrmStats.deleteMany(), cleanupContext)
          await executeWrite('memberDataScienceStats.deleteMany', () => prisma.memberDataScienceStats.deleteMany(), cleanupContext)
          await executeWrite('memberStats.deleteMany', () => prisma.memberStats.deleteMany(), cleanupContext)
        })
        if (!executed) {
          logInfo('Retaining existing stats data for step 3 import', logContext)
        }
        const memberStateDynamoFilename = 'MemberStats.json'
        await importDynamoMemberStat(memberStateDynamoFilename, dateFilter)
        break
      }
      case '4': {
        const memberStatElasticsearchFilename = 'memberstats-2020-01.json'
        await importElasticSearchMemberStat(memberStatElasticsearchFilename, dateFilter)
        break
      }
      case '5': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Member stats history cleanup (step 5)' }, async () => {
          console.log('Clearing member stats history data...')
          const cleanupContext = { ...logContext, operation: 'pre-import-clear' }
          await executeWrite('memberDataScienceHistoryStats.deleteMany', () => prisma.memberDataScienceHistoryStats.deleteMany(), cleanupContext)
          await executeWrite('memberDevelopHistoryStats.deleteMany', () => prisma.memberDevelopHistoryStats.deleteMany(), cleanupContext)
          await executeWrite('memberHistoryStats.deleteMany', () => prisma.memberHistoryStats.deleteMany(), cleanupContext)
        })
        if (!executed) {
          logInfo('Retaining existing stats history for step 5 import', logContext)
        }
        const memberStateHistoryDynamoFilename = 'MemberStatsHistory.json'
        await importDynamoMemberStatHistory(memberStateHistoryDynamoFilename, dateFilter)
        const memberStatePrivateDynamoFilename = 'MemberStatsHistory_Private.json'
        await importDynamoMemberStatHistoryPrivate(memberStatePrivateDynamoFilename, dateFilter)
        break
      }
      case '6': {
        const executed = await withDestructiveGuard({ step, askQuestion, description: 'Distribution stats cleanup (step 6)' }, async () => {
          console.log('Clearing distribution stats data...')
          const cleanupContext = { ...logContext, operation: 'pre-import-clear' }
          await executeWrite('distributionStats.deleteMany', () => prisma.distributionStats.deleteMany(), cleanupContext)
        })
        if (!executed) {
          logInfo('Retaining existing distribution stats; incoming data will be merged', logContext)
        }
        await importDistributionStats()
        break
      }
      case '7': {
        const memberElasticsearchFilename = 'members-2020-01.json'
        await updateMemberStatusFromElasticSearch(memberElasticsearchFilename, dateFilter)
        break
      }
      default:
        throw new Error(`Unsupported step "${step}"`)
    }

    if (shouldRunIntegrityCheck) {
      const afterSnapshot = await getMemberIntegritySnapshot()
      await verifyDataIntegrity(beforeSnapshot, afterSnapshot, logContext)
    }

    logInfo('Finished migration step', { step })
  } finally {
    migrationRuntimeState.step = null
    migrationRuntimeState.dateFilter = null
  }
}

async function main () {
  const configValidation = validateMigrationConfiguration()
  if (!configValidation.isValid) {
    console.error('Migration configuration is invalid. Review logs for details before re-running.')
    process.exit(1)
  }

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
  console.log('7. Update ElasticSearch Member Status')
  console.log('')
  console.log('Destructive clears require --full-reset or ALLOW_DESTRUCTIVE=true and an explicit confirmation. Incremental runs retain existing data by default.')

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  })

  const askQuestion = (prompt) => new Promise(resolve => rl.question(prompt, resolve))

  let selectedStep = null
  try {
    selectedStep = (await askQuestion('Please select your step to run (0-7): ')).trim()
    const validSteps = new Set(['0', '1', '2', '3', '4', '5', '6', '7'])
    if (!validSteps.has(selectedStep)) {
      console.log('Unsupported step selected. Script is finished.')
      return
    }

    console.log(`Running step ${selectedStep} ...`)
    const confirmation = (await askQuestion('Please confirm (yes/no): ')).trim().toLowerCase()
    if (confirmation !== 'yes' && confirmation !== 'y') {
      console.log('Script is finished.')
      return
    }

    let dateFilter = null
    if (['1', '2', '3', '4', '5', '7'].includes(selectedStep)) {
      const dateFilterInput = (await askQuestion('Enter date filter (YYYY-MM-DD UTC; timestamp-less records will be skipped, or press Enter to skip): ')).trim()
      if (dateFilterInput) {
        const parsed = parseDateFilter(dateFilterInput)
        if (parsed) {
          console.log(`Filtering records with recognized timestamps on or after ${parsed.toISOString()} (UTC). Records missing timestamps will be skipped.`)
          dateFilter = parsed
        } else {
          console.log('Invalid date filter input. Expected YYYY-MM-DD; continuing without filtering.')
        }
      }
    }

    console.log('')
    await runMigrationStep(selectedStep, dateFilter, askQuestion)
    console.log('Script is finished.')
  } catch (err) {
    logError('Migration step failed', { step: selectedStep, error: err?.message })
    const label = selectedStep ?? 'unknown'
    console.error(`Migration step ${label} failed: ${err.message}`)
    process.exitCode = 1
  } finally {
    rl.close()
  }
}

if (require.main === module) {
  main()
}

module.exports = {
  fixMemberUpdateData,
  updateMembersWithTraitsAndSkills
}

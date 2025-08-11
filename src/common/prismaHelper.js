const _ = require('lodash')
const helper = require('./helper')
const errors = require('./errors')

const designBasicFields = [
  'name', 'numInquiries', 'submissions', 'passedScreening', 'avgPlacement',
  'screeningSuccessRate', 'submissionRate', 'winPercent'
]

const developSubmissionFields = [
  'appealSuccessRate', 'minScore', 'avgPlacement', 'reviewSuccessRate',
  'maxScore', 'avgScore', 'screeningSuccessRate', 'submissionRate', 'winPercent'
]

const developSubmissionBigIntFields = [
  'numInquiries', 'submissions', 'passedScreening', 'passedReview', 'appeals'
]

const developRankFields = [
  'overallPercentile', 'activeRank', 'overallCountryRank', 'reliability', 'rating',
  'minRating', 'volatility', 'overallSchoolRank', 'overallRank', 'activeSchoolRank',
  'activeCountryRank', 'maxRating', 'activePercentile'
]

const copilotFields = [
  'contests', 'projects', 'failures', 'reposts', 'activeContests', 'activeProjects', 'fulfillment'
]

const srmRankFields = [
  'rating', 'percentile', 'rank', 'countryRank', 'schoolRank',
  'volatility', 'maximumRating', 'minimumRating', 'defaultLanguage', 'competitions'
]

const srmDivisionFields = [
  'problemsSubmitted', 'problemsSysByTest', 'problemsFailed', 'levelName'
]

const marathonRankFields = [
  'rating', 'competitions', 'avgRank', 'avgNumSubmissions', 'bestRank',
  'topFiveFinishes', 'topTenFinishes', 'rank', 'percentile', 'volatility',
  'minimumRating', 'maximumRating', 'countryRank', 'schoolRank', 'defaultLanguage'
]

const auditFields = [
  'createdAt', 'createdBy', 'updatedAt', 'updatedBy'
]

/**
 * Convert member db data to response data
 * @param {Object} member member data from db
 */
function convertMember (member) {
  member.userId = helper.bigIntToNumber(member.userId)
  member.createdAt = member.createdAt.getTime()
  member.updatedAt = member.updatedAt.getTime()
  if (member.maxRating) {
    member.maxRating = _.omit(member.maxRating,
      ['id', 'userId', ...auditFields])
  }
  if (member.addresses) {
    member.addresses = _.map(member.addresses, d => _.omit(d,
      ['id', 'userId', ...auditFields]))
  }
  member.verified = member.verified || false
}

/**
 * Build skill list data with data from db
 * @param {Array} skillList skill list from db
 * @returns skill list in response
 */
function buildMemberSkills (skillList) {
  if (!skillList || skillList.length === 0) {
    return []
  }
  return _.map(skillList, item => {
    const ret = _.pick(item.skill, ['id', 'name'])
    ret.category = _.pick(item.skill.category, ['id', 'name'])
    if (item.displayMode) {
      ret.displayMode = _.pick(item.displayMode, ['id', 'name'])
    }
    // set levels
    if (item.levels && item.levels.length > 0) {
      ret.levels = _.map(item.levels,
        lvl => _.pick(lvl.skillLevel, ['id', 'name', 'description']))
    }
    return ret
  })
}

/**
 * Build prisma filter with member search query
 * @param {Object} query request query parameters
 * @returns member filter used in prisma
 */
function buildSearchMemberFilter (query) {
  const handles = _.isArray(query.handles) ? query.handles : []
  const handlesLower = _.isArray(query.handlesLower) ? query.handlesLower : []
  const userIds = _.isArray(query.userIds) ? query.userIds : []

  const filterList = []
  filterList.push({ status: 'ACTIVE' })
  if (query.userId) {
    filterList.push({ userId: query.userId })
  }
  if (query.handleLower) {
    filterList.push({ handleLower: query.handleLower })
  }
  if (query.handle) {
    filterList.push({ handle: query.handle })
  }
  if (query.email) {
    filterList.push({ email: query.email })
  }
  if (userIds.length > 0) {
    filterList.push({ userId: { in: userIds } })
  }
  if (handlesLower.length > 0) {
    filterList.push({ handleLower: { in: handlesLower } })
  }
  if (handles.length > 0) {
    filterList.push({ handle: { in: handles } })
  }

  const prismaFilter = {
    where: { AND: filterList }
  }
  return prismaFilter
}

/**
 * Convert db data to response structure for member stats
 * @param {Object} member member data
 * @param {Object} statsData stats data from db
 * @param {Array} fields fields return in response
 * @returns Member stats response
 */
function buildStatsResponse (member, statsData, fields) {
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(statsData.groupId),
    handle: member.handle,
    handleLower: member.handleLower,
    challenges: statsData.challenges,
    wins: statsData.wins
  }
  if (member.maxRating) {
    item.maxRating = _.pick(member.maxRating, ['rating', 'track', 'subTrack', 'ratingColor'])
  }
  if (statsData.design) {
    item.DESIGN = {
      challenges: helper.bigIntToNumber(statsData.design.challenges),
      wins: helper.bigIntToNumber(statsData.design.wins),
      mostRecentSubmission: statsData.design.mostRecentSubmission
        ? statsData.design.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.design.mostRecentEventDate
        ? statsData.design.mostRecentEventDate.getTime() : null,
      subTracks: []
    }
    const items = _.get(statsData, 'design.items', [])
    if (items.length > 0) {
      item.DESIGN.subTracks = _.map(items, t => ({
        ..._.pick(t, designBasicFields),
        challenges: helper.bigIntToNumber(t.challenges),
        wins: helper.bigIntToNumber(t.wins),
        id: t.subTrackId,
        mostRecentSubmission: t.mostRecentSubmission
          ? t.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: t.mostRecentEventDate
          ? t.mostRecentEventDate.getTime() : null
      }))
    }
  }
  if (statsData.develop) {
    item.DEVELOP = {
      challenges: helper.bigIntToNumber(statsData.develop.challenges),
      wins: helper.bigIntToNumber(statsData.develop.wins),
      mostRecentSubmission: statsData.develop.mostRecentSubmission
        ? statsData.develop.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.develop.mostRecentEventDate
        ? statsData.develop.mostRecentEventDate.getTime() : null,
      subTracks: []
    }
    const items = _.get(statsData, 'develop.items', [])
    if (items.length > 0) {
      item.DEVELOP.subTracks = _.map(items, t => ({
        challenges: helper.bigIntToNumber(t.challenges),
        wins: helper.bigIntToNumber(t.wins),
        id: t.subTrackId,
        name: t.name,
        mostRecentSubmission: t.mostRecentSubmission ? t.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: t.mostRecentEventDate ? t.mostRecentEventDate.getTime() : null,
        submissions: {
          ..._.pick(t, developSubmissionFields),
          ..._.mapValues(_.pick(t, developSubmissionBigIntFields), v => helper.bigIntToNumber(v))
        },
        rank: _.pick(t, developRankFields)
      }))
    }
  }
  if (statsData.copilot) {
    item.COPILOT = _.pick(statsData.copilot, copilotFields)
  }
  if (statsData.dataScience) {
    item.DATA_SCIENCE = {
      challenges: helper.bigIntToNumber(statsData.dataScience.challenges),
      wins: helper.bigIntToNumber(statsData.dataScience.wins),
      mostRecentSubmission: statsData.dataScience.mostRecentSubmission
        ? statsData.dataScience.mostRecentSubmission.getTime() : null,
      mostRecentEventDate: statsData.dataScience.mostRecentEventDate
        ? statsData.dataScience.mostRecentEventDate.getTime() : null,
      mostRecentEventName: statsData.dataScience.mostRecentEventName
    }
    if (statsData.dataScience.srm) {
      const srmData = statsData.dataScience.srm
      item.DATA_SCIENCE.SRM = {
        challenges: helper.bigIntToNumber(srmData.challenges),
        wins: helper.bigIntToNumber(srmData.wins),
        mostRecentSubmission: srmData.mostRecentSubmission
          ? srmData.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: srmData.mostRecentEventDate
          ? srmData.mostRecentEventDate.getTime() : null,
        mostRecentEventName: srmData.mostRecentEventName,
        rank: _.pick(srmData, srmRankFields)
      }
      if (srmData.challengeDetails && srmData.challengeDetails.length > 0) {
        item.DATA_SCIENCE.SRM.challengeDetails = _.map(srmData.challengeDetails,
          t => _.pick(t, ['challenges', 'levelName', 'failedChallenges']))
      }
      if (srmData.divisions && srmData.divisions.length > 0) {
        const div1Data = _.filter(srmData.divisions, t => t.divisionName === 'division1')
        const div2Data = _.filter(srmData.divisions, t => t.divisionName === 'division2')
        if (div1Data.length > 0) {
          item.DATA_SCIENCE.SRM.division1 = _.map(div1Data, t => _.pick(t, srmDivisionFields))
        }
        if (div2Data.length > 0) {
          item.DATA_SCIENCE.SRM.division2 = _.map(div2Data, t => _.pick(t, srmDivisionFields))
        }
      }
    }
    if (statsData.dataScience.marathon) {
      const marathonData = statsData.dataScience.marathon
      item.DATA_SCIENCE.MARATHON_MATCH = {
        challenges: helper.bigIntToNumber(marathonData.challenges),
        wins: helper.bigIntToNumber(marathonData.wins),
        mostRecentSubmission: marathonData.mostRecentSubmission
          ? marathonData.mostRecentSubmission.getTime() : null,
        mostRecentEventDate: marathonData.mostRecentEventDate
          ? marathonData.mostRecentEventDate.getTime() : null,
        mostRecentEventName: marathonData.mostRecentEventName,
        rank: _.pick(marathonData, marathonRankFields)
      }
    }
  }

  return fields ? _.pick(item, fields) : item
}

/**
 * Convert prisma data to response structure for member stats history
 * @param {Object} member member data
 * @param {Object} historyStats stats history
 * @param {Array} fields fields to return in response
 * @returns response
 */
function buildStatsHistoryResponse (member, historyStats, fields) {
  const item = {
    userId: helper.bigIntToNumber(member.userId),
    groupId: helper.bigIntToNumber(historyStats.groupId),
    handle: member.handle,
    handleLower: member.handleLower
  }
  // collect develop data
  if (historyStats.develop && historyStats.develop.length > 0) {
    item.DEVELOP = { subTracks: [] }
    // group by subTrackId
    const subTrackGroupData = _.groupBy(historyStats.develop, 'subTrackId')
    // for each sub track, build history data
    _.forEach(subTrackGroupData, (trackHistory, subTrackId) => {
      const subTrackItem = {
        id: subTrackId,
        name: trackHistory[0].subTrack
      }
      subTrackItem.history = _.map(trackHistory, h => ({
        ..._.pick(h, ['challengeName', 'newRating']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        ratingDate: h.ratingDate ? h.ratingDate.getTime() : null
      }))
      item.DEVELOP.subTracks.push(subTrackItem)
    })
  }
  // collect data sciencedata
  if (historyStats.dataScience && historyStats.dataScience.length > 0) {
    item.DATA_SCIENCE = {}
    const srmHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'SRM')
    const marathonHistory = _.filter(historyStats.dataScience, t => t.subTrack === 'MARATHON_MATCH')
    if (srmHistory.length > 0) {
      item.DATA_SCIENCE.SRM = {}
      item.DATA_SCIENCE.SRM.history = _.map(srmHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
    if (marathonHistory.length > 0) {
      item.DATA_SCIENCE.MARATHON_MATCH = {}
      item.DATA_SCIENCE.MARATHON_MATCH.history = _.map(marathonHistory, h => ({
        ..._.pick(h, ['challengeName', 'rating', 'placement', 'percentile']),
        challengeId: helper.bigIntToNumber(h.challengeId),
        date: h.date ? h.date.getTime() : null
      }))
    }
  }
  return fields ? _.pick(item, fields) : item
}

// include parameters used to get all member stats
const statsIncludeParams = {
  design: { include: { items: true } },
  develop: { include: { items: true } },
  dataScience: { include: {
    srm: { include: { challengeDetails: true, divisions: true } },
    marathon: true
  } },
  copilot: true
}

// include parameters used to get all member skills
const skillsIncludeParams = {
  levels: { include: { skillLevel: true } },
  skill: { include: { category: true } },
  displayMode: true
}

/**
 * Convert number to date
 * @param {Number} dateNum date number
 * @returns date instance or undefined
 */
function convertDate (dateNum) {
  return dateNum ? new Date(dateNum) : undefined
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
 * Validate subTrack items data
 * @param {Array} updateItems the subTrack data to update
 * @param {Array} existingItems the existing subTrack data
 * @param {String} modelName the model name
 * @returns subTrack items data to create
 */
function validateSubTrackData (updateItems, existingItems, modelName) {
  const itemIds = []
  const itemNames = []
  const toCreateItems = []

  updateItems.forEach(item => {
    if (_.find(itemIds, id => id === item.id)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate id: '${item.id}'`)
    }
    if (item.name && _.find(itemNames, name => name === item.name)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate name: '${item.name}'`)
    }
    itemIds.push(item.id)
    if (item.name) {
      itemNames.push(item.name)
    }
    const foundItem = existingItems.find(eItem => eItem.subTrackId === item.id)
    const nameItem = existingItems.find(eItem => {
      if (eItem.subTrack) {
        return eItem.subTrackId !== item.id && eItem.subTrack === item.name
      }
      return eItem.subTrackId !== item.id && eItem.name === item.name
    })

    if (foundItem && (item.name && (foundItem.subTrack ? item.name !== foundItem.subTrack : item.name !== foundItem.name))) {
      throw new errors.BadRequestError(`${modelName} item with name '${item.name}' is not same as the DB one with same id`)
    }
    if (nameItem) {
      throw new errors.BadRequestError(`${modelName} item has duplicated name '${item.name}' in DB`)
    }
    if (!foundItem && !(item.id && item.name)) {
      throw new errors.BadRequestError(`${modelName} new item must have id and name both`)
    }
    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  return toCreateItems
}

/**
 * Validate level items data
 * @param {Array} updateItems the level data to update
 * @param {Array} existingItems the level subTrack data
 * @param {String} modelName the model name
 * @param {String} itemName the item name
 * @param {Object} schema the joi schema
 * @returns level items data to create
 */
function validateLevelItemsData (updateItems, existingItems, modelName, itemName, schema) {
  const itemLevelNames = []
  const toCreateItems = []
  updateItems.forEach(item => {
    if (_.find(itemLevelNames, ln => ln === item.levelName)) {
      throw new errors.BadRequestError(`${modelName} ${itemName} items contains duplicate level name: '${item.levelName}'`)
    }
    itemLevelNames.push(item.levelName)

    const foundItem = existingItems.find(eItem => {
      if (itemName === 'challengeDetail') {
        return eItem.levelName === item.levelName
      } else {
        return eItem.levelName === item.levelName && eItem.divisionName === itemName
      }
    })
    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  if (toCreateItems.length > 0) {
    const validateRes = schema.validate(toCreateItems)

    if (validateRes.error) {
      throw new errors.BadRequestError(validateRes.error.error)
    }
  }
}

/**
 * Validate history items data
 * @param {Array} updateItems the history data to update
 * @param {Array} existingItems the existing history data
 * @param {String} modelName the model name
 * @returns history items data to create
 */
function validateHistoryData (updateItems, existingItems, modelName) {
  const itemIds = []
  const toCreateItems = []
  updateItems.forEach(item => {
    if (_.find(itemIds, id => id === item.challengeId)) {
      throw new errors.BadRequestError(`${modelName} items contains duplicate id: '${item.challengeId}'`)
    }
    itemIds.push(item.challengeId)

    const foundItem = existingItems.find(eItem => helper.bigIntToNumber(eItem.challengeId) === item.challengeId && eItem.subTrack === item.subTrack)

    if (!foundItem) {
      toCreateItems.push(item)
    }
  })

  return toCreateItems
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
        ..._.omit(elem, ['id', 'subTrackId', 'name']),
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
        ..._.omit(elem, ['id']),
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
        ..._.omit(elem, ['id']),
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
 * Update history items.
 * @param {Array} updateItems items to be updated
 * @param {Array} existingItems existing items in db
 * @param {Object} txModel the tx model
 * @param {Object} parentId the parent Id object
 * @param {String} operatorId the operator Id
 */
async function updateHistoryItems (updateItems, existingItems, txModel, parentId, operatorId) {
  const toUpdate = []
  const toCreate = []

  if (updateItems.length === 0) {
    return
  }

  updateItems.forEach(item => {
    const foundItem = existingItems.find(eItem => eItem.subTrack === item.subTrack && helper.bigIntToNumber(eItem.challengeId) === item.challengeId)
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
        ..._.omit(elem, ['id', 'subTrackId', 'subTrack', 'challengeId']),
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

module.exports = {
  convertMember,
  buildMemberSkills,
  buildStatsResponse,
  buildSearchMemberFilter,
  buildStatsHistoryResponse,
  statsIncludeParams,
  skillsIncludeParams,
  convertDate,
  updateOrCreateModel,
  validateSubTrackData,
  validateLevelItemsData,
  validateHistoryData,
  updateArrayItems,
  updateArrayLevelItems,
  updateArrayDivisionItems,
  updateHistoryItems
}

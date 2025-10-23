const https = require('https')
const fs = require('fs')
const path = require('path')
const config = require('./config')

const BASE_URL = config.apiUrl
const OUTPUT_DIR = config.fileLocation
const DELAY_BETWEEN_REQUESTS = 500
const MAX_RETRIES = 3

const handleList = config.handleList

const distributions = config.distributions
const distributionDir = path.join(OUTPUT_DIR, 'distribution')

const statsHistoryDir = path.join(OUTPUT_DIR, 'statsHistory')

if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true })
}

if (!fs.existsSync(distributionDir)) {
  fs.mkdirSync(distributionDir, { recursive: true })
}

if (!fs.existsSync(statsHistoryDir)) {
  fs.mkdirSync(statsHistoryDir, { recursive: true })
}

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function fetchData (url, retryCount = 0) {
  return new Promise((resolve, reject) => {
    console.log(`Fetching data for url: ${url}`)

    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        if (retryCount < MAX_RETRIES) {
          const delay = Math.pow(2, retryCount) * 1000
          console.log(`Rate limited. Retrying in ${delay}ms...`)
          setTimeout(() => {
            fetchData(url, retryCount + 1)
              .then(resolve)
              .catch(reject)
          }, delay)
          return
        }
        reject(new Error(`Failed to fetch from ${url}. Status code: ${res.statusCode}`))
        return
      }

      let data = ''
      res.on('data', (chunk) => {
        data += chunk
      })

      res.on('end', () => {
        try {
          const result = JSON.parse(data)
          resolve(result)
        } catch (err) {
          reject(err)
        }
      })
    }).on('error', (err) => {
      if (retryCount < MAX_RETRIES) {
        const delay = Math.pow(2, retryCount) * 1000
        console.log(`Error fetching from ${url}. Retrying in ${delay}ms...`)
        setTimeout(() => {
          fetchData(url, retryCount + 1)
            .then(resolve)
            .catch(reject)
        }, delay)
      } else {
        reject(err)
      }
    })
  })
}

async function fetchMemberData (handle, retryCount = 0) {
  const url = `${BASE_URL}?page=1&perPage=10&handle=${encodeURIComponent(handle)}`
  return fetchData(url, retryCount)
}

function saveMemberData (handle, data) {
  const filename = path.join(OUTPUT_DIR, `${handle}.json`)
  const content = JSON.stringify(data, null, 2)

  return new Promise((resolve, reject) => {
    fs.writeFile(filename, content, (err) => {
      if (err) {
        reject(err)
      } else {
        console.log(`Saved data for ${handle} to ${filename}`)
        resolve()
      }
    })
  })
}

async function processHandleList () {
  for (const handle of handleList) {
    try {
      const memberData = await fetchMemberData(handle)

      await saveMemberData(handle, memberData)

      if (handle !== handleList[handleList.length - 1]) {
        await sleep(DELAY_BETWEEN_REQUESTS)
      }
    } catch (err) {
      console.error(`Error processing handle ${handle}:`, err.message)
    }
  }
  console.log('All handles processed!')
}

async function fetchDistributionData (track, subTrack, retryCount = 0) {
  const url = `${BASE_URL}/stats/distribution?subTrack=${subTrack}&track=${track}`
  return fetchData(url, retryCount)
}

async function saveDistributionData (track, subTrack, data) {
  const filename = path.join(distributionDir, `${track}_${subTrack}.json`)
  const content = JSON.stringify(data, null, 2)

  return new Promise((resolve, reject) => {
    fs.writeFile(filename, content, (err) => {
      if (err) {
        reject(err)
      } else {
        console.log(`Saved data for ${track} - ${subTrack} to ${filename}`)
        resolve()
      }
    })
  })
}

async function processDistribution () {
  for (const track of distributions) {
    try {
      const distData = await fetchDistributionData(track.track, track.subTrack)
      await saveDistributionData(track.track, track.subTrack, distData)
      await sleep(DELAY_BETWEEN_REQUESTS)
    } catch (err) {
      console.error(`Error processing distribution for track ${track.subTrack}:`, err.message)
    }
  }
  console.log('All track distribution processed!')
}

async function fetchMemberStatsHistory (handle, retryCount = 0) {
  const url = `${BASE_URL}/${handle}/stats/history`
  return fetchData(url, retryCount)
}

async function saveStatsHistory (handle, jsonData) {
  const filename = path.join(statsHistoryDir, `${handle}.json`)
  const content = JSON.stringify(jsonData, null, 2)

  return new Promise((resolve, reject) => {
    fs.writeFile(filename, content, (err) => {
      if (err) {
        reject(err)
      } else {
        console.log(`Saved data for stats history ${handle} to ${filename}`)
        resolve()
      }
    })
  })
}

async function processStatsHistory () {
  for (const handle of handleList) {
    try {
      const jsonData = await fetchMemberStatsHistory(handle)
      await saveStatsHistory(handle, jsonData)
      await sleep(DELAY_BETWEEN_REQUESTS)
    } catch (err) {
      console.error(`Error processing stats history for member ${handle}:`, err.message)
    }
  }
  console.log('All member stats history processed!')
}

async function main () {
  await processHandleList()
  await processDistribution()
  await processStatsHistory()
}

main()

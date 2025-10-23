const express = require('express')
const cors = require('cors')
const winston = require('winston')

const app = express()
app.set('port', 4000)

app.use(cors())

app.use(express.json())

const logFormat = winston.format.printf(({ level, message }) => {
  return `${new Date().toISOString()} [${level}]: ${message}`
})

const logConsole = new winston.transports.Console({
  format: winston.format.combine(winston.format.colorize(), logFormat)
})
winston.add(logConsole)

// mock M2M
app.post('/v5/auth0', (req, res) => {
  winston.info('Received Auth0 request')
  // return config/test.js#M2M_FULL_ACCESS_TOKEN
  res.status(200).json({ access_token: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL3RvcGNvZGVyLWRldi5hdXRoMC5jb20vIiwic3ViIjoiZW5qdzE4MTBlRHozWFR3U08yUm4yWTljUVRyc3BuM0JAY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vbTJtLnRvcGNvZGVyLWRldi5jb20vIiwiaWF0IjoxNTUwOTA2Mzg4LCJleHAiOjE2ODA5OTI3ODgsImF6cCI6ImVuancxODEwZUR6M1hUd1NPMlJuMlk5Y1FUcnNwbjNCIiwic2NvcGUiOiJhbGw6bWVtYmVycyIsImd0eSI6ImNsaWVudC1jcmVkZW50aWFscyJ9.Eo_cyyPBQfpWp_8-NSFuJI5MvkEV3UJZ3ONLcFZedoA' })
})

// mock event bus
app.post('/v5/bus/events', (req, res) => {
  winston.info('Received bus events')
  winston.info(JSON.stringify(req.body, null, 2))
  res.status(200).json({})
})

// mock group API
const allGroups = ['20000000', '20000001']

app.get('/v5/groups/:groupId', (req, res) => {
  const groupId = req.params.groupId
  winston.info(`Received group request for ${groupId}`)
  if (allGroups.includes(groupId)) {
    res.json({ id: groupId })
  } else {
    res.status(404).json(null)
  }
})

app.get('/v5/groups/memberGroups/:memberId', (req, res) => {
  const memberId = req.params.memberId
  winston.info(`Received member groups request for ${memberId}`)
  res.json(allGroups)
})

app.use((req, res) => {
  res.status(404).json({ error: 'route not found' })
})

app.use((err, req, res, next) => {
  winston.error(err)
  res.status(500).json({
    error: err.message
  })
})

app.listen(app.get('port'), '0.0.0.0', () => {
  winston.info(`Express server listening on port ${app.get('port')}`)
})

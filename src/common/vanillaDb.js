const config = require('config')
const mysql = require('mysql2/promise')
const errors = require('./errors')

let vanillaPool

function getVanillaPool () {
  if (!config.VANILLA_DB_URL) {
    throw new errors.BadRequestError('VANILLA_DB_URL is not configured')
  }

  if (!vanillaPool) {
    vanillaPool = mysql.createPool(config.VANILLA_DB_URL)
  }

  return vanillaPool
}

module.exports = {
  getVanillaPool
}

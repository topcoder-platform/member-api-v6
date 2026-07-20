/*
 * Regression coverage for the production Docker entrypoint.
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { expect } = require('chai')

const entrypointPath = path.resolve(__dirname, '../../docker/entrypoint.js')

describe('Docker entrypoint', () => {
  it('awaits the application bootstrap after migrations succeed', () => {
    const fixtureRoot = fs.mkdtempSync(
      path.join(os.tmpdir(), 'member-entrypoint-')
    )
    const prismaPath = path.join(
      fixtureRoot,
      'node_modules/prisma/build/index.js'
    )
    const mainPath = path.join(fixtureRoot, 'dist/main.js')

    try {
      fs.mkdirSync(path.dirname(prismaPath), { recursive: true })
      fs.mkdirSync(path.dirname(mainPath), { recursive: true })
      fs.writeFileSync(prismaPath, '')
      fs.writeFileSync(mainPath, `
module.exports.bootstrap = () => ({
  then (resolve) {
    process.stdout.write('bootstrap awaited\\n')
    resolve()
  }
})
`)

      const result = spawnSync(process.execPath, [entrypointPath], {
        cwd: fixtureRoot,
        encoding: 'utf8',
        env: {
          ...process.env,
          DATABASE_URL: 'postgresql://test'
        }
      })

      expect(result.status).to.equal(0)
      expect(result.stdout).to.contain('bootstrap awaited')
    } finally {
      fs.rmSync(fixtureRoot, { recursive: true, force: true })
    }
  })
})

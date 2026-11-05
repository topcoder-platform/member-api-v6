const assert = require('node:assert/strict')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

// Rendering a migration from the checked-in schema starts the same schema
// engine as `migrate deploy`, but does not connect to or modify a database.
const result = spawnSync(process.execPath, [
  path.join(process.cwd(), 'node_modules/prisma/build/index.js'),
  'migrate',
  'diff',
  '--from-empty',
  '--to-schema=prisma/schema.prisma',
  '--script'
], {
  encoding: 'utf8',
  timeout: 60000,
  env: {
    ...process.env,
    DATABASE_URL: 'postgresql://runtime_check:runtime_check@127.0.0.1:5432/runtime_check'
  }
})

if (result.error) throw result.error
assert.equal(result.status, 0, `Prisma runtime check failed:\n${result.stderr}\n${result.stdout}`)
assert.match(result.stdout, /CREATE TABLE/, 'Prisma did not render the schema migration')
console.log('Prisma runtime check passed: schema engine rendered the migration SQL.')

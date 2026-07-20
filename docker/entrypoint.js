const { spawn } = require('node:child_process')
const path = require('node:path')

/**
 * Run a Node.js subprocess with inherited input and output streams.
 *
 * @param {string[]} args arguments passed to the Node.js executable
 * @returns {Promise<void>} resolves when the subprocess exits successfully
 * @throws {Error} when the subprocess cannot start or exits unsuccessfully
 */
function runNodeProcess (args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, { stdio: 'inherit' })

    child.once('error', reject)
    child.once('exit', (code, signal) => {
      if (code === 0) {
        resolve()
        return
      }

      const result = signal ? `signal ${signal}` : `exit code ${code}`
      reject(new Error(`Prisma migrations failed with ${result}`))
    })
  })
}

/**
 * Deploy database migrations and start the compiled member API.
 *
 * @returns {Promise<void>} resolves after migrations and application startup
 * @throws {Error} when migration deployment or application startup fails
 */
async function main () {
  console.log('Running Prisma migrations...')

  if (!process.env.DATABASE_URL) {
    console.error('ERROR: DATABASE_URL is not set in environment variables.')
  } else {
    console.log(`DATABASE_URL is present (length: ${process.env.DATABASE_URL.length})`)
  }

  await runNodeProcess([
    path.join(process.cwd(), 'node_modules/prisma/build/index.js'),
    'migrate',
    'deploy',
    '--schema=prisma/schema.prisma'
  ])

  console.log('Starting application...')
  const { bootstrap } = require(path.join(process.cwd(), 'dist/main.js'))
  await bootstrap()
}

main().catch(error => {
  console.error(error)
  process.exitCode = 1
})

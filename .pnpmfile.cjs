const PRISMA_CLIENT_HOST_PACKAGES = new Set([
  'engagements-api-v6',
  'standardized-skills-api',
  'tc-identity-service',
  'tc-payments-api',
  'topcoder-challenge-recources-api',
  'topcoder-challenges-api',
  'topcoder-learning-paths-api'
])

/**
 * Restrict internal API Git dependencies to their checked-in generated Prisma clients.
 * Member API imports only each package's `packages/*-prisma-client` path, so installing
 * the host application's dependencies adds unused services and vulnerable toolchains.
 *
 * @param {Object} pkg package manifest being resolved by pnpm
 * @returns {Object} the manifest with unused host-application dependencies removed
 * @throws {Error} this hook does not intentionally throw
 */
function readPackage (pkg) {
  if (PRISMA_CLIENT_HOST_PACKAGES.has(pkg.name)) {
    pkg.dependencies = {}
    pkg.devDependencies = {}
    pkg.optionalDependencies = {}
    pkg.peerDependencies = {}
    pkg.scripts = {}
  }
  return pkg
}

module.exports = {
  hooks: {
    readPackage
  }
}

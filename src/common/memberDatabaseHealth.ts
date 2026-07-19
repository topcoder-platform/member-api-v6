const prisma = require('./prisma')

let inFlightCheck: Promise<number> | undefined

/**
 * Executes the minimal query used to verify the primary members database.
 *
 * This query is shared by application startup and the public health endpoint so
 * both paths warm and verify the same Prisma client and PostgreSQL connection.
 *
 * @returns The elapsed query duration in milliseconds.
 * @throws Propagates Prisma or PostgreSQL connection and query errors.
 */
async function runMemberDatabaseHealthQuery (): Promise<number> {
  const startedAt = Date.now()
  await prisma.getMembersClient().$queryRaw`SELECT 1`
  return Date.now() - startedAt
}

/**
 * Checks connectivity to the primary members database.
 *
 * Concurrent callers share the active query to prevent simultaneous load
 * balancer probes from opening redundant cold connections. The shared promise
 * is cleared after either success or failure so every later probe performs a
 * fresh database check and failed attempts remain retryable.
 *
 * The returned duration describes the shared database query rather than each
 * caller's wait time, ensuring concurrent load balancer probes make the same
 * readiness decision.
 *
 * @returns The active promise resolving to the query duration in milliseconds.
 * @throws Propagates Prisma or PostgreSQL connection and query errors.
 */
export function checkMemberDatabaseHealth (): Promise<number> {
  if (!inFlightCheck) {
    inFlightCheck = runMemberDatabaseHealthQuery()
      .finally(() => {
        inFlightCheck = undefined
      })
  }

  return inFlightCheck
}

import { PrismaPg } from '@prisma/adapter-pg';

/**
 * Extracts the PostgreSQL schema selected by a Prisma-style connection URL.
 *
 * The `pg` driver does not apply Prisma's `schema` query parameter itself, so
 * Prisma 7 adapter callers pass this value explicitly when it is configured.
 *
 * @param connectionString PostgreSQL connection URL used to create an adapter.
 * @returns The selected schema, or `undefined` for an absent, empty, or malformed URL.
 * @throws This function never throws; malformed URLs defer to normal driver validation.
 */
export function getPostgresSchema(
  connectionString: string,
): string | undefined {
  try {
    return new URL(connectionString).searchParams.get('schema') || undefined;
  } catch {
    return undefined;
  }
}

/**
 * Creates a Prisma 7 PostgreSQL driver adapter from an existing service URL.
 *
 * Member and engagements clients use this helper so the current environment
 * variable contract and Prisma-style `?schema=` behavior remain unchanged.
 *
 * @param connectionString PostgreSQL URL supplied by the existing environment.
 * @param environmentVariable Name reported when the required URL is absent.
 * @returns A configured PostgreSQL adapter for Prisma Client.
 * @throws Error when the connection URL is not configured.
 */
export function createPostgresAdapter(
  connectionString: string | undefined,
  environmentVariable: string,
): PrismaPg {
  if (!connectionString) {
    throw new Error(`${environmentVariable} is not configured`);
  }

  const schema = getPostgresSchema(connectionString);
  return new PrismaPg(
    { connectionString },
    schema ? { schema } : undefined,
  );
}

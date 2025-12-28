import * as fsp from "node:fs/promises";
import {
  QueryableStatement,
  Statement,
  join,
  type Interpolable,
} from "../../sql2.ts";

const comma = new Statement([","], []);

/**
 * Migration function type - receives a sql tagged template function
 * and performs database operations
 */
export type MigrationFn<T extends QueryableStatement> = (
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T
) => Promise<void>;

/**
 * Migration definition with name and up function
 */
export interface Migration<T extends QueryableStatement> {
  name: string;
  up: MigrationFn<T>;
}

/**
 * Result of running migrations
 */
export interface MigrationResult {
  applied: string[];
  batch: number;
}

/**
 * Installs the migrations schema and helper functions.
 * Call this once before using any migration functions.
 */
export async function migrationsPlugin<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T
) {
  const sqlScript = await fsp.readFile(
    new URL("./migrations.sql", import.meta.url),
    "utf-8"
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

/**
 * Runs pending migrations in order.
 * Acquires a lock, runs migrations, records them, and releases the lock.
 *
 * @param sql - The sql tagged template function
 * @param migrations - Array of migration objects with name and up function
 * @param lockerName - Optional identifier for the process running migrations
 * @returns Object containing applied migration names and batch number
 */
export async function runMigrations<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T,
  migrations: Migration<T>[],
  lockerName: string = "sql2-migrations"
): Promise<MigrationResult> {
  // Try to acquire lock
  const lockResult =
    await sql`SELECT migrations.acquire_lock(${lockerName}) as acquired`.query<{
      acquired: boolean;
    }>();

  if (!lockResult.rows[0].acquired) {
    throw new Error(
      "Could not acquire migration lock. Another migration may be in progress."
    );
  }

  try {
    // Get all migration names
    const migrationNames = migrations.map((m) => m.name);

    // Get pending migrations
    const pendingResult =
      await sql`SELECT name FROM migrations.get_pending_migrations(ARRAY[${join(migrationNames, comma)}]::text[])`.query<{
        name: string;
      }>();

    const pendingNames = new Set(pendingResult.rows.map((r) => r.name));
    const pendingMigrations = migrations.filter((m) =>
      pendingNames.has(m.name)
    );

    if (pendingMigrations.length === 0) {
      await sql`SELECT migrations.release_lock()`.exec();
      return { applied: [], batch: 0 };
    }

    // Get batch number for this run
    const batchResult =
      await sql`SELECT migrations.get_next_batch() as batch`.query<{
        batch: number;
      }>();
    const batch = batchResult.rows[0].batch;

    // Run migrations in order
    const applied: string[] = [];
    for (const migration of pendingMigrations) {
      // Run the migration
      await migration.up(sql);

      // Record it
      await sql`SELECT migrations.record_migration(${migration.name}, ${batch})`.query();
      applied.push(migration.name);
    }

    return { applied, batch };
  } finally {
    // Always release lock
    await sql`SELECT migrations.release_lock()`.query();
  }
}

/**
 * Gets the status of all migrations.
 *
 * @param sql - The sql tagged template function
 * @param migrations - Array of migration objects to check against
 * @returns Object with applied, pending, and stats
 */
export async function getMigrationStatus<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T,
  migrations: Migration<T>[]
): Promise<{
  applied: Array<{ name: string; batch: number; migration_time: Date }>;
  pending: string[];
  stats: {
    total_migrations: number;
    total_batches: number;
    last_migration_name: string | null;
    last_migration_time: Date | null;
    last_batch: number;
  };
}> {
  // Get applied migrations
  const appliedResult =
    await sql`SELECT * FROM migrations.get_applied_migrations()`.query<{
      id: number;
      name: string;
      batch: number;
      migration_time: Date;
    }>();

  // Get pending migrations
  const migrationNames = migrations.map((m) => m.name);
  const pendingResult =
    await sql`SELECT name FROM migrations.get_pending_migrations(ARRAY[${join(migrationNames, comma)}]::text[])`.query<{
      name: string;
    }>();

  // Get stats
  const statsResult = await sql`SELECT * FROM migrations.get_stats()`.query<{
    total_migrations: number;
    total_batches: number;
    last_migration_name: string | null;
    last_migration_time: Date | null;
    last_batch: number;
  }>();

  return {
    applied: appliedResult.rows.map((r) => ({
      name: r.name,
      batch: r.batch,
      migration_time: r.migration_time,
    })),
    pending: pendingResult.rows.map((r) => r.name),
    stats: statsResult.rows[0] || {
      total_migrations: 0,
      total_batches: 0,
      last_migration_name: null,
      last_migration_time: null,
      last_batch: 0,
    },
  };
}

/**
 * Checks if the migration lock is currently held.
 *
 * @param sql - The sql tagged template function
 * @returns Lock status information
 */
export async function getLockStatus<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T
): Promise<{
  is_locked: boolean;
  locked_at: Date | null;
  locked_by: string | null;
}> {
  const result = await sql`SELECT * FROM migrations.get_lock_status()`.query<{
    is_locked: boolean;
    locked_at: Date | null;
    locked_by: string | null;
  }>();

  return (
    result.rows[0] || { is_locked: false, locked_at: null, locked_by: null }
  );
}

/**
 * Forces release of the migration lock.
 * Use with caution - only when you're sure no migration is running.
 *
 * @param sql - The sql tagged template function
 */
export async function forceReleaseLock<T extends QueryableStatement>(
  sql: (strings: TemplateStringsArray, ...values: Interpolable[]) => T
): Promise<void> {
  await sql`SELECT migrations.release_lock()`.exec();
}

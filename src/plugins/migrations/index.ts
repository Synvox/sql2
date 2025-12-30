import * as fsp from "node:fs/promises";
import { getSql } from "../../sql2.ts";

/**
 * Migration function type - performs database operations
 */
export type MigrationFn = () => Promise<void>;

/**
 * Migration definition with name and up function
 */
export interface Migration {
  name: string;
  up: MigrationFn;
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
export async function migrationsPlugin() {
  const sql = getSql({ camelize: false });

  const sqlScript = await fsp.readFile(
    new URL("./migrations.sql", import.meta.url),
    "utf-8",
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
 * @param migrations - Array of migration objects with name and up function
 * @param lockerName - Optional identifier for the process running migrations
 * @returns Object containing applied migration names and batch number
 */
export async function runMigrations(
  migrations: Migration[],
  lockerName: string = "sql2-migrations",
): Promise<MigrationResult> {
  const sql = getSql({ camelize: false });
  // Try to acquire lock
  const lockRow = await sql`
    select
      migrations.acquire_lock (${lockerName}) as acquired
  `.first<{
    acquired: boolean;
  }>();

  if (!lockRow!.acquired) {
    throw new Error(
      "Could not acquire migration lock. Another migration may be in progress.",
    );
  }

  try {
    // Get all migration names
    const migrationNames = migrations.map((m) => m.name);

    // Get pending migrations
    const pendingRows = await sql`
      select
        name
      from
        migrations.get_pending_migrations (array[${sql.join(
          migrationNames.map((n) => sql.literal(n)),
        )}]::text[])
    `.all<{
      name: string;
    }>();

    const pendingNames = new Set(pendingRows.map((r) => r.name));
    const pendingMigrations = migrations.filter((m) =>
      pendingNames.has(m.name),
    );

    if (pendingMigrations.length === 0) {
      await sql`
        select
          migrations.release_lock ()
      `.exec();
      return { applied: [], batch: 0 };
    }

    // Get batch number for this run
    const batchRow = await sql`
      select
        migrations.get_next_batch () as batch
    `.first<{
      batch: number;
    }>();
    const batch = batchRow!.batch;

    // Run migrations in order
    const applied: string[] = [];
    for (const migration of pendingMigrations) {
      // Run the migration
      await migration.up();

      // Record it
      await sql`
        select
          migrations.record_migration (
            ${migration.name},
            ${batch}
          )
      `.query();
      applied.push(migration.name);
    }

    return { applied, batch };
  } finally {
    // Always release lock
    await sql`
      select
        migrations.release_lock ()
    `.query();
  }
}

/**
 * Gets the status of all migrations.
 *
 * @param migrations - Array of migration objects to check against
 * @returns Object with applied, pending, and stats
 */
export async function getMigrationStatus(migrations: Migration[]): Promise<{
  applied: Array<{ name: string; batch: number; migrationTime: Date }>;
  pending: string[];
  stats: {
    totalMigrations: number;
    totalBatches: number;
    lastMigrationName: string | null;
    lastMigrationTime: Date | null;
    lastBatch: number;
  };
}> {
  const sql = getSql({ camelize: false });
  // Get applied migrations
  const appliedRows = await sql`
    select
      *
    from
      migrations.get_applied_migrations ()
  `.all<{
    id: number;
    name: string;
    batch: number;
    migration_time: Date;
  }>();

  // Get pending migrations
  const migrationNames = migrations.map((m) => m.name);
  const pendingRows = await sql`
    select
      name
    from
      migrations.get_pending_migrations (array[${sql.join(
        migrationNames.map((n) => sql.literal(n)),
      )}]::text[])
  `.all<{
    name: string;
  }>();

  // Get stats
  const statsRow = await sql`
    select
      *
    from
      migrations.get_stats ()
  `.first<{
    total_migrations: number;
    total_batches: number;
    last_migration_name: string | null;
    last_migration_time: Date | null;
    last_batch: number;
  }>();

  return {
    applied: appliedRows.map((r) => ({
      name: r.name,
      batch: r.batch,
      migrationTime: r.migration_time,
    })),
    pending: pendingRows.map((r) => r.name),
    stats: statsRow
      ? {
          totalMigrations: statsRow.total_migrations,
          totalBatches: statsRow.total_batches,
          lastMigrationName: statsRow.last_migration_name,
          lastMigrationTime: statsRow.last_migration_time,
          lastBatch: statsRow.last_batch,
        }
      : {
          totalMigrations: 0,
          totalBatches: 0,
          lastMigrationName: null,
          lastMigrationTime: null,
          lastBatch: 0,
        },
  };
}

/**
 * Checks if the migration lock is currently held.
 *
 * @returns Lock status information
 */
export async function getLockStatus(): Promise<{
  isLocked: boolean;
  lockedAt: Date | null;
  lockedBy: string | null;
}> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      migrations.get_lock_status ()
  `.first<{
    is_locked: boolean;
    locked_at: Date | null;
    locked_by: string | null;
  }>();

  return row
    ? {
        isLocked: row.is_locked,
        lockedAt: row.locked_at,
        lockedBy: row.locked_by,
      }
    : { isLocked: false, lockedAt: null, lockedBy: null };
}

/**
 * Forces release of the migration lock.
 * Use with caution - only when you're sure no migration is running.
 */
export async function forceReleaseLock(): Promise<void> {
  const sql = getSql({ camelize: false });
  await sql`
    select
      migrations.release_lock ()
  `.exec();
}

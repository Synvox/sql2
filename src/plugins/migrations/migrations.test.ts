import {
  PGlite,
  type PGliteInterface,
  type Transaction,
} from "@electric-sql/pglite";
import * as assert from "node:assert";
import { describe, it } from "node:test";
import { getSql, provideClient, type Client } from "../../sql2.ts";
import {
  forceReleaseLock,
  getLockStatus,
  getMigrationStatus,
  migrationsPlugin,
  runMigrations,
  type Migration,
} from "./index.ts";

const dbRoot = new PGlite();

function makeClient(db: PGlite | Transaction | PGliteInterface): Client {
  return {
    exec: async (query) => {
      await db.exec(query);
    },
    query: async (query, values) => {
      return await db.query(query, values);
    },
    transaction: async (fn) => {
      if ("transaction" in db) {
        return await db.transaction(async (trx) => {
          return await fn(makeClient(trx));
        });
      } else {
        return await fn(makeClient(db));
      }
    },
  };
}

async function withDb(fn: () => Promise<void>) {
  const db = await dbRoot.clone();
  return provideClient(makeClient(db), async () => {
    return await fn();
  });
}

function itWithDb(name: string, fn: () => Promise<void>) {
  it(name, async () => {
    await withDb(async () => {
      await fn();
    });
  });
}

await provideClient(makeClient(dbRoot), async () => {
  await migrationsPlugin();
});

describe("Migrations Plugin", () => {
  describe("Plugin Installation", () => {
    itWithDb("creates migrations schema and tables", async () => {
      const sql = getSql({ camelize: false });

      // Check schema exists
      const schemaResult = await sql`
        select
          schema_name
        from
          information_schema.schemata
        where
          schema_name = 'migrations'
      `.all<{ schema_name: string }>();

      assert.strictEqual(schemaResult.length, 1);
      assert.strictEqual(schemaResult[0].schema_name, "migrations");
    });

    itWithDb("creates migrations table", async () => {
      const sql = getSql({ camelize: false });

      const tableResult = await sql`
        select
          table_name
        from
          information_schema.tables
        where
          table_schema = 'migrations'
          and table_name = 'migrations'
      `.all<{ table_name: string }>();

      assert.strictEqual(tableResult.length, 1);
    });

    itWithDb("creates migrations_lock table", async () => {
      const sql = getSql({ camelize: false });

      const tableResult = await sql`
        select
          table_name
        from
          information_schema.tables
        where
          table_schema = 'migrations'
          and table_name = 'migrations_lock'
      `.all<{ table_name: string }>();

      assert.strictEqual(tableResult.length, 1);
    });

    itWithDb("initializes lock row as unlocked", async () => {
      const sql = getSql({ camelize: false });

      const lockStatus = await getLockStatus();

      assert.strictEqual(lockStatus.isLocked, false);
      assert.strictEqual(lockStatus.lockedAt, null);
      assert.strictEqual(lockStatus.lockedBy, null);
    });

    itWithDb("is idempotent - can be called multiple times", async () => {
      const sql = getSql({ camelize: false });

      // Call plugin installation again
      await migrationsPlugin();
      await migrationsPlugin();

      // Should still work fine
      const lockStatus = await getLockStatus();
      assert.strictEqual(typeof lockStatus.isLocked, "boolean");
    });
  });

  describe("Lock Management", () => {
    itWithDb("acquire_lock returns true when lock is free", async () => {
      const sql = getSql({ camelize: false });

      const result = await sql`
        select
          migrations.acquire_lock ('test-locker') as acquired
      `.first<{ acquired: boolean }>();

      assert.strictEqual(result!.acquired, true);

      // Clean up
      await forceReleaseLock();
    });

    itWithDb("acquire_lock returns false when lock is held", async () => {
      const sql = getSql({ camelize: false });

      // First acquisition
      await sql`
        select
          migrations.acquire_lock ('first-locker') as acquired
      `.first<{ acquired: boolean }>();

      // Second acquisition should fail
      const result = await sql`
        select
          migrations.acquire_lock ('second-locker') as acquired
      `.first<{ acquired: boolean }>();

      assert.strictEqual(result!.acquired, false);

      // Clean up
      await forceReleaseLock();
    });

    itWithDb("release_lock releases the lock", async () => {
      const sql = getSql({ camelize: false });

      // Acquire
      await sql`
        select
          migrations.acquire_lock ('test-locker') as acquired
      `.exec();

      // Verify locked
      let status = await getLockStatus();
      assert.strictEqual(status.isLocked, true);

      // Release
      await forceReleaseLock();

      // Verify unlocked
      status = await getLockStatus();
      assert.strictEqual(status.isLocked, false);
    });

    itWithDb("getLockStatus returns correct lock info", async () => {
      const sql = getSql({ camelize: false });

      // Acquire lock
      await sql`
        select
          migrations.acquire_lock ('my-test-process') as acquired
      `.exec();

      const status = await getLockStatus();

      assert.strictEqual(status.isLocked, true);
      assert.strictEqual(status.lockedBy, "my-test-process");
      assert.ok(status.lockedAt instanceof Date);

      // Clean up
      await forceReleaseLock();
    });

    itWithDb("forceReleaseLock releases lock even if locked", async () => {
      const sql = getSql({ camelize: false });

      // Acquire lock
      await sql`
        select
          migrations.acquire_lock ('test-locker') as acquired
      `.exec();

      // Force release
      await forceReleaseLock();

      const status = await getLockStatus();
      assert.strictEqual(status.isLocked, false);
    });

    itWithDb("is_locked function returns correct status", async () => {
      const sql = getSql({ camelize: false });

      // Initially unlocked
      let result = await sql`
        select
          migrations.is_locked () as locked
      `.first<{ locked: boolean }>();
      assert.strictEqual(result!.locked, false);

      // Acquire
      await sql`
        select
          migrations.acquire_lock ('test')
      `.exec();

      // Now locked
      result = await sql`
        select
          migrations.is_locked () as locked
      `.first<{ locked: boolean }>();
      assert.strictEqual(result!.locked, true);

      // Clean up
      await forceReleaseLock();
    });
  });

  describe("Running Migrations", () => {
    itWithDb("runs a single migration", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_create_test_table",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists test_table_1 (id SERIAL primary key, name TEXT not null)
            `.exec();
          },
        },
      ];

      const result = await runMigrations(migrations);

      assert.strictEqual(result.applied.length, 1);
      assert.strictEqual(
        result.applied[0],
        "20240101_000001_create_test_table",
      );
      assert.strictEqual(result.batch, 1);

      // Verify table was created
      const tableResult = await sql`
        select
          table_name
        from
          information_schema.tables
        where
          table_name = 'test_table_1'
      `.all<{ table_name: string }>();

      assert.strictEqual(tableResult.length, 1);
    });

    itWithDb("runs multiple migrations in order", async () => {
      const sql = getSql({ camelize: false });
      const executionOrder: string[] = [];

      const migrations: Migration[] = [
        {
          name: "20240101_000001_first",
          up: async () => {
            const sql = getSql({ camelize: false });
            executionOrder.push("first");
            await sql`
              create table if not exists multi_test_1 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000002_second",
          up: async () => {
            const sql = getSql({ camelize: false });
            executionOrder.push("second");
            await sql`
              create table if not exists multi_test_2 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000003_third",
          up: async () => {
            const sql = getSql({ camelize: false });
            executionOrder.push("third");
            await sql`
              create table if not exists multi_test_3 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result = await runMigrations(migrations);

      assert.strictEqual(result.applied.length, 3);
      assert.deepStrictEqual(executionOrder, ["first", "second", "third"]);
      assert.strictEqual(result.batch, 1);
    });

    itWithDb("skips already applied migrations", async () => {
      const sql = getSql({ camelize: false });
      let firstRunCount = 0;
      let secondRunCount = 0;

      const migrations: Migration[] = [
        {
          name: "20240101_000001_skip_test",
          up: async () => {
            const sql = getSql({ camelize: false });
            firstRunCount++;
            await sql`
              create table if not exists skip_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      // Run first time
      await runMigrations(migrations);

      // Add second migration
      const allMigrations: Migration[] = [
        ...migrations,
        {
          name: "20240101_000002_skip_test_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            secondRunCount++;
            await sql`
              create table if not exists skip_test_2 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      // Run second time
      const result = await runMigrations(allMigrations);

      assert.strictEqual(firstRunCount, 1); // First migration only ran once
      assert.strictEqual(secondRunCount, 1);
      assert.strictEqual(result.applied.length, 1);
      assert.strictEqual(result.applied[0], "20240101_000002_skip_test_2");
      assert.strictEqual(result.batch, 2); // Second batch
    });

    itWithDb("returns empty result when no pending migrations", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_no_pending",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists no_pending (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      // Run first time
      await runMigrations(migrations);

      // Run again with same migrations
      const result = await runMigrations(migrations);

      assert.strictEqual(result.applied.length, 0);
      assert.strictEqual(result.batch, 0);
    });

    itWithDb(
      "returns empty result when migrations array is empty",
      async () => {
        const sql = getSql({ camelize: false });

        const result = await runMigrations([]);

        assert.strictEqual(result.applied.length, 0);
        assert.strictEqual(result.batch, 0);
      },
    );

    itWithDb("uses custom locker name", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_locker_name_test",
          up: async () => {
            const sql = getSql({ camelize: false });
            // Check locker name during migration
            const status = await getLockStatus();
            assert.strictEqual(status.lockedBy, "custom-locker");

            await sql`
              create table if not exists locker_name_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations, "custom-locker");
    });

    itWithDb("releases lock after successful migrations", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_lock_release_test",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists lock_release_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations);

      const status = await getLockStatus();
      assert.strictEqual(status.isLocked, false);
    });

    itWithDb("releases lock after failed migration", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_fail_migration",
          up: async () => {
            throw new Error("Migration failed intentionally");
          },
        },
      ];

      try {
        await runMigrations(migrations);
        assert.fail("Should have thrown");
      } catch (error) {
        assert.ok(error instanceof Error);
        assert.strictEqual(error.message, "Migration failed intentionally");
      }

      // Lock should be released
      const status = await getLockStatus();
      assert.strictEqual(status.isLocked, false);
    });

    itWithDb("throws when lock cannot be acquired", async () => {
      const sql = getSql({ camelize: false });

      // Acquire lock manually
      await sql`
        select
          migrations.acquire_lock ('blocking-process')
      `.exec();

      const migrations: Migration[] = [
        {
          name: "20240101_000001_blocked",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists blocked (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      try {
        await runMigrations(migrations);
        assert.fail("Should have thrown");
      } catch (error) {
        assert.ok(error instanceof Error);
        assert.ok(error.message.includes("Could not acquire migration lock"));
      }

      // Clean up
      await forceReleaseLock();
    });
  });

  describe("Batch Tracking", () => {
    itWithDb("assigns batch 1 to first migration run", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_batch_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists batch_1_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result = await runMigrations(migrations);
      assert.strictEqual(result.batch, 1);
    });

    itWithDb("increments batch for each run", async () => {
      const sql = getSql({ camelize: false });

      const firstBatchMigrations: Migration[] = [
        {
          name: "20240101_000001_batch_inc_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists batch_inc_1 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result1 = await runMigrations(firstBatchMigrations);
      assert.strictEqual(result1.batch, 1);

      const secondBatchMigrations: Migration[] = [
        ...firstBatchMigrations,
        {
          name: "20240101_000002_batch_inc_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists batch_inc_2 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result2 = await runMigrations(secondBatchMigrations);
      assert.strictEqual(result2.batch, 2);

      const thirdBatchMigrations: Migration[] = [
        ...secondBatchMigrations,
        {
          name: "20240101_000003_batch_inc_3",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists batch_inc_3 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result3 = await runMigrations(thirdBatchMigrations);
      assert.strictEqual(result3.batch, 3);
    });

    itWithDb("groups multiple migrations in same batch", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_same_batch_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists same_batch_1 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000002_same_batch_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists same_batch_2 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations);

      const status = await getMigrationStatus(migrations);

      // Both should be in batch 1
      assert.strictEqual(status.applied.length, 2);
      assert.strictEqual(status.applied[0].batch, 1);
      assert.strictEqual(status.applied[1].batch, 1);
    });

    itWithDb("get_current_batch returns 0 when no migrations", async () => {
      const sql = getSql({ camelize: false });

      const result = await sql`
        select
          migrations.get_current_batch () as batch
      `.first<{ batch: number }>();

      assert.strictEqual(result!.batch, 0);
    });

    itWithDb("get_next_batch returns 1 when no migrations", async () => {
      const sql = getSql({ camelize: false });

      const result = await sql`
        select
          migrations.get_next_batch () as batch
      `.first<{ batch: number }>();

      assert.strictEqual(result!.batch, 1);
    });

    itWithDb("get_migrations_by_batch returns correct migrations", async () => {
      const sql = getSql({ camelize: false });

      // Run two batches
      const firstBatch: Migration[] = [
        {
          name: "20240101_000001_by_batch_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists by_batch_1 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(firstBatch);

      const secondBatch: Migration[] = [
        ...firstBatch,
        {
          name: "20240101_000002_by_batch_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists by_batch_2 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000003_by_batch_3",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists by_batch_3 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(secondBatch);

      // Query batch 2
      const batch2Result = await sql`
        select
          *
        from
          migrations.get_migrations_by_batch (2)
      `.all<{ name: string; batch: number }>();

      assert.strictEqual(batch2Result.length, 2);
      assert.strictEqual(batch2Result[0].name, "20240101_000002_by_batch_2");
      assert.strictEqual(batch2Result[1].name, "20240101_000003_by_batch_3");
    });

    itWithDb(
      "get_latest_batch_migrations returns correct migrations",
      async () => {
        const sql = getSql({ camelize: false });

        // Run two batches
        const firstBatch: Migration[] = [
          {
            name: "20240101_000001_latest_batch_1",
            up: async () => {
              const sql = getSql({ camelize: false });
              await sql`
                create table if not exists latest_batch_1 (id SERIAL primary key)
              `.exec();
            },
          },
        ];

        await runMigrations(firstBatch);

        const secondBatch: Migration[] = [
          ...firstBatch,
          {
            name: "20240101_000002_latest_batch_2",
            up: async () => {
              const sql = getSql({ camelize: false });
              await sql`
                create table if not exists latest_batch_2 (id SERIAL primary key)
              `.exec();
            },
          },
        ];

        await runMigrations(secondBatch);

        const latestResult = await sql`
          select
            *
          from
            migrations.get_latest_batch_migrations ()
        `.all<{ name: string; batch: number }>();

        assert.strictEqual(latestResult.length, 1);
        assert.strictEqual(
          latestResult[0].name,
          "20240101_000002_latest_batch_2",
        );
        assert.strictEqual(latestResult[0].batch, 2);
      },
    );
  });

  describe("Migration Status", () => {
    itWithDb("returns empty status when no migrations applied", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_pending",
          up: async () => {},
        },
      ];

      const status = await getMigrationStatus(migrations);

      assert.strictEqual(status.applied.length, 0);
      assert.strictEqual(status.pending.length, 1);
      assert.strictEqual(status.pending[0], "20240101_000001_pending");
      assert.strictEqual(status.stats.totalMigrations, 0);
      assert.strictEqual(status.stats.totalBatches, 0);
      assert.strictEqual(status.stats.lastMigrationName, null);
      assert.strictEqual(status.stats.lastBatch, 0);
    });

    itWithDb("returns correct applied and pending migrations", async () => {
      const sql = getSql({ camelize: false });

      const initialMigrations: Migration[] = [
        {
          name: "20240101_000001_status_applied",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists status_applied (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(initialMigrations);

      const allMigrations: Migration[] = [
        ...initialMigrations,
        {
          name: "20240101_000002_status_pending",
          up: async () => {},
        },
      ];

      const status = await getMigrationStatus(allMigrations);

      assert.strictEqual(status.applied.length, 1);
      assert.strictEqual(
        status.applied[0].name,
        "20240101_000001_status_applied",
      );
      assert.strictEqual(status.applied[0].batch, 1);
      assert.ok(status.applied[0].migrationTime instanceof Date);

      assert.strictEqual(status.pending.length, 1);
      assert.strictEqual(status.pending[0], "20240101_000002_status_pending");
    });

    itWithDb("returns correct stats", async () => {
      const sql = getSql({ camelize: false });

      const firstBatch: Migration[] = [
        {
          name: "20240101_000001_stats_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists stats_1 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000002_stats_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists stats_2 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(firstBatch);

      const secondBatch: Migration[] = [
        ...firstBatch,
        {
          name: "20240101_000003_stats_3",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists stats_3 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(secondBatch);

      const status = await getMigrationStatus(secondBatch);

      assert.strictEqual(status.stats.totalMigrations, 3);
      assert.strictEqual(status.stats.totalBatches, 2);
      assert.strictEqual(
        status.stats.lastMigrationName,
        "20240101_000003_stats_3",
      );
      assert.strictEqual(status.stats.lastBatch, 2);
      assert.ok(status.stats.lastMigrationTime instanceof Date);
    });

    itWithDb("handles empty migrations list", async () => {
      const sql = getSql({ camelize: false });

      const status = await getMigrationStatus([]);

      assert.strictEqual(status.applied.length, 0);
      assert.strictEqual(status.pending.length, 0);
    });
  });

  describe("SQL Helper Functions", () => {
    itWithDb("has_migration returns false for unknown migration", async () => {
      const sql = getSql({ camelize: false });

      const result = await sql`
        select
          migrations.has_migration ('nonexistent_migration') as has
      `.first<{ has: boolean }>();

      assert.strictEqual(result!.has, false);
    });

    itWithDb("has_migration returns true for applied migration", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_has_migration_test",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists has_migration_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations);

      const result = await sql`
        select
          migrations.has_migration ('20240101_000001_has_migration_test') as has
      `.first<{ has: boolean }>();

      assert.strictEqual(result!.has, true);
    });

    itWithDb("record_migration manually records a migration", async () => {
      const sql = getSql({ camelize: false });

      const result = await sql`
        select
          *
        from
          migrations.record_migration ('manual_migration', 1)
      `.first<{ id: number; name: string; batch: number }>();

      assert.ok(result);
      assert.strictEqual(result.name, "manual_migration");
      assert.strictEqual(result.batch, 1);

      // Verify it's recorded
      const hasResult = await sql`
        select
          migrations.has_migration ('manual_migration') as has
      `.first<{ has: boolean }>();

      assert.strictEqual(hasResult!.has, true);
    });

    itWithDb(
      "record_migration uses next batch when not specified",
      async () => {
        const sql = getSql({ camelize: false });

        // First manual record
        await sql`
          select
            *
          from
            migrations.record_migration ('auto_batch_1')
        `.first();

        // Second should be batch 2
        const result = await sql`
          select
            *
          from
            migrations.record_migration ('auto_batch_2')
        `.first<{ batch: number }>();

        assert.strictEqual(result!.batch, 2);
      },
    );

    itWithDb("get_applied_migrations returns migrations in order", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_order_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists order_1 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000002_order_2",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists order_2 (id SERIAL primary key)
            `.exec();
          },
        },
        {
          name: "20240101_000003_order_3",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists order_3 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations);

      const applied = await sql`
        select
          *
        from
          migrations.get_applied_migrations ()
      `.all<{ name: string }>();

      assert.strictEqual(applied.length, 3);
      assert.strictEqual(applied[0].name, "20240101_000001_order_1");
      assert.strictEqual(applied[1].name, "20240101_000002_order_2");
      assert.strictEqual(applied[2].name, "20240101_000003_order_3");
    });

    itWithDb("get_pending_migrations returns only unapplied", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_pending_test_1",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists pending_test_1 (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      await runMigrations(migrations);

      const pending = await sql`
        select
          *
        from
          migrations.get_pending_migrations (
            array[
              '20240101_000001_pending_test_1',
              '20240101_000002_pending_test_2',
              '20240101_000003_pending_test_3'
            ]::text[]
          )
      `.all<{ name: string }>();

      assert.strictEqual(pending.length, 2);
      assert.strictEqual(pending[0].name, "20240101_000002_pending_test_2");
      assert.strictEqual(pending[1].name, "20240101_000003_pending_test_3");
    });
  });

  describe("Migration Name Uniqueness", () => {
    itWithDb("prevents duplicate migration names", async () => {
      const sql = getSql({ camelize: false });

      await sql`
        select
          *
        from
          migrations.record_migration ('unique_migration', 1)
      `.exec();

      try {
        await sql`
          select
            *
          from
            migrations.record_migration ('unique_migration', 1)
        `.exec();
        assert.fail("Should have thrown for duplicate name");
      } catch (error) {
        assert.ok(error instanceof Error);
        // Unique constraint violation
        assert.ok(
          error.message.includes("unique") ||
            error.message.includes("duplicate"),
        );
      }
    });
  });

  describe("Edge Cases", () => {
    itWithDb("handles migration names with special characters", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "2024-01-01_create_users-table",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists special_chars_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result = await runMigrations(migrations);

      assert.strictEqual(result.applied.length, 1);
      assert.strictEqual(result.applied[0], "2024-01-01_create_users-table");
    });

    itWithDb("handles very long migration names", async () => {
      const sql = getSql({ camelize: false });

      const longName = "20240101_" + "a".repeat(200);

      const migrations: Migration[] = [
        {
          name: longName,
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists long_name_test (id SERIAL primary key)
            `.exec();
          },
        },
      ];

      const result = await runMigrations(migrations);

      assert.strictEqual(result.applied.length, 1);
      assert.strictEqual(result.applied[0], longName);
    });

    itWithDb("migration can query and insert data", async () => {
      const sql = getSql({ camelize: false });

      const migrations: Migration[] = [
        {
          name: "20240101_000001_data_migration",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`
              create table if not exists data_migration_test (id SERIAL primary key, value TEXT not null)
            `.exec();

            await sql`
              insert into
                data_migration_test (value)
              values
                ('initial')
            `.exec();

            const result = await sql`
              select
                value
              from
                data_migration_test
              where
                value = 'initial'
            `.first<{ value: string }>();

            assert.strictEqual(result!.value, "initial");
          },
        },
      ];

      await runMigrations(migrations);
    });

    itWithDb("preserves order even when run multiple times", async () => {
      const sql = getSql({ camelize: false });

      const migrations1: Migration[] = [
        {
          name: "20240101_000001_preserve_order",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`create table if not exists preserve_1 (id SERIAL)`.exec();
          },
        },
      ];

      await runMigrations(migrations1);

      const migrations2: Migration[] = [
        ...migrations1,
        {
          name: "20240101_000002_preserve_order",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`create table if not exists preserve_2 (id SERIAL)`.exec();
          },
        },
      ];

      await runMigrations(migrations2);

      const migrations3: Migration[] = [
        ...migrations2,
        {
          name: "20240101_000003_preserve_order",
          up: async () => {
            const sql = getSql({ camelize: false });
            await sql`create table if not exists preserve_3 (id SERIAL)`.exec();
          },
        },
      ];

      await runMigrations(migrations3);

      const status = await getMigrationStatus(migrations3);

      assert.strictEqual(
        status.applied[0].name,
        "20240101_000001_preserve_order",
      );
      assert.strictEqual(
        status.applied[1].name,
        "20240101_000002_preserve_order",
      );
      assert.strictEqual(
        status.applied[2].name,
        "20240101_000003_preserve_order",
      );
      assert.strictEqual(status.applied[0].batch, 1);
      assert.strictEqual(status.applied[1].batch, 2);
      assert.strictEqual(status.applied[2].batch, 3);
    });
  });
});

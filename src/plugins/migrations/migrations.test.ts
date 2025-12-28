import { PGlite, type PGliteInterface } from "@electric-sql/pglite";
import * as assert from "node:assert";
import { beforeEach, describe, it } from "node:test";
import {
  QueryableStatement,
  Statement,
  join,
  type Interpolable,
} from "../../sql2.ts";
import {
  migrationsPlugin,
  runMigrations,
  getMigrationStatus,
  getLockStatus,
  forceReleaseLock,
  type Migration,
} from "./index.ts";

const dbRoot = new PGlite();
let db: PGliteInterface | null = null;

class RootStatement extends QueryableStatement {
  async exec() {
    if (this.values.length) throw new Error("No parameters are provided.");
    await dbRoot.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return dbRoot.query(this.compile(), this.values);
  }
}

await migrationsPlugin(
  (strings: TemplateStringsArray, ...values: Interpolable[]) =>
    new RootStatement(strings, values)
);

class TestStatement extends QueryableStatement {
  async exec() {
    if (this.values.length) throw new Error("No parameters are provided.");
    await db!.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return db!.query(this.compile(), this.values);
  }
}

let sql = (strings: TemplateStringsArray, ...values: Interpolable[]) =>
  new TestStatement(strings, values);

describe("Migrations Plugin", () => {
  beforeEach(async () => {
    db = await dbRoot.clone();
  });

  describe("Schema Setup", () => {
    it("should create the migrations schema", async () => {
      const result = await sql`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'migrations'
      `.query<{ schema_name: string }>();

      assert.strictEqual(result.rows.length, 1);
      assert.strictEqual(result.rows[0].schema_name, "migrations");
    });

    it("should create the migrations table", async () => {
      const result = await sql`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'migrations' AND table_name = 'migrations'
      `.query<{ table_name: string }>();

      assert.strictEqual(result.rows.length, 1);
    });

    it("should create the migrations_lock table", async () => {
      const result = await sql`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'migrations' AND table_name = 'migrations_lock'
      `.query<{ table_name: string }>();

      assert.strictEqual(result.rows.length, 1);
    });

    it("should have the lock row initialized", async () => {
      const result = await sql`SELECT * FROM migrations.migrations_lock`.query<{
        id: number;
        is_locked: boolean;
      }>();

      assert.strictEqual(result.rows.length, 1);
      assert.strictEqual(result.rows[0].id, 1);
      assert.strictEqual(result.rows[0].is_locked, false);
    });
  });

  describe("Lock Management", () => {
    it("should acquire lock successfully", async () => {
      const result =
        await sql`SELECT migrations.acquire_lock('test-runner') as acquired`.query<{
          acquired: boolean;
        }>();

      assert.strictEqual(result.rows[0].acquired, true);

      const status = await getLockStatus(sql);
      assert.strictEqual(status.is_locked, true);
      assert.strictEqual(status.locked_by, "test-runner");
      assert.ok(status.locked_at);
    });

    it("should fail to acquire lock when already locked", async () => {
      await sql`SELECT migrations.acquire_lock('first-runner')`.exec();

      const result =
        await sql`SELECT migrations.acquire_lock('second-runner') as acquired`.query<{
          acquired: boolean;
        }>();

      assert.strictEqual(result.rows[0].acquired, false);
    });

    it("should release lock successfully", async () => {
      await sql`SELECT migrations.acquire_lock('test-runner')`.exec();
      await sql`SELECT migrations.release_lock()`.exec();

      const status = await getLockStatus(sql);
      assert.strictEqual(status.is_locked, false);
      assert.strictEqual(status.locked_by, null);
      assert.strictEqual(status.locked_at, null);
    });

    it("should force release lock", async () => {
      await sql`SELECT migrations.acquire_lock('test-runner')`.exec();
      await forceReleaseLock(sql);

      const status = await getLockStatus(sql);
      assert.strictEqual(status.is_locked, false);
    });

    it("should check if locked correctly", async () => {
      let result = await sql`SELECT migrations.is_locked() as locked`.query<{
        locked: boolean;
      }>();
      assert.strictEqual(result.rows[0].locked, false);

      await sql`SELECT migrations.acquire_lock('test')`.exec();

      result = await sql`SELECT migrations.is_locked() as locked`.query<{
        locked: boolean;
      }>();
      assert.strictEqual(result.rows[0].locked, true);
    });
  });

  describe("Batch Management", () => {
    it("should start with batch 0", async () => {
      const result =
        await sql`SELECT migrations.get_current_batch() as batch`.query<{
          batch: number;
        }>();

      assert.strictEqual(result.rows[0].batch, 0);
    });

    it("should get next batch as 1 when no migrations", async () => {
      const result =
        await sql`SELECT migrations.get_next_batch() as batch`.query<{
          batch: number;
        }>();

      assert.strictEqual(result.rows[0].batch, 1);
    });

    it("should increment batch number after migrations", async () => {
      await sql`SELECT migrations.record_migration('test_migration_1', 1)`.exec();
      await sql`SELECT migrations.record_migration('test_migration_2', 1)`.exec();

      const currentBatch =
        await sql`SELECT migrations.get_current_batch() as batch`.query<{
          batch: number;
        }>();
      assert.strictEqual(currentBatch.rows[0].batch, 1);

      const nextBatch =
        await sql`SELECT migrations.get_next_batch() as batch`.query<{
          batch: number;
        }>();
      assert.strictEqual(nextBatch.rows[0].batch, 2);
    });
  });

  describe("Migration Recording", () => {
    it("should record a migration", async () => {
      const result = await sql`
        SELECT * FROM migrations.record_migration('20240101_create_users')
      `.query<{
        id: number;
        name: string;
        batch: number;
        migration_time: Date;
      }>();

      assert.strictEqual(result.rows.length, 1);
      assert.strictEqual(result.rows[0].name, "20240101_create_users");
      assert.strictEqual(result.rows[0].batch, 1);
      assert.ok(result.rows[0].migration_time);
    });

    it("should record multiple migrations in same batch", async () => {
      await sql`SELECT migrations.record_migration('migration_1', 1)`.exec();
      await sql`SELECT migrations.record_migration('migration_2', 1)`.exec();
      await sql`SELECT migrations.record_migration('migration_3', 1)`.exec();

      const result =
        await sql`SELECT * FROM migrations.get_migrations_by_batch(1)`.query<{
          name: string;
          batch: number;
        }>();

      assert.strictEqual(result.rows.length, 3);
      assert.ok(result.rows.every((r) => r.batch === 1));
    });

    it("should prevent duplicate migration names", async () => {
      await sql`SELECT migrations.record_migration('unique_migration')`.exec();

      try {
        await sql`SELECT migrations.record_migration('unique_migration')`.exec();
        assert.fail("Should have thrown duplicate error");
      } catch (err: any) {
        assert.ok(
          err.message.includes("duplicate") || err.message.includes("unique")
        );
      }
    });

    it("should check if migration exists", async () => {
      await sql`SELECT migrations.record_migration('existing_migration')`.exec();

      const exists =
        await sql`SELECT migrations.has_migration('existing_migration') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(exists.rows[0].exists, true);

      const notExists =
        await sql`SELECT migrations.has_migration('nonexistent') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(notExists.rows[0].exists, false);
    });
  });

  describe("Migration Queries", () => {
    beforeEach(async () => {
      // Add some migrations across batches
      await sql`SELECT migrations.record_migration('batch1_migration1', 1)`.exec();
      await sql`SELECT migrations.record_migration('batch1_migration2', 1)`.exec();
      await sql`SELECT migrations.record_migration('batch2_migration1', 2)`.exec();
      await sql`SELECT migrations.record_migration('batch3_migration1', 3)`.exec();
      await sql`SELECT migrations.record_migration('batch3_migration2', 3)`.exec();
    });

    it("should get all applied migrations", async () => {
      const result =
        await sql`SELECT * FROM migrations.get_applied_migrations()`.query<{
          name: string;
        }>();

      assert.strictEqual(result.rows.length, 5);
    });

    it("should get migrations by batch", async () => {
      const batch1 =
        await sql`SELECT * FROM migrations.get_migrations_by_batch(1)`.query<{
          name: string;
        }>();
      assert.strictEqual(batch1.rows.length, 2);

      const batch2 =
        await sql`SELECT * FROM migrations.get_migrations_by_batch(2)`.query<{
          name: string;
        }>();
      assert.strictEqual(batch2.rows.length, 1);

      const batch3 =
        await sql`SELECT * FROM migrations.get_migrations_by_batch(3)`.query<{
          name: string;
        }>();
      assert.strictEqual(batch3.rows.length, 2);
    });

    it("should get latest batch migrations", async () => {
      const result =
        await sql`SELECT * FROM migrations.get_latest_batch_migrations()`.query<{
          name: string;
          batch: number;
        }>();

      assert.strictEqual(result.rows.length, 2);
      assert.ok(result.rows.every((r) => r.batch === 3));
    });

    it("should get pending migrations", async () => {
      const allMigrations = [
        "batch1_migration1",
        "batch1_migration2",
        "batch2_migration1",
        "new_migration_1",
        "new_migration_2",
      ];

      const comma = sql`,`;
      const result = await sql`
        SELECT * FROM migrations.get_pending_migrations(ARRAY[${join(allMigrations, comma)}])
      `.query<{ name: string }>();

      assert.strictEqual(result.rows.length, 2);
      assert.deepStrictEqual(result.rows.map((r) => r.name).sort(), [
        "new_migration_1",
        "new_migration_2",
      ]);
    });

    it("should get migration stats", async () => {
      const result = await sql`SELECT * FROM migrations.get_stats()`.query<{
        total_migrations: number;
        total_batches: number;
        last_migration_name: string;
        last_batch: number;
      }>();

      assert.strictEqual(result.rows[0].total_migrations, 5);
      assert.strictEqual(result.rows[0].total_batches, 3);
      assert.strictEqual(
        result.rows[0].last_migration_name,
        "batch3_migration2"
      );
      assert.strictEqual(result.rows[0].last_batch, 3);
    });
  });

  describe("runMigrations Function", () => {
    it("should run pending migrations", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_create_users",
          up: async (sql) => {
            await sql`CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT)`.exec();
          },
        },
        {
          name: "002_add_email",
          up: async (sql) => {
            await sql`ALTER TABLE test_users ADD COLUMN email TEXT`.exec();
          },
        },
      ];

      const result = await runMigrations(sql, migrations);

      assert.strictEqual(result.applied.length, 2);
      assert.deepStrictEqual(result.applied, [
        "001_create_users",
        "002_add_email",
      ]);
      assert.strictEqual(result.batch, 1);

      // Verify tables exist
      const tableCheck = await sql`
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'test_users'
        ORDER BY ordinal_position
      `.query<{ column_name: string }>();

      assert.strictEqual(tableCheck.rows.length, 3);
      assert.deepStrictEqual(
        tableCheck.rows.map((r) => r.column_name),
        ["id", "name", "email"]
      );
    });

    it("should skip already applied migrations", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_first",
          up: async (sql) => {
            await sql`CREATE TABLE first_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
        {
          name: "002_second",
          up: async (sql) => {
            await sql`CREATE TABLE second_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
      ];

      // Run once
      await runMigrations(sql, migrations);

      // Add a new migration
      const updatedMigrations: Migration<TestStatement>[] = [
        ...migrations,
        {
          name: "003_third",
          up: async (sql) => {
            await sql`CREATE TABLE third_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
      ];

      // Run again
      const result = await runMigrations(sql, updatedMigrations);

      assert.strictEqual(result.applied.length, 1);
      assert.deepStrictEqual(result.applied, ["003_third"]);
      assert.strictEqual(result.batch, 2);
    });

    it("should return empty when no pending migrations", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_only",
          up: async (sql) => {
            await sql`CREATE TABLE only_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
      ];

      await runMigrations(sql, migrations);
      const result = await runMigrations(sql, migrations);

      assert.strictEqual(result.applied.length, 0);
      assert.strictEqual(result.batch, 0);
    });

    it("should release lock after successful migrations", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_test",
          up: async () => {},
        },
      ];

      await runMigrations(sql, migrations);

      const status = await getLockStatus(sql);
      assert.strictEqual(status.is_locked, false);
    });

    it("should release lock after failed migration", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_will_fail",
          up: async (sql) => {
            await sql`CREATE TABLE nonexistent.table (id INT)`.exec();
          },
        },
      ];

      try {
        await runMigrations(sql, migrations);
        assert.fail("Should have thrown");
      } catch {
        // Expected
      }

      const status = await getLockStatus(sql);
      assert.strictEqual(status.is_locked, false);
    });
  });

  describe("getMigrationStatus Function", () => {
    it("should return status of all migrations", async () => {
      const migrations: Migration<TestStatement>[] = [
        { name: "001_applied", up: async () => {} },
        { name: "002_applied", up: async () => {} },
        { name: "003_pending", up: async () => {} },
      ];

      // Apply first two
      await runMigrations(sql, migrations.slice(0, 2));

      const status = await getMigrationStatus(sql, migrations);

      assert.strictEqual(status.applied.length, 2);
      assert.deepStrictEqual(
        status.applied.map((m) => m.name),
        ["001_applied", "002_applied"]
      );
      assert.strictEqual(status.pending.length, 1);
      assert.deepStrictEqual(status.pending, ["003_pending"]);
      assert.strictEqual(status.stats.total_migrations, 2);
      assert.strictEqual(status.stats.total_batches, 1);
      assert.strictEqual(status.stats.last_migration_name, "002_applied");
    });

    it("should return empty status when no migrations", async () => {
      const status = await getMigrationStatus(sql, []);

      assert.strictEqual(status.applied.length, 0);
      assert.strictEqual(status.pending.length, 0);
      assert.strictEqual(status.stats.total_migrations, 0);
    });
  });

  describe("Migration Order", () => {
    it("should run migrations in the order provided", async () => {
      const order: string[] = [];

      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_first",
          up: async () => {
            order.push("first");
          },
        },
        {
          name: "002_second",
          up: async () => {
            order.push("second");
          },
        },
        {
          name: "003_third",
          up: async () => {
            order.push("third");
          },
        },
      ];

      await runMigrations(sql, migrations);

      assert.deepStrictEqual(order, ["first", "second", "third"]);
    });

    it("should only run pending migrations in order", async () => {
      const order: string[] = [];

      // Pre-record the first migration
      await sql`SELECT migrations.record_migration('001_first')`.exec();

      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_first",
          up: async () => {
            order.push("first");
          },
        },
        {
          name: "002_second",
          up: async () => {
            order.push("second");
          },
        },
        {
          name: "003_third",
          up: async () => {
            order.push("third");
          },
        },
      ];

      await runMigrations(sql, migrations);

      assert.deepStrictEqual(order, ["second", "third"]);
    });
  });

  describe("Error Handling and Edge Cases", () => {
    it("should not record a failed migration", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_will_fail",
          up: async (sql) => {
            await sql`CREATE TABLE nonexistent_schema.bad_table (id INT)`.exec();
          },
        },
      ];

      try {
        await runMigrations(sql, migrations);
        assert.fail("Should have thrown");
      } catch {
        // Expected
      }

      // Verify the failed migration was NOT recorded
      const recorded =
        await sql`SELECT migrations.has_migration('001_will_fail') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(recorded.rows[0].exists, false);
    });

    it("should not record subsequent migrations after a failure", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_success",
          up: async (sql) => {
            await sql`CREATE TABLE success_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
        {
          name: "002_will_fail",
          up: async (sql) => {
            await sql`CREATE TABLE nonexistent_schema.bad_table (id INT)`.exec();
          },
        },
        {
          name: "003_never_runs",
          up: async (sql) => {
            await sql`CREATE TABLE never_table (id SERIAL PRIMARY KEY)`.exec();
          },
        },
      ];

      try {
        await runMigrations(sql, migrations);
        assert.fail("Should have thrown");
      } catch {
        // Expected
      }

      // First migration should be recorded (it succeeded before the failure)
      const firstRecorded =
        await sql`SELECT migrations.has_migration('001_success') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(firstRecorded.rows[0].exists, true);

      // Failed migration should NOT be recorded
      const failedRecorded =
        await sql`SELECT migrations.has_migration('002_will_fail') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(failedRecorded.rows[0].exists, false);

      // Third migration should NOT be recorded (never ran)
      const thirdRecorded =
        await sql`SELECT migrations.has_migration('003_never_runs') as exists`.query<{
          exists: boolean;
        }>();
      assert.strictEqual(thirdRecorded.rows[0].exists, false);

      // Success table should exist
      const tableExists = await sql`
        SELECT table_name FROM information_schema.tables 
        WHERE table_name = 'success_table'
      `.query<{ table_name: string }>();
      assert.strictEqual(tableExists.rows.length, 1);
    });

    it("should throw when lock cannot be acquired", async () => {
      // Acquire lock manually
      await sql`SELECT migrations.acquire_lock('blocking-process')`.exec();

      const migrations: Migration<TestStatement>[] = [
        { name: "001_test", up: async () => {} },
      ];

      try {
        await runMigrations(sql, migrations);
        assert.fail("Should have thrown lock error");
      } catch (err: any) {
        assert.ok(err.message.includes("Could not acquire migration lock"));
      }
    });

    it("should handle empty migrations array", async () => {
      const result = await runMigrations(sql, []);

      assert.strictEqual(result.applied.length, 0);
      assert.strictEqual(result.batch, 0);
    });

    it("should handle migration names with special characters", async () => {
      const migrations: Migration<TestStatement>[] = [
        {
          name: "001_has,comma",
          up: async () => {},
        },
        {
          name: '002_has"quote',
          up: async () => {},
        },
        {
          name: "003_has'apostrophe",
          up: async () => {},
        },
      ];

      const result = await runMigrations(sql, migrations);

      assert.strictEqual(result.applied.length, 3);
      assert.deepStrictEqual(result.applied, [
        "001_has,comma",
        '002_has"quote',
        "003_has'apostrophe",
      ]);

      // Verify they're recorded correctly
      const status = await getMigrationStatus(sql, migrations);
      assert.strictEqual(status.applied.length, 3);
      assert.strictEqual(status.pending.length, 0);
    });

    it("should allow calling migrationsPlugin multiple times (idempotent)", async () => {
      // migrationsPlugin was already called once at test setup
      // Calling it again should not throw
      await migrationsPlugin(sql);
      await migrationsPlugin(sql);

      // Schema should still work
      const result =
        await sql`SELECT migrations.get_current_batch() as batch`.query<{
          batch: number;
        }>();
      assert.ok(result.rows[0].batch >= 0);
    });
  });

  describe("Real-World Migration Scenario", () => {
    it("should handle a complete migration workflow", async () => {
      // Initial schema
      const v1Migrations: Migration<TestStatement>[] = [
        {
          name: "20240101_000000_create_users",
          up: async (sql) => {
            await sql`
              CREATE TABLE app_users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
              )
            `.exec();
          },
        },
        {
          name: "20240101_000001_create_posts",
          up: async (sql) => {
            await sql`
              CREATE TABLE app_posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES app_users(id),
                title TEXT NOT NULL,
                content TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
              )
            `.exec();
          },
        },
      ];

      // Run initial migrations
      const batch1 = await runMigrations(sql, v1Migrations);
      assert.strictEqual(batch1.applied.length, 2);
      assert.strictEqual(batch1.batch, 1);

      // Insert some data
      await sql`INSERT INTO app_users (username) VALUES ('alice')`.exec();
      await sql`INSERT INTO app_posts (user_id, title, content) VALUES (1, 'Hello', 'World')`.exec();

      // Add new migrations (v2)
      const v2Migrations: Migration<TestStatement>[] = [
        ...v1Migrations,
        {
          name: "20240201_000000_add_user_email",
          up: async (sql) => {
            await sql`ALTER TABLE app_users ADD COLUMN email TEXT`.exec();
          },
        },
        {
          name: "20240201_000001_add_posts_published",
          up: async (sql) => {
            await sql`ALTER TABLE app_posts ADD COLUMN published_at TIMESTAMPTZ`.exec();
          },
        },
      ];

      // Run v2 migrations
      const batch2 = await runMigrations(sql, v2Migrations);
      assert.strictEqual(batch2.applied.length, 2);
      assert.strictEqual(batch2.batch, 2);

      // Verify schema is correct
      const userColumns = await sql`
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'app_users'
        ORDER BY ordinal_position
      `.query<{ column_name: string }>();

      assert.deepStrictEqual(
        userColumns.rows.map((r) => r.column_name),
        ["id", "username", "created_at", "email"]
      );

      // Verify data is preserved
      const users = await sql`SELECT * FROM app_users`.query<{
        username: string;
      }>();
      assert.strictEqual(users.rows[0].username, "alice");

      // Check final status
      const status = await getMigrationStatus(sql, v2Migrations);
      assert.strictEqual(status.applied.length, 4);
      assert.strictEqual(status.pending.length, 0);
      assert.strictEqual(status.stats.total_batches, 2);
    });
  });
});

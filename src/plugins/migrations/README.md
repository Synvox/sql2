# SQL Migrations Plugin

A simple, forward-only migration system for PostgreSQL inspired by Knex migrations. Unlike Knex, this plugin intentionally omits "down" migrations—once applied, migrations are permanent. This design encourages careful migration planning and aligns with production best practices where rollbacks are handled through new forward migrations.

## Features

- **Forward-only migrations**: No down migrations, encouraging immutable schema history
- **Batch tracking**: Migrations are grouped into batches for easy tracking
- **Concurrency-safe**: Advisory lock prevents concurrent migration runs
- **Pure PostgreSQL**: All tracking stored in database tables
- **ACID compliant**: Full transactional guarantees
- **Stale lock recovery**: Automatically recovers from abandoned locks after 30 minutes

## Installation

```bash
npm install sql2
```

## Quick Start

### Project Structure

Organize migrations as individual files in a `migrations/` directory:

```
src/
├── db.ts
├── migrations/
│   ├── 20240101_000000_create_users.ts
│   ├── 20240101_000001_create_posts.ts
│   └── 20240201_000000_add_user_email.ts
└── migrate.ts
```

### Migration File Example

Each migration file exports an `up` function:

```typescript
// src/migrations/20240101_000000_create_users.ts
import type { sql } from "../db.ts";

export async function up(sql: typeof sql) {
  await sql`
    create table users (
      id SERIAL primary key,
      email TEXT not null unique,
      created_at TIMESTAMPTZ default NOW()
    )
  `.exec();
}
```

```typescript
// src/migrations/20240101_000001_create_posts.ts
import type { sql } from "../db.ts";

export async function up(sql: typeof sql) {
  await sql`
    create table posts (
      id SERIAL primary key,
      user_id INTEGER references users (id),
      title TEXT not null,
      content TEXT,
      created_at TIMESTAMPTZ default NOW()
    )
  `.exec();
}
```

### Running Migrations

Import your migrations dynamically and run them:

```typescript
// src/migrate.ts
import { migrationsPlugin, runMigrations } from "sql2/migrations";
import { sql } from "./db.ts";

// Install migrations schema (run once)
await migrationsPlugin(sql);

async function importMigration(name) {
  return {
    name,
    ...(await import(`./migrations/${name}.ts`))
  }
}

// Import migrations in order
const migrations = [
  await importMigration("20240101_000000_create_users"),
  await importMigration("20240101_000001_create_posts"),
  await importMigration("20240201_000000_add_user_email"),
];

// Run pending migrations
const result = await runMigrations(sql, migrations);
console.log(
  `Applied ${result.applied.length} migrations in batch ${result.batch}`,
);
```

### Database Setup

```typescript
// src/db.ts
import { PGlite } from "@electric-sql/pglite";
import { QueryableStatement, type Interpolable } from "sql2";

const db = new PGlite();

export class PGliteStatement extends QueryableStatement {
  async exec() {
    await db.exec(this.compile());
  }
  async query<T>(): Promise<{ rows: T[] }> {
    return db.query(this.compile(), this.values);
  }
}

export const sql = (strings: TemplateStringsArray, ...values: Interpolable[]) =>
  new PGliteStatement(strings, values);
```

## Schema

The plugin creates a `migrations` schema with two tables:

### `migrations.migrations`

Tracks all applied migrations.

| Column         | Type        | Description                                          |
| -------------- | ----------- | ---------------------------------------------------- |
| id             | SERIAL      | Auto-incrementing primary key                        |
| name           | TEXT        | Unique migration name                                |
| batch          | INTEGER     | Batch number (migrations run together share a batch) |
| migration_time | TIMESTAMPTZ | When the migration was applied                       |

### `migrations.migrations_lock`

Prevents concurrent migration runs.

| Column    | Type        | Description                                |
| --------- | ----------- | ------------------------------------------ |
| id        | INTEGER     | Always 1 (single-row table)                |
| is_locked | BOOLEAN     | Whether a migration is in progress         |
| locked_at | TIMESTAMPTZ | When the lock was acquired                 |
| locked_by | TEXT        | Identifier of the process holding the lock |

## API Reference

### Plugin Installation

#### `migrationsPlugin(sql)`

Installs the migrations schema and helper functions. Call once before using migrations.

```typescript
await migrationsPlugin(sql);
```

### Running Migrations

#### `runMigrations(sql, migrations, lockerName?)`

Runs all pending migrations in order.

```typescript
const result = await runMigrations(sql, migrations, "my-app");
// result: { applied: string[], batch: number }
```

**Parameters:**

- `sql` - The sql tagged template function
- `migrations` - Array of migration objects
- `lockerName` - Optional identifier for the running process (default: "sql2-migrations")

**Returns:**

- `applied` - Array of migration names that were applied
- `batch` - The batch number used (0 if no migrations were run)

**Throws:**

- Error if lock cannot be acquired (another migration may be running)

### Migration Status

#### `getMigrationStatus(sql, migrations)`

Gets the status of all migrations.

```typescript
const status = await getMigrationStatus(sql, migrations);
// status: {
//   applied: [{ name, batch, migration_time }],
//   pending: string[],
//   stats: { total_migrations, total_batches, last_migration_name, last_migration_time, last_batch }
// }
```

### Lock Management

#### `getLockStatus(sql)`

Checks the current lock status.

```typescript
const lock = await getLockStatus(sql);
// lock: { is_locked: boolean, locked_at: Date | null, locked_by: string | null }
```

#### `forceReleaseLock(sql)`

Forcefully releases the migration lock. Use with caution—only when you're certain no migration is running.

```typescript
await forceReleaseLock(sql);
```

## SQL Functions

The plugin also provides SQL functions for direct database access:

### Lock Functions

```sql
-- Acquire lock (returns TRUE if successful)
select
  migrations.acquire_lock ('my-process-name');

-- Release lock
select
  migrations.release_lock ();

-- Check if locked
select
  migrations.is_locked ();

-- Get lock details
select
  *
from
  migrations.get_lock_status ();
```

### Batch Functions

```sql
-- Get current batch number (0 if no migrations)
select
  migrations.get_current_batch ();

-- Get next batch number
select
  migrations.get_next_batch ();
```

### Migration Recording

```sql
-- Record a migration as applied
select
  *
from
  migrations.record_migration ('migration_name');

select
  *
from
  migrations.record_migration ('migration_name', 5);

-- specific batch
-- Check if migration exists
select
  migrations.has_migration ('migration_name');
```

### Migration Queries

```sql
-- Get all applied migrations
select
  *
from
  migrations.get_applied_migrations ();

-- Get migrations in a specific batch
select
  *
from
  migrations.get_migrations_by_batch (1);

-- Get latest batch migrations
select
  *
from
  migrations.get_latest_batch_migrations ();

-- Get pending migrations from a list
select
  *
from
  migrations.get_pending_migrations (array['mig1', 'mig2', 'mig3']);

-- Get migration statistics
select
  *
from
  migrations.get_stats ();
```

## Migration Naming Conventions

We recommend using timestamped migration names for proper ordering:

```
YYYYMMDD_HHMMSS_description
```

Examples:

- `20240101_000000_create_users`
- `20240115_143022_add_user_email`
- `20240201_091500_create_posts_table`

## Best Practices

### 1. One Migration Per File

Keep each migration in its own file. This makes it easier to review changes, track history, and maintain the codebase.

### 2. Never Modify Applied Migrations

Once a migration is applied, treat it as immutable. If you need to change something, create a new migration.

### 3. Keep Migrations Small and Focused

Each migration should do one thing. This makes it easier to understand what changed and when.

### 4. Test Migrations in Development

Always test migrations on a copy of production data before deploying.

### 5. Use Transactions Wisely

Each migration runs in its own transaction by default. For operations that can't be transactional (like `CREATE INDEX CONCURRENTLY`), handle appropriately.

### 6. Handle Rollbacks with New Migrations

Need to undo something? Create a new migration that reverses the change:

```typescript
// src/migrations/20240201_000000_drop_user_nickname.ts
import type { sql } from "../db.ts";

export async function up(sql: typeof sql) {
  await sql`
    alter table users
    drop column nickname
  `.exec();
}
```

## Complete Example: Adding Migrations Over Time

As your project evolves, simply add new migration files and update your imports:

```typescript
// src/migrate.ts
import {
  migrationsPlugin,
  runMigrations,
  getMigrationStatus,
} from "sql2/migrations";
import { sql } from "./db.ts";

await migrationsPlugin(sql);

async function importMigration(name) {
  return {
    name,
    ...(await import(`./migrations/${name}.ts`))
  }
}

// Add new migrations to this array as you create them
const migrations = [
  // Initial schema
  await importMigration("20240101_000000_create_users"),
  await importMigration("20240101_000001_create_posts"),

  // Added later
  await importMigration("20240115_000000_add_user_profile"),
  await importMigration("20240115_000001_add_posts_published"),

  // Added even later
  await importMigration("20240201_000000_create_comments"),
];

// Run any pending migrations
const result = await runMigrations(sql, migrations);

if (result.applied.length > 0) {
  console.log(
    `Applied ${result.applied.length} migrations in batch ${result.batch}:`,
  );
  result.applied.forEach((name) => console.log(`  - ${name}`));
} else {
  console.log("No pending migrations");
}

// Check overall status
const status = await getMigrationStatus(sql, migrations);
console.log(
  `\nTotal: ${status.stats.total_migrations} migrations across ${status.stats.total_batches} batches`,
);
```

## Error Handling

```typescript
try {
  await runMigrations(sql, migrations);
} catch (error) {
  if (error.message.includes("Could not acquire migration lock")) {
    console.log("Another migration is in progress, please wait...");

    // Check who has the lock
    const lock = await getLockStatus(sql);
    console.log(`Lock held by: ${lock.locked_by} since ${lock.locked_at}`);
  } else {
    // Migration failed - lock is automatically released
    console.error("Migration failed:", error);
  }
}
```

## Architecture

- **Pure PostgreSQL**: All state stored in standard database tables
- **Idempotent setup**: `migrationsPlugin` can be called multiple times safely
- **Advisory locking**: Uses table-level locking for safety
- **Automatic cleanup**: Lock is always released, even on failure
- **Stale lock recovery**: Locks older than 30 minutes can be acquired by new processes

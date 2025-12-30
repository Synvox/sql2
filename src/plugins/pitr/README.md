# PITR Plugin (Point-in-Time Restore)

A powerful plugin for selective point-in-time restore of individual tables. Instead of restoring an entire database from a backup, you can revert specific tables or rows to any previous state.

## Overview

The PITR plugin provides:

- **Automatic change tracking** via database triggers
- **Complete audit history** of all INSERT, UPDATE, and DELETE operations
- **Point-in-time queries** to see historical data states
- **Selective restore** for entire tables or individual rows
- **Undo functionality** to revert the most recent change
- **Composite primary key support**
- **Column filtering** to exclude large/sensitive columns
- **Transaction-based operations** to view and rollback by transaction ID

## Installation

```typescript
import { pitrPlugin } from "sql2/pitr";

// Install the plugin (creates schema and functions)
await pitrPlugin();
```

## Quick Start

### 1. Enable tracking on a table

```typescript
import { enableTracking } from "sql2/pitr";

await enableTracking({
  schemaName: "public",
  tableName: "orders",
  primaryKeyColumns: ["id"],
});
```

### 2. Make changes (automatically tracked)

```typescript
import { getSql } from "sql2";

const sql = getSql();

await sql`
  insert into
    orders (customer_id, total)
  values
    (1, 100.00)
`.exec();

await sql`
  update orders
  set
    total = 150.00
  where
    id = 1
`.exec();
```

### 3. View history

```typescript
import { getRowHistory } from "sql2/pitr";

const history = await getRowHistory("public", "orders", { id: 1 });
// Returns: [{ operation: 'UPDATE', ... }, { operation: 'INSERT', ... }]
```

### 4. Restore to a previous state

```typescript
import { restoreRow } from "sql2/pitr";

const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
await restoreRow("public", "orders", { id: 1 }, oneHourAgo);
```

## API Reference

### Table Management

#### `enableTracking(config)`

Enables PITR tracking on a table by installing audit triggers.

```typescript
interface TrackingConfig {
  schemaName: string;
  tableName: string;
  primaryKeyColumns: string[];
  trackedColumns?: string[]; // Only track these columns
  excludedColumns?: string[]; // Exclude these columns from tracking
}

const result = await enableTracking({
  schemaName: "public",
  tableName: "users",
  primaryKeyColumns: ["id"],
  excludedColumns: ["password_hash", "large_blob"],
});
// Returns: { trackedTableId: 1, message: "Tracking enabled successfully" }
```

#### `disableTracking(schemaName, tableName, keepHistory?)`

Disables tracking on a table.

```typescript
await disableTracking("public", "users", true); // Keep history
await disableTracking("public", "users", false); // Delete history
```

#### `getTrackedTables()`

Lists all tables with PITR tracking configured.

```typescript
const tables = await getTrackedTables();
// Returns: [{ id, schemaName, tableName, primaryKeyColumns, enabled, auditCount, ... }]
```

### History Queries

#### `getRowHistory(schemaName, tableName, pkValue, limit?)`

Gets the complete change history for a specific row.

```typescript
const history = await getRowHistory("public", "orders", { id: 1 }, 100);

// Each entry contains:
// - id: UUID of the audit entry
// - operation: 'INSERT' | 'UPDATE' | 'DELETE'
// - oldData: Row data before the change (null for INSERT)
// - newData: Row data after the change (null for DELETE)
// - changedColumns: Array of columns that changed (for UPDATE)
// - changedAt: Timestamp of the change
// - changedBy: User who made the change
// - transactionId: PostgreSQL transaction ID
```

#### `getTableHistory(schemaName, tableName, options?)`

Gets change history for an entire table within a time range.

```typescript
const history = await getTableHistory("public", "orders", {
  since: new Date("2024-01-01"),
  until: new Date("2024-01-31"),
  limit: 1000,
});
```

#### `getRowAt(schemaName, tableName, pkValue, asOf)`

Reconstructs the state of a specific row at a given point in time.

```typescript
const yesterdayState = await getRowAt(
  "public",
  "orders",
  { id: 1 },
  new Date(Date.now() - 24 * 60 * 60 * 1000),
);
// Returns: { id: 1, customer_id: 1, total: "100.00", status: "pending" }
// Or null if the row didn't exist at that time
```

#### `getTableAt(schemaName, tableName, asOf)`

Reconstructs the entire table state at a given point in time.

```typescript
const snapshot = await getTableAt("public", "orders", yesterdayNoon);
// Returns: [{ primaryKeyValue: {...}, rowData: {...} }, ...]
```

### Restore Operations

#### `restoreRow(schemaName, tableName, pkValue, asOf)`

Restores a single row to its state at the given point in time.

```typescript
const result = await restoreRow("public", "orders", { id: 1 }, oneHourAgo);
// Returns: {
//   success: true,
//   operation: 'UPDATE' | 'INSERT' | 'DELETE' | 'NO_CHANGE',
//   message: "Row updated to historical state"
// }
```

**Behavior:**

- If the row existed at `asOf` but has since been modified → **UPDATE**
- If the row existed at `asOf` but has since been deleted → **INSERT**
- If the row didn't exist at `asOf` but exists now → **DELETE**
- If the row is already at the historical state → **NO_CHANGE**

#### `restoreRowsWhere(schemaName, tableName, filter, asOf, dryRun?)`

Restores rows matching a filter condition to their state at a given point in time.

```typescript
import { restoreRowsWhere } from "sql2/pitr";

// Restore all orders for user_id = 1 to their state one hour ago
const results = await restoreRowsWhere(
  "public",
  "orders",
  { user_id: 1 }, // Filter condition
  oneHourAgo,
  false,
);
// Returns: [
//   { operation: 'UPDATE', affectedRows: 3, details: "3 rows updated" },
//   { operation: 'INSERT', affectedRows: 1, details: "1 rows restored" }
// ]

// Preview what would happen
const preview = await restoreRowsWhere(
  "public",
  "orders",
  { user_id: 1 },
  oneHourAgo,
  true,
);
```

The filter uses PostgreSQL's JSONB `@>` (contains) operator, so you can filter on any column values that were tracked in the audit history.

#### `restoreTable(schemaName, tableName, asOf, dryRun?)`

Restores an entire table to its state at a given point in time.

```typescript
// Preview what would happen
const preview = await restoreTable("public", "orders", oneHourAgo, true);

// Actually perform the restore
const results = await restoreTable("public", "orders", oneHourAgo, false);
// Returns: [
//   { operation: 'UPDATE', affectedRows: 5, details: "5 rows updated" },
//   { operation: 'INSERT', affectedRows: 2, details: "2 rows restored" },
//   { operation: 'DELETE', affectedRows: 1, details: "1 rows deleted" }
// ]
```

#### `undoLastChange(schemaName, tableName, pkValue)`

Reverts the most recent change to a specific row.

```typescript
const result = await undoLastChange("public", "orders", { id: 1 });
// Restores the row to its state just before the last recorded change
```

### Transaction-Based Operations

#### `getRecentTransactions(limit?)`

Gets a summary of recent transactions with their affected tables.

```typescript
import { getRecentTransactions } from "sql2/pitr";

const transactions = await getRecentTransactions(50);
// Returns: [{
//   transactionId: 12345,
//   changedAt: Date,
//   changedBy: "app_user",
//   tablesAffected: ["public.orders", "public.order_items"],
//   totalChanges: 5,
//   inserts: 2,
//   updates: 2,
//   deletes: 1
// }, ...]
```

#### `getTransactionHistory(transactionId)`

Gets all changes that occurred in a specific transaction across all tracked tables.

```typescript
import { getTransactionHistory } from "sql2/pitr";

const changes = await getTransactionHistory(12345);
// Returns all changes from that transaction, across all tracked tables
```

#### `restoreToTransaction(transactionId, dryRun?)`

Restores all tracked tables to their state just before a specific transaction. This effectively rolls back that transaction and all subsequent changes.

```typescript
import { restoreToTransaction } from "sql2/pitr";

// Preview what would happen
const preview = await restoreToTransaction(12345, true);

// Actually perform the restore
const results = await restoreToTransaction(12345, false);
```

#### `undoTransaction(transactionId, dryRun?)`

Undoes all changes from a specific transaction without affecting subsequent transactions.

```typescript
import { undoTransaction } from "sql2/pitr";

// Undo just transaction 12345, keeping all later changes
const results = await undoTransaction(12345, false);
```

**Key difference from `restoreToTransaction`:**

- `restoreToTransaction` reverts to the state _before_ the transaction, rolling back all subsequent changes too
- `undoTransaction` only reverts the changes made in that specific transaction, preserving later changes

#### `restoreTablesToTransaction(transactionId, tables, dryRun?)`

Restores only specific tables to their state just before a transaction.

```typescript
import { restoreTablesToTransaction } from "sql2/pitr";

// Only restore orders and order_items, not other affected tables
const results = await restoreTablesToTransaction(
  12345,
  [
    { schemaName: "public", tableName: "orders" },
    { schemaName: "public", tableName: "order_items" },
  ],
  false,
);
```

### Maintenance

#### `pruneHistory(olderThan, schemaName?, tableName?)`

Deletes audit log entries older than the specified date.

```typescript
// Prune all tables
const result = await pruneHistory(thirtyDaysAgo);

// Prune specific schema
await pruneHistory(thirtyDaysAgo, "public");

// Prune specific table
await pruneHistory(thirtyDaysAgo, "public", "orders");

// Returns: { deletedCount: 1500, message: "Pruned 1500 entries..." }
```

#### `getStats()`

Returns overall PITR statistics.

```typescript
const stats = await getStats();
// Returns: {
//   totalTrackedTables: 5,
//   activeTrackedTables: 4,
//   totalAuditEntries: 150000,
//   oldestEntry: Date,
//   newestEntry: Date,
//   entriesLast24h: 500,
//   entriesLast7d: 3500
// }
```

#### `getTableStats(schemaName, tableName)`

Returns detailed statistics for a specific table.

```typescript
const stats = await getTableStats("public", "orders");
// Returns: {
//   totalEntries: 5000,
//   inserts: 1000,
//   updates: 3500,
//   deletes: 500,
//   uniqueRowsTracked: 800,
//   oldestEntry: Date,
//   newestEntry: Date,
//   avgChangesPerRow: 6.25
// }
```

## Advanced Usage

### Composite Primary Keys

```typescript
await enableTracking({
  schemaName: "public",
  tableName: "order_items",
  primaryKeyColumns: ["order_id", "product_id"],
});

// Query with composite key
const history = await getRowHistory("public", "order_items", {
  order_id: 1,
  product_id: 100,
});

// Restore with composite key
await restoreRow(
  "public",
  "order_items",
  { order_id: 1, product_id: 100 },
  oneHourAgo,
);
```

### Column Filtering

Exclude large or sensitive columns from tracking:

```typescript
await enableTracking({
  schemaName: "public",
  tableName: "documents",
  primaryKeyColumns: ["id"],
  excludedColumns: ["file_content", "thumbnail_blob"],
});
```

Or only track specific columns:

```typescript
await enableTracking({
  schemaName: "public",
  tableName: "users",
  primaryKeyColumns: ["id"],
  trackedColumns: ["id", "email", "status", "role"],
});
```

### Transaction Grouping

All changes within a single transaction share the same `transactionId`, making it easy to see related changes:

```typescript
const history = await getTableHistory("public", "orders");
const relatedChanges = history.filter((h) => h.transactionId === 12345);
```

## Use Cases

### 1. Accidental Data Modification

```typescript
// Oops! Someone ran an UPDATE without a WHERE clause
// Restore the affected rows

const beforeMistake = new Date("2024-01-15T14:30:00Z");
const results = await restoreTable("public", "orders", beforeMistake);
console.log(
  `Restored ${results.reduce((sum, r) => sum + r.affectedRows, 0)} rows`,
);
```

### 2. Audit Compliance

```typescript
// Show who changed a sensitive record and when
const history = await getRowHistory("public", "users", { id: 42 });

for (const entry of history) {
  console.log(`${entry.changedAt}: ${entry.operation} by ${entry.changedBy}`);
  if (entry.changedColumns) {
    console.log(`  Changed: ${entry.changedColumns.join(", ")}`);
  }
}
```

### 3. Data Recovery Testing

```typescript
// Preview restore before committing
const preview = await restoreTable("public", "orders", oneHourAgo, true);
console.log("Preview:", preview);

// If it looks good, actually restore
if (confirm("Proceed with restore?")) {
  await restoreTable("public", "orders", oneHourAgo, false);
}
```

### 4. Quick Undo

```typescript
// User made a mistake on a single record
await undoLastChange("public", "orders", { id: 123 });
```

### 5. Transaction-Based Recovery

```typescript
// Find recent transactions
const transactions = await getRecentTransactions(20);

// Find a problematic transaction
const badTx = transactions.find(
  (tx) => tx.tablesAffected.includes("public.orders") && tx.deletes > 10,
);

if (badTx) {
  // See exactly what happened in that transaction
  const changes = await getTransactionHistory(badTx.transactionId);

  // Option 1: Undo just that transaction (preserves later changes)
  await undoTransaction(badTx.transactionId);

  // Option 2: Restore to before that transaction (reverts all subsequent changes too)
  // await restoreToTransaction(badTx.transactionId);
}
```

## SQL Functions (Direct Usage)

You can also use the SQL functions directly:

```sql
-- Enable tracking
select
  *
from
  pitr.enable_tracking ('public', 'orders', array['id']);

-- Get row history
select
  *
from
  pitr.get_row_history ('public', 'orders', '{"id": 1}'::JSONB);

-- Get row at point in time
select
  pitr.get_row_at (
    'public',
    'orders',
    '{"id": 1}'::JSONB,
    '2024-01-15 12:00:00'
  );

-- Restore a row
select
  *
from
  pitr.restore_row (
    'public',
    'orders',
    '{"id": 1}'::JSONB,
    '2024-01-15 12:00:00'
  );

-- Get statistics
select
  *
from
  pitr.get_stats ();

select
  *
from
  pitr.get_table_stats ('public', 'orders');
```

## Storage Considerations

The audit log can grow significantly for tables with frequent updates. Consider:

1. **Regular pruning** - Set up a scheduled job to prune old entries:

   ```typescript
   // Keep 90 days of history
   await pruneHistory(new Date(Date.now() - 90 * 24 * 60 * 60 * 1000));
   ```

2. **Column exclusion** - Don't track large blob columns:

   ```typescript
   await enableTracking({
     ...,
     excludedColumns: ["large_text_field", "binary_data"],
   });
   ```

3. **Monitor storage**:
   ```typescript
   const stats = await getStats();
   console.log(`Total audit entries: ${stats.totalAuditEntries}`);
   ```

## Limitations

- **Schema changes** are not tracked. If you add/remove columns after enabling tracking, the historical data will reflect the schema at the time of the change.
- **Bulk operations** (e.g., `UPDATE ... WHERE condition`) create one audit entry per affected row.
- **Generated columns** and computed values are stored as they were at the time of the change.
- **Restores are logged** - restoring data creates new audit entries for the restore operations.

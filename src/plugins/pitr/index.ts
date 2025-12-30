import * as fsp from "node:fs/promises";
import { getSql } from "../../sql2.ts";

/**
 * Configuration for enabling PITR tracking on a table
 */
export interface TrackingConfig {
  schemaName: string;
  tableName: string;
  primaryKeyColumns: string[];
  /** Optional: only track these columns */
  trackedColumns?: string[];
  /** Optional: exclude these columns from tracking (e.g., large blobs) */
  excludedColumns?: string[];
}

/**
 * Result from enabling tracking
 */
export interface EnableTrackingResult {
  trackedTableId: number;
  message: string;
}

/**
 * Result from disabling tracking
 */
export interface DisableTrackingResult {
  success: boolean;
  message: string;
}

/**
 * Information about a tracked table
 */
export interface TrackedTable {
  id: number;
  schemaName: string;
  tableName: string;
  primaryKeyColumns: string[];
  trackedColumns: string[] | null;
  excludedColumns: string[] | null;
  enabled: boolean;
  createdAt: Date;
  auditCount: number;
}

/**
 * A single change in the audit history
 */
export interface AuditEntry {
  id: string;
  operation: "INSERT" | "UPDATE" | "DELETE";
  primaryKeyValue?: Record<string, unknown>;
  oldData: Record<string, unknown> | null;
  newData: Record<string, unknown> | null;
  changedColumns: string[] | null;
  changedAt: Date;
  changedBy: string;
  transactionId: number;
}

/**
 * Result from a restore operation
 */
export interface RestoreResult {
  success: boolean;
  operation: "INSERT" | "UPDATE" | "DELETE" | "NO_CHANGE" | "ERROR";
  message: string;
}

/**
 * Summary of a table restore operation
 */
export interface TableRestoreSummary {
  operation: string;
  affectedRows: number;
  details: string;
}

/**
 * Statistics about PITR storage
 */
export interface PitrStats {
  totalTrackedTables: number;
  activeTrackedTables: number;
  totalAuditEntries: number;
  oldestEntry: Date | null;
  newestEntry: Date | null;
  entriesLast24h: number;
  entriesLast7d: number;
}

/**
 * Statistics for a specific tracked table
 */
export interface TableStats {
  totalEntries: number;
  inserts: number;
  updates: number;
  deletes: number;
  uniqueRowsTracked: number;
  oldestEntry: Date | null;
  newestEntry: Date | null;
  avgChangesPerRow: number;
}

/**
 * A change entry with full table context (for transaction queries)
 */
export interface TransactionChangeEntry {
  id: string;
  schemaName: string;
  tableName: string;
  operation: "INSERT" | "UPDATE" | "DELETE";
  primaryKeyValue: Record<string, unknown>;
  oldData: Record<string, unknown> | null;
  newData: Record<string, unknown> | null;
  changedColumns: string[] | null;
  changedAt: Date;
  changedBy: string;
}

/**
 * Summary of a transaction
 */
export interface TransactionSummary {
  transactionId: number;
  changedAt: Date;
  changedBy: string;
  tablesAffected: string[];
  totalChanges: number;
  inserts: number;
  updates: number;
  deletes: number;
}

/**
 * Result from a multi-table restore operation
 */
export interface MultiTableRestoreSummary {
  schemaName: string;
  tableName: string;
  operation: string;
  affectedRows: number;
  details: string;
}

/**
 * Installs the PITR (Point-in-Time Restore) schema and helper functions.
 * Call this once before using any PITR functions.
 */
export async function pitrPlugin() {
  const sql = getSql({ camelize: false });

  const sqlScript = await fsp.readFile(
    new URL("./pitr.sql", import.meta.url),
    "utf-8",
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

/**
 * Enables PITR tracking on a table.
 * Creates triggers to capture all INSERT, UPDATE, and DELETE operations.
 *
 * @param config - Tracking configuration
 * @returns Result with table ID and message
 */
export async function enableTracking(
  config: TrackingConfig,
): Promise<EnableTrackingResult> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      pitr.enable_tracking (
        ${config.schemaName},
        ${config.tableName},
        array[${sql.join(
          config.primaryKeyColumns.map((col) => sql.literal(col)),
        )}]::text[],
        ${config.trackedColumns
          ? sql`
              array[${sql.join(
                config.trackedColumns.map((col) => sql.literal(col)),
              )}]::text[]
            `
          : sql`null`},
        ${config.excludedColumns
          ? sql`
              array[${sql.join(
                config.excludedColumns.map((col) => sql.literal(col)),
              )}]::text[]
            `
          : sql`null`}
      )
  `.first<{ tracked_table_id: number; message: string }>();

  return {
    trackedTableId: row!.tracked_table_id,
    message: row!.message,
  };
}

/**
 * Disables PITR tracking on a table.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Name of the table
 * @param keepHistory - Whether to keep the audit history (default: true)
 * @returns Result indicating success and message
 */
export async function disableTracking(
  schemaName: string,
  tableName: string,
  keepHistory: boolean = true,
): Promise<DisableTrackingResult> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      pitr.disable_tracking (
        ${schemaName},
        ${tableName},
        ${keepHistory}
      )
  `.first<{ success: boolean; message: string }>();

  return {
    success: row!.success,
    message: row!.message,
  };
}

/**
 * Lists all tables with PITR tracking configured.
 *
 * @returns Array of tracked table information
 */
export async function getTrackedTables(): Promise<TrackedTable[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      pitr.get_tracked_tables ()
  `.all<{
    id: number;
    schema_name: string;
    table_name: string;
    primary_key_columns: string[];
    tracked_columns: string[] | null;
    excluded_columns: string[] | null;
    enabled: boolean;
    created_at: Date;
    audit_count: number;
  }>();

  return rows.map((row) => ({
    id: row.id,
    schemaName: row.schema_name,
    tableName: row.table_name,
    primaryKeyColumns: row.primary_key_columns,
    trackedColumns: row.tracked_columns,
    excludedColumns: row.excluded_columns,
    enabled: row.enabled,
    createdAt: row.created_at,
    auditCount: row.audit_count,
  }));
}

/**
 * Gets the complete change history for a specific row.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param primaryKeyValue - Primary key value as an object
 * @param limit - Maximum number of entries to return (default: 100)
 * @returns Array of audit entries
 */
export async function getRowHistory(
  schemaName: string,
  tableName: string,
  primaryKeyValue: Record<string, unknown>,
  limit: number = 100,
): Promise<AuditEntry[]> {
  const sql = getSql({ camelize: false });
  const pkJson = JSON.stringify(primaryKeyValue);

  const rows = await sql`
    select
      *
    from
      pitr.get_row_history (
        ${schemaName},
        ${tableName},
        ${pkJson}::JSONB,
        ${limit}
      )
  `.all<{
    id: string;
    operation: "INSERT" | "UPDATE" | "DELETE";
    old_data: Record<string, unknown> | null;
    new_data: Record<string, unknown> | null;
    changed_columns: string[] | null;
    changed_at: Date;
    changed_by: string;
    transaction_id: number;
  }>();

  return rows.map((row) => ({
    id: row.id,
    operation: row.operation,
    oldData: row.old_data,
    newData: row.new_data,
    changedColumns: row.changed_columns,
    changedAt: row.changed_at,
    changedBy: row.changed_by,
    transactionId: row.transaction_id,
  }));
}

/**
 * Gets change history for an entire table within a time range.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param options - Optional filters (since, until, limit)
 * @returns Array of audit entries
 */
export async function getTableHistory(
  schemaName: string,
  tableName: string,
  options: {
    since?: Date;
    until?: Date;
    limit?: number;
  } = {},
): Promise<AuditEntry[]> {
  const sql = getSql({ camelize: false });
  const { since = null, until = null, limit = 1000 } = options;
  const sinceStr = since?.toISOString() ?? null;
  const untilStr = until?.toISOString() ?? null;

  const rows = await sql`
    select
      *
    from
      pitr.get_table_history (
        ${schemaName},
        ${tableName},
        ${sinceStr}::TIMESTAMPTZ,
        ${untilStr}::TIMESTAMPTZ,
        ${limit}
      )
  `.all<{
    id: string;
    operation: "INSERT" | "UPDATE" | "DELETE";
    primary_key_value: Record<string, unknown>;
    old_data: Record<string, unknown> | null;
    new_data: Record<string, unknown> | null;
    changed_columns: string[] | null;
    changed_at: Date;
    changed_by: string;
    transaction_id: number;
  }>();

  return rows.map((row) => ({
    id: row.id,
    operation: row.operation,
    primaryKeyValue: row.primary_key_value,
    oldData: row.old_data,
    newData: row.new_data,
    changedColumns: row.changed_columns,
    changedAt: row.changed_at,
    changedBy: row.changed_by,
    transactionId: row.transaction_id,
  }));
}

/**
 * Reconstructs the state of a specific row at a given point in time.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param primaryKeyValue - Primary key value as an object
 * @param asOf - The point in time to query
 * @returns The row data at that time, or null if it didn't exist
 */
export async function getRowAt(
  schemaName: string,
  tableName: string,
  primaryKeyValue: Record<string, unknown>,
  asOf: Date,
): Promise<Record<string, unknown> | null> {
  const sql = getSql({ camelize: false });
  const pkJson = JSON.stringify(primaryKeyValue);
  const asOfStr = asOf.toISOString();

  const row = await sql`
    select
      pitr.get_row_at (
        ${schemaName},
        ${tableName},
        ${pkJson}::JSONB,
        ${asOfStr}::TIMESTAMPTZ
      ) as row_data
  `.first<{ row_data: Record<string, unknown> | null }>();

  return row?.row_data ?? null;
}

/**
 * Reconstructs the entire table state at a given point in time.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param asOf - The point in time to query
 * @returns Array of rows as they existed at that time
 */
export async function getTableAt(
  schemaName: string,
  tableName: string,
  asOf: Date,
): Promise<
  Array<{
    primaryKeyValue: Record<string, unknown>;
    rowData: Record<string, unknown>;
  }>
> {
  const sql = getSql({ camelize: false });
  const asOfStr = asOf.toISOString();

  const rows = await sql`
    select
      *
    from
      pitr.get_table_at (
        ${schemaName},
        ${tableName},
        ${asOfStr}::TIMESTAMPTZ
      )
  `.all<{
    primary_key_value: Record<string, unknown>;
    row_data: Record<string, unknown>;
  }>();

  return rows.map((row) => ({
    primaryKeyValue: row.primary_key_value,
    rowData: row.row_data,
  }));
}

/**
 * Restores a single row to its state at the given point in time.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param primaryKeyValue - Primary key value as an object
 * @param asOf - The point in time to restore to
 * @returns Result indicating what action was taken
 */
export async function restoreRow(
  schemaName: string,
  tableName: string,
  primaryKeyValue: Record<string, unknown>,
  asOf: Date,
): Promise<RestoreResult> {
  const sql = getSql({ camelize: false });
  const pkJson = JSON.stringify(primaryKeyValue);
  const asOfStr = asOf.toISOString();

  const row = await sql`
    select
      *
    from
      pitr.restore_row (
        ${schemaName},
        ${tableName},
        ${pkJson}::JSONB,
        ${asOfStr}::TIMESTAMPTZ
      )
  `.first<{ success: boolean; operation: string; message: string }>();

  return {
    success: row!.success,
    operation: row!.operation as RestoreResult["operation"],
    message: row!.message,
  };
}

/**
 * Restores an entire table to its state at the given point in time.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param asOf - The point in time to restore to
 * @param dryRun - If true, only preview what would happen (default: false)
 * @returns Summary of operations performed
 */
export async function restoreTable(
  schemaName: string,
  tableName: string,
  asOf: Date,
  dryRun: boolean = false,
): Promise<TableRestoreSummary[]> {
  const sql = getSql({ camelize: false });
  const asOfStr = asOf.toISOString();

  const rows = await sql`
    select
      *
    from
      pitr.restore_table (
        ${schemaName},
        ${tableName},
        ${asOfStr}::TIMESTAMPTZ,
        ${dryRun}
      )
  `.all<{ operation: string; affected_rows: number; details: string }>();

  return rows.map((row) => ({
    operation: row.operation,
    affectedRows: row.affected_rows,
    details: row.details,
  }));
}

/**
 * Restores rows matching a filter condition to their state at a given point in time.
 * The filter is matched against the row data (old_data or new_data in audit history).
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param filter - Filter object to match rows (e.g., { user_id: 1 })
 * @param asOf - The point in time to restore to
 * @param dryRun - If true, only preview what would happen (default: false)
 * @returns Summary of operations performed
 */
export async function restoreRowsWhere(
  schemaName: string,
  tableName: string,
  filter: Record<string, unknown>,
  asOf: Date,
  dryRun: boolean = false,
): Promise<TableRestoreSummary[]> {
  const sql = getSql({ camelize: false });
  const filterJson = JSON.stringify(filter);
  const asOfStr = asOf.toISOString();

  const rows = await sql`
    select
      *
    from
      pitr.restore_rows_where (
        ${schemaName},
        ${tableName},
        ${filterJson}::JSONB,
        ${asOfStr}::TIMESTAMPTZ,
        ${dryRun}
      )
  `.all<{ operation: string; affected_rows: number; details: string }>();

  return rows.map((row) => ({
    operation: row.operation,
    affectedRows: row.affected_rows,
    details: row.details,
  }));
}

/**
 * Reverts the most recent change to a specific row.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @param primaryKeyValue - Primary key value as an object
 * @returns Result indicating what action was taken
 */
export async function undoLastChange(
  schemaName: string,
  tableName: string,
  primaryKeyValue: Record<string, unknown>,
): Promise<RestoreResult> {
  const sql = getSql({ camelize: false });
  const pkJson = JSON.stringify(primaryKeyValue);

  const row = await sql`
    select
      *
    from
      pitr.undo_last_change (
        ${schemaName},
        ${tableName},
        ${pkJson}::JSONB
      )
  `.first<{ success: boolean; operation: string; message: string }>();

  return {
    success: row!.success,
    operation: row!.operation as RestoreResult["operation"],
    message: row!.message,
  };
}

/**
 * Deletes audit log entries older than the specified date.
 *
 * @param olderThan - Delete entries older than this date
 * @param schemaName - Optional: only prune this schema
 * @param tableName - Optional: only prune this table (requires schema)
 * @returns Number of deleted entries and message
 */
export async function pruneHistory(
  olderThan: Date,
  schemaName?: string,
  tableName?: string,
): Promise<{ deletedCount: number; message: string }> {
  const sql = getSql({ camelize: false });
  const olderThanStr = olderThan.toISOString();

  const row = await sql`
    select
      *
    from
      pitr.prune_history (
        ${olderThanStr}::TIMESTAMPTZ,
        ${schemaName ?? null},
        ${tableName ?? null}
      )
  `.first<{ deleted_count: number; message: string }>();

  return {
    deletedCount: Number(row!.deleted_count),
    message: row!.message,
  };
}

/**
 * Returns statistics about PITR storage and tracking.
 *
 * @returns Overall PITR statistics
 */
export async function getStats(): Promise<PitrStats> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      pitr.get_stats ()
  `.first<{
    total_tracked_tables: number;
    active_tracked_tables: number;
    total_audit_entries: number;
    oldest_entry: Date | null;
    newest_entry: Date | null;
    entries_last_24h: number;
    entries_last_7d: number;
  }>();

  return {
    totalTrackedTables: row!.total_tracked_tables,
    activeTrackedTables: row!.active_tracked_tables,
    totalAuditEntries: Number(row!.total_audit_entries),
    oldestEntry: row!.oldest_entry,
    newestEntry: row!.newest_entry,
    entriesLast24h: Number(row!.entries_last_24h),
    entriesLast7d: Number(row!.entries_last_7d),
  };
}

/**
 * Returns detailed statistics for a specific tracked table.
 *
 * @param schemaName - Schema containing the table
 * @param tableName - Table name
 * @returns Table-specific statistics
 */
export async function getTableStats(
  schemaName: string,
  tableName: string,
): Promise<TableStats> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      pitr.get_table_stats (
        ${schemaName},
        ${tableName}
      )
  `.first<{
    total_entries: number;
    inserts: number;
    updates: number;
    deletes: number;
    unique_rows_tracked: number;
    oldest_entry: Date | null;
    newest_entry: Date | null;
    avg_changes_per_row: number;
  }>();

  return {
    totalEntries: Number(row!.total_entries),
    inserts: Number(row!.inserts),
    updates: Number(row!.updates),
    deletes: Number(row!.deletes),
    uniqueRowsTracked: Number(row!.unique_rows_tracked),
    oldestEntry: row!.oldest_entry,
    newestEntry: row!.newest_entry,
    avgChangesPerRow: Number(row!.avg_changes_per_row),
  };
}

/**
 * Gets all changes that occurred in a specific transaction across all tracked tables.
 *
 * @param transactionId - The PostgreSQL transaction ID
 * @returns Array of change entries with full table context
 */
export async function getTransactionHistory(
  transactionId: number,
): Promise<TransactionChangeEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      pitr.get_transaction_history (${transactionId})
  `.all<{
    id: string;
    schema_name: string;
    table_name: string;
    operation: "INSERT" | "UPDATE" | "DELETE";
    primary_key_value: Record<string, unknown>;
    old_data: Record<string, unknown> | null;
    new_data: Record<string, unknown> | null;
    changed_columns: string[] | null;
    changed_at: Date;
    changed_by: string;
  }>();

  return rows.map((row) => ({
    id: row.id,
    schemaName: row.schema_name,
    tableName: row.table_name,
    operation: row.operation,
    primaryKeyValue: row.primary_key_value,
    oldData: row.old_data,
    newData: row.new_data,
    changedColumns: row.changed_columns,
    changedAt: row.changed_at,
    changedBy: row.changed_by,
  }));
}

/**
 * Gets a summary of recent transactions with their affected tables.
 *
 * @param limit - Maximum number of transactions to return (default: 50)
 * @returns Array of transaction summaries
 */
export async function getRecentTransactions(
  limit: number = 50,
): Promise<TransactionSummary[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      pitr.get_recent_transactions (${limit})
  `.all<{
    transaction_id: number;
    changed_at: Date;
    changed_by: string;
    tables_affected: string[];
    total_changes: number;
    inserts: number;
    updates: number;
    deletes: number;
  }>();

  return rows.map((row) => ({
    transactionId: Number(row.transaction_id),
    changedAt: row.changed_at,
    changedBy: row.changed_by,
    tablesAffected: row.tables_affected,
    totalChanges: row.total_changes,
    inserts: row.inserts,
    updates: row.updates,
    deletes: row.deletes,
  }));
}

/**
 * Restores all tracked tables to their state just before a specific transaction.
 * This effectively rolls back all changes from that transaction and any subsequent ones.
 *
 * @param transactionId - The transaction ID to restore to (restores to state just before this transaction)
 * @param dryRun - If true, only preview what would happen (default: false)
 * @returns Summary of operations performed per table
 */
export async function restoreToTransaction(
  transactionId: number,
  dryRun: boolean = false,
): Promise<MultiTableRestoreSummary[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      pitr.restore_to_transaction (
        ${transactionId},
        ${dryRun}
      )
  `.all<{
    schema_name: string;
    table_name: string;
    operation: string;
    affected_rows: number;
    details: string;
  }>();

  return rows.map((row) => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    operation: row.operation,
    affectedRows: row.affected_rows,
    details: row.details,
  }));
}

/**
 * Undoes all changes from a specific transaction.
 * Unlike restoreToTransaction, this only reverts the changes made in that specific transaction,
 * not any subsequent transactions.
 *
 * @param transactionId - The transaction ID to undo
 * @param dryRun - If true, only preview what would happen (default: false)
 * @returns Summary of operations performed per table
 */
export async function undoTransaction(
  transactionId: number,
  dryRun: boolean = false,
): Promise<MultiTableRestoreSummary[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      pitr.undo_transaction (
        ${transactionId},
        ${dryRun}
      )
  `.all<{
    schema_name: string;
    table_name: string;
    operation: string;
    affected_rows: number;
    details: string;
  }>();

  return rows.map((row) => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    operation: row.operation,
    affectedRows: row.affected_rows,
    details: row.details,
  }));
}

/**
 * Restores specific tables to their state just before a specific transaction.
 *
 * @param transactionId - The transaction ID to restore to
 * @param tables - Array of table identifiers (schema and table name pairs)
 * @param dryRun - If true, only preview what would happen (default: false)
 * @returns Summary of operations performed per table
 */
export async function restoreTablesToTransaction(
  transactionId: number,
  tables: Array<{ schemaName: string; tableName: string }>,
  dryRun: boolean = false,
): Promise<MultiTableRestoreSummary[]> {
  const sql = getSql({ camelize: false });
  // Convert to JSON format for PostgreSQL
  const tablesJson = JSON.stringify(
    tables.map((t) => ({ schema: t.schemaName, table: t.tableName })),
  );

  const rows = await sql`
    select
      *
    from
      pitr.restore_tables_to_transaction (
        ${transactionId},
        ${tablesJson}::JSONB,
        ${dryRun}
      )
  `.all<{
    schema_name: string;
    table_name: string;
    operation: string;
    affected_rows: number;
    details: string;
  }>();

  return rows.map((row) => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    operation: row.operation,
    affectedRows: row.affected_rows,
    details: row.details,
  }));
}

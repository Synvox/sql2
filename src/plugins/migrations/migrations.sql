-- Migrations Plugin Schema
-- A simple migration system inspired by Knex, but forward-only (no down migrations)
-- Create the migrations schema
CREATE SCHEMA IF NOT EXISTS migrations;
-- Migrations table to track applied migrations
CREATE TABLE IF NOT EXISTS migrations.migrations (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  batch INTEGER NOT NULL,
  migration_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Index for faster lookups by name and batch
CREATE INDEX IF NOT EXISTS idx_migrations_name ON migrations.migrations (name);
CREATE INDEX IF NOT EXISTS idx_migrations_batch ON migrations.migrations (batch);
-- Lock table for preventing concurrent migrations
CREATE TABLE IF NOT EXISTS migrations.migrations_lock (
  id INTEGER PRIMARY KEY DEFAULT 1,
  is_locked BOOLEAN NOT NULL DEFAULT FALSE,
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  CONSTRAINT single_row CHECK (id = 1)
);
-- Insert the single lock row (idempotent)
INSERT INTO migrations.migrations_lock (id, is_locked)
VALUES (1, FALSE)
ON CONFLICT (id) DO NOTHING;
-- Function to acquire the migration lock
-- Returns TRUE if lock was acquired, FALSE otherwise
CREATE OR REPLACE FUNCTION migrations.acquire_lock(locker_name TEXT DEFAULT 'unknown') RETURNS BOOLEAN AS $$
DECLARE rows_updated INTEGER;
BEGIN
UPDATE migrations.migrations_lock
SET is_locked = TRUE,
  locked_at = NOW(),
  locked_by = locker_name
WHERE id = 1
  AND is_locked = FALSE;
GET DIAGNOSTICS rows_updated = ROW_COUNT;
IF rows_updated > 0 THEN RETURN TRUE;
END IF;
-- Check if lock is stale (older than 30 minutes)
UPDATE migrations.migrations_lock
SET is_locked = TRUE,
  locked_at = NOW(),
  locked_by = locker_name
WHERE id = 1
  AND is_locked = TRUE
  AND locked_at < NOW() - INTERVAL '30 minutes';
GET DIAGNOSTICS rows_updated = ROW_COUNT;
RETURN rows_updated > 0;
END;
$$ LANGUAGE plpgsql;
-- Function to release the migration lock
CREATE OR REPLACE FUNCTION migrations.release_lock() RETURNS VOID AS $$ BEGIN
UPDATE migrations.migrations_lock
SET is_locked = FALSE,
  locked_at = NULL,
  locked_by = NULL
WHERE id = 1;
END;
$$ LANGUAGE plpgsql;
-- Function to check if migrations are locked
CREATE OR REPLACE FUNCTION migrations.is_locked() RETURNS BOOLEAN AS $$
DECLARE locked BOOLEAN;
BEGIN
SELECT is_locked INTO locked
FROM migrations.migrations_lock
WHERE id = 1;
RETURN COALESCE(locked, FALSE);
END;
$$ LANGUAGE plpgsql;
-- Function to get the current lock status
CREATE OR REPLACE FUNCTION migrations.get_lock_status() RETURNS TABLE (
    is_locked BOOLEAN,
    locked_at TIMESTAMPTZ,
    locked_by TEXT
  ) AS $$ BEGIN RETURN QUERY
SELECT ml.is_locked,
  ml.locked_at,
  ml.locked_by
FROM migrations.migrations_lock ml
WHERE ml.id = 1;
END;
$$ LANGUAGE plpgsql;
-- Function to get the next batch number
CREATE OR REPLACE FUNCTION migrations.get_next_batch() RETURNS INTEGER AS $$
DECLARE next_batch INTEGER;
BEGIN
SELECT COALESCE(MAX(batch), 0) + 1 INTO next_batch
FROM migrations.migrations;
RETURN next_batch;
END;
$$ LANGUAGE plpgsql;
-- Function to get the current batch number (0 if no migrations)
CREATE OR REPLACE FUNCTION migrations.get_current_batch() RETURNS INTEGER AS $$
DECLARE current_batch INTEGER;
BEGIN
SELECT COALESCE(MAX(batch), 0) INTO current_batch
FROM migrations.migrations;
RETURN current_batch;
END;
$$ LANGUAGE plpgsql;
-- Function to record a migration as applied
CREATE OR REPLACE FUNCTION migrations.record_migration(
    migration_name TEXT,
    batch_number INTEGER DEFAULT NULL
  ) RETURNS TABLE (
    id INTEGER,
    name TEXT,
    batch INTEGER,
    migration_time TIMESTAMPTZ
  ) AS $$
DECLARE actual_batch INTEGER;
BEGIN -- Use provided batch number or get next
IF batch_number IS NULL THEN actual_batch := migrations.get_next_batch();
ELSE actual_batch := batch_number;
END IF;
RETURN QUERY
INSERT INTO migrations.migrations (name, batch)
VALUES (migration_name, actual_batch)
RETURNING migrations.migrations.id,
  migrations.migrations.name,
  migrations.migrations.batch,
  migrations.migrations.migration_time;
END;
$$ LANGUAGE plpgsql;
-- Function to check if a specific migration has been applied
CREATE OR REPLACE FUNCTION migrations.has_migration(migration_name TEXT) RETURNS BOOLEAN AS $$ BEGIN RETURN EXISTS (
    SELECT 1
    FROM migrations.migrations
    WHERE name = migration_name
  );
END;
$$ LANGUAGE plpgsql;
-- Function to get all applied migrations
CREATE OR REPLACE FUNCTION migrations.get_applied_migrations() RETURNS TABLE (
    id INTEGER,
    name TEXT,
    batch INTEGER,
    migration_time TIMESTAMPTZ
  ) AS $$ BEGIN RETURN QUERY
SELECT m.id,
  m.name,
  m.batch,
  m.migration_time
FROM migrations.migrations m
ORDER BY m.id ASC;
END;
$$ LANGUAGE plpgsql;
-- Function to get migrations by batch
CREATE OR REPLACE FUNCTION migrations.get_migrations_by_batch(batch_number INTEGER) RETURNS TABLE (
    id INTEGER,
    name TEXT,
    batch INTEGER,
    migration_time TIMESTAMPTZ
  ) AS $$ BEGIN RETURN QUERY
SELECT m.id,
  m.name,
  m.batch,
  m.migration_time
FROM migrations.migrations m
WHERE m.batch = batch_number
ORDER BY m.id ASC;
END;
$$ LANGUAGE plpgsql;
-- Function to get the latest batch migrations
CREATE OR REPLACE FUNCTION migrations.get_latest_batch_migrations() RETURNS TABLE (
    id INTEGER,
    name TEXT,
    batch INTEGER,
    migration_time TIMESTAMPTZ
  ) AS $$ BEGIN RETURN QUERY
SELECT m.id,
  m.name,
  m.batch,
  m.migration_time
FROM migrations.migrations m
WHERE m.batch = migrations.get_current_batch()
ORDER BY m.id ASC;
END;
$$ LANGUAGE plpgsql;
-- Function to get pending migrations given a list of migration names
-- Returns names that are NOT in the migrations table
CREATE OR REPLACE FUNCTION migrations.get_pending_migrations(migration_names TEXT []) RETURNS TABLE (name TEXT) AS $$ BEGIN RETURN QUERY
SELECT unnest.name
FROM UNNEST(migration_names) AS unnest(name)
WHERE NOT EXISTS (
    SELECT 1
    FROM migrations.migrations m
    WHERE m.name = unnest.name
  )
ORDER BY unnest.name ASC;
END;
$$ LANGUAGE plpgsql;
-- Function to get migration statistics
CREATE OR REPLACE FUNCTION migrations.get_stats() RETURNS TABLE (
    total_migrations INTEGER,
    total_batches INTEGER,
    last_migration_name TEXT,
    last_migration_time TIMESTAMPTZ,
    last_batch INTEGER
  ) AS $$ BEGIN RETURN QUERY
SELECT (
    SELECT COUNT(*)::INTEGER
    FROM migrations.migrations
  ),
  (
    SELECT COUNT(DISTINCT batch)::INTEGER
    FROM migrations.migrations
  ),
  (
    SELECT m.name
    FROM migrations.migrations m
    ORDER BY m.id DESC
    LIMIT 1
  ), (
    SELECT m.migration_time
    FROM migrations.migrations m
    ORDER BY m.id DESC
    LIMIT 1
  ), (
    SELECT COALESCE(MAX(batch), 0)
    FROM migrations.migrations
  );
END;
$$ LANGUAGE plpgsql;
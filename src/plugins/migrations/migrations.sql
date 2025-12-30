-- Migrations Plugin Schema
-- A simple migration system inspired by Knex, but forward-only (no down migrations)
-- Create the migrations schema
create schema if not exists migrations;

-- Migrations table to track applied migrations
create table if not exists migrations.migrations (
  id serial primary key,
  name text not null unique,
  batch integer not null,
  migration_time timestamptz not null default now()
);

-- Index for faster lookups by name and batch
create index if not exists idx_migrations_name on migrations.migrations (name);

create index if not exists idx_migrations_batch on migrations.migrations (batch);

-- Lock table for preventing concurrent migrations
create table if not exists migrations.migrations_lock (
  id integer primary key default 1,
  is_locked boolean not null default false,
  locked_at timestamptz,
  locked_by text,
  constraint single_row check (id = 1)
);

-- Insert the single lock row (idempotent)
insert into
  migrations.migrations_lock (id, is_locked)
values
  (1, false)
on conflict (id) do nothing;

-- Function to acquire the migration lock
-- Returns TRUE if lock was acquired, FALSE otherwise
create or replace function migrations.acquire_lock (locker_name text default 'unknown') returns boolean as $$
declare rows_updated integer;
begin
update migrations.migrations_lock
set is_locked = true,
  locked_at = now(),
  locked_by = locker_name
where id = 1
  and is_locked = false;
get diagnostics rows_updated = row_count;
if rows_updated > 0 then return true;
end if;
-- Check if lock is stale (older than 30 minutes)
update migrations.migrations_lock
set is_locked = true,
  locked_at = now(),
  locked_by = locker_name
where id = 1
  and is_locked = true
  and locked_at < now() - interval '30 minutes';
get diagnostics rows_updated = row_count;
return rows_updated > 0;
end;
$$ language plpgsql;

-- Function to release the migration lock
create or replace function migrations.release_lock () returns void as $$ begin
update migrations.migrations_lock
set is_locked = false,
  locked_at = null,
  locked_by = null
where id = 1;
end;
$$ language plpgsql;

-- Function to check if migrations are locked
create or replace function migrations.is_locked () returns boolean as $$
declare locked boolean;
begin
select is_locked into locked
from migrations.migrations_lock
where id = 1;
return coalesce(locked, false);
end;
$$ language plpgsql;

-- Function to get the current lock status
create or replace function migrations.get_lock_status () returns table (
  is_locked boolean,
  locked_at timestamptz,
  locked_by text
) as $$ begin return query
select ml.is_locked,
  ml.locked_at,
  ml.locked_by
from migrations.migrations_lock ml
where ml.id = 1;
end;
$$ language plpgsql;

-- Function to get the next batch number
create or replace function migrations.get_next_batch () returns integer as $$
declare next_batch integer;
begin
select coalesce(max(batch), 0) + 1 into next_batch
from migrations.migrations;
return next_batch;
end;
$$ language plpgsql;

-- Function to get the current batch number (0 if no migrations)
create or replace function migrations.get_current_batch () returns integer as $$
declare current_batch integer;
begin
select coalesce(max(batch), 0) into current_batch
from migrations.migrations;
return current_batch;
end;
$$ language plpgsql;

-- Function to record a migration as applied
create or replace function migrations.record_migration (
  migration_name text,
  batch_number integer default null
) returns table (
  id integer,
  name text,
  batch integer,
  migration_time timestamptz
) as $$
declare actual_batch integer;
begin -- use provided batch number or get next
if batch_number is null then actual_batch := migrations.get_next_batch();
else actual_batch := batch_number;
end if;
return query
insert into migrations.migrations (name, batch)
values (migration_name, actual_batch)
returning migrations.migrations.id,
  migrations.migrations.name,
  migrations.migrations.batch,
  migrations.migrations.migration_time;
end;
$$ language plpgsql;

-- Function to check if a specific migration has been applied
create or replace function migrations.has_migration (migration_name text) returns boolean as $$ begin return exists (
    select 1
    from migrations.migrations
    where name = migration_name
  );
end;
$$ language plpgsql;

-- Function to get all applied migrations
create or replace function migrations.get_applied_migrations () returns table (
  id integer,
  name text,
  batch integer,
  migration_time timestamptz
) as $$ begin return query
select m.id,
  m.name,
  m.batch,
  m.migration_time
from migrations.migrations m
order by m.id asc;
end;
$$ language plpgsql;

-- Function to get migrations by batch
create or replace function migrations.get_migrations_by_batch (batch_number integer) returns table (
  id integer,
  name text,
  batch integer,
  migration_time timestamptz
) as $$ begin return query
select m.id,
  m.name,
  m.batch,
  m.migration_time
from migrations.migrations m
where m.batch = batch_number
order by m.id asc;
end;
$$ language plpgsql;

-- Function to get the latest batch migrations
create or replace function migrations.get_latest_batch_migrations () returns table (
  id integer,
  name text,
  batch integer,
  migration_time timestamptz
) as $$ begin return query
select m.id,
  m.name,
  m.batch,
  m.migration_time
from migrations.migrations m
where m.batch = migrations.get_current_batch()
order by m.id asc;
end;
$$ language plpgsql;

-- Function to get pending migrations given a list of migration names
-- Returns names that are NOT in the migrations table
create or replace function migrations.get_pending_migrations (migration_names text[]) returns table (name text) as $$ begin return query
select unnest.name
from unnest(migration_names) as unnest(name)
where not exists (
    select 1
    from migrations.migrations m
    where m.name = unnest.name
  )
order by unnest.name asc;
end;
$$ language plpgsql;

-- Function to get migration statistics
create or replace function migrations.get_stats () returns table (
  total_migrations integer,
  total_batches integer,
  last_migration_name text,
  last_migration_time timestamptz,
  last_batch integer
) as $$ begin return query
select (
    select count(*)::integer
    from migrations.migrations
  ),
  (
    select count(distinct batch)::integer
    from migrations.migrations
  ),
  (
    select m.name
    from migrations.migrations m
    order by m.id desc
    limit 1
  ), (
    select m.migration_time
    from migrations.migrations m
    order by m.id desc
    limit 1
  ), (
    select coalesce(max(batch), 0)
    from migrations.migrations
  );
end;
$$ language plpgsql;

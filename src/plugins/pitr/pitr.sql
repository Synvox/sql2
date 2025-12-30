-- Point-in-Time Restore (PITR) Plugin Schema
-- ===========================================
--
-- ## Overview
--
-- This plugin enables selective point-in-time restore for individual tables.
-- Instead of restoring an entire database from a backup, you can revert
-- specific tables or rows to any previous state.
--
-- ## How it works
--
-- 1. **Tracking**: Install triggers on tables you want to track. These triggers
--    capture every INSERT, UPDATE, and DELETE operation.
--
-- 2. **Audit Log**: Changes are stored in `pitr.audit_log` with:
--    - Full row data (before and after)
--    - Operation type
--    - Precise timestamps
--    - Transaction IDs for grouping related changes
--
-- 3. **Restore**: Query historical data or restore tables/rows to any point.
--
-- ========================================
-- SCHEMA
-- ========================================
create schema if not exists pitr;

-- ========================================
-- TABLE DEFINITIONS
-- ========================================
create table if not exists pitr.tracked_tables (
  id serial primary key,
  schema_name text not null,
  table_name text not null,
  primary_key_columns text[] not null,
  tracked_columns text[],
  excluded_columns text[],
  trigger_name text not null,
  function_name text not null,
  enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (schema_name, table_name)
);

create table if not exists pitr.audit_log (
  id uuid primary key default gen_random_uuid(),
  tracked_table_id integer not null references pitr.tracked_tables (id) on delete cascade,
  operation text not null check (operation in ('INSERT', 'UPDATE', 'DELETE')),
  primary_key_value jsonb not null,
  old_data jsonb,
  new_data jsonb,
  changed_columns text[],
  transaction_id bigint not null default txid_current(),
  changed_at timestamptz not null default clock_timestamp(),
  changed_by text not null default current_user,
  application_name text default current_setting('application_name', true)
);

create index if not exists idx_audit_log_table_time on pitr.audit_log (tracked_table_id, changed_at desc);

create index if not exists idx_audit_log_pk_time on pitr.audit_log (
  tracked_table_id,
  primary_key_value,
  changed_at desc
);

create index if not exists idx_audit_log_transaction on pitr.audit_log (transaction_id);

create index if not exists idx_audit_log_changed_at on pitr.audit_log (changed_at desc);

-- ========================================
-- HELPER FUNCTIONS
-- ========================================
create or replace function pitr._extract_pk_value (p_row_data jsonb, p_pk_columns text[]) returns jsonb as $pitr_extract$
declare v_result jsonb := '{}';
v_col text;
begin foreach v_col in array p_pk_columns loop v_result := v_result || jsonb_build_object(v_col, p_row_data->v_col);
end loop;
return v_result;
end;
$pitr_extract$ language plpgsql immutable;

create or replace function pitr._filter_columns (
  p_row_data jsonb,
  p_tracked_columns text[],
  p_excluded_columns text[]
) returns jsonb as $pitr_filter$
declare v_result jsonb;
v_key text;
begin if p_tracked_columns is not null
and array_length(p_tracked_columns, 1) > 0 then v_result := '{}';
foreach v_key in array p_tracked_columns loop if p_row_data ? v_key then v_result := v_result || jsonb_build_object(v_key, p_row_data->v_key);
end if;
end loop;
return v_result;
end if;
v_result := p_row_data;
if p_excluded_columns is not null
and array_length(p_excluded_columns, 1) > 0 then foreach v_key in array p_excluded_columns loop v_result := v_result - v_key;
end loop;
end if;
return v_result;
end;
$pitr_filter$ language plpgsql immutable;

create or replace function pitr._get_changed_columns (p_old_data jsonb, p_new_data jsonb) returns text[] as $pitr_changed$
declare v_result text [] := array []::text [];
v_key text;
v_all_keys text [];
begin
select array(
    select distinct jsonb_object_keys
    from (
        select jsonb_object_keys(coalesce(p_old_data, '{}'))
        union
        select jsonb_object_keys(coalesce(p_new_data, '{}'))
      ) keys
    order by jsonb_object_keys
  ) into v_all_keys;
foreach v_key in array v_all_keys loop if not (
  coalesce(p_old_data->v_key, 'null'::jsonb) = coalesce(p_new_data->v_key, 'null'::jsonb)
) then v_result := array_append(v_result, v_key);
end if;
end loop;
return v_result;
end;
$pitr_changed$ language plpgsql immutable;

create or replace function pitr._get_table_id (p_schema_name text, p_table_name text) returns integer as $pitr_get_id$
declare v_table_id integer;
begin
select id into v_table_id
from pitr.tracked_tables
where schema_name = p_schema_name
  and table_name = p_table_name;
if v_table_id is null then raise exception 'Table %.% is not tracked by pitr',
p_schema_name,
p_table_name;
end if;
return v_table_id;
end;
$pitr_get_id$ language plpgsql stable;

-- Generic audit trigger function that looks up config from tracked_tables
create or replace function pitr._generic_audit_trigger () returns trigger as $pitr_audit$
declare v_table_config record;
v_old_data jsonb;
v_new_data jsonb;
v_pk_value jsonb;
v_changed_columns text [];
begin -- look up the table config
select * into v_table_config
from pitr.tracked_tables
where schema_name = TG_TABLE_SCHEMA
  and table_name = TG_TABLE_NAME
  and enabled = true;
if v_table_config is null then -- table not tracked or disabled, just pass through
if TG_OP = 'DELETE' then return old;
else return new;
end if;
end if;
-- Convert row data to JSONB
if TG_OP = 'DELETE' then v_old_data := to_jsonb(old);
v_new_data := null;
v_pk_value := pitr._extract_pk_value(v_old_data, v_table_config.primary_key_columns);
elsif TG_OP = 'INSERT' then v_old_data := null;
v_new_data := to_jsonb(new);
v_pk_value := pitr._extract_pk_value(v_new_data, v_table_config.primary_key_columns);
else -- update
v_old_data := to_jsonb(old);
v_new_data := to_jsonb(new);
v_pk_value := pitr._extract_pk_value(v_new_data, v_table_config.primary_key_columns);
-- Skip if no actual changes
if v_old_data = v_new_data then return new;
end if;
end if;
-- Filter columns if needed
if v_old_data is not null then v_old_data := pitr._filter_columns(
  v_old_data,
  v_table_config.tracked_columns,
  v_table_config.excluded_columns
);
end if;
if v_new_data is not null then v_new_data := pitr._filter_columns(
  v_new_data,
  v_table_config.tracked_columns,
  v_table_config.excluded_columns
);
end if;
-- Get changed columns for UPDATE
if TG_OP = 'UPDATE' then v_changed_columns := pitr._get_changed_columns(v_old_data, v_new_data);
-- Skip if only excluded columns changed
if array_length(v_changed_columns, 1) is null
or array_length(v_changed_columns, 1) = 0 then return new;
end if;
end if;
-- Insert audit record
insert into pitr.audit_log (
    tracked_table_id,
    operation,
    primary_key_value,
    old_data,
    new_data,
    changed_columns
  )
values (
    v_table_config.id,
    TG_OP,
    v_pk_value,
    v_old_data,
    v_new_data,
    v_changed_columns
  );
if TG_OP = 'DELETE' then return old;
else return new;
end if;
end;
$pitr_audit$ language plpgsql;

-- ========================================
-- TABLE MANAGEMENT FUNCTIONS
-- ========================================
create or replace function pitr.enable_tracking (
  p_schema_name text,
  p_table_name text,
  p_pk_columns text[],
  p_tracked_columns text[] default null,
  p_excluded_columns text[] default null
) returns table (tracked_table_id integer, message text) as $pitr_enable$
declare v_table_id integer;
v_trigger_name text;
v_func_name text;
v_existing_id integer;
v_table_exists boolean;
begin -- verify table exists
select exists (
    select 1
    from information_schema.tables t
    where t.table_schema = p_schema_name
      and t.table_name = p_table_name
  ) into v_table_exists;
if not v_table_exists then raise exception 'Table %.% does not exist',
p_schema_name,
p_table_name;
end if;
-- Check if already tracked
select id into v_existing_id
from pitr.tracked_tables tt
where tt.schema_name = p_schema_name
  and tt.table_name = p_table_name;
if v_existing_id is not null then -- re-enable if disabled
update pitr.tracked_tables tt
set enabled = true,
  primary_key_columns = p_pk_columns,
  tracked_columns = p_tracked_columns,
  excluded_columns = p_excluded_columns,
  updated_at = now()
where tt.id = v_existing_id;
return query
select v_existing_id,
  'tracking re-enabled and updated'::text;
return;
end if;
-- Generate unique names
v_trigger_name := format('pitr_audit_%s_%s', p_schema_name, p_table_name);
v_func_name := 'pitr._generic_audit_trigger';
-- Insert tracking record
insert into pitr.tracked_tables (
    schema_name,
    table_name,
    primary_key_columns,
    tracked_columns,
    excluded_columns,
    trigger_name,
    function_name
  )
values (
    p_schema_name,
    p_table_name,
    p_pk_columns,
    p_tracked_columns,
    p_excluded_columns,
    v_trigger_name,
    v_func_name
  )
returning id into v_table_id;
-- Create the trigger
execute format(
  'create trigger %I after insert or update or delete on %I.%I for each row execute function pitr._generic_audit_trigger()',
  v_trigger_name,
  p_schema_name,
  p_table_name
);
return query
select v_table_id,
  'Tracking enabled successfully'::text;
end;
$pitr_enable$ language plpgsql;

create or replace function pitr.disable_tracking (
  p_schema_name text,
  p_table_name text,
  p_keep_history boolean default true
) returns table (success boolean, message text) as $pitr_disable$
declare v_table_id integer;
v_trigger_name text;
begin -- get tracking info
select id,
  trigger_name into v_table_id,
  v_trigger_name
from pitr.tracked_tables
where schema_name = p_schema_name
  and table_name = p_table_name;
if v_table_id is null then return query
select false,
  format(
    'table %.% is not tracked',
    p_schema_name,
    p_table_name
  )::text;
return;
end if;
-- Drop the trigger
execute format(
  'drop trigger if exists %I on %I.%I',
  v_trigger_name,
  p_schema_name,
  p_table_name
);
if p_keep_history then -- mark as disabled but keep the record and history
update pitr.tracked_tables
set enabled = false,
  updated_at = now()
where id = v_table_id;
return query
select true,
  'Tracking disabled, history preserved'::text;
else -- delete everything including history
delete from pitr.tracked_tables
where id = v_table_id;
return query
select true,
  'Tracking disabled, history deleted'::text;
end if;
end;
$pitr_disable$ language plpgsql;

create or replace function pitr.get_tracked_tables () returns table (
  id integer,
  schema_name text,
  table_name text,
  primary_key_columns text[],
  tracked_columns text[],
  excluded_columns text[],
  enabled boolean,
  created_at timestamptz,
  audit_count bigint
) as $pitr_list$ begin return query
select tt.id,
  tt.schema_name,
  tt.table_name,
  tt.primary_key_columns,
  tt.tracked_columns,
  tt.excluded_columns,
  tt.enabled,
  tt.created_at,
  count(al.id)::bigint as audit_count
from pitr.tracked_tables tt
  left join pitr.audit_log al on al.tracked_table_id = tt.id
group by tt.id
order by tt.schema_name,
  tt.table_name;
end;
$pitr_list$ language plpgsql stable;

-- ========================================
-- HISTORY & QUERY FUNCTIONS
-- ========================================
create or replace function pitr.get_row_history (
  p_schema_name text,
  p_table_name text,
  p_pk_value jsonb,
  p_limit integer default 100
) returns table (
  id uuid,
  operation text,
  old_data jsonb,
  new_data jsonb,
  changed_columns text[],
  changed_at timestamptz,
  changed_by text,
  transaction_id bigint
) as $pitr_row_hist$
declare v_table_id integer;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
return query
select al.id,
  al.operation,
  al.old_data,
  al.new_data,
  al.changed_columns,
  al.changed_at,
  al.changed_by,
  al.transaction_id
from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and al.primary_key_value @> p_pk_value
  and p_pk_value @> al.primary_key_value
order by al.changed_at desc
limit p_limit;
end;
$pitr_row_hist$ language plpgsql stable;

create or replace function pitr.get_table_history (
  p_schema_name text,
  p_table_name text,
  p_since timestamptz default null,
  p_until timestamptz default null,
  p_limit integer default 1000
) returns table (
  id uuid,
  operation text,
  primary_key_value jsonb,
  old_data jsonb,
  new_data jsonb,
  changed_columns text[],
  changed_at timestamptz,
  changed_by text,
  transaction_id bigint
) as $pitr_table_hist$
declare v_table_id integer;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
return query
select al.id,
  al.operation,
  al.primary_key_value,
  al.old_data,
  al.new_data,
  al.changed_columns,
  al.changed_at,
  al.changed_by,
  al.transaction_id
from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and (
    p_since is null
    or al.changed_at >= p_since
  )
  and (
    p_until is null
    or al.changed_at <= p_until
  )
order by al.changed_at desc
limit p_limit;
end;
$pitr_table_hist$ language plpgsql stable;

create or replace function pitr.get_row_at (
  p_schema_name text,
  p_table_name text,
  p_pk_value jsonb,
  p_as_of timestamptz
) returns jsonb as $pitr_row_at$
declare v_table_id integer;
v_result jsonb;
v_operation text;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
select al.operation,
  case
    when al.operation = 'DELETE' then null
    else coalesce(al.new_data, al.old_data)
  end into v_operation,
  v_result
from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and al.primary_key_value @> p_pk_value
  and p_pk_value @> al.primary_key_value
  and al.changed_at <= p_as_of
order by al.changed_at desc
limit 1;
if v_operation = 'DELETE' then return null;
end if;
return v_result;
end;
$pitr_row_at$ language plpgsql stable;

create or replace function pitr.get_table_at (
  p_schema_name text,
  p_table_name text,
  p_as_of timestamptz
) returns table (primary_key_value jsonb, row_data jsonb) as $pitr_table_at$
declare v_table_id integer;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
return query with latest_states as (
  select distinct on (al.primary_key_value) al.primary_key_value,
    al.operation,
    al.new_data,
    al.old_data
  from pitr.audit_log al
  where al.tracked_table_id = v_table_id
    and al.changed_at <= p_as_of
  order by al.primary_key_value,
    al.changed_at desc
)
select ls.primary_key_value,
  case
    when ls.operation = 'DELETE' then null
    else coalesce(ls.new_data, ls.old_data)
  end as row_data
from latest_states ls
where ls.operation != 'DELETE';
end;
$pitr_table_at$ language plpgsql stable;

-- ========================================
-- RESTORE FUNCTIONS
-- ========================================
create or replace function pitr.restore_row (
  p_schema_name text,
  p_table_name text,
  p_pk_value jsonb,
  p_as_of timestamptz
) returns table (success boolean, operation text, message text) as $pitr_restore_row$
declare v_table_id integer;
v_pk_columns text [];
v_historical_data jsonb;
v_current_data jsonb;
v_sql text;
v_where_clause text;
v_set_clause text;
v_insert_cols text;
v_insert_vals text;
v_key text;
v_keys text [];
begin -- get table info
select tt.id,
  tt.primary_key_columns into v_table_id,
  v_pk_columns
from pitr.tracked_tables tt
where tt.schema_name = p_schema_name
  and tt.table_name = p_table_name;
if v_table_id is null then return query
select false,
  'ERROR'::text,
  format(
    'Table %.% is not tracked',
    p_schema_name,
    p_table_name
  )::text;
return;
end if;
-- Get historical state
v_historical_data := pitr.get_row_at(p_schema_name, p_table_name, p_pk_value, p_as_of);
-- Build WHERE clause for PK
v_where_clause := '';
foreach v_key in array v_pk_columns loop if v_where_clause != '' then v_where_clause := v_where_clause || ' and ';
end if;
v_where_clause := v_where_clause || format('%I = %L', v_key, p_pk_value->>v_key);
end loop;
-- Check if row currently exists
execute format(
  'select to_jsonb(t) from %I.%I t where %s',
  p_schema_name,
  p_table_name,
  v_where_clause
) into v_current_data;
-- Determine action
if v_historical_data is null
and v_current_data is null then return query
select true,
  'NO_CHANGE'::text,
  'Row did not exist at specified time and does not exist now'::text;
elsif v_historical_data is null
and v_current_data is not null then execute format(
  'delete from %I.%I where %s',
  p_schema_name,
  p_table_name,
  v_where_clause
);
return query
select true,
  'DELETE'::text,
  'Row deleted (did not exist at specified time)'::text;
elsif v_historical_data is not null
and v_current_data is null then
select array_agg(k) into v_keys
from jsonb_object_keys(v_historical_data) k;
v_insert_cols := '';
v_insert_vals := '';
foreach v_key in array v_keys loop if v_insert_cols != '' then v_insert_cols := v_insert_cols || ', ';
v_insert_vals := v_insert_vals || ', ';
end if;
v_insert_cols := v_insert_cols || format('%I', v_key);
v_insert_vals := v_insert_vals || format('%L', v_historical_data->>v_key);
end loop;
execute format(
  'insert into %I.%I (%s) values (%s)',
  p_schema_name,
  p_table_name,
  v_insert_cols,
  v_insert_vals
);
return query
select true,
  'INSERT'::text,
  'Row restored (was deleted after specified time)'::text;
else if v_historical_data = v_current_data then return query
select true,
  'NO_CHANGE'::text,
  'Row is already at the specified state'::text;
return;
end if;
select array_agg(k) into v_keys
from jsonb_object_keys(v_historical_data) k;
v_set_clause := '';
foreach v_key in array v_keys loop if v_key = any(v_pk_columns) then continue;
end if;
if v_set_clause != '' then v_set_clause := v_set_clause || ', ';
end if;
v_set_clause := v_set_clause || format('%I = %L', v_key, v_historical_data->>v_key);
end loop;
if v_set_clause = '' then return query
select true,
  'NO_CHANGE'::text,
  'Only primary key columns present, no update needed'::text;
return;
end if;
execute format(
  'update %I.%I set %s where %s',
  p_schema_name,
  p_table_name,
  v_set_clause,
  v_where_clause
);
return query
select true,
  'UPDATE'::text,
  'Row updated to historical state'::text;
end if;
end;
$pitr_restore_row$ language plpgsql;

create or replace function pitr.restore_table (
  p_schema_name text,
  p_table_name text,
  p_as_of timestamptz,
  p_dry_run boolean default false
) returns table (
  operation text,
  affected_rows integer,
  details text
) as $pitr_restore_table$
declare v_table_id integer;
v_pk_columns text [];
v_historical record;
v_inserts integer := 0;
v_updates integer := 0;
v_deletes integer := 0;
v_no_change integer := 0;
v_result record;
v_hist_data jsonb;
v_curr_data jsonb;
v_where text;
v_key text;
begin
select tt.id,
  tt.primary_key_columns into v_table_id,
  v_pk_columns
from pitr.tracked_tables tt
where tt.schema_name = p_schema_name
  and tt.table_name = p_table_name;
if v_table_id is null then return query
select 'ERROR'::text,
  0,
  format(
    'Table %.% is not tracked',
    p_schema_name,
    p_table_name
  )::text;
return;
end if;
for v_historical in
select distinct al.primary_key_value
from pitr.audit_log al
where al.tracked_table_id = v_table_id loop if not p_dry_run then
select * into v_result
from pitr.restore_row(
    p_schema_name,
    p_table_name,
    v_historical.primary_key_value,
    p_as_of
  );
case
  v_result.operation
  when 'INSERT' then v_inserts := v_inserts + 1;
when 'UPDATE' then v_updates := v_updates + 1;
when 'DELETE' then v_deletes := v_deletes + 1;
else v_no_change := v_no_change + 1;
end case
;
else v_hist_data := pitr.get_row_at(
  p_schema_name,
  p_table_name,
  v_historical.primary_key_value,
  p_as_of
);
v_where := '';
foreach v_key in array v_pk_columns loop if v_where != '' then v_where := v_where || ' and ';
end if;
v_where := v_where || format(
  '%I = %L',
  v_key,
  v_historical.primary_key_value->>v_key
);
end loop;
execute format(
  'select to_jsonb(t) from %I.%I t where %s',
  p_schema_name,
  p_table_name,
  v_where
) into v_curr_data;
if v_hist_data is null
and v_curr_data is null then v_no_change := v_no_change + 1;
elsif v_hist_data is null
and v_curr_data is not null then v_deletes := v_deletes + 1;
elsif v_hist_data is not null
and v_curr_data is null then v_inserts := v_inserts + 1;
elsif v_hist_data = v_curr_data then v_no_change := v_no_change + 1;
else v_updates := v_updates + 1;
end if;
end if;
end loop;
if p_dry_run then return query
select 'DRY_RUN'::text,
  0,
  'No changes made (preview mode)'::text;
end if;
if v_inserts > 0 then return query
select 'INSERT'::text,
  v_inserts,
  format('%s rows restored', v_inserts)::text;
end if;
if v_updates > 0 then return query
select 'UPDATE'::text,
  v_updates,
  format('%s rows updated', v_updates)::text;
end if;
if v_deletes > 0 then return query
select 'DELETE'::text,
  v_deletes,
  format('%s rows deleted', v_deletes)::text;
end if;
if v_inserts = 0
and v_updates = 0
and v_deletes = 0 then return query
select 'NO_CHANGE'::text,
  v_no_change,
  'Table is already at the specified state'::text;
end if;
end;
$pitr_restore_table$ language plpgsql;

-- Restore rows matching a filter to their state at a given point in time
-- p_filter is a JSONB object that will be matched against the row data
-- e.g., {"user_id": 1} will restore all rows where user_id = 1
create or replace function pitr.restore_rows_where (
  p_schema_name text,
  p_table_name text,
  p_filter jsonb,
  p_as_of timestamptz,
  p_dry_run boolean default false
) returns table (
  operation text,
  affected_rows integer,
  details text
) as $pitr_restore_where$
declare v_table_id integer;
v_pk_columns text [];
v_row record;
v_result record;
v_inserts integer := 0;
v_updates integer := 0;
v_deletes integer := 0;
v_no_change integer := 0;
v_matched integer := 0;
begin
select tt.id,
  tt.primary_key_columns into v_table_id,
  v_pk_columns
from pitr.tracked_tables tt
where tt.schema_name = p_schema_name
  and tt.table_name = p_table_name;
if v_table_id is null then return query
select 'error'::text,
  0,
  format(
    'table %.% is not tracked',
    p_schema_name,
    p_table_name
  )::text;
return;
end if;
-- Find all distinct rows that have ever existed and match the filter
for v_row in
select distinct al.primary_key_value
from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and (
    -- Match filter against old_data or new_data
    (
      al.old_data is not null
      and al.old_data @> p_filter
    )
    or (
      al.new_data is not null
      and al.new_data @> p_filter
    )
  ) loop v_matched := v_matched + 1;
if not p_dry_run then
select * into v_result
from pitr.restore_row(
    p_schema_name,
    p_table_name,
    v_row.primary_key_value,
    p_as_of
  );
case
  v_result.operation
  when 'INSERT' then v_inserts := v_inserts + 1;
when 'UPDATE' then v_updates := v_updates + 1;
when 'DELETE' then v_deletes := v_deletes + 1;
else v_no_change := v_no_change + 1;
end case
;
else -- dry run: just count what would happen
declare v_hist_data jsonb;
v_curr_data jsonb;
v_where text := '';
v_key text;
begin foreach v_key in array v_pk_columns loop if v_where != '' then v_where := v_where || ' and ';
end if;
v_where := v_where || format(
  '%I = %L',
  v_key,
  v_row.primary_key_value->>v_key
);
end loop;
v_hist_data := pitr.get_row_at(
  p_schema_name,
  p_table_name,
  v_row.primary_key_value,
  p_as_of
);
execute format(
  'select to_jsonb(t) from %I.%I t where %s',
  p_schema_name,
  p_table_name,
  v_where
) into v_curr_data;
if v_hist_data is null
and v_curr_data is null then v_no_change := v_no_change + 1;
elsif v_hist_data is null
and v_curr_data is not null then v_deletes := v_deletes + 1;
elsif v_hist_data is not null
and v_curr_data is null then v_inserts := v_inserts + 1;
elsif v_hist_data = v_curr_data then v_no_change := v_no_change + 1;
else v_updates := v_updates + 1;
end if;
end;
end if;
end loop;
if p_dry_run then return query
select 'DRY_RUN'::text,
  v_matched,
  format('%s rows matched filter', v_matched)::text;
end if;
if v_inserts > 0 then return query
select 'INSERT'::text,
  v_inserts,
  format('%s rows restored', v_inserts)::text;
end if;
if v_updates > 0 then return query
select 'UPDATE'::text,
  v_updates,
  format('%s rows updated', v_updates)::text;
end if;
if v_deletes > 0 then return query
select 'DELETE'::text,
  v_deletes,
  format('%s rows deleted', v_deletes)::text;
end if;
if v_inserts = 0
and v_updates = 0
and v_deletes = 0 then return query
select 'NO_CHANGE'::text,
  v_no_change,
  format('%s rows already at target state', v_no_change)::text;
end if;
end;
$pitr_restore_where$ language plpgsql;

create or replace function pitr.undo_last_change (
  p_schema_name text,
  p_table_name text,
  p_pk_value jsonb
) returns table (success boolean, operation text, message text) as $pitr_undo$
declare v_table_id integer;
v_last_change record;
v_restore_time timestamptz;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
select * into v_last_change
from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and al.primary_key_value @> p_pk_value
  and p_pk_value @> al.primary_key_value
order by al.changed_at desc
limit 1;
if v_last_change is null then return query
select false,
  'ERROR'::text,
  'No changes found for this row'::text;
return;
end if;
v_restore_time := v_last_change.changed_at - interval '1 microsecond';
return query
select *
from pitr.restore_row(
    p_schema_name,
    p_table_name,
    p_pk_value,
    v_restore_time
  );
end;
$pitr_undo$ language plpgsql;

-- ========================================
-- MAINTENANCE FUNCTIONS
-- ========================================
create or replace function pitr.prune_history (
  p_older_than timestamptz,
  p_schema_name text default null,
  p_table_name text default null
) returns table (deleted_count bigint, message text) as $pitr_prune$
declare v_table_id integer;
v_deleted bigint;
begin if p_table_name is not null
and p_schema_name is null then raise exception 'Schema_name is required when table_name is specified';
end if;
if p_schema_name is not null
and p_table_name is not null then v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
delete from pitr.audit_log al
where al.tracked_table_id = v_table_id
  and al.changed_at < p_older_than;
get diagnostics v_deleted = row_count;
return query
select v_deleted,
  format(
    'pruned %s entries from %s.%s',
    v_deleted,
    p_schema_name,
    p_table_name
  )::text;
elsif p_schema_name is not null then
delete from pitr.audit_log al using pitr.tracked_tables tt
where al.tracked_table_id = tt.id
  and tt.schema_name = p_schema_name
  and al.changed_at < p_older_than;
get diagnostics v_deleted = row_count;
return query
select v_deleted,
  format(
    'pruned %s entries from schema %s',
    v_deleted,
    p_schema_name
  )::text;
else
delete from pitr.audit_log al
where al.changed_at < p_older_than;
get diagnostics v_deleted = row_count;
return query
select v_deleted,
  format(
    'pruned %s entries from all tracked tables',
    v_deleted
  )::text;
end if;
end;
$pitr_prune$ language plpgsql;

create or replace function pitr.get_stats () returns table (
  total_tracked_tables integer,
  active_tracked_tables integer,
  total_audit_entries bigint,
  oldest_entry timestamptz,
  newest_entry timestamptz,
  entries_last_24h bigint,
  entries_last_7d bigint
) as $pitr_stats$ begin return query
select (
    select count(*)::integer
    from pitr.tracked_tables
  ),
  (
    select count(*)::integer
    from pitr.tracked_tables
    where enabled
  ),
  (
    select count(*)::bigint
    from pitr.audit_log
  ),
  (
    select min(changed_at)
    from pitr.audit_log
  ),
  (
    select max(changed_at)
    from pitr.audit_log
  ),
  (
    select count(*)::bigint
    from pitr.audit_log
    where changed_at >= now() - interval '24 hours'
  ),
  (
    select count(*)::bigint
    from pitr.audit_log
    where changed_at >= now() - interval '7 days'
  );
end;
$pitr_stats$ language plpgsql stable;

create or replace function pitr.get_table_stats (p_schema_name text, p_table_name text) returns table (
  total_entries bigint,
  inserts bigint,
  updates bigint,
  deletes bigint,
  unique_rows_tracked bigint,
  oldest_entry timestamptz,
  newest_entry timestamptz,
  avg_changes_per_row numeric
) as $pitr_table_stats$
declare v_table_id integer;
begin v_table_id := pitr._get_table_id(p_schema_name, p_table_name);
return query
select count(*)::bigint as total_entries,
  count(*) filter (
    where al.operation = 'INSERT'
  )::bigint as inserts,
  count(*) filter (
    where al.operation = 'UPDATE'
  )::bigint as updates,
  count(*) filter (
    where al.operation = 'DELETE'
  )::bigint as deletes,
  count(distinct al.primary_key_value)::bigint as unique_rows_tracked,
  min(al.changed_at) as oldest_entry,
  max(al.changed_at) as newest_entry,
  round(
    count(*)::numeric / nullif(count(distinct al.primary_key_value), 0),
    2
  ) as avg_changes_per_row
from pitr.audit_log al
where al.tracked_table_id = v_table_id;
end;
$pitr_table_stats$ language plpgsql stable;

-- ========================================
-- TRANSACTION-BASED FUNCTIONS
-- ========================================
-- Get all changes that occurred in a specific transaction
create or replace function pitr.get_transaction_history (p_transaction_id bigint) returns table (
  id uuid,
  schema_name text,
  table_name text,
  operation text,
  primary_key_value jsonb,
  old_data jsonb,
  new_data jsonb,
  changed_columns text[],
  changed_at timestamptz,
  changed_by text
) as $pitr_tx_hist$ begin return query
select al.id,
  tt.schema_name,
  tt.table_name,
  al.operation,
  al.primary_key_value,
  al.old_data,
  al.new_data,
  al.changed_columns,
  al.changed_at,
  al.changed_by
from pitr.audit_log al
  join pitr.tracked_tables tt on al.tracked_table_id = tt.id
where al.transaction_id = p_transaction_id
order by al.changed_at asc;
end;
$pitr_tx_hist$ language plpgsql stable;

-- Get a summary of recent transactions with their affected tables
create or replace function pitr.get_recent_transactions (p_limit integer default 50) returns table (
  transaction_id bigint,
  changed_at timestamptz,
  changed_by text,
  tables_affected text[],
  total_changes integer,
  inserts integer,
  updates integer,
  deletes integer
) as $pitr_recent_tx$ begin return query
select al.transaction_id,
  min(al.changed_at) as changed_at,
  min(al.changed_by) as changed_by,
  array_agg(distinct tt.schema_name || '.' || tt.table_name) as tables_affected,
  count(*)::integer as total_changes,
  count(*) filter (
    where al.operation = 'INSERT'
  )::integer as inserts,
  count(*) filter (
    where al.operation = 'UPDATE'
  )::integer as updates,
  count(*) filter (
    where al.operation = 'DELETE'
  )::integer as deletes
from pitr.audit_log al
  join pitr.tracked_tables tt on al.tracked_table_id = tt.id
group by al.transaction_id
order by min(al.changed_at) desc
limit p_limit;
end;
$pitr_recent_tx$ language plpgsql stable;

-- Restore all tracked tables to their state just before the specified transaction
create or replace function pitr.restore_to_transaction (
  p_transaction_id bigint,
  p_dry_run boolean default false
) returns table (
  schema_name text,
  table_name text,
  operation text,
  affected_rows integer,
  details text
) as $pitr_restore_tx$
declare v_tx_time timestamptz;
v_table record;
v_restore_result record;
begin -- get the timestamp just before the transaction
select min(al.changed_at) - interval '1 microsecond' into v_tx_time
from pitr.audit_log al
where al.transaction_id = p_transaction_id;
if v_tx_time is null then return query
select null::text,
  null::text,
  'ERROR'::text,
  0,
  format(
    'transaction %s not found in audit log',
    p_transaction_id
  )::text;
return;
end if;
-- Get all tables that have changes at or after this transaction
for v_table in
select distinct tt.schema_name,
  tt.table_name
from pitr.audit_log al
  join pitr.tracked_tables tt on al.tracked_table_id = tt.id
where al.changed_at >= v_tx_time + interval '1 microsecond'
order by tt.schema_name,
  tt.table_name loop for v_restore_result in
select *
from pitr.restore_table(
    v_table.schema_name,
    v_table.table_name,
    v_tx_time,
    p_dry_run
  ) loop return query
select v_table.schema_name,
  v_table.table_name,
  v_restore_result.operation,
  v_restore_result.affected_rows,
  v_restore_result.details;
end loop;
end loop;
end;
$pitr_restore_tx$ language plpgsql;

-- Undo all changes from a specific transaction
create or replace function pitr.undo_transaction (
  p_transaction_id bigint,
  p_dry_run boolean default false
) returns table (
  schema_name text,
  table_name text,
  operation text,
  affected_rows integer,
  details text
) as $pitr_undo_tx$
declare v_change record;
v_pk_columns text [];
v_where_clause text;
v_key text;
v_historical_data jsonb;
v_current_data jsonb;
v_inserts integer := 0;
v_updates integer := 0;
v_deletes integer := 0;
v_no_change integer := 0;
v_current_table text := '';
v_current_schema text := '';
v_set_clause text;
v_insert_cols text;
v_insert_vals text;
v_keys text [];
begin -- process changes in reverse order (most recent first within the transaction)
for v_change in
select al.id,
  tt.schema_name,
  tt.table_name,
  tt.primary_key_columns,
  al.operation,
  al.primary_key_value,
  al.old_data,
  al.new_data,
  al.changed_columns
from pitr.audit_log al
  join pitr.tracked_tables tt on al.tracked_table_id = tt.id
where al.transaction_id = p_transaction_id
order by al.changed_at desc loop -- track when we switch tables for reporting
  if v_current_schema != v_change.schema_name
  or v_current_table != v_change.table_name then -- report previous table results
  if v_current_table != '' then if v_inserts > 0 then return query
select v_current_schema,
  v_current_table,
  'INSERT'::text,
  v_inserts,
  format('%s rows restored', v_inserts)::text;
end if;
if v_updates > 0 then return query
select v_current_schema,
  v_current_table,
  'UPDATE'::text,
  v_updates,
  format('%s rows updated', v_updates)::text;
end if;
if v_deletes > 0 then return query
select v_current_schema,
  v_current_table,
  'DELETE'::text,
  v_deletes,
  format('%s rows deleted', v_deletes)::text;
end if;
if v_inserts = 0
and v_updates = 0
and v_deletes = 0 then return query
select v_current_schema,
  v_current_table,
  'NO_CHANGE'::text,
  v_no_change,
  'No changes needed'::text;
end if;
end if;
v_current_schema := v_change.schema_name;
v_current_table := v_change.table_name;
v_inserts := 0;
v_updates := 0;
v_deletes := 0;
v_no_change := 0;
end if;
v_pk_columns := v_change.primary_key_columns;
-- Build WHERE clause for PK
v_where_clause := '';
foreach v_key in array v_pk_columns loop if v_where_clause != '' then v_where_clause := v_where_clause || ' and ';
end if;
v_where_clause := v_where_clause || format(
  '%I = %L',
  v_key,
  v_change.primary_key_value->>v_key
);
end loop;
-- Determine what to undo based on the original operation
if v_change.operation = 'INSERT' then -- undo insert = delete the row
if not p_dry_run then execute format(
  'delete from %I.%I where %s',
  v_change.schema_name,
  v_change.table_name,
  v_where_clause
);
end if;
v_deletes := v_deletes + 1;
elsif v_change.operation = 'DELETE' then -- undo delete = insert the old data
if not p_dry_run then
select array_agg(k) into v_keys
from jsonb_object_keys(v_change.old_data) k;
v_insert_cols := '';
v_insert_vals := '';
foreach v_key in array v_keys loop if v_insert_cols != '' then v_insert_cols := v_insert_cols || ', ';
v_insert_vals := v_insert_vals || ', ';
end if;
v_insert_cols := v_insert_cols || format('%I', v_key);
v_insert_vals := v_insert_vals || format('%L', v_change.old_data->>v_key);
end loop;
execute format(
  'insert into %I.%I (%s) values (%s)',
  v_change.schema_name,
  v_change.table_name,
  v_insert_cols,
  v_insert_vals
);
end if;
v_inserts := v_inserts + 1;
elsif v_change.operation = 'UPDATE' then -- undo update = restore only the columns that were changed
if not p_dry_run then -- only revert the columns that were actually changed in this transaction
-- This preserves any subsequent changes to other columns
v_set_clause := '';
if v_change.changed_columns is not null
and array_length(v_change.changed_columns, 1) > 0 then foreach v_key in array v_change.changed_columns loop if v_key = any(v_pk_columns) then continue;
end if;
if v_set_clause != '' then v_set_clause := v_set_clause || ', ';
end if;
v_set_clause := v_set_clause || format('%I = %L', v_key, v_change.old_data->>v_key);
end loop;
end if;
if v_set_clause != '' then execute format(
  'update %I.%I set %s where %s',
  v_change.schema_name,
  v_change.table_name,
  v_set_clause,
  v_where_clause
);
end if;
end if;
v_updates := v_updates + 1;
else v_no_change := v_no_change + 1;
end if;
end loop;
-- Report final table results
if v_current_table != '' then if p_dry_run then return query
select v_current_schema,
  v_current_table,
  'DRY_RUN'::text,
  0,
  'No changes made (preview mode)'::text;
end if;
if v_inserts > 0 then return query
select v_current_schema,
  v_current_table,
  'INSERT'::text,
  v_inserts,
  format('%s rows restored', v_inserts)::text;
end if;
if v_updates > 0 then return query
select v_current_schema,
  v_current_table,
  'UPDATE'::text,
  v_updates,
  format('%s rows updated', v_updates)::text;
end if;
if v_deletes > 0 then return query
select v_current_schema,
  v_current_table,
  'DELETE'::text,
  v_deletes,
  format('%s rows deleted', v_deletes)::text;
end if;
if v_inserts = 0
and v_updates = 0
and v_deletes = 0
and not p_dry_run then return query
select v_current_schema,
  v_current_table,
  'NO_CHANGE'::text,
  v_no_change,
  'No changes needed'::text;
end if;
end if;
end;
$pitr_undo_tx$ language plpgsql;

-- Restore specified tables to state before a specific transaction
-- p_tables should be a JSONB array of objects: [{"schema": "public", "table": "orders"}, ...]
create or replace function pitr.restore_tables_to_transaction (
  p_transaction_id bigint,
  p_tables jsonb,
  p_dry_run boolean default false
) returns table (
  schema_name text,
  table_name text,
  operation text,
  affected_rows integer,
  details text
) as $pitr_restore_tables_tx$
declare v_tx_time timestamptz;
v_table_entry record;
v_restore_result record;
begin -- get the timestamp just before the transaction
select min(al.changed_at) - interval '1 microsecond' into v_tx_time
from pitr.audit_log al
where al.transaction_id = p_transaction_id;
if v_tx_time is null then return query
select null::text,
  null::text,
  'ERROR'::text,
  0,
  format(
    'transaction %s not found in audit log',
    p_transaction_id
  )::text;
return;
end if;
-- Restore each specified table
for v_table_entry in
select elem->>'schema' as schema_name,
  elem->>'table' as table_name
from jsonb_array_elements(p_tables) as elem loop for v_restore_result in
select *
from pitr.restore_table(
    v_table_entry.schema_name,
    v_table_entry.table_name,
    v_tx_time,
    p_dry_run
  ) loop return query
select v_table_entry.schema_name,
  v_table_entry.table_name,
  v_restore_result.operation,
  v_restore_result.affected_rows,
  v_restore_result.details;
end loop;
end loop;
end;
$pitr_restore_tables_tx$ language plpgsql;

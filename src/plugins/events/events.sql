-- Events Plugin Schema
-- An event sourcing system for PostgreSQL with streams, subscriptions, and projections
-- Create the events schema
create schema if not exists events;
-- ========================================
-- Lookup Tables
-- ========================================
-- Categories define aggregate types (e.g., "order", "user", "inventory")
-- Convention: use kebab-case identifiers
create table if not exists events.categories (
  id text primary key -- e.g., "order", "user", "inventory"
);
-- Types define event types scoped to a category (e.g., "order/created", "order/item-added")
-- Convention: use kebab-case identifiers
create table if not exists events.types (
  id text not null,
  -- e.g., "created", "item-added", "shipped"
  category_id text not null references events.categories (id) on delete cascade,
  primary key (category_id, id)
);
-- ========================================
-- Core Tables
-- ========================================
-- Streams represent aggregates
create table if not exists events.streams (
  id uuid primary key default gen_random_uuid(),
  category_id text not null references events.categories (id),
  version bigint not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
-- Index for category lookups
create index if not exists idx_streams_category on events.streams (category_id);
-- Events table - the core append-only event log
create table if not exists events.events (
  position bigserial primary key,
  id uuid not null default gen_random_uuid() unique,
  stream_id uuid not null references events.streams (id) on delete cascade,
  stream_version bigint not null,
  -- Event type (scoped to category via stream)
  category_id text not null,
  type_id text not null,
  -- Event payload
  data jsonb not null default '{}',
  metadata jsonb not null default '{}',
  created_at timestamptz not null default now(),
  -- Constraints
  constraint unique_stream_version unique (stream_id, stream_version),
  constraint fk_events_type foreign key (category_id, type_id) references events.types (category_id, id)
);
-- Indexes
create index if not exists idx_events_stream on events.events (stream_id, stream_version);
create index if not exists idx_events_type on events.events (category_id, type_id);
create index if not exists idx_events_category on events.events (category_id);
create index if not exists idx_events_position on events.events (position);
create index if not exists idx_events_created_at on events.events (created_at);
-- Subscriptions track consumer positions in the event stream
create table if not exists events.subscriptions (
  name text primary key,
  -- Position tracking
  last_position bigint not null default 0,
  -- Last processed global position
  last_processed_at timestamptz,
  -- When last event was processed
  -- Consumer state
  active boolean not null default true,
  -- Whether subscription is active
  -- Timestamps
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
-- Subscription filter by event types
create table if not exists events.subscription_filter_types (
  subscription_name text not null references events.subscriptions (name) on delete cascade,
  category_id text not null,
  type_id text not null,
  primary key (subscription_name, category_id, type_id),
  foreign key (category_id, type_id) references events.types (category_id, id) on delete cascade
);
-- Subscription filter by streams
create table if not exists events.subscription_filter_streams (
  subscription_name text not null references events.subscriptions (name) on delete cascade,
  stream_id uuid not null references events.streams (id) on delete cascade,
  primary key (subscription_name, stream_id)
);
-- Event claims for concurrent polling with SKIP LOCKED
-- Tracks which events are currently being processed by which worker
create table if not exists events.event_claims (
  subscription_name text not null references events.subscriptions (name) on delete cascade,
  position bigint not null,
  claimed_at timestamptz not null default now(),
  expires_at timestamptz not null,
  primary key (subscription_name, position)
);
-- Index for claim expiration cleanup
create index if not exists idx_event_claims_expires on events.event_claims (expires_at);
-- Snapshots for projection state
create table if not exists events.snapshots (
  stream_id uuid not null references events.streams (id) on delete cascade,
  name text not null,
  -- Snapshot name (e.g., "aggregate-state")
  -- Snapshot data
  version bigint not null,
  -- Stream version at snapshot time
  state jsonb not null,
  -- Serialized state
  -- Timestamps
  created_at timestamptz not null default now(),
  primary key (stream_id, name)
);
-- Index for snapshot lookups
create index if not exists idx_snapshots_stream on events.snapshots (stream_id);
-- ========================================
-- Category & Type Registration
-- ========================================
-- Register a category
create or replace function events.register_category (p_id text) returns text as $$ begin
insert into events.categories (id)
values (p_id) on conflict (id) do nothing;
return p_id;
end;
$$ language plpgsql;
-- Unregister a category (cascades to types)
create or replace function events.unregister_category (p_id text) returns boolean as $$
declare v_deleted integer;
begin
delete from events.categories
where id = p_id;
get diagnostics v_deleted = row_count;
return v_deleted > 0;
end;
$$ language plpgsql;
-- List all categories
create or replace function events.list_categories () returns table (out_id text) as $$ begin return query
select id
from events.categories
order by id;
end;
$$ language plpgsql;
-- Register an event type for a category
create or replace function events.register_event_type (p_category text, p_id text) returns table (out_category_id text, out_id text) as $$ begin -- ensure category exists
insert into events.categories (id)
values (p_category) on conflict (id) do nothing;
-- Register type
insert into events.types (category_id, id)
values (p_category, p_id) on conflict (category_id, id) do nothing;
return query
select p_category,
  p_id;
end;
$$ language plpgsql;
-- Unregister an event type
create or replace function events.unregister_event_type (p_category text, p_id text) returns boolean as $$
declare v_deleted integer;
begin
delete from events.types
where category_id = p_category
  and id = p_id;
get diagnostics v_deleted = row_count;
return v_deleted > 0;
end;
$$ language plpgsql;
-- List all types for a category (or all types if category is NULL)
create or replace function events.list_event_types (p_category text default null) returns table (out_category_id text, out_id text) as $$ begin return query
select t.category_id,
  t.id
from events.types t
where p_category is null
  or t.category_id = p_category
order by t.category_id,
  t.id;
end;
$$ language plpgsql;
-- ========================================
-- Stream Management Functions
-- ========================================
-- Create or get a stream
create or replace function events.ensure_stream (
    p_stream_id uuid,
    p_category text default 'default'
  ) returns table (
    out_id uuid,
    out_category_id text,
    out_version bigint,
    out_created_at timestamptz
  ) as $$ begin return query
insert into events.streams (id, category_id)
values (p_stream_id, p_category) on conflict (id) do
update
set updated_at = now()
returning streams.id,
  streams.category_id,
  streams.version,
  streams.created_at;
end;
$$ language plpgsql;
-- Get stream info
create or replace function events.get_stream (p_stream_id uuid) returns table (
    out_id uuid,
    out_category_id text,
    out_version bigint,
    out_event_count bigint,
    out_created_at timestamptz,
    out_updated_at timestamptz
  ) as $$ begin return query
select s.id,
  s.category_id,
  s.version,
  (
    select count(*)
    from events.events e
    where e.stream_id = s.id
  ),
  s.created_at,
  s.updated_at
from events.streams s
where s.id = p_stream_id;
end;
$$ language plpgsql;
-- List streams with optional category filter
create or replace function events.list_streams (
    p_category text default null,
    p_limit integer default 100,
    p_offset integer default 0
  ) returns table (
    out_id uuid,
    out_category_id text,
    out_version bigint,
    out_created_at timestamptz
  ) as $$ begin return query
select s.id,
  s.category_id,
  s.version,
  s.created_at
from events.streams s
where p_category is null
  or s.category_id = p_category
order by s.created_at desc
limit p_limit offset p_offset;
end;
$$ language plpgsql;
-- Delete a stream and all its events
create or replace function events.delete_stream (p_stream_id uuid) returns boolean as $$
declare deleted_count integer;
begin
delete from events.streams
where id = p_stream_id;
get diagnostics deleted_count = row_count;
return deleted_count > 0;
end;
$$ language plpgsql;
-- ========================================
-- Event Append Functions
-- ========================================
-- Append a single event to a stream with optimistic concurrency
create or replace function events.append (
    p_stream_id uuid,
    p_type text,
    p_data jsonb default '{}',
    p_metadata jsonb default '{}',
    p_expected_version bigint default null,
    p_category text default 'default'
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_created_at timestamptz
  ) as $$
declare v_current_version bigint;
v_new_version bigint;
v_stream_exists boolean;
v_stream_category text;
begin -- get stream info
select version,
  category_id into v_current_version,
  v_stream_category
from events.streams
where id = p_stream_id for
update;
v_stream_exists := found;
-- Handle expected version check
if p_expected_version is not null then if p_expected_version = -1 then if v_stream_exists then raise exception 'Stream "%" already exists (expected_version=-1)',
p_stream_id using errcode = 'P0001';
end if;
elsif not v_stream_exists then if p_expected_version > 0 then raise exception 'Stream "%" does not exist (expected_version=%)',
p_stream_id,
p_expected_version using errcode = 'P0001';
end if;
elsif v_current_version != p_expected_version then raise exception 'Concurrency conflict: stream "%" at version %, expected %',
p_stream_id,
v_current_version,
p_expected_version using errcode = 'P0001';
end if;
end if;
-- Create stream if needed
if not v_stream_exists then
insert into events.streams (id, category_id)
values (p_stream_id, p_category);
v_current_version := 0;
v_stream_category := p_category;
end if;
v_new_version := coalesce(v_current_version, 0) + 1;
-- Update stream version
update events.streams
set version = v_new_version,
  updated_at = now()
where id = p_stream_id;
-- Insert event (with category for FK constraint)
return query
insert into events.events (
    stream_id,
    stream_version,
    category_id,
    type_id,
    data,
    metadata
  )
values (
    p_stream_id,
    v_new_version,
    v_stream_category,
    p_type,
    p_data,
    p_metadata
  )
returning events.position,
  events.id,
  events.stream_id,
  events.stream_version,
  events.category_id as category,
  events.type_id,
  events.created_at;
end;
$$ language plpgsql;
-- ========================================
-- Event Reading Functions
-- ========================================
-- Read events from a specific stream
create or replace function events.read_stream (
    p_stream_id uuid,
    p_from_version bigint default 0,
    p_limit integer default 100,
    p_direction text default 'forward'
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin if p_direction = 'backward' then return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.stream_id = p_stream_id
  and e.stream_version <= p_from_version
order by e.stream_version desc
limit p_limit;
else return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.stream_id = p_stream_id
  and e.stream_version > p_from_version
order by e.stream_version asc
limit p_limit;
end if;
end;
$$ language plpgsql;
-- Read all events globally (for projections/subscriptions)
create or replace function events.read_all (
    p_from_position bigint default 0,
    p_limit integer default 100,
    p_filter_types text [] default null,
    p_filter_streams uuid [] default null
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_category_id text,
    out_stream_version bigint,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select e.position,
  e.id,
  e.stream_id,
  e.category_id,
  e.stream_version,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.position > p_from_position
  and (
    p_filter_types is null
    or e.type_id = any(p_filter_types)
    or (e.category_id || '/' || e.type_id) = any(p_filter_types)
  )
  and (
    p_filter_streams is null
    or e.stream_id = any(p_filter_streams)
  )
order by e.position asc
limit p_limit;
end;
$$ language plpgsql;
-- Read events by type
create or replace function events.read_by_type (
    p_category text,
    p_type text,
    p_from_position bigint default 0,
    p_limit integer default 100
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.category_id = p_category
  and e.type_id = p_type
  and e.position > p_from_position
order by e.position asc
limit p_limit;
end;
$$ language plpgsql;
-- Read events by category
create or replace function events.read_by_category (
    p_category text,
    p_from_position bigint default 0,
    p_limit integer default 100
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.category_id = p_category
  and e.position > p_from_position
order by e.position asc
limit p_limit;
end;
$$ language plpgsql;
-- Get a single event by ID
create or replace function events.get_event (p_event_id uuid) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.id = p_event_id;
end;
$$ language plpgsql;
-- Get event by position
create or replace function events.get_event_at_position (p_position bigint) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_stream_version bigint,
    out_category_id text,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select e.position,
  e.id,
  e.stream_id,
  e.stream_version,
  e.category_id,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
where e.position = p_position;
end;
$$ language plpgsql;
-- ========================================
-- Subscription Management Functions
-- ========================================
-- Create or update a subscription
create or replace function events.create_subscription (
    p_name text,
    p_filter_types text [] default null,
    p_filter_streams uuid [] default null,
    p_start_position bigint default 0
  ) returns table (
    out_name text,
    out_filter_types text [],
    out_filter_streams uuid [],
    out_last_position bigint,
    out_active boolean,
    out_created_at timestamptz
  ) as $$
declare v_event_type text;
v_slash_pos integer;
v_category text;
v_type text;
v_stream_id uuid;
begin -- insert or update subscription
insert into events.subscriptions (name, last_position)
values (p_name, p_start_position) on conflict (name) do
update
set updated_at = now();
-- Clear existing filters
delete from events.subscription_filter_types
where subscription_name = p_name;
delete from events.subscription_filter_streams
where subscription_name = p_name;
-- Insert type filters
if p_filter_types is not null then foreach v_event_type in array p_filter_types loop v_slash_pos := position('/' in v_event_type);
if v_slash_pos > 0 then v_category := substring(
  v_event_type
  from 1 for v_slash_pos - 1
);
v_type := substring(
  v_event_type
  from v_slash_pos + 1
);
insert into events.subscription_filter_types (subscription_name, category_id, type_id)
values (p_name, v_category, v_type) on conflict do nothing;
end if;
end loop;
end if;
-- Insert stream filters
if p_filter_streams is not null then foreach v_stream_id in array p_filter_streams loop
insert into events.subscription_filter_streams (subscription_name, stream_id)
values (p_name, v_stream_id) on conflict do nothing;
end loop;
end if;
-- Return with filters as arrays
return query
select s.name,
  array(
    select category_id || '/' || type_id
    from events.subscription_filter_types
    where subscription_name = s.name
  ),
  array(
    select stream_id
    from events.subscription_filter_streams
    where subscription_name = s.name
  ),
  s.last_position,
  s.active,
  s.created_at
from events.subscriptions s
where s.name = p_name;
end;
$$ language plpgsql;
-- Get subscription info
create or replace function events.get_subscription (p_name text) returns table (
    out_name text,
    out_filter_types text [],
    out_filter_streams uuid [],
    out_last_position bigint,
    out_last_processed_at timestamptz,
    out_active boolean,
    out_created_at timestamptz,
    out_events_behind bigint
  ) as $$ begin return query
select s.name,
  array(
    select category_id || '/' || type_id
    from events.subscription_filter_types
    where subscription_name = s.name
  ),
  array(
    select stream_id
    from events.subscription_filter_streams
    where subscription_name = s.name
  ),
  s.last_position,
  s.last_processed_at,
  s.active,
  s.created_at,
  (
    select coalesce(max(e.position), 0) - s.last_position
    from events.events e
  )
from events.subscriptions s
where s.name = p_name;
end;
$$ language plpgsql;
-- Poll for new events on a subscription
-- Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access
create or replace function events.poll (
    p_subscription_name text,
    p_batch_size integer default 100,
    p_claim_timeout integer default 300
  ) returns table (
    out_position bigint,
    out_id uuid,
    out_stream_id uuid,
    out_category_id text,
    out_stream_version bigint,
    out_type_id text,
    out_data jsonb,
    out_metadata jsonb,
    out_created_at timestamptz
  ) as $$
declare v_last_position bigint;
v_has_type_filters boolean;
v_has_stream_filters boolean;
v_claim_expiry timestamptz;
begin -- get subscription position
select last_position into v_last_position
from events.subscriptions
where name = p_subscription_name
  and active = true;
if v_last_position is null then raise exception 'Subscription "%" not found or inactive',
p_subscription_name;
end if;
-- Check if filters exist
select exists(
    select 1
    from events.subscription_filter_types
    where subscription_name = p_subscription_name
  ) into v_has_type_filters;
select exists(
    select 1
    from events.subscription_filter_streams
    where subscription_name = p_subscription_name
  ) into v_has_stream_filters;
-- Calculate claim expiry time
v_claim_expiry := now() + (p_claim_timeout * interval '1 second');
-- Claim and return matching events using SKIP LOCKED
-- This ensures each event is only processed by one worker
return query with candidate_events as (
  select e.position
  from events.events e
  where e.position > v_last_position -- type filter: if filters exist, must match
    and (
      not v_has_type_filters
      or exists(
        select 1
        from events.subscription_filter_types sft
        where sft.subscription_name = p_subscription_name
          and sft.category_id = e.category_id
          and sft.type_id = e.type_id
      )
    ) -- stream filter: if filters exist, must match
    and (
      not v_has_stream_filters
      or exists(
        select 1
        from events.subscription_filter_streams sfs
        where sfs.subscription_name = p_subscription_name
          and sfs.stream_id = e.stream_id
      )
    ) -- exclude already claimed events (unless claim expired)
    and not exists(
      select 1
      from events.event_claims ec
      where ec.subscription_name = p_subscription_name
        and ec.position = e.position
        and ec.expires_at > now()
    )
  order by e.position asc
  limit p_batch_size for
  update of e skip locked
),
claimed_positions as (
  insert into events.event_claims (
      subscription_name,
      position,
      expires_at
    )
  select p_subscription_name,
    ce.position,
    v_claim_expiry
  from candidate_events ce on conflict (subscription_name, position) do
  update
  set claimed_at = now(),
    expires_at = v_claim_expiry
  where events.event_claims.expires_at <= now()
  returning position
)
select e.position,
  e.id,
  e.stream_id,
  e.category_id,
  e.stream_version,
  e.type_id,
  e.data,
  e.metadata,
  e.created_at
from events.events e
  inner join claimed_positions cp on e.position = cp.position
order by e.position asc;
end;
$$ language plpgsql;
-- Acknowledge events up to a position
create or replace function events.ack (p_subscription_name text, p_position bigint) returns boolean as $$
declare updated_count integer;
begin -- Remove claim for this event
delete from events.event_claims
where subscription_name = p_subscription_name
  and position = p_position;
-- Update subscription position
update events.subscriptions
set last_position = p_position,
  last_processed_at = now(),
  updated_at = now()
where name = p_subscription_name
  and active = true
  and last_position < p_position;
-- Only move forward
get diagnostics updated_count = row_count;
return updated_count > 0;
end;
$$ language plpgsql;
-- Set subscription active state
create or replace function events.set_subscription_active (p_name text, p_active boolean) returns boolean as $$
declare updated_count integer;
begin
update events.subscriptions
set active = p_active,
  updated_at = now()
where name = p_name;
get diagnostics updated_count = row_count;
return updated_count > 0;
end;
$$ language plpgsql;
-- Reset subscription position
create or replace function events.reset_subscription (p_name text, p_position bigint default 0) returns boolean as $$
declare updated_count integer;
begin
update events.subscriptions
set last_position = p_position,
  last_processed_at = null,
  updated_at = now()
where name = p_name;
get diagnostics updated_count = row_count;
return updated_count > 0;
end;
$$ language plpgsql;
-- Clean up expired event claims
-- This should be called periodically to prevent table bloat
create or replace function events.cleanup_expired_claims () returns bigint as $$
declare deleted_count bigint;
begin
delete from events.event_claims
where expires_at <= now();
get diagnostics deleted_count = row_count;
return deleted_count;
end;
$$ language plpgsql;
-- Clean up claims for a specific subscription
-- Useful when a worker crashes or needs to release its claims
create or replace function events.cleanup_subscription_claims (p_subscription_name text) returns bigint as $$
declare deleted_count bigint;
begin
delete from events.event_claims
where subscription_name = p_subscription_name;
get diagnostics deleted_count = row_count;
return deleted_count;
end;
$$ language plpgsql;
-- List all subscriptions
create or replace function events.list_subscriptions () returns table (
    out_name text,
    out_filter_types text [],
    out_last_position bigint,
    out_active boolean,
    out_created_at timestamptz,
    out_events_behind bigint
  ) as $$ begin return query
select s.name,
  array(
    select category_id || '/' || type_id
    from events.subscription_filter_types
    where subscription_name = s.name
  ),
  s.last_position,
  s.active,
  s.created_at,
  (
    select coalesce(max(e.position), 0)
    from events.events e
  ) - s.last_position
from events.subscriptions s
order by s.name;
end;
$$ language plpgsql;
-- Delete a subscription
create or replace function events.delete_subscription (p_name text) returns boolean as $$
declare deleted_count integer;
begin
delete from events.subscriptions
where name = p_name;
get diagnostics deleted_count = row_count;
return deleted_count > 0;
end;
$$ language plpgsql;
-- ========================================
-- Snapshot Functions
-- ========================================
-- Save a snapshot
create or replace function events.save_snapshot (
    p_stream_id uuid,
    p_name text,
    p_version bigint,
    p_state jsonb
  ) returns table (
    out_stream_id uuid,
    out_name text,
    out_version bigint,
    out_created_at timestamptz
  ) as $$ begin return query
insert into events.snapshots (stream_id, name, version, state)
values (p_stream_id, p_name, p_version, p_state) on conflict (stream_id, name) do
update
set version = excluded.version,
  state = excluded.state,
  created_at = now()
returning snapshots.stream_id,
  snapshots.name,
  snapshots.version,
  snapshots.created_at;
end;
$$ language plpgsql;
-- Load a snapshot
create or replace function events.load_snapshot (
    p_stream_id uuid,
    p_name text default 'aggregate-state'
  ) returns table (
    out_stream_id uuid,
    out_name text,
    out_version bigint,
    out_state jsonb,
    out_created_at timestamptz
  ) as $$ begin return query
select snap.stream_id,
  snap.name,
  snap.version,
  snap.state,
  snap.created_at
from events.snapshots snap
where snap.stream_id = p_stream_id
  and snap.name = p_name;
end;
$$ language plpgsql;
-- Delete a snapshot
create or replace function events.delete_snapshot (
    p_stream_id uuid,
    p_name text default 'aggregate-state'
  ) returns boolean as $$
declare deleted_count integer;
begin
delete from events.snapshots
where stream_id = p_stream_id
  and name = p_name;
get diagnostics deleted_count = row_count;
return deleted_count > 0;
end;
$$ language plpgsql;
-- ========================================
-- Statistics Functions
-- ========================================
-- Get global statistics
create or replace function events.get_stats () returns table (
    out_total_events bigint,
    out_total_streams bigint,
    out_total_subscriptions bigint,
    out_max_position bigint,
    out_events_today bigint,
    out_events_this_hour bigint
  ) as $$ begin return query
select (
    select count(*)
    from events.events
  ),
  (
    select count(*)
    from events.streams
  ),
  (
    select count(*)
    from events.subscriptions
  ),
  (
    select coalesce(max(position), 0)
    from events.events
  ),
  (
    select count(*)
    from events.events
    where created_at >= current_date
  ),
  (
    select count(*)
    from events.events
    where created_at >= now() - interval '1 hour'
  );
end;
$$ language plpgsql;
-- Get stream statistics
create or replace function events.get_stream_stats (p_stream_id uuid default null) returns table (
    out_stream_id uuid,
    out_category_id text,
    out_event_count bigint,
    out_version bigint,
    out_first_event_at timestamptz,
    out_last_event_at timestamptz
  ) as $$ begin return query
select s.id,
  s.category_id,
  count(e.position),
  s.version,
  min(e.created_at),
  max(e.created_at)
from events.streams s
  left join events.events e on e.stream_id = s.id
where p_stream_id is null
  or s.id = p_stream_id
group by s.id,
  s.category_id,
  s.version
order by s.id;
end;
$$ language plpgsql;
-- Get event type statistics
create or replace function events.get_type_stats () returns table (
    out_type_id text,
    out_count bigint,
    out_first_at timestamptz,
    out_last_at timestamptz
  ) as $$ begin return query
select e.type_id,
  count(*),
  min(e.created_at),
  max(e.created_at)
from events.events e
group by e.type_id
order by count(*) desc;
end;
$$ language plpgsql;
-- Get category statistics
create or replace function events.get_category_stats () returns table (
    out_category_id text,
    out_stream_count bigint,
    out_event_count bigint
  ) as $$ begin return query
select s.category_id,
  count(distinct s.id),
  count(e.position)
from events.streams s
  left join events.events e on e.stream_id = s.id
group by s.category_id
order by count(e.position) desc;
end;
$$ language plpgsql;
-- ========================================
-- Event Type Registry
-- ========================================
-- Registry for event types with auto-generated views
-- ========================================
-- Aggregate Registry
-- ========================================
-- Registry for aggregate types with reducer definitions
create table if not exists events.aggregate_types (
  name text primary key,
  category_id text not null references events.categories (id),
  -- Stream category this aggregate handles
  initial_state jsonb not null,
  -- Initial state before any events
  reducers jsonb not null,
  -- {"EventType": "SQL expression", ...}
  function_name text not null,
  -- Auto-snapshot: save snapshot after replaying this many events (NULL = disabled)
  snapshot_threshold integer default null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
-- Register an aggregate type and create a loader function
create or replace function events.register_aggregate (
    p_name text,
    p_category text,
    p_initial_state jsonb,
    p_reducers jsonb,
    -- {"EventType": {"field": "expression"}, ...}
    p_snapshot_threshold integer default null -- auto-snapshot after replaying n events (null = disabled)
  ) returns table (
    out_name text,
    out_function_name text,
    out_category_id text
  ) as $$
declare v_function_name text;
v_reducer_cases text;
v_snapshot_name text;
begin -- generate function name (replace hyphens with underscores for valid sql identifiers)
v_function_name := 'events.load_' || lower(regexp_replace(p_name, '([A-Z])', '_\1', 'g'));
v_function_name := regexp_replace(
  v_function_name,
  'events\.load__',
  'events.load_'
);
-- Replace hyphens with underscores for valid SQL function names
v_function_name := replace(v_function_name, '-', '_');
-- Generate snapshot name for auto-snapshotting
v_snapshot_name := p_name || '-auto';
-- Build reducer CASE statements (each expression returns the new state)
select string_agg(
    format(
      $case$
      when %L then v_state := %s;
$case$,
event_type,
expr
),
''
order by event_type
) into v_reducer_cases
from jsonb_each_text(p_reducers) as r(event_type, expr);
-- Drop existing function if exists (to allow recreation)
execute format(
  'drop function if exists %s(uuid)',
  v_function_name
);
-- Create loader function that returns JSONB with auto-snapshot support
execute format(
  $func$
  create or replace function %s(p_stream_id uuid) returns jsonb as $body$
  declare v_state jsonb;
v_event record;
v_snapshot record;
v_from_version bigint := 0;
v_events_replayed integer := 0;
v_snapshot_threshold integer := %s;
v_snapshot_name text := %L;
begin -- try to load from auto-snapshot
select out_state,
  out_version into v_snapshot
from events.load_snapshot(p_stream_id, v_snapshot_name);
if v_snapshot.out_state is not null then v_state := jsonb_build_object('stream_id', p_stream_id) || v_snapshot.out_state;
v_state := jsonb_set(
  v_state,
  '{version}',
  to_jsonb(v_snapshot.out_version)
);
v_from_version := v_snapshot.out_version;
end if;
-- Initialize state if no snapshot was loaded
if v_state is null then v_state := jsonb_build_object('stream_id', p_stream_id, 'version', 0) || %L::jsonb;
end if;
-- Replay events (from snapshot version if loaded, otherwise from start)
for v_event in
select e.type_id as type,
  e.data,
  e.stream_version
from events.events e
where e.stream_id = p_stream_id
  and e.stream_version > v_from_version
order by e.stream_version loop v_state := jsonb_set(
    v_state,
    '{version}',
    to_jsonb(v_event.stream_version)
  );
v_events_replayed := v_events_replayed + 1;
case
  v_event.type %s
  else null;
-- Unknown event type, skip
end case
;
end loop;
-- Auto-save snapshot if we replayed enough events
if v_snapshot_threshold is not null
and v_events_replayed >= v_snapshot_threshold then -- remove stream_id from state before saving (it's redundant in snapshot)
perform events.save_snapshot(
  p_stream_id,
  v_snapshot_name,
  (v_state->>'version')::bigint,
  v_state - 'stream_id'
);
end if;
return v_state;
end;
$body$ language plpgsql;
$func$,
v_function_name,
coalesce(p_snapshot_threshold::text, 'null'),
v_snapshot_name,
p_initial_state::text,
v_reducer_cases
);
-- Save to registry
insert into events.aggregate_types (
    name,
    category_id,
    initial_state,
    reducers,
    function_name,
    snapshot_threshold
  )
values (
    p_name,
    p_category,
    p_initial_state,
    p_reducers,
    v_function_name,
    p_snapshot_threshold
  ) on conflict (name) do
update
set category_id = excluded.category_id,
  initial_state = excluded.initial_state,
  reducers = excluded.reducers,
  function_name = excluded.function_name,
  snapshot_threshold = excluded.snapshot_threshold,
  updated_at = now();
return query
select p_name,
  v_function_name,
  p_category;
end;
$$ language plpgsql;
-- Unregister an aggregate type
create or replace function events.unregister_aggregate (p_name text) returns boolean as $$
declare v_function_name text;
v_deleted integer;
begin
select function_name into v_function_name
from events.aggregate_types
where name = p_name;
if v_function_name is not null then -- drop the loader function
execute format(
  'drop function if exists %s(uuid)',
  v_function_name
);
end if;
delete from events.aggregate_types
where name = p_name;
get diagnostics v_deleted = row_count;
return v_deleted > 0;
end;
$$ language plpgsql;
-- List all registered aggregate types
create or replace function events.list_aggregates () returns table (
    out_name text,
    out_function_name text,
    out_category_id text,
    out_event_types text [],
    out_created_at timestamptz
  ) as $$ begin return query
select at.name,
  at.function_name,
  at.category_id,
  array(
    select jsonb_object_keys(at.reducers)
  ),
  at.created_at
from events.aggregate_types at
order by at.name;
end;
$$ language plpgsql;
-- Load an aggregate by name (returns JSONB for dynamic typing)
-- Internal: called by loadRegisteredAggregate in TypeScript
create or replace function events._load_aggregate_dynamic (p_aggregate_name text, p_stream_id uuid) returns jsonb as $$
declare v_function_name text;
v_result jsonb;
begin -- get function name
select function_name into v_function_name
from events.aggregate_types
where name = p_aggregate_name;
if v_function_name is null then raise exception 'Aggregate "%" is not registered',
p_aggregate_name;
end if;
-- Call the loader function (returns JSONB, handles snapshots internally)
execute format('select %s($1)', v_function_name) into v_result using p_stream_id;
return v_result;
end;
$$ language plpgsql;
-- ========================================
-- Projections
-- ========================================
-- Projections table - stores registered projections with their handlers
create table if not exists events.projections (
  name text primary key,
  -- Sync handlers (SQL triggers) - stored as "category/type" -> SQL statement
  sync_handlers jsonb not null default '{}',
  -- Trigger info (for sync handlers)
  trigger_name text,
  trigger_function_name text,
  -- Subscription name (for async handlers)
  subscription_name text references events.subscriptions (name) on delete
  set null,
    -- Timestamps
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
-- Projection async handler types (join table)
create table if not exists events.projection_async_types (
  projection_name text not null references events.projections (name) on delete cascade,
  category_id text not null,
  type_id text not null,
  primary key (projection_name, category_id, type_id),
  foreign key (category_id, type_id) references events.types (category_id, id) on delete cascade
);
-- Register a projection with sync handlers (creates trigger)
-- Internal: called by registerProjection in TypeScript
-- Handler keys are in "category/type" format
create or replace function events._register_projection_sync (
    p_name text,
    p_sync_handlers jsonb -- { "category/type": "sql statement", ... }
  ) returns table (
    out_name text,
    out_trigger_name text,
    out_trigger_function_name text,
    out_event_types text []
  ) as $$
declare v_trigger_name text;
v_trigger_function_name text;
v_function_body text;
v_event_type_key text;
v_handler_sql text;
v_case_statements text := '';
v_event_types text [];
v_category text;
v_type text;
v_slash_pos integer;
begin -- generate names
v_trigger_name := 'projection_' || lower(regexp_replace(p_name, '[^A-Za-z0-9]', '_', 'g')) || '_trigger';
v_trigger_function_name := 'events.projection_' || lower(regexp_replace(p_name, '[^A-Za-z0-9]', '_', 'g')) || '_fn';
-- Extract event types from handlers
select array_agg(key) into v_event_types
from jsonb_object_keys(p_sync_handlers) as key;
-- Build CASE statements for each handler (key is "category/type")
for v_event_type_key,
v_handler_sql in
select *
from jsonb_each_text(p_sync_handlers) loop -- parse category/type
  v_slash_pos := position('/' in v_event_type_key);
if v_slash_pos = 0 then raise exception 'Invalid handler key "%". expected "category/type" format.',
v_event_type_key;
end if;
v_category := substring(
  v_event_type_key
  from 1 for v_slash_pos - 1
);
v_type := substring(
  v_event_type_key
  from v_slash_pos + 1
);
-- Build CASE with both category and type check
v_case_statements := v_case_statements || format(
  e'\n    when new.category_id = %L and new.type_id = %L then\n      %s;',
  v_category,
  v_type,
  v_handler_sql
);
end loop;
-- Build the trigger function
v_function_body := format(
  e'
create or replace function %s()
returns trigger as $trigger$
begin
  case%s
    else null;
  end case;
  return new;
end;
$trigger$ language plpgsql;',
  v_trigger_function_name,
  v_case_statements
);
-- Create the trigger function
execute v_function_body;
-- Drop existing trigger if any
execute format(
  'drop trigger if exists %I on events.events',
  v_trigger_name
);
-- Create the trigger (fires for all inserts, filtering done in function)
execute format(
  'create trigger %I after insert on events.events for each row execute function %s()',
  v_trigger_name,
  v_trigger_function_name
);
-- Update or insert projection record
insert into events.projections (
    name,
    sync_handlers,
    trigger_name,
    trigger_function_name,
    updated_at
  )
values (
    p_name,
    p_sync_handlers,
    v_trigger_name,
    v_trigger_function_name,
    now()
  ) on conflict (name) do
update
set sync_handlers = excluded.sync_handlers,
  trigger_name = excluded.trigger_name,
  trigger_function_name = excluded.trigger_function_name,
  updated_at = now();
return query
select p_name,
  v_trigger_name,
  v_trigger_function_name,
  v_event_types;
end;
$$ language plpgsql;
-- Register async handlers for a projection (creates subscription)
-- Internal: called by registerProjection in TypeScript
-- p_async_types contains "category/type" format strings
create or replace function events._register_projection_async (
    p_name text,
    p_async_types text [],
    p_start_position bigint default null
  ) returns table (
    out_name text,
    out_subscription_name text,
    out_event_types text []
  ) as $$
declare v_subscription_name text;
v_start_pos bigint;
v_event_type text;
v_slash_pos integer;
v_category text;
v_type text;
begin v_subscription_name := 'projection:' || p_name;
-- Determine start position (default to current max position)
if p_start_position is null then
select coalesce(max(position), 0) into v_start_pos
from events.events;
else v_start_pos := p_start_position;
end if;
-- Create or update subscription
insert into events.subscriptions (name, last_position, active)
values (v_subscription_name, v_start_pos, true) on conflict (name) do
update
set active = true,
  updated_at = now();
-- Clear existing type filters for this subscription
delete from events.subscription_filter_types
where subscription_name = v_subscription_name;
-- Update or create projection record FIRST (so foreign key constraints work)
insert into events.projections (name, subscription_name)
values (p_name, v_subscription_name) on conflict (name) do
update
set subscription_name = excluded.subscription_name,
  updated_at = now();
-- Parse and insert type filters into subscription
if p_async_types is not null then foreach v_event_type in array p_async_types loop v_slash_pos := position('/' in v_event_type);
if v_slash_pos > 0 then v_category := substring(
  v_event_type
  from 1 for v_slash_pos - 1
);
v_type := substring(
  v_event_type
  from v_slash_pos + 1
);
insert into events.subscription_filter_types (subscription_name, category_id, type_id)
values (v_subscription_name, v_category, v_type) on conflict do nothing;
end if;
end loop;
end if;
-- Clear existing async type entries for this projection
delete from events.projection_async_types
where projection_name = p_name;
-- Insert into projection_async_types table
if p_async_types is not null then foreach v_event_type in array p_async_types loop v_slash_pos := position('/' in v_event_type);
if v_slash_pos > 0 then v_category := substring(
  v_event_type
  from 1 for v_slash_pos - 1
);
v_type := substring(
  v_event_type
  from v_slash_pos + 1
);
insert into events.projection_async_types (projection_name, category_id, type_id)
values (p_name, v_category, v_type) on conflict do nothing;
end if;
end loop;
end if;
return query
select p_name,
  v_subscription_name,
  p_async_types;
end;
$$ language plpgsql;
-- Unregister a projection (drops trigger and subscription)
create or replace function events.unregister_projection (p_name text) returns boolean as $$
declare v_projection events.projections %rowtype;
begin -- get projection info
select * into v_projection
from events.projections
where name = p_name;
if not found then return false;
end if;
-- Drop trigger if exists
if v_projection.trigger_name is not null then execute format(
  'drop trigger if exists %I on events.events',
  v_projection.trigger_name
);
end if;
-- Drop trigger function if exists
if v_projection.trigger_function_name is not null then execute format(
  'drop function if exists %s()',
  v_projection.trigger_function_name
);
end if;
-- Delete subscription if exists
if v_projection.subscription_name is not null then
delete from events.subscriptions
where name = v_projection.subscription_name;
end if;
-- Delete projection record
delete from events.projections
where name = p_name;
return true;
end;
$$ language plpgsql;
-- List all projections
create or replace function events.list_projections () returns table (
    out_name text,
    out_sync_types text [],
    out_async_types text [],
    out_trigger_name text,
    out_subscription_name text,
    out_subscription_position bigint,
    out_events_behind bigint,
    out_created_at timestamptz
  ) as $$
declare v_max_position bigint;
begin
select coalesce(max(position), 0) into v_max_position
from events.events;
return query
select p.name,
  array(
    select jsonb_object_keys(p.sync_handlers)
  ),
  array(
    select category_id || '/' || type_id
    from events.projection_async_types
    where projection_name = p.name
  ),
  p.trigger_name,
  p.subscription_name,
  s.last_position,
  case
    when s.name is not null then v_max_position - s.last_position
    else 0
  end,
  p.created_at
from events.projections p
  left join events.subscriptions s on s.name = p.subscription_name
order by p.name;
end;
$$ language plpgsql;
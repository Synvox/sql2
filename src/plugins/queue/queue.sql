-- Queue Plugin Schema
-- A robust job queue using SELECT FOR UPDATE SKIP LOCKED, inspired by pgboss
-- Create the queue schema
create schema if not exists queue;

-- Job states lookup table
create table if not exists texts (id text primary key);

-- Insert default job states
insert into
  texts (id)
values
  ('created'),
  -- Job is waiting to be processed
  ('active'),
  -- Job is currently being processed
  ('completed'),
  -- Job finished successfully
  ('failed'),
  -- Job failed (may be retried)
  ('expired'),
  -- Job exceeded its expiration time
  ('cancelled') -- job was manually cancelled
on conflict (id) do nothing;

-- Queues table to define queue configurations
create table if not exists queue.queues (
  name text primary key,
  -- Retry configuration
  retry_limit integer not null default 3,
  retry_delay integer not null default 60,
  -- seconds
  retry_backoff boolean not null default true,
  -- exponential backoff
  -- Timeout configuration
  expire_in integer not null default 900,
  -- 15 minutes default
  -- Retention
  retain_completed integer not null default 86400,
  -- 24 hours
  retain_failed integer not null default 604800,
  -- 7 days
  -- Dead letter queue
  dead_letter text references queue.queues (name),
  -- Metadata
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Jobs table
create table if not exists queue.jobs (
  id uuid primary key default gen_random_uuid(),
  queue_name text not null references queue.queues (name) on delete cascade,
  -- Job identification
  singleton_key text,
  -- For unique jobs
  -- Payload
  data jsonb not null default '{}',
  -- State
  state text not null default 'created' references texts (id),
  -- Scheduling
  start_after timestamptz not null default now(),
  expire_at timestamptz,
  -- When the job expires (calculated from expire_in when fetched)
  -- Retry tracking
  retry_count integer not null default 0,
  retry_limit integer not null default 3,
  retry_delay integer not null default 60,
  retry_backoff boolean not null default true,
  -- Priority (higher = more important)
  priority integer not null default 0,
  -- Output
  output jsonb,
  -- Timestamps
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  -- Keep reference to dead-lettered job
  dead_letter_id uuid references queue.jobs (id),
  -- Error tracking
  last_error text
);

-- Index for efficient job fetching (most important query)
create index if not exists idx_jobs_fetch on queue.jobs (
  queue_name,
  state,
  start_after,
  priority desc,
  created_at
)
where
  state = 'created';

-- Index for singleton key enforcement
create unique index if not exists idx_jobs_singleton on queue.jobs (queue_name, singleton_key)
where
  singleton_key is not null
  and state in ('created', 'active');

-- Index for state lookups
create index if not exists idx_jobs_state on queue.jobs (state);

-- Index for expiration checks
create index if not exists idx_jobs_expire on queue.jobs (expire_at)
where
  state = 'active'
  and expire_at is not null;

-- Index for cleanup (completed/failed jobs)
create index if not exists idx_jobs_cleanup on queue.jobs (completed_at)
where
  state in ('completed', 'failed', 'expired', 'cancelled');

-- Schedule table for cron-like recurring jobs
create table if not exists queue.schedules (
  name text primary key,
  queue_name text not null references queue.queues (name) on delete cascade,
  -- Schedule configuration
  cron text not null,
  -- Cron expression
  timezone text not null default 'utc',
  -- Job template
  data jsonb not null default '{}',
  priority integer not null default 0,
  -- State
  enabled boolean not null default true,
  -- Tracking
  last_run_at timestamptz,
  next_run_at timestamptz,
  -- Metadata
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- ========================================
-- Queue Management Functions
-- ========================================
-- Create or update a queue
create or replace function queue.create_queue (
  p_name text,
  p_retry_limit integer default 3,
  p_retry_delay integer default 60,
  p_retry_backoff boolean default true,
  p_expire_in integer default 900,
  p_retain_completed integer default 86400,
  p_retain_failed integer default 604800,
  p_dead_letter text default null
) returns table (
  out_name text,
  out_retry_limit integer,
  out_retry_delay integer,
  out_retry_backoff boolean,
  out_expire_in integer,
  out_retain_completed integer,
  out_retain_failed integer,
  out_dead_letter text,
  out_created_at timestamptz
) as $$ begin return query
insert into queue.queues (
    name,
    retry_limit,
    retry_delay,
    retry_backoff,
    expire_in,
    retain_completed,
    retain_failed,
    dead_letter
  )
values (
    p_name,
    p_retry_limit,
    p_retry_delay,
    p_retry_backoff,
    p_expire_in,
    p_retain_completed,
    p_retain_failed,
    p_dead_letter
  ) on conflict (name) do
update
set retry_limit = excluded.retry_limit,
  retry_delay = excluded.retry_delay,
  retry_backoff = excluded.retry_backoff,
  expire_in = excluded.expire_in,
  retain_completed = excluded.retain_completed,
  retain_failed = excluded.retain_failed,
  dead_letter = excluded.dead_letter,
  updated_at = now()
returning queues.name,
  queues.retry_limit,
  queues.retry_delay,
  queues.retry_backoff,
  queues.expire_in,
  queues.retain_completed,
  queues.retain_failed,
  queues.dead_letter,
  queues.created_at;
end;
$$ language plpgsql;

-- Get queue configuration
create or replace function queue.get_queue (p_name text) returns table (
  out_name text,
  out_retry_limit integer,
  out_retry_delay integer,
  out_retry_backoff boolean,
  out_expire_in integer,
  out_retain_completed integer,
  out_retain_failed integer,
  out_dead_letter text,
  out_created_at timestamptz,
  out_job_counts jsonb
) as $$ begin return query
select q.name,
  q.retry_limit,
  q.retry_delay,
  q.retry_backoff,
  q.expire_in,
  q.retain_completed,
  q.retain_failed,
  q.dead_letter,
  q.created_at,
  (
    select jsonb_object_agg(state, count)
    from (
        select j.state::text,
          count(*)::integer as count
        from queue.jobs j
        where j.queue_name = q.name
        group by j.state
      ) counts
  ) as job_counts
from queue.queues q
where q.name = p_name;
end;
$$ language plpgsql;

-- List all queues with job counts
create or replace function queue.list_queues () returns table (
  out_name text,
  out_retry_limit integer,
  out_expire_in integer,
  out_dead_letter text,
  out_created_at timestamptz,
  out_created_count bigint,
  out_active_count bigint,
  out_completed_count bigint,
  out_failed_count bigint
) as $$ begin return query
select q.name,
  q.retry_limit,
  q.expire_in,
  q.dead_letter,
  q.created_at,
  coalesce(
    sum(
      case
        when j.state = 'created' then 1
        else 0
      end
    ),
    0
  ) as created_count,
  coalesce(
    sum(
      case
        when j.state = 'active' then 1
        else 0
      end
    ),
    0
  ) as active_count,
  coalesce(
    sum(
      case
        when j.state = 'completed' then 1
        else 0
      end
    ),
    0
  ) as completed_count,
  coalesce(
    sum(
      case
        when j.state = 'failed' then 1
        else 0
      end
    ),
    0
  ) as failed_count
from queue.queues q
  left join queue.jobs j on j.queue_name = q.name
group by q.name,
  q.retry_limit,
  q.expire_in,
  q.dead_letter,
  q.created_at
order by q.name;
end;
$$ language plpgsql;

-- Delete a queue (and all its jobs)
create or replace function queue.delete_queue (p_name text) returns boolean as $$
declare deleted_count integer;
begin
delete from queue.queues
where name = p_name;
get diagnostics deleted_count = row_count;
return deleted_count > 0;
end;
$$ language plpgsql;

-- ========================================
-- Job Management Functions
-- ========================================
-- Send a job to a queue
create or replace function queue.send (
  p_queue_name text,
  p_data jsonb default '{}',
  p_options jsonb default '{}'
) returns table (
  out_id uuid,
  out_queue_name text,
  out_state text,
  out_singleton_key text,
  out_priority integer,
  out_start_after timestamptz,
  out_created_at timestamptz
) as $$
declare v_queue queue.queues %rowtype;
v_start_after timestamptz;
v_singleton_key text;
v_priority integer;
v_retry_limit integer;
v_retry_delay integer;
v_retry_backoff boolean;
begin -- get queue config
select * into v_queue
from queue.queues q
where q.name = p_queue_name;
if v_queue is null then raise exception 'Queue "%" does not exist',
p_queue_name;
end if;
-- Parse options
v_start_after := coalesce(
  (p_options->>'start_after')::timestamptz,
  now() + (
    coalesce((p_options->>'delay')::integer, 0) * interval '1 second'
  )
);
v_singleton_key := p_options->>'singleton_key';
v_priority := coalesce((p_options->>'priority')::integer, 0);
v_retry_limit := coalesce(
  (p_options->>'retry_limit')::integer,
  v_queue.retry_limit
);
v_retry_delay := coalesce(
  (p_options->>'retry_delay')::integer,
  v_queue.retry_delay
);
v_retry_backoff := coalesce(
  (p_options->>'retry_backoff')::boolean,
  v_queue.retry_backoff
);
-- Insert job (singleton handling via unique index)
return query
insert into queue.jobs (
    queue_name,
    data,
    singleton_key,
    priority,
    start_after,
    retry_limit,
    retry_delay,
    retry_backoff
  )
values (
    p_queue_name,
    p_data,
    v_singleton_key,
    v_priority,
    v_start_after,
    v_retry_limit,
    v_retry_delay,
    v_retry_backoff
  ) on conflict (queue_name, singleton_key)
where singleton_key is not null
  and state in ('created', 'active') do nothing
returning jobs.id,
  jobs.queue_name,
  jobs.state,
  jobs.singleton_key,
  jobs.priority,
  jobs.start_after,
  jobs.created_at;
end;
$$ language plpgsql;

-- Send multiple jobs (batch insert)
create or replace function queue.send_batch (
  p_queue_name text,
  p_jobs jsonb -- array of {data, options?}
) returns table (
  out_id uuid,
  out_queue_name text,
  out_state text,
  out_priority integer,
  out_created_at timestamptz
) as $$
declare v_queue queue.queues %rowtype;
begin -- get queue config
select * into v_queue
from queue.queues q
where q.name = p_queue_name;
if v_queue is null then raise exception 'Queue "%" does not exist',
p_queue_name;
end if;
-- Insert all jobs
return query
insert into queue.jobs (
    queue_name,
    data,
    singleton_key,
    priority,
    start_after,
    retry_limit,
    retry_delay,
    retry_backoff
  )
select p_queue_name,
  coalesce(job->>'data', '{}')::jsonb,
  job->'options'->>'singleton_key',
  coalesce((job->'options'->>'priority')::integer, 0),
  coalesce(
    (job->'options'->>'start_after')::timestamptz,
    now() + (
      coalesce((job->'options'->>'delay')::integer, 0) * interval '1 second'
    )
  ),
  coalesce(
    (job->'options'->>'retry_limit')::integer,
    v_queue.retry_limit
  ),
  coalesce(
    (job->'options'->>'retry_delay')::integer,
    v_queue.retry_delay
  ),
  coalesce(
    (job->'options'->>'retry_backoff')::boolean,
    v_queue.retry_backoff
  )
from jsonb_array_elements(p_jobs) as job on conflict (queue_name, singleton_key)
where singleton_key is not null
  and state in ('created', 'active') do nothing
returning jobs.id,
  jobs.queue_name,
  jobs.state,
  jobs.priority,
  jobs.created_at;
end;
$$ language plpgsql;

-- Fetch jobs for processing using SELECT FOR UPDATE SKIP LOCKED
create or replace function queue.fetch (p_queue_name text, p_batch_size integer default 1) returns table (
  out_id uuid,
  out_queue_name text,
  out_data jsonb,
  out_singleton_key text,
  out_priority integer,
  out_retry_count integer,
  out_created_at timestamptz,
  out_started_at timestamptz,
  out_expire_at timestamptz
) as $$
declare v_queue queue.queues %rowtype;
begin -- get queue config
select * into v_queue
from queue.queues q
where q.name = p_queue_name;
if v_queue is null then raise exception 'Queue "%" does not exist',
p_queue_name;
end if;
return query with selected_jobs as (
  select j.id
  from queue.jobs j
  where j.queue_name = p_queue_name
    and j.state = 'created'
    and j.start_after <= now()
  order by j.priority desc,
    j.created_at asc
  limit p_batch_size for
  update skip locked
)
update queue.jobs j
set state = 'active',
  started_at = now(),
  expire_at = now() + (v_queue.expire_in * interval '1 second')
from selected_jobs s
where j.id = s.id
returning j.id,
  j.queue_name,
  j.data,
  j.singleton_key,
  j.priority,
  j.retry_count,
  j.created_at,
  j.started_at,
  j.expire_at;
end;
$$ language plpgsql;

-- Complete a job successfully
create or replace function queue.complete (p_job_id uuid, p_output jsonb default null) returns table (
  out_id uuid,
  out_queue_name text,
  out_state text,
  out_completed_at timestamptz
) as $$ begin return query
update queue.jobs
set state = 'completed',
  output = p_output,
  completed_at = now()
where id = p_job_id
  and state = 'active'
returning jobs.id,
  jobs.queue_name,
  jobs.state,
  jobs.completed_at;
end;
$$ language plpgsql;

-- Fail a job
create or replace function queue.fail (p_job_id uuid, p_error text default null) returns table (
  out_id uuid,
  out_queue_name text,
  out_state text,
  out_retry_count integer,
  out_will_retry boolean,
  out_next_retry_at timestamptz
) as $$
declare v_job queue.jobs %rowtype;
v_queue queue.queues %rowtype;
v_new_state text;
v_next_retry timestamptz;
v_will_retry boolean;
v_delay integer;
begin -- get job
select * into v_job
from queue.jobs
where id = p_job_id
  and state = 'active';
if v_job is null then return;
end if;
-- Check if we can retry
if v_job.retry_count < v_job.retry_limit then v_will_retry := true;
v_new_state := 'created';
-- Calculate retry delay (with optional exponential backoff)
v_delay := v_job.retry_delay;
if v_job.retry_backoff then v_delay := v_delay * power(2, v_job.retry_count);
end if;
v_next_retry := now() + (v_delay * interval '1 second');
else v_will_retry := false;
v_new_state := 'failed';
v_next_retry := null;
-- Check for dead letter queue
select * into v_queue
from queue.queues
where name = v_job.queue_name;
if v_queue.dead_letter is not null then
insert into queue.jobs (
    queue_name,
    data,
    priority,
    dead_letter_id
  )
values (
    v_queue.dead_letter,
    v_job.data,
    v_job.priority,
    v_job.id
  );
end if;
end if;
return query
update queue.jobs
set state = v_new_state,
  retry_count = case
    when v_will_retry then retry_count + 1
    else retry_count
  end,
  start_after = coalesce(v_next_retry, start_after),
  started_at = case
    when v_will_retry then null
    else started_at
  end,
  expire_at = null,
  completed_at = case
    when not v_will_retry then now()
    else null
  end,
  last_error = p_error
where id = p_job_id
returning jobs.id,
  jobs.queue_name,
  jobs.state,
  jobs.retry_count,
  v_will_retry,
  v_next_retry;
end;
$$ language plpgsql;

-- Cancel a job
create or replace function queue.cancel (p_job_id uuid) returns table (
  out_id uuid,
  out_queue_name text,
  out_previous_state text,
  out_cancelled boolean
) as $$
declare v_previous_state text;
v_queue_name text;
begin
select state,
  queue_name into v_previous_state,
  v_queue_name
from queue.jobs
where id = p_job_id
  and state in ('created', 'active');
if v_previous_state is null then return query
select p_job_id,
  null::text,
  null::text,
  false;
return;
end if;
update queue.jobs
set state = 'cancelled',
  completed_at = now()
where id = p_job_id
  and state in ('created', 'active');
return query
select p_job_id,
  v_queue_name,
  v_previous_state,
  true;
end;
$$ language plpgsql;

-- Get job by ID
create or replace function queue.get_job (p_job_id uuid) returns table (
  out_id uuid,
  out_queue_name text,
  out_state text,
  out_data jsonb,
  out_output jsonb,
  out_singleton_key text,
  out_priority integer,
  out_retry_count integer,
  out_retry_limit integer,
  out_start_after timestamptz,
  out_expire_at timestamptz,
  out_created_at timestamptz,
  out_started_at timestamptz,
  out_completed_at timestamptz,
  out_last_error text,
  out_dead_letter_id uuid
) as $$ begin return query
select j.id,
  j.queue_name,
  j.state,
  j.data,
  j.output,
  j.singleton_key,
  j.priority,
  j.retry_count,
  j.retry_limit,
  j.start_after,
  j.expire_at,
  j.created_at,
  j.started_at,
  j.completed_at,
  j.last_error,
  j.dead_letter_id
from queue.jobs j
where j.id = p_job_id;
end;
$$ language plpgsql;

-- List jobs in a queue with optional state filter
create or replace function queue.list_jobs (
  p_queue_name text,
  p_state text default null,
  p_limit integer default 100,
  p_offset integer default 0
) returns table (
  out_id uuid,
  out_state text,
  out_data jsonb,
  out_priority integer,
  out_retry_count integer,
  out_start_after timestamptz,
  out_created_at timestamptz,
  out_started_at timestamptz,
  out_completed_at timestamptz,
  out_last_error text
) as $$ begin return query
select j.id,
  j.state,
  j.data,
  j.priority,
  j.retry_count,
  j.start_after,
  j.created_at,
  j.started_at,
  j.completed_at,
  j.last_error
from queue.jobs j
where j.queue_name = p_queue_name
  and (
    p_state is null
    or j.state = p_state
  )
order by case
    j.state
    when 'active' then 1
    when 'created' then 2
    when 'failed' then 3
    when 'completed' then 4
    else 5
  end,
  j.created_at desc
limit p_limit offset p_offset;
end;
$$ language plpgsql;

-- ========================================
-- Maintenance Functions
-- ========================================
-- Expire stale active jobs
create or replace function queue.expire_jobs () returns table (out_expired_count bigint, out_queue_name text) as $$ begin return query with expired as (
    update queue.jobs j
    set state = 'expired',
      completed_at = now(),
      last_error = 'job expired after timeout'
    where j.state = 'active'
      and j.expire_at is not null
      and j.expire_at < now()
    returning j.queue_name
  )
select count(*) as expired_count,
  e.queue_name
from expired e
group by e.queue_name;
end;
$$ language plpgsql;

-- Clean up old completed/failed jobs based on retention settings
create or replace function queue.cleanup () returns table (out_deleted_count bigint, out_queue_name text) as $$ begin return query with deleted as (
    delete from queue.jobs j using queue.queues q
    where j.queue_name = q.name
      and j.completed_at is not null
      and (
        (
          j.state = 'completed'
          and j.completed_at < now() - (q.retain_completed * interval '1 second')
        )
        or (
          j.state in ('failed', 'expired', 'cancelled')
          and j.completed_at < now() - (q.retain_failed * interval '1 second')
        )
      )
    returning j.queue_name
  )
select count(*) as deleted_count,
  d.queue_name
from deleted d
group by d.queue_name;
end;
$$ language plpgsql;

-- Purge all jobs from a queue (by state)
create or replace function queue.purge (p_queue_name text, p_state text default null) returns bigint as $$
declare deleted_count bigint;
begin if p_state is null then
delete from queue.jobs
where queue_name = p_queue_name;
else
delete from queue.jobs
where queue_name = p_queue_name
  and state = p_state;
end if;
get diagnostics deleted_count = row_count;
return deleted_count;
end;
$$ language plpgsql;

-- ========================================
-- Statistics Functions
-- ========================================
-- Get queue statistics
create or replace function queue.get_stats (p_queue_name text default null) returns table (
  out_queue_name text,
  out_created integer,
  out_active integer,
  out_completed integer,
  out_failed integer,
  out_expired integer,
  out_cancelled integer,
  out_oldest_job_age interval,
  out_avg_completion_time interval
) as $$ begin return query
select j.queue_name,
  count(*) filter (
    where j.state = 'created'
  )::integer as created,
  count(*) filter (
    where j.state = 'active'
  )::integer as active,
  count(*) filter (
    where j.state = 'completed'
  )::integer as completed,
  count(*) filter (
    where j.state = 'failed'
  )::integer as failed,
  count(*) filter (
    where j.state = 'expired'
  )::integer as expired,
  count(*) filter (
    where j.state = 'cancelled'
  )::integer as cancelled,
  max(now() - j.created_at) filter (
    where j.state = 'created'
  ) as oldest_job_age,
  avg(j.completed_at - j.started_at) filter (
    where j.state = 'completed'
      and j.started_at is not null
  ) as avg_completion_time
from queue.jobs j
where p_queue_name is null
  or j.queue_name = p_queue_name
group by j.queue_name
order by j.queue_name;
end;
$$ language plpgsql;

-- Get recent job activity (for monitoring)
create or replace function queue.get_activity (
  p_queue_name text default null,
  p_minutes integer default 60
) returns table (
  out_queue_name text,
  out_time_bucket timestamptz,
  out_jobs_created integer,
  out_jobs_completed integer,
  out_jobs_failed integer
) as $$ begin return query with time_buckets as (
    select generate_series(
        date_trunc(
          'minute',
          now() - (p_minutes * interval '1 minute')
        ),
        date_trunc('minute', now()),
        interval '1 minute'
      ) as bucket
  )
select coalesce(j.queue_name, p_queue_name) as queue_name,
  tb.bucket as time_bucket,
  count(*) filter (
    where j.created_at >= tb.bucket
      and j.created_at < tb.bucket + interval '1 minute'
  )::integer as jobs_created,
  count(*) filter (
    where j.completed_at >= tb.bucket
      and j.completed_at < tb.bucket + interval '1 minute'
      and j.state = 'completed'
  )::integer as jobs_completed,
  count(*) filter (
    where j.completed_at >= tb.bucket
      and j.completed_at < tb.bucket + interval '1 minute'
      and j.state = 'failed'
  )::integer as jobs_failed
from time_buckets tb
  left join queue.jobs j on (
    (
      p_queue_name is null
      or j.queue_name = p_queue_name
    )
    and (
      (
        j.created_at >= tb.bucket
        and j.created_at < tb.bucket + interval '1 minute'
      )
      or (
        j.completed_at >= tb.bucket
        and j.completed_at < tb.bucket + interval '1 minute'
      )
    )
  )
group by tb.bucket,
  j.queue_name
order by tb.bucket desc;
end;
$$ language plpgsql;

-- ========================================
-- Schedule Functions (Cron-like)
-- ========================================
-- Create or update a schedule
create or replace function queue.create_schedule (
  p_name text,
  p_queue_name text,
  p_cron text,
  p_data jsonb default '{}',
  p_timezone text default 'utc',
  p_priority integer default 0
) returns table (
  out_name text,
  out_queue_name text,
  out_cron text,
  out_timezone text,
  out_data jsonb,
  out_priority integer,
  out_enabled boolean,
  out_created_at timestamptz
) as $$ begin -- validate queue exists
  if not exists (
    select 1
    from queue.queues q
    where q.name = p_queue_name
  ) then raise exception 'Queue "%" does not exist',
  p_queue_name;
end if;
return query
insert into queue.schedules (
    name,
    queue_name,
    cron,
    timezone,
    data,
    priority
  )
values (
    p_name,
    p_queue_name,
    p_cron,
    p_timezone,
    p_data,
    p_priority
  ) on conflict (name) do
update
set queue_name = excluded.queue_name,
  cron = excluded.cron,
  timezone = excluded.timezone,
  data = excluded.data,
  priority = excluded.priority,
  updated_at = now()
returning schedules.name,
  schedules.queue_name,
  schedules.cron,
  schedules.timezone,
  schedules.data,
  schedules.priority,
  schedules.enabled,
  schedules.created_at;
end;
$$ language plpgsql;

-- Enable/disable a schedule
create or replace function queue.set_schedule_enabled (p_name text, p_enabled boolean) returns boolean as $$
declare updated_count integer;
begin
update queue.schedules
set enabled = p_enabled,
  updated_at = now()
where name = p_name;
get diagnostics updated_count = row_count;
return updated_count > 0;
end;
$$ language plpgsql;

-- Delete a schedule
create or replace function queue.delete_schedule (p_name text) returns boolean as $$
declare deleted_count integer;
begin
delete from queue.schedules
where name = p_name;
get diagnostics deleted_count = row_count;
return deleted_count > 0;
end;
$$ language plpgsql;

-- List all schedules
create or replace function queue.list_schedules (p_queue_name text default null) returns table (
  out_name text,
  out_queue_name text,
  out_cron text,
  out_timezone text,
  out_data jsonb,
  out_priority integer,
  out_enabled boolean,
  out_last_run_at timestamptz,
  out_next_run_at timestamptz,
  out_created_at timestamptz
) as $$ begin return query
select s.name,
  s.queue_name,
  s.cron,
  s.timezone,
  s.data,
  s.priority,
  s.enabled,
  s.last_run_at,
  s.next_run_at,
  s.created_at
from queue.schedules s
where p_queue_name is null
  or s.queue_name = p_queue_name
order by s.name;
end;
$$ language plpgsql;

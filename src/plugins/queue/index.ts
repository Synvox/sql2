import * as fsp from "node:fs/promises";
import { getSql } from "../../sql2.ts";

// ========================================
// Types
// ========================================

/**
 * Job state in the queue
 */
export type JobState =
  | "created"
  | "active"
  | "completed"
  | "failed"
  | "expired"
  | "cancelled";

/**
 * Queue configuration
 */
export interface Queue {
  name: string;
  retryLimit: number;
  retryDelay: number;
  retryBackoff: boolean;
  expireIn: number;
  retainCompleted: number;
  retainFailed: number;
  deadLetter: string | null;
  createdAt: Date;
}

/**
 * Queue with job counts
 */
export interface QueueWithCounts extends Queue {
  jobCounts: Record<JobState, number> | null;
}

/**
 * Queue listing with basic job counts
 */
export interface QueueSummary {
  name: string;
  retryLimit: number;
  expireIn: number;
  deadLetter: string | null;
  createdAt: Date;
  createdCount: number;
  activeCount: number;
  completedCount: number;
  failedCount: number;
}

/**
 * Options for creating a queue
 */
export interface CreateQueueOptions {
  retryLimit?: number;
  retryDelay?: number;
  retryBackoff?: boolean;
  expireIn?: number;
  retainCompleted?: number;
  retainFailed?: number;
  deadLetter?: string;
}

/**
 * A job in the queue
 */
export interface Job<T = unknown> {
  id: string;
  queueName: string;
  state: JobState;
  data: T;
  output: unknown | null;
  singletonKey: string | null;
  priority: number;
  retryCount: number;
  retryLimit: number;
  startAfter: Date;
  expireAt: Date | null;
  createdAt: Date;
  startedAt: Date | null;
  completedAt: Date | null;
  lastError: string | null;
  deadLetterId: string | null;
}

/**
 * A fetched job ready for processing
 */
export interface FetchedJob<T = unknown> {
  id: string;
  queueName: string;
  data: T;
  singletonKey: string | null;
  priority: number;
  retryCount: number;
  createdAt: Date;
  startedAt: Date;
  expireAt: Date;
}

/**
 * Options for sending a job
 */
export interface SendOptions {
  /** Delay in seconds before the job becomes available */
  delay?: number;
  /** Specific time when the job should become available */
  startAfter?: Date;
  /** Unique key for singleton jobs (only one active job per key) */
  singletonKey?: string;
  /** Job priority (higher = more important) */
  priority?: number;
  /** Override queue's retry limit */
  retryLimit?: number;
  /** Override queue's retry delay */
  retryDelay?: number;
  /** Override queue's retry backoff setting */
  retryBackoff?: boolean;
}

/**
 * Result of sending a job
 */
export interface SendResult {
  id: string;
  queueName: string;
  state: JobState;
  singletonKey: string | null;
  priority: number;
  startAfter: Date;
  createdAt: Date;
}

/**
 * Result of completing a job
 */
export interface CompleteResult {
  id: string;
  queueName: string;
  state: JobState;
  completedAt: Date;
}

/**
 * Result of failing a job
 */
export interface FailResult {
  id: string;
  queueName: string;
  state: JobState;
  retryCount: number;
  willRetry: boolean;
  nextRetryAt: Date | null;
}

/**
 * Result of cancelling a job
 */
export interface CancelResult {
  id: string;
  queueName: string | null;
  previousState: JobState | null;
  cancelled: boolean;
}

/**
 * Job listing entry (summary)
 */
export interface JobListEntry<T = unknown> {
  id: string;
  state: JobState;
  data: T;
  priority: number;
  retryCount: number;
  startAfter: Date;
  createdAt: Date;
  startedAt: Date | null;
  completedAt: Date | null;
  lastError: string | null;
}

/**
 * Result of expiring jobs
 */
export interface ExpireResult {
  expiredCount: number;
  queueName: string;
}

/**
 * Result of cleanup
 */
export interface CleanupResult {
  deletedCount: number;
  queueName: string;
}

/**
 * Queue statistics
 */
export interface QueueStats {
  queueName: string;
  created: number;
  active: number;
  completed: number;
  failed: number;
  expired: number;
  cancelled: number;
  oldestJobAge: string | null;
  avgCompletionTime: string | null;
}

/**
 * Activity data point for monitoring
 */
export interface ActivityDataPoint {
  queueName: string;
  timeBucket: Date;
  jobsCreated: number;
  jobsCompleted: number;
  jobsFailed: number;
}

/**
 * Schedule configuration
 */
export interface Schedule {
  name: string;
  queueName: string;
  cron: string;
  timezone: string;
  data: unknown;
  priority: number;
  enabled: boolean;
  lastRunAt: Date | null;
  nextRunAt: Date | null;
  createdAt: Date;
}

/**
 * Options for creating a schedule
 */
export interface CreateScheduleOptions {
  timezone?: string;
  data?: unknown;
  priority?: number;
}

// ========================================
// Plugin Installation
// ========================================

/**
 * Installs the queue schema and helper functions.
 * Call this once before using any queue functions.
 */
export async function queuePlugin() {
  const sql = getSql({ camelize: false });

  const sqlScript = await fsp.readFile(
    new URL("./queue.sql", import.meta.url),
    "utf-8",
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

// ========================================
// Queue Management
// ========================================

/**
 * Creates or updates a queue with the given configuration.
 *
 * @param name - Queue name
 * @param options - Queue configuration options
 * @returns The created/updated queue
 */
export async function createQueue(
  name: string,
  options: CreateQueueOptions = {},
): Promise<Queue> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      queue.create_queue (
        ${name},
        ${options.retryLimit ?? 3},
        ${options.retryDelay ?? 60},
        ${options.retryBackoff ?? true},
        ${options.expireIn ?? 900},
        ${options.retainCompleted ?? 86400},
        ${options.retainFailed ?? 604800},
        ${options.deadLetter ?? null}
      )
  `.first<{
    out_name: string;
    out_retry_limit: number;
    out_retry_delay: number;
    out_retry_backoff: boolean;
    out_expire_in: number;
    out_retain_completed: number;
    out_retain_failed: number;
    out_dead_letter: string | null;
    out_created_at: Date;
  }>();

  return {
    name: row!.out_name,
    retryLimit: row!.out_retry_limit,
    retryDelay: row!.out_retry_delay,
    retryBackoff: row!.out_retry_backoff,
    expireIn: row!.out_expire_in,
    retainCompleted: row!.out_retain_completed,
    retainFailed: row!.out_retain_failed,
    deadLetter: row!.out_dead_letter,
    createdAt: row!.out_created_at,
  };
}

/**
 * Gets a queue by name with job counts.
 *
 * @param name - Queue name
 * @returns The queue with counts, or null if not found
 */
export async function getQueue(name: string): Promise<QueueWithCounts | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      queue.get_queue (${name})
  `.first<{
    out_name: string;
    out_retry_limit: number;
    out_retry_delay: number;
    out_retry_backoff: boolean;
    out_expire_in: number;
    out_retain_completed: number;
    out_retain_failed: number;
    out_dead_letter: string | null;
    out_created_at: Date;
    out_job_counts: Record<string, number> | null;
  }>();

  if (!row) return null;

  return {
    name: row.out_name,
    retryLimit: row.out_retry_limit,
    retryDelay: row.out_retry_delay,
    retryBackoff: row.out_retry_backoff,
    expireIn: row.out_expire_in,
    retainCompleted: row.out_retain_completed,
    retainFailed: row.out_retain_failed,
    deadLetter: row.out_dead_letter,
    createdAt: row.out_created_at,
    jobCounts: row.out_job_counts as Record<JobState, number> | null,
  };
}

/**
 * Lists all queues with basic job counts.
 *
 * @returns Array of queue summaries
 */
export async function listQueues(): Promise<QueueSummary[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.list_queues ()
  `.all<{
    out_name: string;
    out_retry_limit: number;
    out_expire_in: number;
    out_dead_letter: string | null;
    out_created_at: Date;
    out_created_count: number;
    out_active_count: number;
    out_completed_count: number;
    out_failed_count: number;
  }>();

  return rows.map((row) => ({
    name: row.out_name,
    retryLimit: row.out_retry_limit,
    expireIn: row.out_expire_in,
    deadLetter: row.out_dead_letter,
    createdAt: row.out_created_at,
    createdCount: Number(row.out_created_count),
    activeCount: Number(row.out_active_count),
    completedCount: Number(row.out_completed_count),
    failedCount: Number(row.out_failed_count),
  }));
}

/**
 * Deletes a queue and all its jobs.
 *
 * @param name - Queue name to delete
 * @returns True if queue was deleted
 */
export async function deleteQueue(name: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      queue.delete_queue (${name}) as deleted
  `.first<{
    deleted: boolean;
  }>();
  return row?.deleted ?? false;
}

// ========================================
// Job Management
// ========================================

/**
 * Sends a job to a queue.
 *
 * @param queueName - Target queue name
 * @param data - Job payload
 * @param options - Job options (delay, priority, singleton, etc.)
 * @returns The created job, or null if singleton key conflict
 */
export async function send<D = unknown>(
  queueName: string,
  data: D,
  options: SendOptions = {},
): Promise<SendResult | null> {
  const sql = getSql({ camelize: false });
  const optionsJson = JSON.stringify({
    delay: options.delay,
    start_after: options.startAfter?.toISOString(),
    singleton_key: options.singletonKey,
    priority: options.priority,
    retry_limit: options.retryLimit,
    retry_delay: options.retryDelay,
    retry_backoff: options.retryBackoff,
  });

  const row = await sql`
    select
      *
    from
      queue.send (
        ${queueName},
        ${JSON.stringify(data)}::jsonb,
        ${optionsJson}::jsonb
      )
  `.first<{
    out_id: string;
    out_queue_name: string;
    out_state: JobState;
    out_singleton_key: string | null;
    out_priority: number;
    out_start_after: Date;
    out_created_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.out_id,
    queueName: row.out_queue_name,
    state: row.out_state,
    singletonKey: row.out_singleton_key,
    priority: row.out_priority,
    startAfter: row.out_start_after,
    createdAt: row.out_created_at,
  };
}

/**
 * Sends multiple jobs to a queue in a batch.
 *
 * @param queueName - Target queue name
 * @param jobs - Array of job data and options
 * @returns Array of created jobs
 */
export async function sendBatch<D = unknown>(
  queueName: string,
  jobs: Array<{ data: D; options?: SendOptions }>,
): Promise<SendResult[]> {
  const sql = getSql({ camelize: false });
  const jobsJson = JSON.stringify(
    jobs.map((job) => ({
      data: JSON.stringify(job.data),
      options: job.options
        ? {
            delay: job.options.delay,
            start_after: job.options.startAfter?.toISOString(),
            singleton_key: job.options.singletonKey,
            priority: job.options.priority,
            retry_limit: job.options.retryLimit,
            retry_delay: job.options.retryDelay,
            retry_backoff: job.options.retryBackoff,
          }
        : undefined,
    })),
  );

  const rows = await sql`
    select
      *
    from
      queue.send_batch (
        ${queueName},
        ${jobsJson}::jsonb
      )
  `.all<{
    out_id: string;
    out_queue_name: string;
    out_state: JobState;
    out_priority: number;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    id: row.out_id,
    queueName: row.out_queue_name,
    state: row.out_state,
    singletonKey: null,
    priority: row.out_priority,
    startAfter: row.out_created_at, // Approximation, actual value not returned
    createdAt: row.out_created_at,
  }));
}

/**
 * Fetches jobs from a queue for processing.
 * Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access.
 *
 * @param queueName - Queue to fetch from
 * @param batchSize - Number of jobs to fetch (default: 1)
 * @returns Array of fetched jobs
 */
export async function fetch<D = unknown>(
  queueName: string,
  batchSize: number = 1,
): Promise<FetchedJob<D>[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.fetch (
        ${queueName},
        ${batchSize}
      )
  `.all<{
    out_id: string;
    out_queue_name: string;
    out_data: D;
    out_singleton_key: string | null;
    out_priority: number;
    out_retry_count: number;
    out_created_at: Date;
    out_started_at: Date;
    out_expire_at: Date;
  }>();

  return rows.map((row) => ({
    id: row.out_id,
    queueName: row.out_queue_name,
    data: row.out_data,
    singletonKey: row.out_singleton_key,
    priority: row.out_priority,
    retryCount: row.out_retry_count,
    createdAt: row.out_created_at,
    startedAt: row.out_started_at,
    expireAt: row.out_expire_at,
  }));
}

/**
 * Marks a job as completed.
 *
 * @param jobId - Job ID to complete
 * @param output - Optional output data to store
 * @returns The completion result, or null if job not found/not active
 */
export async function complete(
  jobId: string,
  output?: unknown,
): Promise<CompleteResult | null> {
  const sql = getSql({ camelize: false });
  const outputJson = output !== undefined ? JSON.stringify(output) : null;

  const row = await sql`
    select
      *
    from
      queue.complete (
        ${jobId}::uuid,
        ${outputJson}::jsonb
      )
  `.first<{
    out_id: string;
    out_queue_name: string;
    out_state: JobState;
    out_completed_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.out_id,
    queueName: row.out_queue_name,
    state: row.out_state,
    completedAt: row.out_completed_at,
  };
}

/**
 * Marks a job as failed. May trigger a retry based on configuration.
 *
 * @param jobId - Job ID to fail
 * @param error - Optional error message
 * @returns The failure result with retry information
 */
export async function fail(
  jobId: string,
  error?: string,
): Promise<FailResult | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      queue.fail (
        ${jobId}::uuid,
        ${error ?? null}
      )
  `.first<{
    out_id: string;
    out_queue_name: string;
    out_state: JobState;
    out_retry_count: number;
    out_will_retry: boolean;
    out_next_retry_at: Date | null;
  }>();

  if (!row) return null;

  return {
    id: row.out_id,
    queueName: row.out_queue_name,
    state: row.out_state,
    retryCount: row.out_retry_count,
    willRetry: row.out_will_retry,
    nextRetryAt: row.out_next_retry_at,
  };
}

/**
 * Cancels a job (if it's in created or active state).
 *
 * @param jobId - Job ID to cancel
 * @returns The cancellation result
 */
export async function cancel(jobId: string): Promise<CancelResult> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      queue.cancel (${jobId}::uuid)
  `.first<{
    out_id: string;
    out_queue_name: string | null;
    out_previous_state: JobState | null;
    out_cancelled: boolean;
  }>();

  return {
    id: row!.out_id,
    queueName: row!.out_queue_name,
    previousState: row!.out_previous_state,
    cancelled: row!.out_cancelled,
  };
}

/**
 * Gets a job by ID.
 *
 * @param jobId - Job ID to retrieve
 * @returns The job, or null if not found
 */
export async function getJob<D = unknown>(
  jobId: string,
): Promise<Job<D> | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      queue.get_job (${jobId}::uuid)
  `.first<{
    out_id: string;
    out_queue_name: string;
    out_state: JobState;
    out_data: D;
    out_output: unknown | null;
    out_singleton_key: string | null;
    out_priority: number;
    out_retry_count: number;
    out_retry_limit: number;
    out_start_after: Date;
    out_expire_at: Date | null;
    out_created_at: Date;
    out_started_at: Date | null;
    out_completed_at: Date | null;
    out_last_error: string | null;
    out_dead_letter_id: string | null;
  }>();

  if (!row) return null;

  return {
    id: row.out_id,
    queueName: row.out_queue_name,
    state: row.out_state,
    data: row.out_data,
    output: row.out_output,
    singletonKey: row.out_singleton_key,
    priority: row.out_priority,
    retryCount: row.out_retry_count,
    retryLimit: row.out_retry_limit,
    startAfter: row.out_start_after,
    expireAt: row.out_expire_at,
    createdAt: row.out_created_at,
    startedAt: row.out_started_at,
    completedAt: row.out_completed_at,
    lastError: row.out_last_error,
    deadLetterId: row.out_dead_letter_id,
  };
}

/**
 * Lists jobs in a queue.
 *
 * @param queueName - Queue to list jobs from
 * @param options - Filter options
 * @returns Array of job list entries
 */
export async function listJobs<D = unknown>(
  queueName: string,
  options: { state?: JobState; limit?: number; offset?: number } = {},
): Promise<JobListEntry<D>[]> {
  const sql = getSql({ camelize: false });
  const { state = null, limit = 100, offset = 0 } = options;

  const rows = await sql`
    select
      *
    from
      queue.list_jobs (
        ${queueName},
        ${state},
        ${limit},
        ${offset}
      )
  `.all<{
    out_id: string;
    out_state: JobState;
    out_data: D;
    out_priority: number;
    out_retry_count: number;
    out_start_after: Date;
    out_created_at: Date;
    out_started_at: Date | null;
    out_completed_at: Date | null;
    out_last_error: string | null;
  }>();

  return rows.map((row) => ({
    id: row.out_id,
    state: row.out_state,
    data: row.out_data,
    priority: row.out_priority,
    retryCount: row.out_retry_count,
    startAfter: row.out_start_after,
    createdAt: row.out_created_at,
    startedAt: row.out_started_at,
    completedAt: row.out_completed_at,
    lastError: row.out_last_error,
  }));
}

// ========================================
// Maintenance
// ========================================

/**
 * Expires jobs that have exceeded their timeout.
 * Should be called periodically (e.g., every minute).
 *
 * @returns Array of results by queue
 */
export async function expireJobs(): Promise<ExpireResult[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.expire_jobs ()
  `.all<{
    out_expired_count: number;
    out_queue_name: string;
  }>();

  return rows.map((row) => ({
    expiredCount: Number(row.out_expired_count),
    queueName: row.out_queue_name,
  }));
}

/**
 * Cleans up old completed/failed jobs based on retention settings.
 * Should be called periodically (e.g., every hour).
 *
 * @returns Array of results by queue
 */
export async function cleanup(): Promise<CleanupResult[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.cleanup ()
  `.all<{
    out_deleted_count: number;
    out_queue_name: string;
  }>();

  return rows.map((row) => ({
    deletedCount: Number(row.out_deleted_count),
    queueName: row.out_queue_name,
  }));
}

/**
 * Purges all jobs from a queue (optionally filtered by state).
 *
 * @param queueName - Queue to purge
 * @param state - Optional state to filter by
 * @returns Number of deleted jobs
 */
export async function purge(
  queueName: string,
  state?: JobState,
): Promise<number> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      queue.purge (
        ${queueName},
        ${state ?? null}
      ) as deleted_count
  `.first<{ deleted_count: number }>();
  return Number(row?.deleted_count ?? 0);
}

// ========================================
// Statistics
// ========================================

/**
 * Gets statistics for a queue or all queues.
 *
 * @param queueName - Optional queue name to filter by
 * @returns Array of queue statistics
 */
export async function getStats(queueName?: string): Promise<QueueStats[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.get_stats (${queueName ?? null})
  `.all<{
    out_queue_name: string;
    out_created: number;
    out_active: number;
    out_completed: number;
    out_failed: number;
    out_expired: number;
    out_cancelled: number;
    out_oldest_job_age: string | null;
    out_avg_completion_time: string | null;
  }>();

  return rows.map((row) => ({
    queueName: row.out_queue_name,
    created: row.out_created,
    active: row.out_active,
    completed: row.out_completed,
    failed: row.out_failed,
    expired: row.out_expired,
    cancelled: row.out_cancelled,
    oldestJobAge: row.out_oldest_job_age,
    avgCompletionTime: row.out_avg_completion_time,
  }));
}

/**
 * Gets recent job activity for monitoring.
 *
 * @param queueName - Optional queue name to filter by
 * @param minutes - Number of minutes of history (default: 60)
 * @returns Array of activity data points
 */
export async function getActivity(
  queueName?: string,
  minutes: number = 60,
): Promise<ActivityDataPoint[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.get_activity (
        ${queueName ?? null},
        ${minutes}
      )
  `.all<{
    out_queue_name: string;
    out_time_bucket: Date;
    out_jobs_created: number;
    out_jobs_completed: number;
    out_jobs_failed: number;
  }>();

  return rows.map((row) => ({
    queueName: row.out_queue_name,
    timeBucket: row.out_time_bucket,
    jobsCreated: row.out_jobs_created,
    jobsCompleted: row.out_jobs_completed,
    jobsFailed: row.out_jobs_failed,
  }));
}

// ========================================
// Schedules
// ========================================

/**
 * Creates or updates a schedule for recurring jobs.
 *
 * @param name - Schedule name
 * @param queueName - Target queue
 * @param cron - Cron expression
 * @param options - Schedule options
 * @returns The created/updated schedule
 */
export async function createSchedule(
  name: string,
  queueName: string,
  cron: string,
  options: CreateScheduleOptions = {},
): Promise<Schedule> {
  const sql = getSql({ camelize: false });
  const dataJson = JSON.stringify(options.data ?? {});

  const row = await sql`
    select
      *
    from
      queue.create_schedule (
        ${name},
        ${queueName},
        ${cron},
        ${dataJson}::jsonb,
        ${options.timezone ?? "UTC"},
        ${options.priority ?? 0}
      )
  `.first<{
    out_name: string;
    out_queue_name: string;
    out_cron: string;
    out_timezone: string;
    out_data: unknown;
    out_priority: number;
    out_enabled: boolean;
    out_created_at: Date;
  }>();

  return {
    name: row!.out_name,
    queueName: row!.out_queue_name,
    cron: row!.out_cron,
    timezone: row!.out_timezone,
    data: row!.out_data,
    priority: row!.out_priority,
    enabled: row!.out_enabled,
    lastRunAt: null,
    nextRunAt: null,
    createdAt: row!.out_created_at,
  };
}

/**
 * Enables or disables a schedule.
 *
 * @param name - Schedule name
 * @param enabled - Whether to enable or disable
 * @returns True if schedule was updated
 */
export async function setScheduleEnabled(
  name: string,
  enabled: boolean,
): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      queue.set_schedule_enabled (
        ${name},
        ${enabled}
      ) as updated
  `.first<{ updated: boolean }>();
  return row?.updated ?? false;
}

/**
 * Deletes a schedule.
 *
 * @param name - Schedule name to delete
 * @returns True if schedule was deleted
 */
export async function deleteSchedule(name: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      queue.delete_schedule (${name}) as deleted
  `.first<{ deleted: boolean }>();
  return row?.deleted ?? false;
}

/**
 * Lists all schedules.
 *
 * @param queueName - Optional queue name to filter by
 * @returns Array of schedules
 */
export async function listSchedules(queueName?: string): Promise<Schedule[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      queue.list_schedules (${queueName ?? null})
  `.all<{
    out_name: string;
    out_queue_name: string;
    out_cron: string;
    out_timezone: string;
    out_data: unknown;
    out_priority: number;
    out_enabled: boolean;
    out_last_run_at: Date | null;
    out_next_run_at: Date | null;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    name: row.out_name,
    queueName: row.out_queue_name,
    cron: row.out_cron,
    timezone: row.out_timezone,
    data: row.out_data,
    priority: row.out_priority,
    enabled: row.out_enabled,
    lastRunAt: row.out_last_run_at,
    nextRunAt: row.out_next_run_at,
    createdAt: row.out_created_at,
  }));
}

// ========================================
// Worker Utilities
// ========================================

/**
 * Options for the work function
 */
export interface WorkOptions {
  /** Number of jobs to fetch at once */
  batchSize?: number;
  /** How long to wait between polls when queue is empty (ms) */
  pollingInterval?: number;
  /** Maximum concurrent jobs to process */
  concurrency?: number;
}

/**
 * Worker handler function
 */
export type WorkerHandler<T = unknown, R = unknown> = (
  job: FetchedJob<T>,
) => Promise<R>;

/**
 * Creates a simple polling worker for processing jobs.
 * Returns a control object to stop the worker.
 *
 * @param queueName - Queue to process
 * @param handler - Job handler function
 * @param options - Worker options
 * @returns Control object with stop() method
 */
export function work<D = unknown, R = unknown>(
  queueName: string,
  handler: WorkerHandler<D, R>,
  options: WorkOptions = {},
): { stop: () => void } {
  const { batchSize = 1, pollingInterval = 1000 } = options;

  let running = true;

  const poll = async () => {
    while (running) {
      try {
        const jobs = await fetch<D>(queueName, batchSize);

        if (jobs.length === 0) {
          // No jobs available, wait before polling again
          await new Promise((resolve) => setTimeout(resolve, pollingInterval));
          continue;
        }

        // Process jobs
        await Promise.all(
          jobs.map(async (job) => {
            try {
              const output = await handler(job);
              await complete(job.id, output);
            } catch (err) {
              const error = err instanceof Error ? err.message : String(err);
              await fail(job.id, error);
            }
          }),
        );
      } catch (err) {
        // Log error but keep running
        console.error("Worker error:", err);
        await new Promise((resolve) => setTimeout(resolve, pollingInterval));
      }
    }
  };

  // Start polling
  poll();

  return {
    stop: () => {
      running = false;
    },
  };
}

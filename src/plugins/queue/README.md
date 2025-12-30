# Queue Plugin

A robust PostgreSQL-based job queue using `SELECT FOR UPDATE SKIP LOCKED`, inspired by [pg-boss](https://github.com/timgit/pg-boss). This plugin provides reliable, transactional job processing with automatic retries, priority queues, and dead letter handling.

## Overview

The Queue plugin provides:

- **Atomic job fetching** using `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent processing
- **Priority queues** to process high-priority jobs first
- **Automatic retries** with exponential backoff
- **Singleton jobs** to prevent duplicate processing
- **Delayed jobs** for scheduling future work
- **Dead letter queues** for failed job handling
- **Job expiration** to prevent stuck jobs
- **Batch operations** for efficient bulk inserts
- **Scheduled jobs** with cron expressions (scheduler integration required)
- **Statistics and monitoring** for observability

## Installation

```typescript
import { queuePlugin } from "sql2/queue";

// Install the plugin (creates schema and functions)
await queuePlugin();
```

## Quick Start

### 1. Create a queue

```typescript
import { createQueue } from "sql2/queue";

await createQueue("email-notifications", {
  retryLimit: 3,
  retryDelay: 60, // 60 seconds
  retryBackoff: true, // Exponential backoff
  expireIn: 900, // 15 minutes timeout
});
```

### 2. Send jobs

```typescript
import { send } from "sql2/queue";

// Simple job
await send("email-notifications", {
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up.",
});

// Job with options
await send(
  "email-notifications",
  { to: "user@example.com", template: "reminder" },
  {
    priority: 10, // Higher priority
    delay: 3600, // Start after 1 hour
    singletonKey: "user-123-reminder", // Prevent duplicates
  },
);
```

### 3. Process jobs

```typescript
import { fetch, complete, fail } from "sql2/queue";

// Fetch a batch of jobs
const jobs = await fetch("email-notifications", 5);

for (const job of jobs) {
  try {
    await sendEmail(job.data);
    await complete(job.id, { sent: true });
  } catch (error) {
    await fail(job.id, error.message);
  }
}
```

### 4. Use the built-in worker (optional)

```typescript
import { work } from "sql2/queue";

const worker = work(
  "email-notifications",
  async (job) => {
    await sendEmail(job.data);
    return { sent: true }; // Output stored on job
  },
  {
    batchSize: 5,
    pollingInterval: 1000,
  },
);

// Later: stop the worker gracefully
worker.stop();
```

## API Reference

### Queue Management

#### `createQueue(name, options?)`

Creates or updates a queue with the given configuration.

```typescript
interface CreateQueueOptions {
  retryLimit?: number; // Max retries (default: 3)
  retryDelay?: number; // Delay between retries in seconds (default: 60)
  retryBackoff?: boolean; // Use exponential backoff (default: true)
  expireIn?: number; // Job timeout in seconds (default: 900)
  retainCompleted?: number; // Keep completed jobs for N seconds (default: 86400)
  retainFailed?: number; // Keep failed jobs for N seconds (default: 604800)
  deadLetter?: string; // Dead letter queue name
}

const queue = await createQueue("my-queue", {
  retryLimit: 5,
  retryDelay: 30,
  deadLetter: "failed-jobs",
});
```

#### `getQueue(name)`

Gets a queue by name with job counts.

```typescript
const queue = await getQueue("my-queue");
// Returns: { name, retryLimit, ..., jobCounts: { created: 5, active: 2, ... } }
```

#### `listQueues()`

Lists all queues with basic job counts.

```typescript
const queues = await listQueues();
// Returns: [{ name, createdCount, activeCount, completedCount, failedCount, ... }]
```

#### `deleteQueue(name)`

Deletes a queue and all its jobs.

```typescript
const deleted = await deleteQueue("old-queue"); // true/false
```

### Job Operations

#### `send(queueName, data, options?)`

Sends a job to a queue.

```typescript
interface SendOptions {
  delay?: number; // Delay in seconds
  startAfter?: Date; // Specific start time
  singletonKey?: string; // Unique key (prevents duplicates)
  priority?: number; // Higher = more important
  retryLimit?: number; // Override queue setting
  retryDelay?: number; // Override queue setting
  retryBackoff?: boolean; // Override queue setting
}

const result = await send(
  "my-queue",
  { task: "process" },
  {
    priority: 10,
    delay: 300,
    singletonKey: "unique-task-123",
  },
);
// Returns: { id, queueName, state, startAfter, ... } or null if singleton conflict
```

#### `sendBatch(queueName, jobs)`

Sends multiple jobs efficiently.

```typescript
const results = await sendBatch("my-queue", [
  { data: { n: 1 } },
  { data: { n: 2 }, options: { priority: 5 } },
  { data: { n: 3 }, options: { delay: 60 } },
]);
```

#### `fetch(queueName, batchSize?)`

Fetches jobs for processing using `SELECT FOR UPDATE SKIP LOCKED`.

```typescript
const jobs = await fetch("my-queue", 10);

// Each job contains:
// - id: Job UUID
// - data: Job payload
// - retryCount: Number of previous attempts
// - createdAt, startedAt, expireAt: Timestamps
```

Jobs are returned in priority order (highest first), then by creation time.

#### `complete(jobId, output?)`

Marks a job as successfully completed.

```typescript
const result = await complete(job.id, { processedAt: new Date() });
```

#### `fail(jobId, error?)`

Marks a job as failed. If retries remain, the job will be rescheduled.

```typescript
const result = await fail(job.id, "Connection timeout");
// Returns: { id, state, retryCount, willRetry, nextRetryAt }
```

#### `cancel(jobId)`

Cancels a created or active job.

```typescript
const result = await cancel(job.id);
// Returns: { id, cancelled, previousState }
```

#### `getJob(jobId)`

Gets full job details by ID.

```typescript
const job = await getJob("job-uuid");
// Returns: { id, queueName, state, data, output, retryCount, lastError, ... }
```

#### `listJobs(queueName, options?)`

Lists jobs in a queue with optional filtering.

```typescript
const jobs = await listJobs("my-queue", {
  state: "failed",
  limit: 50,
  offset: 0,
});
```

### Maintenance

#### `expireJobs()`

Expires active jobs that have exceeded their timeout. Run periodically.

```typescript
const results = await expireJobs();
// Returns: [{ queueName, expiredCount }]
```

#### `cleanup()`

Removes old completed/failed jobs based on retention settings. Run periodically.

```typescript
const results = await cleanup();
// Returns: [{ queueName, deletedCount }]
```

#### `purge(queueName, state?)`

Removes all jobs from a queue (optionally filtered by state).

```typescript
// Purge all jobs
const count = await purge("my-queue");

// Purge only completed jobs
const count = await purge("my-queue", "completed");
```

### Statistics

#### `getStats(queueName?)`

Gets job statistics for a queue or all queues.

```typescript
const stats = await getStats("my-queue");
// Returns: [{
//   queueName, created, active, completed, failed, expired, cancelled,
//   oldestJobAge, avgCompletionTime
// }]
```

#### `getActivity(queueName?, minutes?)`

Gets per-minute job activity for monitoring dashboards.

```typescript
const activity = await getActivity("my-queue", 60);
// Returns: [{ queueName, timeBucket, jobsCreated, jobsCompleted, jobsFailed }]
```

### Schedules

#### `createSchedule(name, queueName, cron, options?)`

Creates a schedule for recurring jobs. Note: You'll need a separate scheduler to check and create jobs based on these schedules.

```typescript
const schedule = await createSchedule(
  "daily-cleanup",
  "maintenance",
  "0 0 * * *", // Midnight daily
  {
    timezone: "America/New_York",
    data: { type: "cleanup" },
    priority: 5,
  },
);
```

#### `setScheduleEnabled(name, enabled)`

Enables or disables a schedule.

```typescript
await setScheduleEnabled("daily-cleanup", false);
```

#### `listSchedules(queueName?)`

Lists all schedules.

```typescript
const schedules = await listSchedules();
```

#### `deleteSchedule(name)`

Deletes a schedule.

```typescript
await deleteSchedule("old-schedule");
```

## Job States

| State       | Description                           |
| ----------- | ------------------------------------- |
| `created`   | Job is waiting to be processed        |
| `active`    | Job is currently being processed      |
| `completed` | Job finished successfully             |
| `failed`    | Job failed and exhausted all retries  |
| `expired`   | Job exceeded its timeout while active |
| `cancelled` | Job was manually cancelled            |

## How It Works

### Safe Concurrent Access

The queue uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` to safely fetch jobs:

```sql
select
  *
from
  queue.jobs
where
  queue_name = 'my-queue'
  and state = 'created'
  and start_after <= NOW()
order by
  priority desc,
  created_at asc
limit
  10
for update
  SKIP LOCKED
```

This ensures:

- Each job is only processed by one worker
- Workers don't block each other
- No jobs are lost or duplicated

### Retry Logic

When a job fails:

1. If `retryCount < retryLimit`, the job is rescheduled:
   - State becomes `created`
   - `startAfter` is set based on `retryDelay` (with optional exponential backoff)
   - `retryCount` is incremented

2. If retries are exhausted:
   - State becomes `failed`
   - If a dead letter queue is configured, a copy is sent there

### Singleton Jobs

When sending with `singletonKey`:

- A unique index prevents duplicate jobs with the same key
- Only applies to `created` and `active` jobs
- Once completed/failed/cancelled, the key can be reused

## Example: Complete Worker Setup

```typescript
import {
  queuePlugin,
  createQueue,
  send,
  fetch,
  complete,
  fail,
  expireJobs,
  cleanup,
} from "sql2/queue";

// Initialize
await queuePlugin();

// Create queue with dead letter
await createQueue("dlq");
await createQueue("tasks", {
  retryLimit: 3,
  retryDelay: 60,
  expireIn: 300,
  deadLetter: "dlq",
});

// Producer: send jobs
await send("tasks", { action: "process", id: 123 });

// Consumer: process jobs
async function processJobs() {
  while (true) {
    const jobs = await fetch("tasks", 5);

    if (jobs.length === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    await Promise.all(
      jobs.map(async (job) => {
        try {
          const result = await processTask(job.data);
          await complete(job.id, result);
        } catch (error) {
          await fail(job.id, error.message);
        }
      }),
    );
  }
}

// Maintenance: run periodically
async function maintenance() {
  // Expire stale jobs (every minute)
  await expireJobs();

  // Clean up old jobs (every hour)
  await cleanup();
}

setInterval(maintenance, 60000);
processJobs();
```

## Performance Tips

1. **Use appropriate batch sizes**: Fetch multiple jobs at once to reduce round trips.

2. **Index your data**: If you frequently query jobs by data fields, consider adding indexes.

3. **Clean up regularly**: Run `cleanup()` to prevent table bloat.

4. **Tune retention**: Set `retainCompleted` and `retainFailed` based on your needs.

5. **Monitor queue depth**: Use `getStats()` to track queue health.

6. **Use priorities wisely**: High-priority jobs are always processed first.

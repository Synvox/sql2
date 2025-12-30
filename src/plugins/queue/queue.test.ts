import {
  PGlite,
  type PGliteInterface,
  type Transaction,
} from "@electric-sql/pglite";
import * as assert from "node:assert";
import { describe, it } from "node:test";
import { getSql, provideClient, type Client } from "../../sql2.ts";
import {
  cancel,
  cleanup,
  complete,
  createQueue,
  createSchedule,
  deleteQueue,
  deleteSchedule,
  expireJobs,
  fail,
  fetch,
  getActivity,
  getJob,
  getQueue,
  getStats,
  listJobs,
  listQueues,
  listSchedules,
  purge,
  queuePlugin,
  send,
  sendBatch,
  setScheduleEnabled,
  work,
  type FetchedJob,
} from "./index.ts";

const dbRoot = new PGlite();

function makeClient(db: PGlite | Transaction | PGliteInterface): Client {
  return {
    exec: async (query) => {
      await db.exec(query);
    },
    query: async (query, values) => {
      return await db.query(query, values);
    },
    transaction: async (fn) => {
      if ("transaction" in db) {
        return await db.transaction(async (trx) => {
          return await fn(makeClient(trx));
        });
      } else {
        return await fn(makeClient(db));
      }
    },
  };
}

async function withDb(fn: () => Promise<void>) {
  const db = await dbRoot.clone();
  return provideClient(makeClient(db), async () => {
    return await fn();
  });
}

function itWithDb(name: string, fn: () => Promise<void>) {
  it(name, async () => {
    await withDb(async () => {
      await fn();
    });
  });
}

await provideClient(makeClient(dbRoot), async () => {
  await queuePlugin();
});

describe("Queue Plugin", () => {
  describe("Queue Management", () => {
    itWithDb("should create a queue with default options", async () => {
      const queue = await createQueue("test-queue");

      assert.strictEqual(queue.name, "test-queue");
      assert.strictEqual(queue.retryLimit, 3);
      assert.strictEqual(queue.retryDelay, 60);
      assert.strictEqual(queue.retryBackoff, true);
      assert.strictEqual(queue.expireIn, 900);
      assert.strictEqual(queue.retainCompleted, 86400);
      assert.strictEqual(queue.retainFailed, 604800);
      assert.strictEqual(queue.deadLetter, null);
      assert.ok(queue.createdAt instanceof Date);
    });

    itWithDb("should create a queue with custom options", async () => {
      const queue = await createQueue("custom-queue", {
        retryLimit: 5,
        retryDelay: 120,
        retryBackoff: false,
        expireIn: 1800,
        retainCompleted: 3600,
        retainFailed: 7200,
      });

      assert.strictEqual(queue.name, "custom-queue");
      assert.strictEqual(queue.retryLimit, 5);
      assert.strictEqual(queue.retryDelay, 120);
      assert.strictEqual(queue.retryBackoff, false);
      assert.strictEqual(queue.expireIn, 1800);
      assert.strictEqual(queue.retainCompleted, 3600);
      assert.strictEqual(queue.retainFailed, 7200);
    });

    itWithDb("should update an existing queue", async () => {
      await createQueue("update-queue", { retryLimit: 3 });
      const updated = await createQueue("update-queue", {
        retryLimit: 10,
      });

      assert.strictEqual(updated.retryLimit, 10);
    });

    itWithDb("should get a queue by name", async () => {
      await createQueue("get-queue");
      const queue = await getQueue("get-queue");

      assert.ok(queue);
      assert.strictEqual(queue.name, "get-queue");
      assert.strictEqual(queue.retryLimit, 3);
    });

    itWithDb("should return null for non-existent queue", async () => {
      const queue = await getQueue("non-existent-queue");
      assert.strictEqual(queue, null);
    });

    itWithDb("should get queue with job counts", async () => {
      await createQueue("count-queue");
      await send("count-queue", { task: "test1" });
      await send("count-queue", { task: "test2" });

      const queue = await getQueue("count-queue");
      assert.ok(queue);
      assert.ok(queue.jobCounts);
      assert.strictEqual(queue.jobCounts.created, 2);
    });

    itWithDb("should list all queues", async () => {
      await createQueue("list-queue-1");
      await createQueue("list-queue-2");

      const queues = await listQueues();
      const names = queues.map((q) => q.name);

      assert.ok(names.includes("list-queue-1"));
      assert.ok(names.includes("list-queue-2"));
    });

    itWithDb("should delete a queue", async () => {
      await createQueue("delete-queue");

      const deleted = await deleteQueue("delete-queue");
      assert.strictEqual(deleted, true);

      const queue = await getQueue("delete-queue");
      assert.strictEqual(queue, null);
    });

    itWithDb(
      "should return false when deleting non-existent queue",
      async () => {
        const deleted = await deleteQueue("non-existent-queue");
        assert.strictEqual(deleted, false);
      },
    );

    itWithDb("should create a queue with dead letter queue", async () => {
      await createQueue("dead-letter-queue");
      const queue = await createQueue("main-queue", {
        deadLetter: "dead-letter-queue",
      });

      assert.strictEqual(queue.deadLetter, "dead-letter-queue");
    });
  });

  describe("Job Sending", () => {
    itWithDb("should send a job to a queue", async () => {
      await createQueue("send-queue");
      const result = await send("send-queue", { message: "hello" });

      assert.ok(result);
      assert.ok(result.id);
      assert.strictEqual(result.queueName, "send-queue");
      assert.strictEqual(result.state, "created");
      assert.strictEqual(result.priority, 0);
      assert.ok(result.createdAt instanceof Date);
    });

    itWithDb("should send a job with delay", async () => {
      await createQueue("delay-queue");
      const before = new Date();
      const result = await send(
        "delay-queue",
        { task: "delayed" },
        { delay: 60 },
      );

      assert.ok(result);
      const startAfter = new Date(result.startAfter);
      assert.ok(startAfter > before);
    });

    itWithDb("should send a job with start after", async () => {
      await createQueue("start-after-queue");
      const futureDate = new Date(Date.now() + 60000);
      const result = await send(
        "start-after-queue",
        { task: "scheduled" },
        { startAfter: futureDate },
      );

      assert.ok(result);
      const startAfter = new Date(result.startAfter);
      assert.ok(startAfter.getTime() >= futureDate.getTime() - 1000);
    });

    itWithDb("should send a job with priority", async () => {
      await createQueue("priority-queue");
      const result = await send(
        "priority-queue",
        { task: "important" },
        { priority: 10 },
      );

      assert.ok(result);
      assert.strictEqual(result.priority, 10);
    });

    itWithDb("should send a job with singleton key", async () => {
      await createQueue("singleton-queue");
      const result1 = await send(
        "singleton-queue",
        { task: "unique" },
        { singletonKey: "unique-key" },
      );

      assert.ok(result1);
      assert.strictEqual(result1.singletonKey, "unique-key");

      // Second send with same key should return null
      const result2 = await send(
        "singleton-queue",
        { task: "duplicate" },
        { singletonKey: "unique-key" },
      );

      assert.strictEqual(result2, null);
    });

    itWithDb(
      "should allow same singleton key after job completes",
      async () => {
        await createQueue("singleton-complete-queue");

        const result1 = await send(
          "singleton-complete-queue",
          { task: "first" },
          { singletonKey: "reusable-key" },
        );
        assert.ok(result1);

        // Fetch and complete the job
        const jobs = await fetch("singleton-complete-queue", 1);
        assert.strictEqual(jobs.length, 1);
        await complete(jobs[0].id);

        // Now we can send another with the same key
        const result2 = await send(
          "singleton-complete-queue",
          { task: "second" },
          { singletonKey: "reusable-key" },
        );
        assert.ok(result2);
      },
    );

    itWithDb("should send batch of jobs", async () => {
      await createQueue("batch-queue");

      const results = await sendBatch("batch-queue", [
        { data: { task: 1 } },
        { data: { task: 2 } },
        { data: { task: 3 }, options: { priority: 5 } },
      ]);

      assert.strictEqual(results.length, 3);
      assert.ok(results.every((r) => r.queueName === "batch-queue"));
      assert.ok(results.every((r) => r.state === "created"));
    });

    itWithDb(
      "should throw error when sending to non-existent queue",
      async () => {
        await assert.rejects(async () => {
          await send("non-existent-queue", { task: "test" });
        }, /Queue "non-existent-queue" does not exist/);
      },
    );

    itWithDb("should send job with custom retry options", async () => {
      await createQueue("custom-retry-queue");

      const result = await send(
        "custom-retry-queue",
        { task: "test" },
        { retryLimit: 10, retryDelay: 30, retryBackoff: false },
      );

      assert.ok(result);

      const job = await getJob(result.id);
      assert.ok(job);
      assert.strictEqual(job.retryLimit, 10);
    });
  });

  describe("Job Fetching", () => {
    itWithDb("should fetch a job from the queue", async () => {
      await createQueue("fetch-queue");
      await send("fetch-queue", { message: "fetch me" });

      const jobs = await fetch("fetch-queue", 1);

      assert.strictEqual(jobs.length, 1);
      assert.strictEqual(jobs[0].queueName, "fetch-queue");
      assert.deepStrictEqual(jobs[0].data, { message: "fetch me" });
      assert.ok(jobs[0].startedAt instanceof Date);
      assert.ok(jobs[0].expireAt instanceof Date);
    });

    itWithDb("should fetch multiple jobs", async () => {
      await createQueue("multi-fetch-queue");
      await send("multi-fetch-queue", { task: 1 });
      await send("multi-fetch-queue", { task: 2 });
      await send("multi-fetch-queue", { task: 3 });

      const jobs = await fetch("multi-fetch-queue", 2);
      assert.strictEqual(jobs.length, 2);
    });

    itWithDb("should return empty array when no jobs available", async () => {
      await createQueue("empty-queue");

      const jobs = await fetch("empty-queue", 1);
      assert.strictEqual(jobs.length, 0);
    });

    itWithDb("should fetch jobs by priority order", async () => {
      await createQueue("priority-fetch-queue");

      await send("priority-fetch-queue", { task: "low" }, { priority: 1 });
      await send("priority-fetch-queue", { task: "high" }, { priority: 10 });
      await send("priority-fetch-queue", { task: "medium" }, { priority: 5 });

      const jobs = await fetch("priority-fetch-queue", 3);

      assert.strictEqual(jobs.length, 3);
      assert.deepStrictEqual(jobs[0].data, { task: "high" });
      assert.deepStrictEqual(jobs[1].data, { task: "medium" });
      assert.deepStrictEqual(jobs[2].data, { task: "low" });
    });

    itWithDb("should not fetch jobs with future start_after", async () => {
      await createQueue("future-queue");

      await send("future-queue", { task: "now" }, {});
      await send(
        "future-queue",
        { task: "future" },
        { startAfter: new Date(Date.now() + 60000) },
      );

      const jobs = await fetch("future-queue", 10);
      assert.strictEqual(jobs.length, 1);
      assert.deepStrictEqual(jobs[0].data, { task: "now" });
    });

    itWithDb("should not fetch already active jobs", async () => {
      await createQueue("active-queue");
      await send("active-queue", { task: "test" });

      const jobs1 = await fetch("active-queue", 1);
      assert.strictEqual(jobs1.length, 1);

      const jobs2 = await fetch("active-queue", 1);
      assert.strictEqual(jobs2.length, 0);
    });

    itWithDb("should set expire_at when fetching", async () => {
      await createQueue("expire-fetch-queue", { expireIn: 300 });
      await send("expire-fetch-queue", { task: "test" });

      const jobs = await fetch("expire-fetch-queue", 1);
      assert.strictEqual(jobs.length, 1);
      assert.ok(jobs[0].expireAt instanceof Date);

      const expectedExpireAt = new Date(
        jobs[0].startedAt.getTime() + 300 * 1000,
      );
      const diff = Math.abs(
        jobs[0].expireAt.getTime() - expectedExpireAt.getTime(),
      );
      assert.ok(diff < 2000); // Within 2 seconds
    });
  });

  describe("Job Completion", () => {
    itWithDb("should complete a job", async () => {
      await createQueue("complete-queue");
      await send("complete-queue", { task: "complete me" });

      const [job] = await fetch("complete-queue", 1);
      const result = await complete(job.id);

      assert.ok(result);
      assert.strictEqual(result.id, job.id);
      assert.strictEqual(result.state, "completed");
      assert.ok(result.completedAt instanceof Date);
    });

    itWithDb("should complete a job with output", async () => {
      await createQueue("complete-output-queue");
      await send("complete-output-queue", { task: "process" });

      const [job] = await fetch("complete-output-queue", 1);
      await complete(job.id, { result: "success", count: 42 });

      const completedJob = await getJob(job.id);
      assert.ok(completedJob);
      assert.strictEqual(completedJob.state, "completed");
      assert.deepStrictEqual(completedJob.output, {
        result: "success",
        count: 42,
      });
    });

    itWithDb("should return null when completing non-active job", async () => {
      await createQueue("non-active-queue");
      const sendResult = await send("non-active-queue", { task: "test" });

      // Try to complete without fetching first
      const result = await complete(sendResult!.id);
      assert.strictEqual(result, null);
    });
  });

  describe("Job Failure", () => {
    itWithDb("should fail a job with retry", async () => {
      await createQueue("fail-retry-queue", { retryLimit: 3 });
      await send("fail-retry-queue", { task: "will fail" });

      const [job] = await fetch("fail-retry-queue", 1);
      const result = await fail(job.id, "Test error");

      assert.ok(result);
      assert.strictEqual(result.id, job.id);
      assert.strictEqual(result.state, "created"); // Back to created for retry
      assert.strictEqual(result.retryCount, 1);
      assert.strictEqual(result.willRetry, true);
      assert.ok(result.nextRetryAt instanceof Date);
    });

    itWithDb("should permanently fail job after retry limit", async () => {
      const sql = getSql();
      await createQueue("fail-perm-queue", { retryLimit: 1 });
      await send("fail-perm-queue", { task: "will fail permanently" });

      // First attempt
      let [job] = await fetch("fail-perm-queue", 1);
      await fail(job.id, "Error 1");

      // Update start_after to now so retry is available immediately
      await sql`
        update queue.jobs
        set
          start_after = now()
        where
          id = ${job.id}::uuid
      `.all();

      // Second attempt (retry)
      [job] = await fetch("fail-perm-queue", 1);
      const result = await fail(job.id, "Error 2");

      assert.ok(result);
      assert.strictEqual(result.state, "failed");
      assert.strictEqual(result.willRetry, false);
      assert.strictEqual(result.nextRetryAt, null);
    });

    itWithDb("should store last error", async () => {
      await createQueue("error-queue", { retryLimit: 0 });
      await send("error-queue", { task: "test" });

      const [job] = await fetch("error-queue", 1);
      await fail(job.id, "Something went wrong");

      const failedJob = await getJob(job.id);
      assert.ok(failedJob);
      assert.strictEqual(failedJob.lastError, "Something went wrong");
    });

    itWithDb("should use exponential backoff for retries", async () => {
      await createQueue("backoff-queue", {
        retryLimit: 5,
        retryDelay: 10,
        retryBackoff: true,
      });
      await send("backoff-queue", { task: "test" });

      const [job] = await fetch("backoff-queue", 1);
      const failResult = await fail(job.id, "Error");

      assert.ok(failResult);
      assert.ok(failResult.nextRetryAt);

      // First retry should be ~10 seconds
      const delay = failResult.nextRetryAt.getTime() - Date.now();
      assert.ok(delay >= 9000 && delay <= 11000);
    });

    itWithDb(
      "should send to dead letter queue on permanent failure",
      async () => {
        await createQueue("dead-letter-dest");
        await createQueue("dead-letter-src", {
          retryLimit: 0,
          deadLetter: "dead-letter-dest",
        });

        await send("dead-letter-src", { task: "will die" });
        const [job] = await fetch("dead-letter-src", 1);
        await fail(job.id, "Fatal error");

        // Check dead letter queue
        const dlJobs = await listJobs("dead-letter-dest");
        assert.strictEqual(dlJobs.length, 1);
        assert.deepStrictEqual(dlJobs[0].data, { task: "will die" });
      },
    );
  });

  describe("Job Cancellation", () => {
    itWithDb("should cancel a created job", async () => {
      await createQueue("cancel-queue");
      const sendResult = await send("cancel-queue", { task: "cancel me" });

      const result = await cancel(sendResult!.id);

      assert.strictEqual(result.cancelled, true);
      assert.strictEqual(result.previousState, "created");

      const job = await getJob(sendResult!.id);
      assert.ok(job);
      assert.strictEqual(job.state, "cancelled");
    });

    itWithDb("should cancel an active job", async () => {
      await createQueue("cancel-active-queue");
      await send("cancel-active-queue", { task: "cancel me" });

      const [job] = await fetch("cancel-active-queue", 1);
      const result = await cancel(job.id);

      assert.strictEqual(result.cancelled, true);
      assert.strictEqual(result.previousState, "active");
    });

    itWithDb("should not cancel completed job", async () => {
      await createQueue("cancel-completed-queue");
      await send("cancel-completed-queue", { task: "done" });

      const [job] = await fetch("cancel-completed-queue", 1);
      await complete(job.id);

      const result = await cancel(job.id);
      assert.strictEqual(result.cancelled, false);
    });
  });

  describe("Job Retrieval", () => {
    itWithDb("should get job by id", async () => {
      await createQueue("get-job-queue");
      const sendResult = await send("get-job-queue", {
        message: "hello",
        count: 42,
      });

      const job = await getJob(sendResult!.id);

      assert.ok(job);
      assert.strictEqual(job.id, sendResult!.id);
      assert.strictEqual(job.queueName, "get-job-queue");
      assert.strictEqual(job.state, "created");
      assert.deepStrictEqual(job.data, { message: "hello", count: 42 });
    });

    itWithDb("should return null for non-existent job", async () => {
      const job = await getJob("00000000-0000-0000-0000-000000000000");
      assert.strictEqual(job, null);
    });

    itWithDb("should list jobs in a queue", async () => {
      await createQueue("list-jobs-queue");
      await send("list-jobs-queue", { task: 1 });
      await send("list-jobs-queue", { task: 2 });
      await send("list-jobs-queue", { task: 3 });

      const jobs = await listJobs("list-jobs-queue");
      assert.strictEqual(jobs.length, 3);
    });

    itWithDb("should list jobs filtered by state", async () => {
      await createQueue("list-state-queue");
      await send("list-state-queue", { task: 1 });
      await send("list-state-queue", { task: 2 });

      // Fetch and complete one
      const [job] = await fetch("list-state-queue", 1);
      await complete(job.id);

      const createdJobs = await listJobs("list-state-queue", {
        state: "created",
      });
      assert.strictEqual(createdJobs.length, 1);

      const completedJobs = await listJobs("list-state-queue", {
        state: "completed",
      });
      assert.strictEqual(completedJobs.length, 1);
    });

    itWithDb("should list jobs with pagination", async () => {
      await createQueue("paginate-queue");
      for (let i = 0; i < 10; i++) {
        await send("paginate-queue", { task: i });
      }

      const page1 = await listJobs("paginate-queue", {
        limit: 5,
        offset: 0,
      });
      const page2 = await listJobs("paginate-queue", {
        limit: 5,
        offset: 5,
      });

      assert.strictEqual(page1.length, 5);
      assert.strictEqual(page2.length, 5);
    });
  });

  describe("Maintenance", () => {
    itWithDb("should expire stale active jobs", async () => {
      const sql = getSql();
      await createQueue("expire-queue", { expireIn: 1 }); // 1 second expire
      await send("expire-queue", { task: "will expire" });

      const [job] = await fetch("expire-queue", 1);

      // Manually set expire_at to the past
      await sql`
        update queue.jobs
        set
          expire_at = now() - interval '1 second'
        where
          id = ${job.id}::uuid
      `.all();

      const results = await expireJobs();
      const queueResult = results.find((r) => r.queueName === "expire-queue");
      assert.ok(queueResult);
      assert.strictEqual(queueResult.expiredCount, 1);

      const expiredJob = await getJob(job.id);
      assert.ok(expiredJob);
      assert.strictEqual(expiredJob.state, "expired");
    });

    itWithDb("should cleanup old completed jobs", async () => {
      const sql = getSql();
      await createQueue("cleanup-queue", { retainCompleted: 1 }); // 1 second retention
      await send("cleanup-queue", { task: "will be cleaned" });

      const [job] = await fetch("cleanup-queue", 1);
      await complete(job.id);

      // Manually set completed_at to the past
      await sql`
        update queue.jobs
        set
          completed_at = now() - interval '2 seconds'
        where
          id = ${job.id}::uuid
      `.all();

      const results = await cleanup();
      const queueResult = results.find((r) => r.queueName === "cleanup-queue");
      assert.ok(queueResult);
      assert.strictEqual(queueResult.deletedCount, 1);

      const cleanedJob = await getJob(job.id);
      assert.strictEqual(cleanedJob, null);
    });

    itWithDb("should cleanup old failed jobs", async () => {
      const sql = getSql();
      await createQueue("cleanup-failed-queue", {
        retryLimit: 0,
        retainFailed: 1,
      });
      await send("cleanup-failed-queue", { task: "will fail" });

      const [job] = await fetch("cleanup-failed-queue", 1);
      await fail(job.id, "Error");

      // Manually set completed_at to the past
      await sql`
        update queue.jobs
        set
          completed_at = now() - interval '2 seconds'
        where
          id = ${job.id}::uuid
      `.all();

      const results = await cleanup();
      const queueResult = results.find(
        (r) => r.queueName === "cleanup-failed-queue",
      );
      assert.ok(queueResult);
      assert.strictEqual(queueResult.deletedCount, 1);
    });

    itWithDb("should purge all jobs from a queue", async () => {
      await createQueue("purge-queue");
      await send("purge-queue", { task: 1 });
      await send("purge-queue", { task: 2 });
      await send("purge-queue", { task: 3 });

      const deletedCount = await purge("purge-queue");
      assert.strictEqual(deletedCount, 3);

      const jobs = await listJobs("purge-queue");
      assert.strictEqual(jobs.length, 0);
    });

    itWithDb("should purge jobs by state", async () => {
      await createQueue("purge-state-queue");
      await send("purge-state-queue", { task: 1 });
      await send("purge-state-queue", { task: 2 });

      const [job] = await fetch("purge-state-queue", 1);
      await complete(job.id);

      const deletedCount = await purge("purge-state-queue", "completed");
      assert.strictEqual(deletedCount, 1);

      const remainingJobs = await listJobs("purge-state-queue");
      assert.strictEqual(remainingJobs.length, 1);
      assert.strictEqual(remainingJobs[0].state, "created");
    });
  });

  describe("Statistics", () => {
    itWithDb("should get queue stats", async () => {
      await createQueue("stats-queue");
      await send("stats-queue", { task: 1 });
      await send("stats-queue", { task: 2 });
      await send("stats-queue", { task: 3 });

      const [job] = await fetch("stats-queue", 1);
      await complete(job.id);

      const stats = await getStats("stats-queue");
      assert.strictEqual(stats.length, 1);
      assert.strictEqual(stats[0].queueName, "stats-queue");
      assert.strictEqual(stats[0].created, 2);
      assert.strictEqual(stats[0].completed, 1);
    });

    itWithDb("should get stats for all queues", async () => {
      await createQueue("stats-all-1");
      await createQueue("stats-all-2");
      await send("stats-all-1", { task: 1 });
      await send("stats-all-2", { task: 2 });

      const stats = await getStats();
      const queueNames = stats.map((s) => s.queueName);

      assert.ok(queueNames.includes("stats-all-1"));
      assert.ok(queueNames.includes("stats-all-2"));
    });

    itWithDb("should get activity data", async () => {
      await createQueue("activity-queue");
      await send("activity-queue", { task: "test" });

      const activity = await getActivity("activity-queue", 5);
      assert.ok(Array.isArray(activity));
      // Activity should have data points for each minute in the range
    });
  });

  describe("Schedules", () => {
    itWithDb("should create a schedule", async () => {
      await createQueue("schedule-queue");

      const schedule = await createSchedule(
        "daily-report",
        "schedule-queue",
        "0 9 * * *",
        {
          data: { report: "daily" },
          timezone: "America/New_York",
          priority: 5,
        },
      );

      assert.strictEqual(schedule.name, "daily-report");
      assert.strictEqual(schedule.queueName, "schedule-queue");
      assert.strictEqual(schedule.cron, "0 9 * * *");
      assert.strictEqual(schedule.timezone, "America/New_York");
      assert.strictEqual(schedule.priority, 5);
      assert.strictEqual(schedule.enabled, true);
    });

    itWithDb("should update an existing schedule", async () => {
      await createQueue("schedule-update-queue");

      await createSchedule("updatable", "schedule-update-queue", "0 0 * * *");
      const updated = await createSchedule(
        "updatable",
        "schedule-update-queue",
        "0 6 * * *",
      );

      assert.strictEqual(updated.cron, "0 6 * * *");
    });

    itWithDb("should enable/disable a schedule", async () => {
      await createQueue("toggle-queue");
      await createSchedule("toggleable", "toggle-queue", "0 0 * * *");

      const disabled = await setScheduleEnabled("toggleable", false);
      assert.strictEqual(disabled, true);

      const schedules = await listSchedules();
      const schedule = schedules.find((s) => s.name === "toggleable");
      assert.ok(schedule);
      assert.strictEqual(schedule.enabled, false);
    });

    itWithDb("should delete a schedule", async () => {
      await createQueue("delete-schedule-queue");
      await createSchedule("deletable", "delete-schedule-queue", "0 0 * * *");

      const deleted = await deleteSchedule("deletable");
      assert.strictEqual(deleted, true);

      const schedules = await listSchedules();
      const schedule = schedules.find((s) => s.name === "deletable");
      assert.strictEqual(schedule, undefined);
    });

    itWithDb("should list schedules", async () => {
      await createQueue("list-schedule-queue");
      await createSchedule("schedule-1", "list-schedule-queue", "0 0 * * *");
      await createSchedule("schedule-2", "list-schedule-queue", "0 12 * * *");

      const schedules = await listSchedules("list-schedule-queue");
      assert.strictEqual(schedules.length, 2);
    });

    itWithDb(
      "should throw error when creating schedule for non-existent queue",
      async () => {
        await assert.rejects(async () => {
          await createSchedule("orphan", "non-existent-queue", "0 0 * * *");
        }, /Queue "non-existent-queue" does not exist/);
      },
    );
  });

  describe("Worker", () => {
    itWithDb("should process jobs with worker", async () => {
      await createQueue("worker-queue");

      const processed: unknown[] = [];

      // Send some jobs
      await send("worker-queue", { task: 1 });
      await send("worker-queue", { task: 2 });

      // Create worker
      const worker = work(
        "worker-queue",
        async (job: FetchedJob<{ task: number }>) => {
          processed.push(job.data.task);
          return { processed: job.data.task };
        },
      );

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 100));

      worker.stop();

      // Check that jobs were processed
      const completedJobs = await listJobs("worker-queue", {
        state: "completed",
      });
      assert.strictEqual(completedJobs.length, 2);
    });

    itWithDb("should handle job failures in worker", async () => {
      await createQueue("worker-fail-queue", { retryLimit: 0 });

      await send("worker-fail-queue", { task: "will fail" });

      const worker = work("worker-fail-queue", async () => {
        throw new Error("Processing failed");
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 100));

      worker.stop();

      const failedJobs = await listJobs("worker-fail-queue", {
        state: "failed",
      });
      assert.strictEqual(failedJobs.length, 1);
      assert.strictEqual(failedJobs[0].lastError, "Processing failed");
    });
  });

  describe("Concurrent Access", () => {
    itWithDb("should handle concurrent fetches safely", async () => {
      await createQueue("concurrent-queue");

      // Create many jobs
      for (let i = 0; i < 10; i++) {
        await send("concurrent-queue", { task: i });
      }

      // Simulate concurrent fetches
      const fetches = await Promise.all([
        fetch("concurrent-queue", 5),
        fetch("concurrent-queue", 5),
        fetch("concurrent-queue", 5),
      ]);

      const allJobs = fetches.flat();
      const uniqueIds = new Set(allJobs.map((j) => j.id));

      // All fetched jobs should be unique
      assert.strictEqual(allJobs.length, uniqueIds.size);
      assert.strictEqual(allJobs.length, 10); // All 10 jobs should be fetched
    });
  });

  describe("Edge Cases", () => {
    itWithDb("should handle empty data", async () => {
      await createQueue("empty-data-queue");
      const result = await send("empty-data-queue", {});

      assert.ok(result);

      const job = await getJob(result.id);
      assert.ok(job);
      assert.deepStrictEqual(job.data, {});
    });

    itWithDb("should handle complex nested data", async () => {
      await createQueue("complex-data-queue");

      const complexData = {
        users: [
          { id: 1, name: "Alice", tags: ["admin", "active"] },
          { id: 2, name: "Bob", tags: ["user"] },
        ],
        metadata: {
          nested: {
            deeply: {
              value: 42,
            },
          },
        },
        nullValue: null,
        boolTrue: true,
        boolFalse: false,
      };

      const result = await send("complex-data-queue", complexData);
      assert.ok(result);

      const job = await getJob(result.id);
      assert.ok(job);
      assert.deepStrictEqual(job.data, complexData);
    });

    itWithDb("should handle special characters in queue names", async () => {
      await createQueue("queue-with-dashes");
      await createQueue("queue_with_underscores");
      await createQueue("queue.with.dots");

      const queues = await listQueues();
      const names = queues.map((q) => q.name);

      assert.ok(names.includes("queue-with-dashes"));
      assert.ok(names.includes("queue_with_underscores"));
      assert.ok(names.includes("queue.with.dots"));
    });

    itWithDb("should handle zero retry limit", async () => {
      await createQueue("zero-retry-queue", { retryLimit: 0 });
      await send("zero-retry-queue", { task: "no retries" });

      const [job] = await fetch("zero-retry-queue", 1);
      const result = await fail(job.id, "First and only failure");

      assert.ok(result);
      assert.strictEqual(result.state, "failed");
      assert.strictEqual(result.willRetry, false);
    });

    itWithDb("should handle very high priority values", async () => {
      await createQueue("high-priority-queue");

      await send("high-priority-queue", { task: "normal" }, { priority: 0 });
      await send(
        "high-priority-queue",
        { task: "high" },
        { priority: 1000000 },
      );

      const jobs = await fetch("high-priority-queue", 2);
      assert.deepStrictEqual(jobs[0].data, { task: "high" });
      assert.deepStrictEqual(jobs[1].data, { task: "normal" });
    });
  });
});

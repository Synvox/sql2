import {
  PGlite,
  type PGliteInterface,
  type Transaction,
} from "@electric-sql/pglite";
import * as assert from "node:assert";
import { randomUUID } from "node:crypto";
import { describe, it } from "node:test";
import { getSql, provideClient, type Client } from "../../sql2.ts";
import {
  ack,
  append,
  cleanupExpiredClaims,
  cleanupSubscriptionClaims,
  createSubscription,
  deleteSnapshot,
  deleteStream,
  deleteSubscription,
  ensureStream,
  eventsPlugin,
  getCategoryStats,
  getEvent,
  getEventAtPosition,
  getProjectionHandlers,
  getStats,
  getStream,
  getStreamStats,
  getSubscription,
  getTypeStats,
  listAggregates,
  listCategories,
  listEventTypes,
  listProjections,
  listStreams,
  listSubscriptions,
  loadAggregate,
  loadRegisteredAggregate,
  loadSnapshot,
  poll,
  readAll,
  readByCategory,
  readByType,
  readStream,
  registerAggregate,
  registerCategory,
  registerEventType,
  registerProjection,
  resetSubscription,
  saveSnapshot,
  setSubscriptionActive,
  startProjections,
  unregisterAggregate,
  unregisterProjection,
  type Event,
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

await provideClient(makeClient(dbRoot), async () => {
  await eventsPlugin();
});

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

describe("Events Plugin", () => {
  describe("Category & Type Registration", () => {
    itWithDb("should register a category", async () => {
      const sql = getSql({ camelize: false });
      const id = await registerCategory("test-category");
      assert.strictEqual(id, "test-category");
    });

    itWithDb("should list registered categories", async () => {
      const sql = getSql({ camelize: false });
      await registerCategory("category-a");
      await registerCategory("category-b");

      const categories = await listCategories();
      assert.ok(categories.includes("category-a"));
      assert.ok(categories.includes("category-b"));
    });

    itWithDb("should register an event type", async () => {
      const sql = getSql({ camelize: false });
      const result = await registerEventType("order/created");
      assert.strictEqual(result.categoryId, "order");
      assert.strictEqual(result.id, "created");
    });

    itWithDb("should throw for invalid event type format", async () => {
      const sql = getSql({ camelize: false });
      await assert.rejects(
        async () => await registerEventType("invalid-format"),
        /Invalid event type format/,
      );
    });

    itWithDb("should list event types", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("test-cat/type-a");
      await registerEventType("test-cat/type-b");
      await registerEventType("other-cat/type-c");

      const allTypes = await listEventTypes();
      assert.ok(
        allTypes.some((t) => t.categoryId === "test-cat" && t.id === "type-a"),
      );
      assert.ok(
        allTypes.some((t) => t.categoryId === "test-cat" && t.id === "type-b"),
      );
      assert.ok(
        allTypes.some((t) => t.categoryId === "other-cat" && t.id === "type-c"),
      );

      const filteredTypes = await listEventTypes("test-cat");
      assert.strictEqual(filteredTypes.length, 2);
      assert.ok(filteredTypes.every((t) => t.categoryId === "test-cat"));
    });
  });

  describe("Stream Management", () => {
    itWithDb("should ensure a stream exists", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("stream-test");

      const stream = await ensureStream(streamId, "stream-test");
      assert.strictEqual(stream.id, streamId);
      assert.strictEqual(stream.categoryId, "stream-test");
      assert.strictEqual(stream.version, 0);
      assert.ok(stream.createdAt instanceof Date);
    });

    itWithDb("should get a stream by ID", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("get-stream-test");
      await ensureStream(streamId, "get-stream-test");

      const stream = await getStream(streamId);
      assert.ok(stream !== null);
      assert.strictEqual(stream.id, streamId);
      assert.strictEqual(stream.categoryId, "get-stream-test");
      assert.strictEqual(stream.version, 0);
      assert.strictEqual(stream.eventCount, 0);
    });

    itWithDb("should return null for non-existent stream", async () => {
      const sql = getSql({ camelize: false });
      const stream = await getStream(randomUUID());
      assert.strictEqual(stream, null);
    });

    itWithDb("should list streams", async () => {
      const sql = getSql({ camelize: false });
      await registerCategory("list-streams-cat");
      const id1 = randomUUID();
      const id2 = randomUUID();

      await ensureStream(id1, "list-streams-cat");
      await ensureStream(id2, "list-streams-cat");

      const streams = await listStreams({ category: "list-streams-cat" });
      assert.strictEqual(streams.length, 2);
    });

    itWithDb("should delete a stream", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("delete-stream-cat");
      await ensureStream(streamId, "delete-stream-cat");

      const deleted = await deleteStream(streamId);
      assert.strictEqual(deleted, true);

      const stream = await getStream(streamId);
      assert.strictEqual(stream, null);
    });
  });

  describe("Event Appending", () => {
    itWithDb("should append an event to a stream", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("append-test/created");

      const result = await append("append-test/created", streamId, {
        name: "Test",
      });

      assert.ok(result.position > 0);
      assert.ok(result.id);
      assert.strictEqual(result.streamId, streamId);
      assert.strictEqual(result.streamVersion, 1);
      assert.strictEqual(result.categoryId, "append-test");
      assert.strictEqual(result.typeId, "created");
    });

    itWithDb("should append with metadata", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("meta-test/created");

      const result = await append(
        "meta-test/created",
        streamId,
        { name: "Test" },
        { metadata: { userId: "user-123" } },
      );

      assert.strictEqual(result.streamVersion, 1);
    });

    itWithDb("should increment stream version on each append", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("version-test/event");

      const result1 = await append("version-test/event", streamId, {
        n: 1,
      });
      const result2 = await append("version-test/event", streamId, {
        n: 2,
      });
      const result3 = await append("version-test/event", streamId, {
        n: 3,
      });

      assert.strictEqual(result1.streamVersion, 1);
      assert.strictEqual(result2.streamVersion, 2);
      assert.strictEqual(result3.streamVersion, 3);
    });

    itWithDb(
      "should enforce expectedVersion for optimistic concurrency",
      async () => {
        const sql = getSql({ camelize: false });
        const streamId = randomUUID();
        await registerEventType("concurrency/event");

        await append("concurrency/event", streamId, { n: 1 });

        // Should fail with wrong expected version
        await assert.rejects(
          async () =>
            await append(
              "concurrency/event",
              streamId,
              { n: 2 },
              { expectedVersion: 0 },
            ),
          /Concurrency conflict/,
        );

        // Should succeed with correct expected version
        const result = await append(
          "concurrency/event",
          streamId,
          { n: 2 },
          { expectedVersion: 1 },
        );
        assert.strictEqual(result.streamVersion, 2);
      },
    );

    itWithDb(
      "should enforce expectedVersion=-1 means stream must not exist",
      async () => {
        const sql = getSql({ camelize: false });
        const streamId = randomUUID();
        await registerEventType("new-stream/created");

        // Should succeed for new stream
        const result = await append(
          "new-stream/created",
          streamId,
          { n: 1 },
          { expectedVersion: -1 },
        );
        assert.strictEqual(result.streamVersion, 1);

        // Should fail if stream already exists
        await assert.rejects(
          async () =>
            await append(
              "new-stream/created",
              streamId,
              { n: 2 },
              { expectedVersion: -1 },
            ),
          /already exists/,
        );
      },
    );
  });

  describe("Event Reading", () => {
    itWithDb("should read events from a stream", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("read-test/event");

      await append("read-test/event", streamId, { n: 1 });
      await append("read-test/event", streamId, { n: 2 });
      await append("read-test/event", streamId, { n: 3 });

      const events = await readStream(streamId);
      assert.strictEqual(events.length, 3);
      assert.deepStrictEqual(events[0].data, { n: 1 });
      assert.deepStrictEqual(events[1].data, { n: 2 });
      assert.deepStrictEqual(events[2].data, { n: 3 });
    });

    itWithDb("should read events from a specific version", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("read-from/event");

      await append("read-from/event", streamId, { n: 1 });
      await append("read-from/event", streamId, { n: 2 });
      await append("read-from/event", streamId, { n: 3 });

      const events = await readStream(streamId, { fromVersion: 1 });
      assert.strictEqual(events.length, 2);
      assert.deepStrictEqual(events[0].data, { n: 2 });
      assert.deepStrictEqual(events[1].data, { n: 3 });
    });

    itWithDb("should read events backward", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("backward/event");

      await append("backward/event", streamId, { n: 1 });
      await append("backward/event", streamId, { n: 2 });
      await append("backward/event", streamId, { n: 3 });

      const events = await readStream(streamId, {
        fromVersion: 3,
        direction: "backward",
      });
      assert.strictEqual(events.length, 3);
      assert.deepStrictEqual(events[0].data, { n: 3 });
      assert.deepStrictEqual(events[1].data, { n: 2 });
      assert.deepStrictEqual(events[2].data, { n: 1 });
    });

    itWithDb("should read all events globally", async () => {
      const sql = getSql({ camelize: false });
      const streamId1 = randomUUID();
      const streamId2 = randomUUID();
      await registerEventType("read-all/event");

      await append("read-all/event", streamId1, { source: 1 });
      await append("read-all/event", streamId2, { source: 2 });
      await append("read-all/event", streamId1, { source: 1 });

      const events = await readAll({
        filterTypes: ["read-all/event"],
      });

      assert.ok(events.length >= 3);
    });

    itWithDb("should read events by type", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("by-type/type-a");
      await registerEventType("by-type/type-b");

      await append("by-type/type-a", streamId, { type: "a" });
      await append("by-type/type-b", streamId, { type: "b" });
      await append("by-type/type-a", streamId, { type: "a" });

      const events = await readByType("by-type/type-a");
      assert.ok(events.every((e) => e.typeId === "type-a"));
    });

    itWithDb("should read events by category", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("by-cat/type-a");
      await registerEventType("by-cat/type-b");

      await append("by-cat/type-a", streamId, { type: "a" });
      await append("by-cat/type-b", streamId, { type: "b" });

      const events = await readByCategory("by-cat");
      assert.ok(events.every((e) => e.categoryId === "by-cat"));
      assert.strictEqual(events.length, 2);
    });

    itWithDb("should get event by ID", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("get-by-id/event");

      const appendResult = await append("get-by-id/event", streamId, {
        name: "test",
      });
      const event = await getEvent(appendResult.id);

      assert.ok(event !== null);
      assert.strictEqual(event.id, appendResult.id);
      assert.deepStrictEqual(event.data, { name: "test" });
    });

    itWithDb("should get event by position", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("get-by-pos/event");

      const appendResult = await append("get-by-pos/event", streamId, {
        name: "test",
      });
      const event = await getEventAtPosition(appendResult.position);

      assert.ok(event !== null);
      assert.strictEqual(event.position, appendResult.position);
      assert.deepStrictEqual(event.data, { name: "test" });
    });
  });

  describe("Subscriptions", () => {
    itWithDb("should create a subscription", async () => {
      const sql = getSql({ camelize: false });
      const sub = await createSubscription("test-sub-1");

      assert.strictEqual(sub.name, "test-sub-1");
      assert.strictEqual(sub.lastPosition, 0);
      assert.strictEqual(sub.active, true);
    });

    itWithDb("should create subscription with filters", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("sub-filter/type-a");
      await registerEventType("sub-filter/type-b");

      const sub = await createSubscription("filtered-sub", {
        filterTypes: ["sub-filter/type-a", "sub-filter/type-b"],
        startPosition: 10,
      });

      assert.strictEqual(sub.lastPosition, 10);
      assert.deepStrictEqual(sub.filterTypes, [
        "sub-filter/type-a",
        "sub-filter/type-b",
      ]);
    });

    itWithDb("should get subscription details", async () => {
      const sql = getSql({ camelize: false });
      await createSubscription("get-sub-test");

      const sub = await getSubscription("get-sub-test");
      assert.ok(sub !== null);
      assert.strictEqual(sub.name, "get-sub-test");
      assert.ok(typeof sub.eventsBehind === "number");
    });

    itWithDb("should list all subscriptions", async () => {
      const sql = getSql({ camelize: false });
      await createSubscription("list-sub-1");
      await createSubscription("list-sub-2");

      const subs = await listSubscriptions();
      assert.ok(subs.some((s) => s.name === "list-sub-1"));
      assert.ok(subs.some((s) => s.name === "list-sub-2"));
    });

    itWithDb("should poll for new events", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("poll-test/event");

      await createSubscription("poll-sub");

      await append("poll-test/event", streamId, { n: 1 });
      await append("poll-test/event", streamId, { n: 2 });

      const events = await poll("poll-sub", 10);
      assert.ok(events.length >= 2);
    });

    itWithDb("should acknowledge events", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("ack-test/event");

      await createSubscription("ack-sub");
      const result = await append("ack-test/event", streamId, { n: 1 });

      await poll("ack-sub", 10);
      const acked = await ack("ack-sub", result.position);
      assert.strictEqual(acked, true);

      const sub = await getSubscription("ack-sub");
      assert.ok(sub!.lastPosition >= result.position);
    });

    itWithDb("should set subscription active state", async () => {
      const sql = getSql({ camelize: false });
      await createSubscription("active-test-sub");

      await setSubscriptionActive("active-test-sub", false);
      let sub = await getSubscription("active-test-sub");
      assert.strictEqual(sub!.active, false);

      await setSubscriptionActive("active-test-sub", true);
      sub = await getSubscription("active-test-sub");
      assert.strictEqual(sub!.active, true);
    });

    itWithDb("should reset subscription position", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("reset-test/event");

      await createSubscription("reset-sub", { startPosition: 100 });

      await resetSubscription("reset-sub", 0);
      const sub = await getSubscription("reset-sub");
      assert.strictEqual(sub!.lastPosition, 0);
    });

    itWithDb("should cleanup expired claims", async () => {
      const sql = getSql({ camelize: false });
      const count = await cleanupExpiredClaims();
      assert.ok(typeof count === "number");
    });

    itWithDb("should cleanup subscription claims", async () => {
      const sql = getSql({ camelize: false });
      await createSubscription("cleanup-claims-sub");
      const count = await cleanupSubscriptionClaims("cleanup-claims-sub");
      assert.ok(typeof count === "number");
    });

    itWithDb("should delete a subscription", async () => {
      const sql = getSql({ camelize: false });
      await createSubscription("delete-sub");

      const deleted = await deleteSubscription("delete-sub");
      assert.strictEqual(deleted, true);

      const sub = await getSubscription("delete-sub");
      assert.strictEqual(sub, null);
    });
  });

  describe("Snapshots", () => {
    itWithDb("should save and load a snapshot", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("snapshot-test");
      await ensureStream(streamId, "snapshot-test");

      const state = { count: 42, name: "test" };
      const saveResult = await saveSnapshot(streamId, 5, state);

      assert.strictEqual(saveResult.streamId, streamId);
      assert.strictEqual(saveResult.version, 5);

      const snapshot = await loadSnapshot(streamId);
      assert.ok(snapshot !== null);
      assert.deepStrictEqual(snapshot.state, state);
      assert.strictEqual(snapshot.version, 5);
    });

    itWithDb("should save snapshot with custom name", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("snapshot-name-test");
      await ensureStream(streamId, "snapshot-name-test");

      await saveSnapshot(streamId, 10, { data: "custom" }, "custom-snapshot");

      const snapshot = await loadSnapshot(streamId, "custom-snapshot");
      assert.ok(snapshot !== null);
      assert.strictEqual(snapshot.name, "custom-snapshot");
    });

    itWithDb("should return null for non-existent snapshot", async () => {
      const sql = getSql({ camelize: false });
      const snapshot = await loadSnapshot(randomUUID());
      assert.strictEqual(snapshot, null);
    });

    itWithDb("should delete a snapshot", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("delete-snap-test");
      await ensureStream(streamId, "delete-snap-test");

      await saveSnapshot(streamId, 1, { data: "test" });
      const deleted = await deleteSnapshot(streamId);
      assert.strictEqual(deleted, true);

      const snapshot = await loadSnapshot(streamId);
      assert.strictEqual(snapshot, null);
    });
  });

  describe("Statistics", () => {
    itWithDb("should get global stats", async () => {
      const sql = getSql({ camelize: false });
      const stats = await getStats();

      assert.ok(typeof stats.totalEvents === "number");
      assert.ok(typeof stats.totalStreams === "number");
      assert.ok(typeof stats.totalSubscriptions === "number");
      assert.ok(typeof stats.maxPosition === "number");
      assert.ok(typeof stats.eventsToday === "number");
      assert.ok(typeof stats.eventsThisHour === "number");
    });

    itWithDb("should get stream stats", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("stats-test/event");

      await append("stats-test/event", streamId, { n: 1 });
      await append("stats-test/event", streamId, { n: 2 });

      const stats = await getStreamStats(streamId);
      assert.strictEqual(stats.length, 1);
      assert.strictEqual(stats[0].streamId, streamId);
      assert.strictEqual(stats[0].eventCount, 2);
      assert.strictEqual(stats[0].version, 2);
    });

    itWithDb("should get type stats", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("type-stats/event");

      await append("type-stats/event", streamId, { n: 1 });

      const stats = await getTypeStats();
      assert.ok(Array.isArray(stats));
      assert.ok(stats.some((s) => s.typeId === "event"));
    });

    itWithDb("should get category stats", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("cat-stats/event");

      await append("cat-stats/event", streamId, { n: 1 });

      const stats = await getCategoryStats();
      assert.ok(Array.isArray(stats));
      assert.ok(stats.some((s) => s.categoryId === "cat-stats"));
    });
  });

  describe("Aggregates", () => {
    itWithDb("should load aggregate using reducer", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("aggregate/created");
      await registerEventType("aggregate/updated");

      interface OrderState {
        total: number;
        status: string;
      }

      await append("aggregate/created", streamId, {
        total: 100,
        status: "pending",
      });
      await append("aggregate/updated", streamId, { status: "shipped" });

      const reducer = (state: OrderState, event: Event): OrderState => {
        if (event.typeId === "created") {
          return event.data as OrderState;
        }
        if (event.typeId === "updated") {
          return { ...state, ...(event.data as Partial<OrderState>) };
        }
        return state;
      };

      const { state, version } = await loadAggregate<OrderState>(
        streamId,
        reducer,
        { total: 0, status: "unknown" },
      );

      assert.strictEqual(state.total, 100);
      assert.strictEqual(state.status, "shipped");
      assert.strictEqual(version, 2);
    });

    itWithDb("should load aggregate with snapshot", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("snap-agg/event");

      interface CounterState {
        count: number;
      }

      // Append 5 events to get to version 5
      for (let i = 1; i <= 5; i++) {
        await append("snap-agg/event", streamId, { increment: 10 });
      }

      // Create snapshot at version 5 with computed state (50 = 5 * 10)
      await saveSnapshot(streamId, 5, { count: 50 });

      // Add more events after snapshot
      await append("snap-agg/event", streamId, { increment: 1 });
      await append("snap-agg/event", streamId, { increment: 2 });

      const reducer = (state: CounterState, event: Event): CounterState => {
        const data = event.data as { increment: number };
        return { count: state.count + data.increment };
      };

      const { state, version } = await loadAggregate<CounterState>(
        streamId,
        reducer,
        { count: 0 },
      );

      assert.strictEqual(state.count, 53); // 50 (snapshot) + 1 + 2 = 53
      assert.strictEqual(version, 7); // 5 (snapshot) + 2 new events = 7
    });

    itWithDb("should register an aggregate type", async () => {
      const sql = getSql({ camelize: false });
      await registerCategory("reg-agg");
      await registerEventType("reg-agg/created");
      await registerEventType("reg-agg/updated");

      const result = await registerAggregate("test-aggregate", {
        categoryId: "reg-agg",
        initialState: { value: 0 },
        reducers: {
          created: sql`
            v_state || jsonb_build_object('value', (v_event.data ->> 'value')::int)
          `,
          updated: sql`
            v_state || jsonb_build_object(
              'value',
              (v_state ->> 'value')::int + (v_event.data ->> 'increment')::int
            )
          `,
        },
      });

      assert.strictEqual(result.name, "test-aggregate");
      assert.ok(result.functionName.includes("load_"));
      assert.strictEqual(result.categoryId, "reg-agg");
    });

    itWithDb("should list registered aggregates", async () => {
      const sql = getSql({ camelize: false });
      await registerCategory("list-agg");
      await registerEventType("list-agg/event");

      await registerAggregate("list-test-agg", {
        categoryId: "list-agg",
        initialState: {},
        reducers: {
          event: sql`v_state`,
        },
      });

      const aggregates = await listAggregates();
      assert.ok(aggregates.some((a) => a.name === "list-test-agg"));
    });

    itWithDb("should load registered aggregate", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("load-reg-agg");
      await registerEventType("load-reg-agg/init");
      await registerEventType("load-reg-agg/add");

      await registerAggregate("counter-agg", {
        categoryId: "load-reg-agg",
        initialState: { count: 0 },
        reducers: {
          init: sql`
            v_state || jsonb_build_object('count', (v_event.data ->> 'count')::int)
          `,
          add: sql`
            v_state || jsonb_build_object(
              'count',
              (v_state ->> 'count')::int + (v_event.data ->> 'amount')::int
            )
          `,
        },
      });

      await append("load-reg-agg/init", streamId, { count: 10 });
      await append("load-reg-agg/add", streamId, { amount: 5 });
      await append("load-reg-agg/add", streamId, { amount: 3 });

      const state = await loadRegisteredAggregate<{ count: number }>(
        "counter-agg",
        streamId,
      );

      assert.ok(state !== null);
      assert.strictEqual(state.count, 18); // 10 + 5 + 3
      assert.strictEqual(state.version, 3);
    });

    itWithDb("should unregister an aggregate", async () => {
      const sql = getSql({ camelize: false });
      await registerCategory("unreg-agg");
      await registerEventType("unreg-agg/event");

      await registerAggregate("unreg-test", {
        categoryId: "unreg-agg",
        initialState: {},
        reducers: {
          event: sql`v_state`,
        },
      });

      const result = await unregisterAggregate("unreg-test");
      assert.strictEqual(result, true);

      const aggregates = await listAggregates();
      assert.ok(!aggregates.some((a) => a.name === "unreg-test"));
    });
  });

  describe("Projections", () => {
    itWithDb("should register a projection with sync handlers", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("proj-sync/created");

      // Create a test table for the projection
      await sql`
        create table if not exists test_projections (id uuid primary key, name text)
      `.exec();

      const projection = await registerProjection("sync-projection", {
        handlers: {
          "proj-sync/created": sql`
            insert into
              test_projections (id, name)
            values
              (new.stream_id, new.data ->> 'name')
            on conflict (id) do update
            set
              name = excluded.name
          `,
        },
      });

      assert.strictEqual(projection.name, "sync-projection");
      assert.ok(projection.triggerName !== null);
      assert.ok(projection.syncTypes.includes("proj-sync/created"));
    });

    itWithDb("should list projections", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("list-proj/event");

      await registerProjection("list-test-projection", {
        handlers: {
          "list-proj/event": sql`
            select
              1
          `,
        },
      });

      const projections = await listProjections();
      assert.ok(projections.some((p) => p.name === "list-test-projection"));
    });

    itWithDb("should unregister a projection", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("unreg-proj/event");

      await registerProjection("unreg-test-projection", {
        handlers: {
          "unreg-proj/event": sql`
            select
              1
          `,
        },
      });

      const result = await unregisterProjection("unreg-test-projection");
      assert.strictEqual(result, true);

      const projections = await listProjections();
      assert.ok(!projections.some((p) => p.name === "unreg-test-projection"));
    });

    itWithDb("should get projection handlers from registry", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("handler-reg/event");

      const asyncHandler = async (event: Event) => {
        // no-op async handler for testing
      };

      await registerProjection("handler-test-projection", {
        handlers: {
          "handler-reg/event": asyncHandler,
        },
      });

      const handlers = getProjectionHandlers("handler-test-projection");
      assert.ok(handlers !== undefined);
      assert.ok(handlers.has("handler-reg/event"));
    });

    itWithDb("should start and stop projection runner", async () => {
      const sql = getSql({ camelize: false });
      await registerEventType("runner-test/event");

      await registerProjection("runner-projection", {
        handlers: {
          "runner-test/event": async () => {},
        },
      });

      const runner = startProjections({
        projections: ["runner-projection"],
        pollingInterval: 100,
      });

      assert.strictEqual(runner.isRunning(), true);

      runner.stop();

      // Give it a moment to stop
      await new Promise((resolve) => setTimeout(resolve, 150));
      assert.strictEqual(runner.isRunning(), false);
    });
  });

  describe("Edge Cases", () => {
    itWithDb("should handle reading from empty stream", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("empty-stream");
      await ensureStream(streamId, "empty-stream");

      const events = await readStream(streamId);
      assert.strictEqual(events.length, 0);
    });

    itWithDb("should handle limit parameter when reading events", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("limit-test/event");

      for (let i = 0; i < 10; i++) {
        await append("limit-test/event", streamId, { n: i });
      }

      const events = await readStream(streamId, { limit: 3 });
      assert.strictEqual(events.length, 3);
      assert.deepStrictEqual(events[0].data, { n: 0 });
      assert.deepStrictEqual(events[2].data, { n: 2 });
    });

    itWithDb("should handle subscription with stream filters", async () => {
      const sql = getSql({ camelize: false });
      const targetStreamId = randomUUID();
      const otherStreamId = randomUUID();
      await registerEventType("stream-filter/event");

      // Ensure both streams exist
      await ensureStream(targetStreamId, "stream-filter");
      await ensureStream(otherStreamId, "stream-filter");

      // Create subscription filtering to specific stream
      await createSubscription("stream-filtered-sub", {
        filterStreams: [targetStreamId],
      });

      // Append events to both streams
      await append("stream-filter/event", targetStreamId, {
        target: true,
      });
      await append("stream-filter/event", otherStreamId, {
        target: false,
      });

      // Poll should only return events from target stream
      const events = await poll("stream-filtered-sub", 100);
      assert.ok(events.every((e) => e.streamId === targetStreamId));
    });

    itWithDb(
      "should correctly update stream version after events",
      async () => {
        const sql = getSql({ camelize: false });
        const streamId = randomUUID();
        await registerEventType("version-update/event");

        await append("version-update/event", streamId, { n: 1 });
        await append("version-update/event", streamId, { n: 2 });

        const stream = await getStream(streamId);
        assert.ok(stream !== null);
        assert.strictEqual(stream.version, 2);
        assert.strictEqual(stream.eventCount, 2);
      },
    );

    itWithDb("should handle multiple event types in same stream", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("multi-type/created");
      await registerEventType("multi-type/updated");
      await registerEventType("multi-type/deleted");

      await append("multi-type/created", streamId, { action: "create" });
      await append("multi-type/updated", streamId, { action: "update" });
      await append("multi-type/deleted", streamId, { action: "delete" });

      const events = await readStream(streamId);
      assert.strictEqual(events.length, 3);
      assert.strictEqual(events[0].typeId, "created");
      assert.strictEqual(events[1].typeId, "updated");
      assert.strictEqual(events[2].typeId, "deleted");
    });

    itWithDb(
      "should handle concurrent subscription polling correctly",
      async () => {
        const sql = getSql({ camelize: false });
        const streamId = randomUUID();
        await registerEventType("concurrent/event");

        await createSubscription("concurrent-sub");

        // Add events
        await append("concurrent/event", streamId, { n: 1 });
        await append("concurrent/event", streamId, { n: 2 });

        // Simulate concurrent polling by polling twice quickly
        const [batch1, batch2] = await Promise.all([
          poll("concurrent-sub", 1),
          poll("concurrent-sub", 1),
        ]);

        // Each batch should get different events (SKIP LOCKED behavior)
        const allPositions = [...batch1, ...batch2].map((e) => e.position);
        const uniquePositions = new Set(allPositions);
        assert.strictEqual(
          uniquePositions.size,
          allPositions.length,
          "Concurrent polls should get unique events",
        );
      },
    );

    itWithDb("should snapshot update state correctly", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("snapshot-update");
      await ensureStream(streamId, "snapshot-update");

      // Save initial snapshot
      await saveSnapshot(streamId, 1, { count: 10 });

      // Update snapshot
      await saveSnapshot(streamId, 5, { count: 50 });

      const snapshot = await loadSnapshot(streamId);
      assert.ok(snapshot !== null);
      assert.strictEqual(snapshot.version, 5);
      assert.deepStrictEqual(snapshot.state, { count: 50 });
    });

    itWithDb("should handle readAll with filterStreams", async () => {
      const sql = getSql({ camelize: false });
      const stream1 = randomUUID();
      const stream2 = randomUUID();
      await registerEventType("filter-streams/event");

      await ensureStream(stream1, "filter-streams");
      await ensureStream(stream2, "filter-streams");

      await append("filter-streams/event", stream1, { source: 1 });
      await append("filter-streams/event", stream2, { source: 2 });
      await append("filter-streams/event", stream1, { source: 1 });

      const events = await readAll({ filterStreams: [stream1] });
      assert.ok(events.every((e) => e.streamId === stream1));
    });

    itWithDb(
      "should correctly report events behind in subscription",
      async () => {
        const sql = getSql({ camelize: false });
        const streamId = randomUUID();
        await registerEventType("behind/event");

        await createSubscription("behind-sub");

        // Append some events
        await append("behind/event", streamId, { n: 1 });
        await append("behind/event", streamId, { n: 2 });
        await append("behind/event", streamId, { n: 3 });

        const sub = await getSubscription("behind-sub");
        assert.ok(sub !== null);
        assert.ok(sub.eventsBehind >= 3);
      },
    );

    itWithDb("should handle aggregate with snapshotThreshold", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerCategory("auto-snap");
      await registerEventType("auto-snap/add");

      // Register aggregate with snapshot threshold
      await registerAggregate("auto-snap-agg", {
        categoryId: "auto-snap",
        initialState: { total: 0 },
        reducers: {
          add: sql`
            v_state || jsonb_build_object(
              'total',
              (v_state ->> 'total')::int + (v_event.data ->> 'amount')::int
            )
          `,
        },
        snapshotThreshold: 3,
      });

      // Add events
      await append("auto-snap/add", streamId, { amount: 10 });
      await append("auto-snap/add", streamId, { amount: 20 });
      await append("auto-snap/add", streamId, { amount: 30 });

      // Load aggregate (should trigger auto-snapshot due to threshold)
      const state = await loadRegisteredAggregate<{ total: number }>(
        "auto-snap-agg",
        streamId,
      );

      assert.ok(state !== null);
      assert.strictEqual(state.total, 60); // 10 + 20 + 30
    });

    itWithDb("should return null for non-existent event by ID", async () => {
      const sql = getSql({ camelize: false });
      const event = await getEvent(randomUUID());
      assert.strictEqual(event, null);
    });

    itWithDb(
      "should return null for non-existent event by position",
      async () => {
        const sql = getSql({ camelize: false });
        const event = await getEventAtPosition(999999999);
        assert.strictEqual(event, null);
      },
    );

    itWithDb("should handle deleting non-existent stream", async () => {
      const sql = getSql({ camelize: false });
      const deleted = await deleteStream(randomUUID());
      assert.strictEqual(deleted, false);
    });

    itWithDb("should handle deleting non-existent subscription", async () => {
      const sql = getSql({ camelize: false });
      const deleted = await deleteSubscription("non-existent-sub");
      assert.strictEqual(deleted, false);
    });

    itWithDb("should handle deleting non-existent snapshot", async () => {
      const sql = getSql({ camelize: false });
      const deleted = await deleteSnapshot(randomUUID());
      assert.strictEqual(deleted, false);
    });

    itWithDb("should update stream updatedAt on append", async () => {
      const sql = getSql({ camelize: false });
      const streamId = randomUUID();
      await registerEventType("updated-at/event");

      await append("updated-at/event", streamId, { n: 1 });
      const stream1 = await getStream(streamId);

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 50));

      await append("updated-at/event", streamId, { n: 2 });
      const stream2 = await getStream(streamId);

      assert.ok(stream1 !== null && stream2 !== null);
      assert.ok(stream2.updatedAt >= stream1.updatedAt);
    });
  });
});

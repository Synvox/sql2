# Events Plugin

A PostgreSQL-based event sourcing system with streams, projections, aggregates, and subscriptions.

## Overview

The Events plugin provides a complete event store for building event-sourced applications:

- **Append-only event log** with global ordering
- **Streams** for organizing events by aggregate
- **Categories and types** with referential integrity
- **Projections** with hybrid sync/async handlers
- **Aggregates** with server-side state computation
- **Optimistic concurrency** control
- **Snapshots** for efficient aggregate loading
- **Concurrent polling** using `SELECT FOR UPDATE SKIP LOCKED` for safe multi-worker event processing

## Installation

```typescript
import { eventsPlugin } from "sql2/events";

await eventsPlugin();
```

## Quick Start

### 1. Register Categories and Event Types

Before appending events, register the categories and event types you'll use. Event types use a `category/type` format:

```typescript
import { registerCategory, registerEventType } from "sql2/events";

// Register categories (kebab-case by convention)
await registerCategory("order");
await registerCategory("user");

// Register event types using "category/type" format
await registerEventType("order/created");
await registerEventType("order/item-added");
await registerEventType("order/shipped");
await registerEventType("order/cancelled");

await registerEventType("user/created");
await registerEventType("user/email-verified");
```

### 2. Append Events

Events are appended using the `category/type` format, making the category explicit:

```typescript
import { append } from "sql2/events";
import { randomUUID } from "crypto";

const orderId = randomUUID();

// Append a single event (format: "category/type")
await append("order/created", orderId, { customer_id: "cust-1" });

// Append with optimistic concurrency
await append(
  "order/item-added",
  orderId,
  { sku: "widget", qty: 2, price: 29.99 },
  { expectedVersion: 1 },
);

// Append more events
await append(
  "order/item-added",
  orderId,
  { sku: "gadget", qty: 1, price: 49.99 },
  { expectedVersion: 2 },
);
await append(
  "order/shipped",
  orderId,
  { carrier: "FedEx" },
  { expectedVersion: 3 },
);
```

### 3. Build Projections

Projections transform events into read models. Use **sync handlers** (SQL) for critical updates and **async handlers** (functions) for side effects:

```typescript
import {
  registerProjection,
  startProjections,
} from "sql2/events";
import { getSql } from "sql2";

const sql = getSql();

await registerProjection("order-processing", {
  handlers: {
    // SYNC: Runs in same transaction as append
    // Handler keys use "category/type" format
    "order/created": sql`
      insert into
        order_summaries (id, customer_id, status, total)
      values
        (
          NEW.stream_id,
          NEW.data ->> 'customer_id',
          'pending',
          0
        )
    `,

    "order/item-added": sql`
      update order_summaries
      set
        total = total + (NEW.data ->> 'price')::NUMERIC * (NEW.data ->> 'qty')::INTEGER
      where
        id = NEW.stream_id
    `,

    // ASYNC: Runs after commit via polling
    "order/shipped": async (event) => {
      await sendShippingNotification(event.streamId);
    },
  },
});

// Start async handler processing
const runner = startProjections();

// Graceful shutdown
runner.stop();
```

### 4. Load Aggregates

Register aggregates to compute state directly in PostgreSQL. State is returned as JSONB:

```typescript
import {
  registerAggregate,
  loadRegisteredAggregate,
} from "sql2/events";
import { getSql } from "sql2";

const sql = getSql();

await registerAggregate("order", {
  categoryId: "order",
  initialState: { customer_id: "", total: 0, status: "pending" },
  reducers: {
    // Like Redux: (state, event) => newState
    created: sql`
      v_state || jsonb_build_object(
        'customer_id',
        v_event.data ->> 'customer_id',
        'status',
        'pending'
      )
    `,
    "item-added": sql`
      v_state || jsonb_build_object(
        'total',
        (v_state ->> 'total')::NUMERIC + (v_event.data ->> 'price')::NUMERIC
      )
    `,
    shipped: sql`v_state || jsonb_build_object('status', 'shipped')`,
  },
});

// Load aggregate state (computed in PostgreSQL!)
const order = await loadRegisteredAggregate("order", orderId);
// { stream_id: "550e8400-...", version: 4, customer_id: "cust-1", total: 109.97, status: "shipped" }
```

## Categories and Event Types

The plugin uses lookup tables to enforce referential integrity for categories and event types. Event types are scoped to categories.

### Registering Categories

```typescript
import {
  registerCategory,
  unregisterCategory,
  listCategories,
} from "sql2/events";

// Register a category
await registerCategory("order");

// List all categories
const categories = await listCategories();
// ["order", "user", ...]

// Unregister (cascades to types and events)
await unregisterCategory("order");
```

### Registering Event Types

```typescript
import {
  registerEventType,
  unregisterEventType,
  listEventTypes,
} from "sql2/events";

// Register event types using "category/type" format
await registerEventType("order/created");
await registerEventType("order/item-added");
await registerEventType("order/shipped");

// List types for a category
const orderTypes = await listEventTypes("order");
// [{ categoryId: "order", id: "created" }, { categoryId: "order", id: "shipped" }, ...]

// List all types
const allTypes = await listEventTypes();

// Unregister a type
await unregisterEventType("order/cancelled");
```

### Naming Convention

Categories and event types use **kebab-case** by convention:

| Good             | Avoid           |
| ---------------- | --------------- |
| `order`          | `Order`         |
| `user-account`   | `UserAccount`   |
| `item-added`     | `ItemAdded`     |
| `email-verified` | `EmailVerified` |

## Projections

Projections are the primary way to build read models from events. The plugin supports **hybrid projections** with both sync and async handlers.

### Handler Types

| Type      | Syntax           | When it Runs          | Use For                      |
| --------- | ---------------- | --------------------- | ---------------------------- |
| **Sync**  | `sql\`...\``     | In trigger, same TX   | Critical updates, counters   |
| **Async** | `async () => {}` | After commit, polling | Notifications, external APIs |

### Sync Handlers (SQL Triggers)

Sync handlers run inside the same transaction as the append. If they fail, the append rolls back.

```typescript
import { getSql } from "sql2";

const sql = getSql();

await registerProjection("inventory", {
  handlers: {
    "order/item-added": sql`
      update inventory
      set
        reserved = reserved + (NEW.data ->> 'qty')::INTEGER
      where
        sku = NEW.data ->> 'sku'
    `,
    "order/cancelled": sql`
      update inventory
      set
        reserved = reserved - (NEW.data ->> 'qty')::INTEGER
      where
        sku = NEW.data ->> 'sku'
    `,
  },
});
```

**Available variables in sync handlers:**

| Variable             | Description            |
| -------------------- | ---------------------- |
| `NEW.stream_id`      | Stream ID (UUID)       |
| `NEW.type_id`        | Event type             |
| `NEW.category_id`    | Event category         |
| `NEW.data`           | Event data (JSONB)     |
| `NEW.data->>'field'` | Field as TEXT          |
| `NEW.metadata`       | Event metadata (JSONB) |
| `NEW.position`       | Global position        |
| `NEW.stream_version` | Version in stream      |
| `NEW.created_at`     | Timestamp              |

### Async Handlers (TypeScript Functions)

Async handlers run after the transaction commits, via subscription polling. They're ideal for side effects.

```typescript
await registerProjection("notifications", {
  handlers: {
    "order/shipped": async (event) => {
      const customer = await getCustomer(event.data.customer_id);
      await sendEmail(customer.email, "Your order shipped!");
    },
    "order/delivered": async (event) => {
      await analytics.track("order_delivered", { orderId: event.streamId });
    },
  },
});

// Start processing
const runner = startProjections({
  pollingInterval: 1000,
  onError: (err, event) => console.error(`Failed: ${event.id}`, err),
});
```

### Mixed Handlers

Combine sync and async handlers in one projection:

```typescript
import { getSql } from "sql2";

const sql = getSql();

await registerProjection("order-processing", {
  handlers: {
    // Sync: update read model immediately
    "order/created": sql`
      insert into
        order_summaries (id, customer_id, status)
      values
        (
          NEW.stream_id,
          NEW.data ->> 'customer_id',
          'pending'
        )
    `,

    // Async: send notification after commit
    "order/shipped": async (event) => {
      await notifyCustomer(event.streamId, "shipped");
    },
  },
});
```

### Managing Projections

```typescript
import { listProjections, unregisterProjection } from "sql2/events";

// List all projections
const projections = await listProjections();
// [{ name, syncTypes, asyncTypes, eventsBehind, triggerName, subscriptionName }]

// Unregister (removes trigger and subscription)
await unregisterProjection("order-processing");
```

## Aggregates

Aggregates compute state from events. Register them to create PostgreSQL functions that load state server-side.

### Registering Aggregates

```typescript
import { registerAggregate } from "sql2/events";
import { getSql } from "sql2";

const sql = getSql();

await registerAggregate("cart", {
  categoryId: "cart",
  initialState: { items: 0, subtotal: 0, discount: 0, total: 0 },
  reducers: {
    // Each reducer returns the new state (like Redux)
    "item-added": sql`
      v_state || jsonb_build_object(
        'items',
        (v_state ->> 'items')::INTEGER + (v_event.data ->> 'qty')::INTEGER,
        'subtotal',
        (v_state ->> 'subtotal')::NUMERIC + (v_event.data ->> 'price')::NUMERIC,
        'total',
        (v_state ->> 'subtotal')::NUMERIC + (v_event.data ->> 'price')::NUMERIC - (v_state ->> 'discount')::NUMERIC
      )
    `,
    "coupon-applied": sql`
      v_state || jsonb_build_object(
        'discount',
        (v_event.data ->> 'amount')::NUMERIC,
        'total',
        (v_state ->> 'subtotal')::NUMERIC - (v_event.data ->> 'amount')::NUMERIC
      )
    `,
  },
});
```

### Loading Aggregates

```typescript
import { loadRegisteredAggregate } from "sql2/events";
import { getSql } from "sql2";

const sql = getSql();

// Load via TypeScript helper
const cart = await loadRegisteredAggregate("cart", cartId);

// Or call the generated SQL function directly
const result = await sql`
  select
    *
  from
    events.load_cart (${cartId}::uuid)
`.first();
```

### Reducer Variables

Since aggregates return JSONB, access state fields using JSONB operators:

| Variable                         | Description              |
| -------------------------------- | ------------------------ |
| `v_state->>'field'`              | State field as TEXT      |
| `(v_state->>'field')::TYPE`      | State field cast to type |
| `v_event.data`                   | Event data (JSONB)       |
| `v_event.data->>'field'`         | Event field as TEXT      |
| `(v_event.data->>'field')::TYPE` | Event field cast to type |

### TypeScript Reducers

For complex logic, use TypeScript reducers instead:

```typescript
import { loadAggregate, saveSnapshot } from "sql2/events";

const reducer = (state, event) => {
  switch (event.typeId) {
    case "created":
      return {
        ...state,
        customerId: event.data.customer_id,
        status: "pending",
      };
    case "item-added":
      return {
        ...state,
        total: state.total + event.data.price * event.data.qty,
      };
    case "shipped":
      return { ...state, status: "shipped" };
    default:
      return state;
  }
};

const initialState = { customerId: "", total: 0, status: "draft" };

const { state, version } = await loadAggregate(
  orderId,
  reducer,
  initialState,
);

// Optionally save a snapshot for faster loading
await saveSnapshot(orderId, version, state);
```

## Reading Events

### Read a Stream

```typescript
import { readStream } from "sql2/events";

// Read all events for a stream
const events = await readStream(orderId);

// With options
const events = await readStream(orderId, {
  fromVersion: 5,
  limit: 100,
  direction: "backward",
});
```

### Read All Events

```typescript
import { readAll } from "sql2/events";

const events = await readAll({
  fromPosition: 0,
  limit: 1000,
  filterTypes: ["order/created", "order/shipped"],
});
```

### Read by Category or Type

```typescript
import { readByCategory, readByType } from "sql2/events";

// All events in a category
const orderEvents = await readByCategory("order");

// Events of a specific type (using "category/type" format)
const shippedEvents = await readByType("order/shipped");
```

## Subscriptions

Subscriptions track reading position for consumers. The plugin uses `SELECT FOR UPDATE SKIP LOCKED` to enable **safe concurrent polling** - multiple workers can poll the same subscription without processing duplicate events.

### Basic Usage

```typescript
import {
  createSubscription,
  poll,
  ack,
} from "sql2/events";

// Create a subscription (use "category/type" format)
const sub = await createSubscription("order-processor", {
  filterTypes: ["order/created", "order/shipped"],
});

// Poll for new events (safe for concurrent workers)
const events = await poll("order-processor", 100);

// Process and acknowledge each event
for (const event of events) {
  await processEvent(event);
  await ack("order-processor", event.position);
}
```

### Concurrent Workers

Multiple workers can safely poll the same subscription:

```typescript
// Worker 1, 2, 3... can all run concurrently
const events = await poll("order-processor", 10, 300); // 300s claim timeout

for (const event of events) {
  // Each event is claimed exclusively by this worker
  await processEvent(event);
  await ack("order-processor", event.position);
}
```

This ensures:

- Each event is processed by only one worker
- Workers don't block each other
- No events are lost or duplicated

### Claim Cleanup

Event claims automatically expire, but you can manually clean them up:

```typescript
import {
  cleanupExpiredClaims,
  cleanupSubscriptionClaims,
} from "sql2/events";

// Clean up all expired claims (run periodically)
const removed = await cleanupExpiredClaims();

// Clean up all claims for a specific subscription (e.g., when a worker crashes)
await cleanupSubscriptionClaims("order-processor");
```

## Snapshots

Save and load snapshots for faster aggregate loading:

```typescript
import {
  saveSnapshot,
  loadSnapshot,
  deleteSnapshot,
} from "sql2/events";

// Save a snapshot
await saveSnapshot(orderId, version, currentState);

// Load latest snapshot
const snapshot = await loadSnapshot(orderId);
// { streamId, version, state, createdAt }

// Delete a snapshot
await deleteSnapshot(orderId);
```

## Statistics

```typescript
import {
  getStats,
  getStreamStats,
  getTypeStats,
  getCategoryStats,
} from "sql2/events";

// Global statistics
const stats = await getStats();
// { totalEvents, totalStreams, totalSubscriptions, maxPosition }

// Per-stream statistics
const streamStats = await getStreamStats(orderId);
// [{ streamId, category, eventCount, version, createdAt, updatedAt }]

// Type statistics
const typeStats = await getTypeStats();
// [{ type, count, firstSeen, lastSeen }]

// Category statistics
const catStats = await getCategoryStats();
// [{ category, streamCount, eventCount, firstSeen, lastSeen }]
```

## API Reference

### Core Functions

| Function                                      | Description                                    |
| --------------------------------------------- | ---------------------------------------------- |
| `eventsPlugin()`                              | Initialize the events schema                   |
| `append(eventType, streamId, data, options?)` | Append event (`eventType` = `"category/type"`) |
| `readStream(streamId, options?)`              | Read stream events                             |
| `readAll(options?)`                           | Read all events                                |
| `readByCategory(category, options?)`          | Read by category                               |
| `readByType(eventType, options?)`             | Read by type (`eventType` = `"category/type"`) |

### Category & Type Registration

| Function                         | Description                                             |
| -------------------------------- | ------------------------------------------------------- |
| `registerCategory(id)`           | Register a category                                     |
| `unregisterCategory(id)`         | Unregister a category                                   |
| `listCategories()`               | List all categories                                     |
| `registerEventType(eventType)`   | Register event type (`eventType` = `"category/type"`)   |
| `unregisterEventType(eventType)` | Unregister event type (`eventType` = `"category/type"`) |
| `listEventTypes(category?)`      | List event types                                        |

### Projections

| Function                            | Description                |
| ----------------------------------- | -------------------------- |
| `registerProjection(name, options)` | Register hybrid projection |
| `unregisterProjection(name)`        | Unregister projection      |
| `listProjections()`                 | List all projections       |
| `startProjections(options?)`        | Start async processing     |
| `getProjectionHandlers(name)`       | Get async handlers         |

### Aggregates

| Function                                    | Description                |
| ------------------------------------------- | -------------------------- |
| `registerAggregate(name, options)`          | Register SQL aggregate     |
| `unregisterAggregate(name)`                 | Unregister aggregate       |
| `listAggregates()`                          | List registered aggregates |
| `loadRegisteredAggregate(name, streamId)`   | Load via SQL function      |
| `loadAggregate(streamId, reducer, initial)` | Load via TS reducer        |

### Subscriptions

| Function                             | Description         |
| ------------------------------------ | ------------------- |
| `createSubscription(name, options?)` | Create subscription |
| `deleteSubscription(name)`           | Delete subscription |
| `poll(name, batchSize?, timeout?)`   | Poll for events     |
| `ack(name, position)`                | Acknowledge events  |

### Snapshots

| Function                                 | Description          |
| ---------------------------------------- | -------------------- |
| `saveSnapshot(streamId, version, state)` | Save snapshot        |
| `loadSnapshot(streamId)`                 | Load latest snapshot |
| `deleteSnapshot(streamId)`               | Delete snapshot      |

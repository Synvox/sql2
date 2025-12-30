import * as fsp from "node:fs/promises";
import { getSql, Statement } from "../../sql2.ts";

// ========================================
// Helpers
// ========================================

/**
 * Parses a "category/type" event type string into its components.
 * @example parseEventType("order/created") // { category: "order", type: "created" }
 */
function parseEventType(eventType: string): { category: string; type: string } {
  const slashIndex = eventType.indexOf("/");
  if (
    slashIndex === -1 ||
    slashIndex === 0 ||
    slashIndex === eventType.length - 1
  ) {
    throw new Error(
      `Invalid event type format "${eventType}". Expected "category/type" (e.g., "order/created").`,
    );
  }
  return {
    category: eventType.slice(0, slashIndex),
    type: eventType.slice(slashIndex + 1),
  };
}

// ========================================
// Types
// ========================================

/**
 * A stream represents an aggregate or event category
 */
export interface Stream {
  id: string;
  categoryId: string;
  version: number;
  createdAt: Date;
}

/**
 * Stream with additional statistics
 */
export interface StreamWithStats extends Stream {
  eventCount: number;
  updatedAt: Date;
}

/**
 * An event in the event store
 */
export interface Event<T = unknown, M = unknown> {
  position: number;
  id: string;
  streamId: string;
  streamVersion: number;
  categoryId: string;
  typeId: string;
  data: T;
  metadata: M;
  createdAt: Date;
}

/**
 * An event with stream category info (alias for backwards compatibility)
 */
export interface EventWithCategory<T = unknown, M = unknown>
  extends Event<T, M> {
  streamCategoryId: string;
}

/**
 * An event type entry from the lookup table
 */
export interface EventTypeEntry {
  categoryId: string;
  id: string;
}

/**
 * Options for appending an event
 */
export interface AppendOptions {
  /** Expected stream version for optimistic concurrency (-1 = stream must not exist, null = any) */
  expectedVersion?: number | null;
  /** Event metadata */
  metadata?: unknown;
}

/**
 * Result of appending an event
 */
export interface AppendResult {
  position: number;
  id: string;
  streamId: string;
  streamVersion: number;
  categoryId: string;
  typeId: string;
  createdAt: Date;
}

/**
 * Event data for batch append
 */
export interface EventData<T = unknown, M = unknown> {
  type: string;
  data?: T;
  metadata?: M;
}

/**
 * Options for reading events
 */
export interface ReadOptions {
  /** Start reading from this version/position */
  fromVersion?: number;
  /** Maximum number of events to return */
  limit?: number;
  /** Direction to read ('forward' or 'backward') */
  direction?: "forward" | "backward";
}

/**
 * Options for reading all events
 */
export interface ReadAllOptions {
  /** Start reading from this global position */
  fromPosition?: number;
  /** Maximum number of events to return */
  limit?: number;
  /** Filter by event types in "category/type" format */
  filterTypes?: string[];
  /** Filter by specific stream IDs */
  filterStreams?: string[];
}

/**
 * Subscription configuration
 */
export interface Subscription {
  name: string;
  /** Event type filters in "category/type" format */
  filterTypes: string[] | null;
  filterStreams: string[] | null;
  lastPosition: number;
  lastProcessedAt: Date | null;
  active: boolean;
  createdAt: Date;
  eventsBehind: number;
}

/**
 * Options for creating a subscription
 */
export interface CreateSubscriptionOptions {
  /** Filter by event types in "category/type" format (e.g., ["order/created", "order/shipped"]) */
  filterTypes?: string[];
  /** Filter by specific stream IDs */
  filterStreams?: string[];
  /** Starting position (default: 0 = from beginning) */
  startPosition?: number;
}

/**
 * Snapshot of aggregate state
 */
export interface Snapshot<T = unknown> {
  streamId: string;
  name: string;
  version: number;
  state: T;
  createdAt: Date;
}

/**
 * Global statistics
 */
export interface GlobalStats {
  totalEvents: number;
  totalStreams: number;
  totalSubscriptions: number;
  maxPosition: number;
  eventsToday: number;
  eventsThisHour: number;
}

/**
 * Stream statistics
 */
export interface StreamStats {
  streamId: string;
  categoryId: string;
  eventCount: number;
  version: number;
  firstEventAt: Date | null;
  lastEventAt: Date | null;
}

/**
 * Event type statistics
 */
export interface TypeStats {
  typeId: string;
  count: number;
  firstAt: Date;
  lastAt: Date;
}

/**
 * Category statistics
 */
export interface CategoryStats {
  categoryId: string;
  streamCount: number;
  eventCount: number;
}

// ========================================
// Plugin Installation
// ========================================

/**
 * Installs the events schema and helper functions.
 * Call this once before using any events functions.
 */
export async function eventsPlugin() {
  const sql = getSql({ camelize: false });

  const sqlScript = await fsp.readFile(
    new URL("./events.sql", import.meta.url),
    "utf-8",
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

// ========================================
// Category & Type Registration
// ========================================

/**
 * Registers a category. Categories must be registered before use.
 * Convention: use kebab-case identifiers (e.g., "order", "user", "shopping-cart")
 *
 * @param id - Category ID (kebab-case recommended)
 * @returns The registered category ID
 */
export async function registerCategory(id: string): Promise<string> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.register_category (${id}) as id
  `.first<{ id: string }>();
  return row!.id;
}

/**
 * Unregisters a category. This will cascade delete all associated types.
 *
 * @param id - Category ID
 * @returns True if the category was deleted
 */
export async function unregisterCategory(id: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.unregister_category (${id}) as deleted
  `.first<{ deleted: boolean }>();
  return row!.deleted;
}

/**
 * Lists all registered categories.
 *
 * @returns Array of category IDs
 */
export async function listCategories(): Promise<string[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.list_categories ()
  `.all<{ out_id: string }>();
  return rows.map((r) => r.out_id);
}

/**
 * Registers an event type. Types must be registered before use.
 *
 * @param eventType - Event type in "category/type" format (e.g., "order/created")
 * @returns The registered event type
 *
 * @example
 * await registerEventType(sql, "order/created");
 * await registerEventType(sql, "order/item-added");
 */
export async function registerEventType(
  eventType: string,
): Promise<EventTypeEntry> {
  const sql = getSql({ camelize: false });
  const { category, type } = parseEventType(eventType);
  const row = await sql`
    select
      *
    from
      events.register_event_type (
        ${category},
        ${type}
      )
  `.first<{ out_category_id: string; out_id: string }>();
  return { categoryId: row!.out_category_id, id: row!.out_id };
}

/**
 * Unregisters an event type.
 *
 * @param eventType - Event type in "category/type" format (e.g., "order/created")
 * @returns True if the type was deleted
 */
export async function unregisterEventType(eventType: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const { category, type } = parseEventType(eventType);
  const row = await sql`
    select
      events.unregister_event_type (
        ${category},
        ${type}
      ) as deleted
  `.first<{ deleted: boolean }>();
  return row!.deleted;
}

/**
 * Lists all registered event types, optionally filtered by category.
 *
 * @param category - Optional category to filter by
 * @returns Array of registered event types
 */
export async function listEventTypes(
  category?: string,
): Promise<EventTypeEntry[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.list_event_types (${category ?? null})
  `.all<{ out_category_id: string; out_id: string }>();
  return rows.map((r) => ({ categoryId: r.out_category_id, id: r.out_id }));
}

// ========================================
// Stream Management
// ========================================

/**
 * Ensures a stream exists, creating it if necessary.
 *
 * @param streamId - Stream ID
 * @param category - Stream category (default: 'default')
 * @returns The stream
 */
export async function ensureStream(
  streamId: string,
  category: string = "default",
): Promise<Stream> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.ensure_stream (
        ${streamId},
        ${category}
      )
  `.first<{
    out_id: string;
    out_category_id: string;
    out_version: number;
    out_created_at: Date;
  }>();

  return {
    id: row!.out_id,
    categoryId: row!.out_category_id,
    version: Number(row!.out_version),
    createdAt: row!.out_created_at,
  };
}

/**
 * Gets a stream by ID with statistics.
 *
 * @param streamId - Stream ID
 * @returns The stream with stats, or null if not found
 */
export async function getStream(
  streamId: string,
): Promise<StreamWithStats | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.get_stream (${streamId})
  `.first<{
    out_id: string;
    out_category_id: string;
    out_version: number;
    out_event_count: number;
    out_created_at: Date;
    out_updated_at: Date;
  }>();

  if (!row) return null;

  return {
    id: row.out_id,
    categoryId: row.out_category_id,
    version: Number(row.out_version),
    eventCount: Number(row.out_event_count),
    createdAt: row.out_created_at,
    updatedAt: row.out_updated_at,
  };
}

/**
 * Lists streams with optional category filter.
 *
 * @param options - Filter options
 * @returns Array of streams
 */
export async function listStreams(
  options: { category?: string; limit?: number; offset?: number } = {},
): Promise<Stream[]> {
  const sql = getSql({ camelize: false });
  const { category = null, limit = 100, offset = 0 } = options;

  const rows = await sql`
    select
      *
    from
      events.list_streams (
        ${category},
        ${limit},
        ${offset}
      )
  `.all<{
    out_id: string;
    out_category_id: string;
    out_version: number;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    id: row.out_id,
    categoryId: row.out_category_id,
    version: Number(row.out_version),
    createdAt: row.out_created_at,
  }));
}

/**
 * Deletes a stream and all its events.
 *
 * @param streamId - Stream ID to delete
 * @returns True if stream was deleted
 */
export async function deleteStream(streamId: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.delete_stream (${streamId}) as deleted
  `.first<{ deleted: boolean }>();
  return row?.deleted ?? false;
}

// ========================================
// Event Appending
// ========================================

/**
 * Appends an event to a stream.
 *
 * @param eventType - Event type in "category/type" format (e.g., "order/created")
 * @param streamId - Target stream ID
 * @param data - Event payload
 * @param options - Append options (expectedVersion, metadata)
 * @returns The appended event info
 * @throws Error if concurrency check fails
 *
 * @example
 * await append(sql, "order/created", streamId, { total: 100 });
 * await append(sql, "order/item-added", streamId, { item: "Widget" });
 */
export async function append<D = unknown>(
  eventType: string,
  streamId: string,
  data: D,
  options: AppendOptions = {},
): Promise<AppendResult> {
  const sql = getSql({ camelize: false });
  const { category, type } = parseEventType(eventType);
  const expectedVersion = options.expectedVersion ?? null;
  const metadata = options.metadata ?? {};

  const row = await sql`
    select
      *
    from
      events.append (
        ${streamId},
        ${type},
        ${JSON.stringify(data)}::jsonb,
        ${JSON.stringify(metadata)}::jsonb,
        ${expectedVersion}::bigint,
        ${category}
      )
  `.first<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_created_at: Date;
  }>();

  return {
    position: Number(row!.out_position),
    id: row!.out_id,
    streamId: row!.out_stream_id,
    streamVersion: Number(row!.out_stream_version),
    categoryId: row!.out_category_id,
    typeId: row!.out_type_id,
    createdAt: row!.out_created_at,
  };
}

// ========================================
// Event Reading
// ========================================

/**
 * Reads events from a specific stream.
 *
 * @param streamId - Stream to read from
 * @param options - Read options
 * @returns Array of events
 */
export async function readStream<D = unknown, M = unknown>(
  streamId: string,
  options: ReadOptions = {},
): Promise<Event<D, M>[]> {
  const sql = getSql({ camelize: false });
  const { fromVersion = 0, limit = 100, direction = "forward" } = options;

  const rows = await sql`
    select
      *
    from
      events.read_stream (
        ${streamId},
        ${fromVersion}::bigint,
        ${limit},
        ${direction}
      )
  `.all<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  }));
}

/**
 * Reads all events globally with optional filters.
 *
 * @param options - Read options with filters
 * @returns Array of events with category info
 */
export async function readAll<D = unknown, M = unknown>(
  options: ReadAllOptions = {},
): Promise<EventWithCategory<D, M>[]> {
  const sql = getSql({ camelize: false });

  const {
    fromPosition = 0,
    limit = 100,
    filterTypes = null,
    filterStreams = null,
  } = options;

  const rows = await sql`
    select
      *
    from
      events.read_all (
        ${fromPosition}::bigint,
        ${limit},
        ${filterTypes
          ? sql`
              array[${sql.join(filterTypes.map((t) => sql.literal(t)))}]::text[]
            `
          : sql`null`},
        ${filterStreams
          ? sql`
              array[${sql.join(
                filterStreams.map((s) => sql.literal(s)),
              )}]::uuid[]
            `
          : sql`null`}
      )
  `.all<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_category_id: string;
    out_stream_version: number;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    streamCategoryId: row.out_category_id, // backwards compatibility
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  }));
}

/**
 * Reads events by type.
 *
 * @param eventType - Event type in "category/type" format (e.g., "order/created")
 * @param options - Read options
 * @returns Array of events
 *
 * @example
 * const events = await readByType(sql, "order/created");
 */
export async function readByType<D = unknown, M = unknown>(
  eventType: string,
  options: { fromPosition?: number; limit?: number } = {},
): Promise<Event<D, M>[]> {
  const sql = getSql({ camelize: false });
  const { category, type } = parseEventType(eventType);
  const { fromPosition = 0, limit = 100 } = options;

  const rows = await sql`
    select
      *
    from
      events.read_by_type (
        ${category},
        ${type},
        ${fromPosition}::bigint,
        ${limit}
      )
  `.all<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  }));
}

/**
 * Reads events by category.
 *
 * @param category - Category to filter by
 * @param options - Read options
 * @returns Array of events
 */
export async function readByCategory<D = unknown, M = unknown>(
  category: string,
  options: { fromPosition?: number; limit?: number } = {},
): Promise<Event<D, M>[]> {
  const sql = getSql({ camelize: false });
  const { fromPosition = 0, limit = 100 } = options;

  const rows = await sql`
    select
      *
    from
      events.read_by_category (
        ${category},
        ${fromPosition}::bigint,
        ${limit}
      )
  `.all<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  }));
}

/**
 * Gets a single event by ID.
 *
 * @param eventId - Event UUID
 * @returns The event, or null if not found
 */
export async function getEvent<D = unknown, M = unknown>(
  eventId: string,
): Promise<Event<D, M> | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.get_event (${eventId}::uuid)
  `.first<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  if (!row) return null;

  return {
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  };
}

/**
 * Gets an event by global position.
 *
 * @param position - Global position
 * @returns The event, or null if not found
 */
export async function getEventAtPosition<D = unknown, M = unknown>(
  position: number,
): Promise<Event<D, M> | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.get_event_at_position (${position}::bigint)
  `.first<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_stream_version: number;
    out_category_id: string;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  if (!row) return null;

  return {
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  };
}

// ========================================
// Subscriptions
// ========================================

/**
 * Creates or updates a subscription.
 *
 * @param name - Subscription name
 * @param options - Subscription options
 * @returns The subscription
 */
export async function createSubscription(
  name: string,
  options: CreateSubscriptionOptions = {},
): Promise<Omit<Subscription, "lastProcessedAt" | "eventsBehind">> {
  const sql = getSql({ camelize: false });
  const {
    filterTypes = null,
    filterStreams = null,
    startPosition = 0,
  } = options;

  const row = await sql`
    select
      *
    from
      events.create_subscription (
        ${name},
        ${filterTypes
          ? sql`
              array[${sql.join(filterTypes.map((t) => sql.literal(t)))}]::text[]
            `
          : sql`null`},
        ${filterStreams
          ? sql`
              array[${sql.join(
                filterStreams.map((s) => sql.literal(s)),
              )}]::uuid[]
            `
          : sql`null`},
        ${startPosition}::bigint
      )
  `.first<{
    out_name: string;
    out_filter_types: string[] | null;
    out_filter_streams: string[] | null;
    out_last_position: number;
    out_active: boolean;
    out_created_at: Date;
  }>();

  return {
    name: row!.out_name,
    filterTypes: row!.out_filter_types,
    filterStreams: row!.out_filter_streams,
    lastPosition: Number(row!.out_last_position),
    active: row!.out_active,
    createdAt: row!.out_created_at,
  };
}

/**
 * Gets a subscription by name.
 *
 * @param name - Subscription name
 * @returns The subscription, or null if not found
 */
export async function getSubscription(
  name: string,
): Promise<Subscription | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.get_subscription (${name})
  `.first<{
    out_name: string;
    out_filter_types: string[] | null;
    out_filter_streams: string[] | null;
    out_last_position: number;
    out_last_processed_at: Date | null;
    out_active: boolean;
    out_created_at: Date;
    out_events_behind: number;
  }>();

  if (!row) return null;

  return {
    name: row.out_name,
    filterTypes: row.out_filter_types,
    filterStreams: row.out_filter_streams,
    lastPosition: Number(row.out_last_position),
    lastProcessedAt: row.out_last_processed_at,
    active: row.out_active,
    createdAt: row.out_created_at,
    eventsBehind: Number(row.out_events_behind),
  };
}

/**
 * Polls for new events on a subscription.
 * Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access.
 * Multiple workers can poll the same subscription without processing duplicates.
 *
 * @param subscriptionName - Subscription to poll
 * @param batchSize - Maximum events to return
 * @param claimTimeout - How long to hold claim in seconds (default: 300)
 * @returns Array of events
 */
export async function poll<D = unknown, M = unknown>(
  subscriptionName: string,
  batchSize: number = 100,
  claimTimeout: number = 300,
): Promise<EventWithCategory<D, M>[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.poll (
        ${subscriptionName},
        ${batchSize},
        ${claimTimeout}
      )
  `.all<{
    out_position: number;
    out_id: string;
    out_stream_id: string;
    out_category_id: string;
    out_stream_version: number;
    out_type_id: string;
    out_data: D;
    out_metadata: M;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    position: Number(row.out_position),
    id: row.out_id,
    streamId: row.out_stream_id,
    streamVersion: Number(row.out_stream_version),
    categoryId: row.out_category_id,
    streamCategoryId: row.out_category_id, // backwards compatibility
    typeId: row.out_type_id,
    data: row.out_data,
    metadata: row.out_metadata,
    createdAt: row.out_created_at,
  }));
}

/**
 * Acknowledges events up to a position.
 *
 * @param subscriptionName - Subscription name
 * @param position - Position to acknowledge up to
 * @returns True if acknowledged
 */
export async function ack(
  subscriptionName: string,
  position: number,
): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.ack (
        ${subscriptionName},
        ${position}::bigint
      ) as acknowledged
  `.first<{ acknowledged: boolean }>();
  return row?.acknowledged ?? false;
}

/**
 * Sets subscription active state.
 *
 * @param name - Subscription name
 * @param active - Whether to activate or deactivate
 * @returns True if updated
 */
export async function setSubscriptionActive(
  name: string,
  active: boolean,
): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.set_subscription_active (
        ${name},
        ${active}
      ) as updated
  `.first<{ updated: boolean }>();
  return row?.updated ?? false;
}

/**
 * Resets subscription position.
 *
 * @param name - Subscription name
 * @param position - Position to reset to (default: 0)
 * @returns True if reset
 */
export async function resetSubscription(
  name: string,
  position: number = 0,
): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.reset_subscription (
        ${name},
        ${position}::bigint
      ) as
    reset
  `.first<{ reset: boolean }>();
  return row?.reset ?? false;
}

/**
 * Cleans up expired event claims.
 * Should be called periodically to prevent table bloat from stale claims.
 *
 * @returns Number of expired claims removed
 */
export async function cleanupExpiredClaims(): Promise<number> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.cleanup_expired_claims () as count
  `.first<{ count: number }>();
  return row?.count ?? 0;
}

/**
 * Cleans up all claims for a specific subscription.
 * Useful when a worker crashes or needs to release its claims.
 *
 * @param subscriptionName - Subscription name
 * @returns Number of claims removed
 */
export async function cleanupSubscriptionClaims(
  subscriptionName: string,
): Promise<number> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.cleanup_subscription_claims (${subscriptionName}) as count
  `.first<{ count: number }>();
  return row?.count ?? 0;
}

/**
 * Lists all subscriptions.
 *
 * @returns Array of subscription summaries
 */
export async function listSubscriptions(): Promise<
  Array<{
    name: string;
    filterTypes: string[] | null;
    lastPosition: number;
    active: boolean;
    createdAt: Date;
    eventsBehind: number;
  }>
> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.list_subscriptions ()
  `.all<{
    out_name: string;
    out_filter_types: string[] | null;
    out_last_position: number;
    out_active: boolean;
    out_created_at: Date;
    out_events_behind: number;
  }>();

  return rows.map((row) => ({
    name: row.out_name,
    filterTypes: row.out_filter_types,
    lastPosition: Number(row.out_last_position),
    active: row.out_active,
    createdAt: row.out_created_at,
    eventsBehind: Number(row.out_events_behind),
  }));
}

/**
 * Deletes a subscription.
 *
 * @param name - Subscription name
 * @returns True if deleted
 */
export async function deleteSubscription(name: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.delete_subscription (${name}) as deleted
  `.first<{ deleted: boolean }>();
  return row?.deleted ?? false;
}

// ========================================
// Snapshots
// ========================================

/**
 * Saves a snapshot of aggregate state.
 *
 * @param streamId - Stream ID
 * @param version - Stream version at snapshot time
 * @param state - State to save
 * @param name - Snapshot name (default: 'aggregate-state')
 * @returns The saved snapshot info
 */
export async function saveSnapshot<S = unknown>(
  streamId: string,
  version: number,
  state: S,
  name: string = "aggregate-state",
): Promise<Omit<Snapshot<S>, "state">> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.save_snapshot (
        ${streamId},
        ${name},
        ${version}::bigint,
        ${JSON.stringify(state)}::jsonb
      )
  `.first<{
    out_stream_id: string;
    out_name: string;
    out_version: number;
    out_created_at: Date;
  }>();

  return {
    streamId: row!.out_stream_id,
    name: row!.out_name,
    version: Number(row!.out_version),
    createdAt: row!.out_created_at,
  };
}

/**
 * Loads a snapshot.
 *
 * @param streamId - Stream ID
 * @param name - Snapshot name (default: 'aggregate-state')
 * @returns The snapshot, or null if not found
 */
export async function loadSnapshot<S = unknown>(
  streamId: string,
  name: string = "aggregate-state",
): Promise<Snapshot<S> | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.load_snapshot (
        ${streamId},
        ${name}
      )
  `.first<{
    out_stream_id: string;
    out_name: string;
    out_version: number;
    out_state: S;
    out_created_at: Date;
  }>();

  if (!row) return null;

  return {
    streamId: row.out_stream_id,
    name: row.out_name,
    version: Number(row.out_version),
    state: row.out_state,
    createdAt: row.out_created_at,
  };
}

/**
 * Deletes a snapshot.
 *
 * @param streamId - Stream ID
 * @param name - Snapshot name (default: 'aggregate-state')
 * @returns True if deleted
 */
export async function deleteSnapshot(
  streamId: string,
  name: string = "aggregate-state",
): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.delete_snapshot (
        ${streamId},
        ${name}
      ) as deleted
  `.first<{ deleted: boolean }>();
  return row?.deleted ?? false;
}

// ========================================
// Statistics
// ========================================

/**
 * Gets global statistics.
 *
 * @returns Global stats
 */
export async function getStats(): Promise<GlobalStats> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      *
    from
      events.get_stats ()
  `.first<{
    out_total_events: number;
    out_total_streams: number;
    out_total_subscriptions: number;
    out_max_position: number;
    out_events_today: number;
    out_events_this_hour: number;
  }>();

  return {
    totalEvents: Number(row!.out_total_events),
    totalStreams: Number(row!.out_total_streams),
    totalSubscriptions: Number(row!.out_total_subscriptions),
    maxPosition: Number(row!.out_max_position),
    eventsToday: Number(row!.out_events_today),
    eventsThisHour: Number(row!.out_events_this_hour),
  };
}

/**
 * Gets stream statistics.
 *
 * @param streamId - Optional stream ID to filter by
 * @returns Array of stream stats
 */
export async function getStreamStats(
  streamId?: string,
): Promise<StreamStats[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.get_stream_stats (${streamId ?? null})
  `.all<{
    out_stream_id: string;
    out_category_id: string;
    out_event_count: number;
    out_version: number;
    out_first_event_at: Date | null;
    out_last_event_at: Date | null;
  }>();

  return rows.map((row) => ({
    streamId: row.out_stream_id,
    categoryId: row.out_category_id,
    eventCount: Number(row.out_event_count),
    version: Number(row.out_version),
    firstEventAt: row.out_first_event_at,
    lastEventAt: row.out_last_event_at,
  }));
}

/**
 * Gets event type statistics.
 *
 * @returns Array of type stats
 */
export async function getTypeStats(): Promise<TypeStats[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.get_type_stats ()
  `.all<{
    out_type_id: string;
    out_count: number;
    out_first_at: Date;
    out_last_at: Date;
  }>();

  return rows.map((row) => ({
    typeId: row.out_type_id,
    count: Number(row.out_count),
    firstAt: row.out_first_at,
    lastAt: row.out_last_at,
  }));
}

/**
 * Gets category statistics.
 *
 * @returns Array of category stats
 */
export async function getCategoryStats(): Promise<CategoryStats[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.get_category_stats ()
  `.all<{
    out_category_id: string;
    out_stream_count: number;
    out_event_count: number;
  }>();

  return rows.map((row) => ({
    categoryId: row.out_category_id,
    streamCount: Number(row.out_stream_count),
    eventCount: Number(row.out_event_count),
  }));
}

// ========================================
// Consumer Utilities
// ========================================

/**
 * Options for the subscribe function
 */
export interface SubscribeOptions {
  /** Number of events to fetch at once */
  batchSize?: number;
  /** How long to wait between polls when no events (ms) */
  pollingInterval?: number;
}

/**
 * Event handler function
 */
export type EventHandler<D = unknown, M = unknown> = (
  event: EventWithCategory<D, M>,
) => Promise<void>;

/**
 * Creates a simple polling consumer for processing events.
 * Returns a control object to stop the consumer.
 *
 * @param subscriptionName - Subscription to consume from
 * @param handler - Event handler function
 * @param options - Consumer options
 * @returns Control object with stop() method
 */
export function subscribe<D = unknown, M = unknown>(
  subscriptionName: string,
  handler: EventHandler<D, M>,
  options: SubscribeOptions = {},
): { stop: () => void } {
  const sql = getSql({ camelize: false });
  const { batchSize = 100, pollingInterval = 1000 } = options;

  let running = true;

  const consume = async () => {
    while (running) {
      try {
        const events = await poll<D, M>(subscriptionName, batchSize);

        if (events.length === 0) {
          await new Promise((resolve) => setTimeout(resolve, pollingInterval));
          continue;
        }

        // Process events sequentially to maintain order
        for (const event of events) {
          if (!running) break;
          await handler(event);
          await ack(subscriptionName, event.position);
        }
      } catch (err) {
        console.error("Consumer error:", err);
        await new Promise((resolve) => setTimeout(resolve, pollingInterval));
      }
    }
  };

  // Start consuming
  consume();

  return {
    stop: () => {
      running = false;
    },
  };
}

// ========================================
// Aggregate Helper
// ========================================

/**
 * Helper to load an aggregate by replaying events from a snapshot.
 *
 * @param streamId - Stream ID
 * @param reducer - Function to apply events to state
 * @param initialState - Initial state if no snapshot
 * @param snapshotName - Snapshot name (default: 'aggregate-state')
 * @returns Current state and version
 */
export async function loadAggregate<S, D = unknown, M = unknown>(
  streamId: string,
  reducer: (state: S, event: Event<D, M>) => S,
  initialState: S,
  snapshotName: string = "aggregate-state",
): Promise<{ state: S; version: number }> {
  const sql = getSql({ camelize: false });
  // Try to load snapshot
  const snapshot = await loadSnapshot<S>(streamId, snapshotName);

  let state = snapshot?.state ?? initialState;
  let fromVersion = snapshot?.version ?? 0;

  // Load events after snapshot
  const events = await readStream<D, M>(streamId, {
    fromVersion,
    limit: 10000, // Adjust based on expected aggregate size
  });

  // Apply events
  for (const event of events) {
    state = reducer(state, event);
    fromVersion = event.streamVersion;
  }

  return { state, version: fromVersion };
}

// ========================================
// Event Type Registry
// ========================================

/**
 * Registered event type info
 */

// ========================================
// Aggregate Registry
// ========================================

/**
 * Reducer definition for an event type.
 * Maps field names to SQL expressions (as Statement objects) that compute the new value.
 *
 * Available variables in expressions:
 * - v_state.field_name: Current state fields
 * - v_event.data: Event data as JSONB
 * - v_event.data->>'field': Access event data field as text
 * - (v_event.data->>'field')::TYPE: Cast event data to specific type
 */
/**
 * Reducer definitions map event types to SQL expressions that return JSONB to merge into state.
 */
export type ReducerDefinition = Record<string, Statement>;

/**
 * Options for registering an aggregate
 */
export interface RegisterAggregateOptions<S> {
  /** Stream category this aggregate handles */
  categoryId: string;
  /** Initial state before any events */
  initialState: S;
  /**
   * Reducer definitions for each event type.
   * Each reducer is a SQL expression that returns the new state (JSONB).
   * Like Redux, the reducer receives the current state and returns the next state.
   *
   * Available variables:
   * - `v_state` - Current state (JSONB)
   * - `v_event.data` - Event data (JSONB)
   *
   * @example
   * {
   *   created: sql`v_state || jsonb_build_object('customer_id', v_event.data->>'customer_id', 'status', 'pending')`,
   *   "item-added": sql`v_state || jsonb_build_object('total', (v_state->>'total')::NUMERIC + (v_event.data->>'price')::NUMERIC)`,
   *   shipped: sql`v_state || jsonb_build_object('status', 'shipped')`,
   * }
   */
  reducers: ReducerDefinition;
  /**
   * Auto-snapshot threshold. When set, the aggregate loader will automatically
   * save a snapshot after replaying this many events. This provides "snapshot on read"
   * behavior where snapshots stay up-to-date without explicit management.
   *
   * Set to null or omit to disable auto-snapshotting.
   *
   * @example
   * snapshotThreshold: 10 // Save snapshot after replaying 10+ events
   */
  snapshotThreshold?: number | null;
}

/**
 * Registered aggregate type info
 */
export interface RegisteredAggregateType {
  name: string;
  functionName: string;
  categoryId: string;
  eventTypes: string[];
  createdAt: Date;
}

/**
 * Registers an aggregate type and creates a typed loader function.
 * The generated function can be called directly from SQL to load aggregate state.
 *
 * @param name - Aggregate name (e.g., "order")
 * @param options - Aggregate configuration
 * @returns The registered aggregate info including the generated function name
 *
 * @example
 * await registerAggregate(sql, "order", {
 *   category: "order",
 *   initialState: { customer_id: "", total: 0, status: "unknown" },
 *   reducers: {
 *     // Like Redux: (state, event) => newState
 *     created: sql`v_state || jsonb_build_object('customer_id', v_event.data->>'customer_id', 'status', 'pending')`,
 *     "item-added": sql`v_state || jsonb_build_object('total', (v_state->>'total')::NUMERIC + (v_event.data->>'price')::NUMERIC)`,
 *     shipped: sql`v_state || jsonb_build_object('status', 'shipped')`,
 *   }
 * });
 *
 * // Now you can load aggregates directly from SQL:
 * // SELECT * FROM events.load_order('order-123');
 */
export async function registerAggregate<S>(
  name: string,
  options: RegisterAggregateOptions<S>,
): Promise<RegisteredAggregateType> {
  const sql = getSql({ camelize: false });
  // Compile reducer Statement objects to strings
  const compiledReducers: Record<string, string> = {};
  for (const [eventType, stmt] of Object.entries(options.reducers)) {
    if (stmt.values.length > 0) {
      throw new Error(
        `Reducer for event type "${eventType}" contains parameterized values. ` +
          `Reducers must be raw SQL expressions without parameters. ` +
          `Use raw SQL like sql\`v_state || jsonb_build_object('field', v_event.data->>'field')\` ` +
          `instead of sql\`v_state || jsonb_build_object('field', \${value})\`.`,
      );
    }
    compiledReducers[eventType] = stmt.compile();
  }

  const row = await sql`
    select
      *
    from
      events.register_aggregate (
        ${name},
        ${options.categoryId},
        ${JSON.stringify(options.initialState)}::jsonb,
        ${JSON.stringify(compiledReducers)}::jsonb,
        ${options.snapshotThreshold ?? null}
      )
  `.first<{
    out_name: string;
    out_function_name: string;
    out_category_id: string;
  }>();

  return {
    name: row!.out_name,
    functionName: row!.out_function_name,
    categoryId: row!.out_category_id,
    eventTypes: Object.keys(options.reducers),
    createdAt: new Date(),
  };
}

/**
 * Unregisters an aggregate type and drops its function/type.
 *
 * @param name - Aggregate name to unregister
 * @returns True if the aggregate was unregistered
 */
export async function unregisterAggregate(name: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.unregister_aggregate (${name}) as unregistered
  `.first<{ unregistered: boolean }>();
  return row?.unregistered ?? false;
}

/**
 * Lists all registered aggregate types.
 *
 * @returns Array of registered aggregate types
 */
export async function listAggregates(): Promise<RegisteredAggregateType[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.list_aggregates ()
  `.all<{
    out_name: string;
    out_function_name: string;
    out_category_id: string;
    out_state_schema: Record<string, string>;
    out_event_types: string[];
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    name: row.out_name,
    functionName: row.out_function_name,
    categoryId: row.out_category_id,
    eventTypes: row.out_event_types,
    createdAt: row.out_created_at,
  }));
}

/**
 * Loads an aggregate using its registered loader function.
 * This executes the SQL reducer on the server side.
 *
 * @param aggregateName - Registered aggregate name
 * @param streamId - Stream ID to load
 * @returns The aggregate state, or null if stream doesn't exist
 */
export async function loadRegisteredAggregate<
  S extends Record<string, unknown>,
>(
  aggregateName: string,
  streamId: string,
): Promise<(S & { stream_id: string; version: number }) | null> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events._load_aggregate_dynamic (
        ${aggregateName},
        ${streamId}
      ) as state
  `.first<{ state: S & { stream_id: string; version: number } }>();

  const state = row?.state;

  if (!state || state.version === 0) {
    // Check if stream exists but has no events vs doesn't exist
    const streamRow = await sql`
      select
        1
      from
        events.streams
      where
        id = ${streamId}
    `.first();

    if (!streamRow) {
      return null;
    }
  }

  return state ?? null;
}

// ========================================
// Projections
// ========================================

/**
 * Handler type for projections.
 * - Statement: Sync handler (runs via PostgreSQL trigger in same transaction)
 * - Function: Async handler (runs via subscription after commit)
 */
export type ProjectionHandler<D = unknown, M = unknown> =
  | Statement
  | ((event: Event<D, M>) => Promise<void>);

/**
 * Options for registering a projection
 */
export interface ProjectionOptions {
  /**
   * Handlers for each event type, keyed by "category/type".
   * - Statement (sql`...`): Runs synchronously in trigger
   * - Function: Runs asynchronously via subscription
   *
   * @example
   * {
   *   "order/created": sql`INSERT INTO summaries ...`,
   *   "order/shipped": async (event) => { ... },
   * }
   */
  handlers: Record<string, ProjectionHandler>;
  /** Start position for async handlers (default: current max position) */
  startPosition?: number;
}

/**
 * Registered projection info
 */
export interface RegisteredProjection {
  name: string;
  /** Sync handler event types (category/type format) */
  syncTypes: string[];
  /** Async handler event types (category/type format) */
  asyncTypes: string[];
  triggerName: string | null;
  subscriptionName: string | null;
  subscriptionPosition: number | null;
  eventsBehind: number;
  createdAt: Date;
}

/**
 * Projection runner that processes async handlers
 */
export interface ProjectionRunner {
  /** Stop the projection runner */
  stop: () => void;
  /** Check if runner is active */
  isRunning: () => boolean;
}

// Store async handlers in memory (keyed by projection name)
const asyncHandlerRegistry = new Map<
  string,
  Map<string, ProjectionHandler<unknown, unknown>>
>();

/**
 * Registers a projection with sync and/or async handlers.
 *
 * - Sync handlers (Statement): Run via PostgreSQL trigger in the same transaction
 * - Async handlers (Function): Run via subscription polling after commit
 *
 * Handler keys use "category/type" format to specify which events to handle.
 *
 * @param name - Projection name
 * @param options - Projection configuration
 * @returns The registered projection info
 *
 * @example
 * await registerProjection(sql, "order_processing", {
 *   handlers: {
 *     // Sync SQL handler - runs in transaction
 *     "order/created": sql`
 *       INSERT INTO order_summaries (id, customer_id, status)
 *       VALUES (NEW.stream_id, NEW.data->>'customer_id', 'pending')
 *     `,
 *     // Async function handler - runs after commit
 *     "order/shipped": async (event, sql) => {
 *       await sendShippingNotification(event.data.email);
 *     },
 *   },
 * });
 */
export async function registerProjection<D = unknown, M = unknown>(
  name: string,
  options: ProjectionOptions,
): Promise<RegisteredProjection> {
  const sql = getSql({ camelize: false });
  const { handlers, startPosition } = options;

  // Validate handler keys are in category/type format
  for (const eventType of Object.keys(handlers)) {
    parseEventType(eventType); // Throws if invalid format
  }

  // Separate sync (Statement) and async (Function) handlers
  const syncHandlers: Record<string, string> = {};
  const asyncHandlers: Map<string, ProjectionHandler<D, M>> = new Map();
  const asyncTypes: string[] = [];

  for (const [eventType, handler] of Object.entries(handlers)) {
    if (handler instanceof Statement) {
      if (handler.values.length > 0) {
        throw new Error(
          `Sync handler for event type "${eventType}" contains parameterized values. ` +
            `Sync handlers must be raw SQL without parameters. ` +
            `Use raw SQL like sql\`INSERT INTO table (col) VALUES (NEW.data->>'field')\` ` +
            `instead of sql\`INSERT INTO table (col) VALUES (\${value})\`.`,
        );
      }
      syncHandlers[eventType] = handler.compile();
    } else if (typeof handler === "function") {
      asyncHandlers.set(eventType, handler);
      asyncTypes.push(eventType);
    }
  }

  // Register sync handlers (creates trigger)
  if (Object.keys(syncHandlers).length > 0) {
    await sql`
      select
        *
      from
        events._register_projection_sync (
          ${name},
          ${JSON.stringify(syncHandlers)}::jsonb
        )
    `.query();
  }

  // Register async handlers (creates subscription)
  if (asyncTypes.length > 0) {
    await sql`
      select
        *
      from
        events._register_projection_async (
          ${name},
          array[${sql.join(asyncTypes.map((t) => sql.literal(t)))}]::text[],
          ${startPosition ?? null}::bigint
        )
    `.query();

    // Store async handlers in memory for the runner
    asyncHandlerRegistry.set(
      name,
      asyncHandlers as Map<string, ProjectionHandler<unknown, unknown>>,
    );
  }

  // Return projection info
  const projections = await listProjections();
  const projection = projections.find((p) => p.name === name);

  if (!projection) {
    throw new Error(`Failed to register projection "${name}"`);
  }

  return projection;
}

/**
 * Unregisters a projection and removes its trigger/subscription.
 *
 * @param name - Projection name to unregister
 * @returns True if the projection was unregistered
 */
export async function unregisterProjection(name: string): Promise<boolean> {
  const sql = getSql({ camelize: false });
  const row = await sql`
    select
      events.unregister_projection (${name}) as unregistered
  `.first<{ unregistered: boolean }>();

  // Remove from async handler registry
  asyncHandlerRegistry.delete(name);

  return row?.unregistered ?? false;
}

/**
 * Lists all registered projections.
 *
 * @returns Array of registered projections
 */
export async function listProjections(): Promise<RegisteredProjection[]> {
  const sql = getSql({ camelize: false });
  const rows = await sql`
    select
      *
    from
      events.list_projections ()
  `.all<{
    out_name: string;
    out_sync_types: string[];
    out_async_types: string[];
    out_trigger_name: string | null;
    out_subscription_name: string | null;
    out_subscription_position: number | null;
    out_events_behind: number;
    out_created_at: Date;
  }>();

  return rows.map((row) => ({
    name: row.out_name,
    syncTypes: row.out_sync_types ?? [],
    asyncTypes: row.out_async_types ?? [],
    triggerName: row.out_trigger_name,
    subscriptionName: row.out_subscription_name,
    subscriptionPosition: row.out_subscription_position
      ? Number(row.out_subscription_position)
      : null,
    eventsBehind: Number(row.out_events_behind ?? 0),
    createdAt: row.out_created_at,
  }));
}

/**
 * Starts a projection runner that processes async handlers.
 *
 * @param options - Runner options
 * @returns A runner control object with stop() method
 *
 * @example
 * const runner = startProjections(sql, {
 *   projections: ["order_processing"], // or omit for all
 *   pollingInterval: 1000,
 * });
 *
 * // Later: stop the runner
 * runner.stop();
 */
export function startProjections(
  options: {
    projections?: string[];
    pollingInterval?: number;
    batchSize?: number;
    onError?: (error: Error, event: Event) => void;
  } = {},
): ProjectionRunner {
  const sql = getSql({ camelize: false });
  const {
    projections: projectionFilter,
    pollingInterval = 1000,
    batchSize = 100,
    onError,
  } = options;

  let running = true;
  const abortController = new AbortController();

  const processProjection = async (projectionName: string) => {
    const handlers = asyncHandlerRegistry.get(projectionName);
    if (!handlers || handlers.size === 0) return;

    const subscriptionName = `projection:${projectionName}`;

    // Poll for events
    const events = await poll(subscriptionName, batchSize);

    for (const event of events) {
      // Look up handler by "category/type" format
      const eventKey = `${event.categoryId}/${event.typeId}`;
      const handler = handlers.get(eventKey);
      if (handler && typeof handler === "function") {
        try {
          await handler(event as Event<unknown, unknown>);
          await ack(subscriptionName, event.position);
        } catch (error) {
          if (onError) {
            onError(error as Error, event);
          } else {
            console.error(
              `Projection "${projectionName}" error on event ${event.id}:`,
              error,
            );
          }
          // Don't ack on error - will retry
          break;
        }
      } else {
        // No handler for this event, just ack it
        await ack(subscriptionName, event.position);
      }
    }
  };

  const runLoop = async () => {
    while (running) {
      try {
        // Get projections to process
        let projectionsToProcess: string[];

        if (projectionFilter) {
          projectionsToProcess = projectionFilter.filter((name) =>
            asyncHandlerRegistry.has(name),
          );
        } else {
          projectionsToProcess = Array.from(asyncHandlerRegistry.keys());
        }

        // Process each projection
        await Promise.all(
          projectionsToProcess.map((name) => processProjection(name)),
        );
      } catch (error) {
        console.error("Projection runner error:", error);
      }

      // Wait for next poll
      await new Promise<void>((resolve) => {
        const timeout = setTimeout(resolve, pollingInterval);
        abortController.signal.addEventListener("abort", () => {
          clearTimeout(timeout);
          resolve();
        });
      });
    }
  };

  // Start the loop
  runLoop();

  return {
    stop: () => {
      running = false;
      abortController.abort();
    },
    isRunning: () => running,
  };
}

/**
 * Gets the async handlers registered for a projection.
 * Useful for testing or debugging.
 *
 * @param name - Projection name
 * @returns Map of event type to handler, or undefined if not found
 */
export function getProjectionHandlers(
  name: string,
): Map<string, ProjectionHandler<unknown, unknown>> | undefined {
  return asyncHandlerRegistry.get(name);
}

# Event Handling

Reference for event handlers, projections, and the write-then-wait pattern in `@dcb-es/event-store-postgres`.

## Overview

> **Note:** Event handling is a separate, optional layer on top of the core [EventStore interface](../core/event-store-interface.md). You can use `PostgresEventStore` with just `append()`, `read()`, and `subscribe()` without any of the handler infrastructure below.

Event handling in the Postgres adapter provides:

- **`runHandler()`** -- subscribe-based handler runner with atomic bookmark+projection updates and NOTIFY on progress
- **`waitUntilProcessed()`** -- wait for a handler to reach a specific position (for synchronous read-after-write)
- **`ensureHandlersInstalled()`** -- idempotent schema and handler registration
- **`HandlerCatchup`** -- batch-oriented handler runner (prefer `runHandler` for new code)

---

## Handler bookmarks table

All handler infrastructure shares a bookmarks table that tracks each handler's last processed position.

### DDL

```sql
CREATE TABLE IF NOT EXISTS _handler_bookmarks (
    handler_id             TEXT PRIMARY KEY,
    last_sequence_position BIGINT
);
```

The table name defaults to `_handler_bookmarks` but can be overridden via the `bookmarkTableName` option on `runHandler()` and `waitUntilProcessed()`, or via the `tablePrefix` option on `HandlerCatchup`.

### Semantics

- Each handler is identified by a unique string (`handler_id`).
- `last_sequence_position` records the position of the last event successfully processed by that handler.
- New handlers are registered with position `0`, meaning they will process all events from the beginning.
- The bookmark is updated atomically within the same transaction as the handler's projection writes, ensuring exactly-once processing semantics (assuming idempotent projections on replay).

---

## ensureHandlersInstalled()

```typescript
import { ensureHandlersInstalled } from "@dcb-es/event-store-postgres"

await ensureHandlersInstalled(pool, ["projectionA", "projectionB"])
```

**Signature:**

```typescript
async function ensureHandlersInstalled(
    pool: Pool,
    handlerIds: string[],
    tableName: string
): Promise<void>
```

This function:

1. Creates the bookmarks table if it does not exist
2. Registers all handler IDs with `INSERT ... ON CONFLICT DO NOTHING`, initializing their position to `0`

Idempotent -- safe to call on every application startup. Existing handlers retain their current position.

Internally, `ensureHandlersInstalled` calls `registerHandlers` which performs:

```sql
INSERT INTO _handler_bookmarks (handler_id, last_sequence_position)
SELECT handler_id, 0
FROM unnest($1::text[]) handler_id
ON CONFLICT (handler_id) DO NOTHING
```

---

## runHandler()

The primary way to run event handlers. Subscribe-based, with atomic projection+bookmark updates and NOTIFY on bookmark advance.

```typescript
import { runHandler } from "@dcb-es/event-store-postgres"

const { promise } = runHandler({
    pool,
    eventStore,
    handlerName: "courseProjection",
    handlerFactory: (client) => ({
        when: {
            courseWasRegistered: async ({ event }) => {
                await client.query(
                    "INSERT INTO courses (id, title) VALUES ($1, $2)",
                    [event.data.courseId, event.data.title]
                )
            }
        }
    })
})
```

### HandlerRunnerOptions

```typescript
interface HandlerRunnerOptions {
    pool: Pool
    eventStore: EventStore
    handlerName: string
    handlerFactory: (client: PoolClient) => EventHandler<any, any>
    bookmarkTableName?: string    // defaults to "_handler_bookmarks"
    signal?: AbortSignal
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pool` | `Pool` | (required) | Connection pool for bookmark reads, transaction management, and NOTIFY |
| `eventStore` | `EventStore` | (required) | The event store to subscribe to |
| `handlerName` | `string` | (required) | Unique handler ID. Must match a registered ID in the bookmarks table. Must not contain `:` (used as notification delimiter). |
| `handlerFactory` | `(client: PoolClient) => EventHandler` | (required) | Factory that creates a handler bound to the transaction client. Called once per event. The handler's projection writes use this client, making them atomic with the bookmark update. |
| `bookmarkTableName` | `string` | `"_handler_bookmarks"` | Name of the bookmarks table |
| `signal` | `AbortSignal` | `undefined` | When aborted, the handler finishes processing the current event and exits cleanly |

### RunningHandler

```typescript
interface RunningHandler {
    promise: Promise<void>
}
```

The returned `promise` resolves when the signal is aborted (after completing the current event) or rejects on unrecoverable error. Callers decide retry policy.

### How it works

1. **Load bookmark:** Read `last_sequence_position` from the bookmarks table for this handler. Throws if the handler is not registered (call `ensureHandlersInstalled` first).

2. **Build subscribe query:** Instantiate the handler factory once (with a dummy client) to inspect its `when` keys and optional `tagFilter`. If the handler has specific event types and a tag filter, build a scoped `Query`; otherwise use `Query.all()`.

3. **Subscribe:** Call `eventStore.subscribe(query, { after: position, signal })` to get a live event stream starting after the handler's last processed position.

4. **For each event, in its own transaction:**
   ```
   BEGIN
     -> handlerFactory(client)          -- create handler bound to txn client
     -> handler.when[event.type](event) -- run projection (if handler handles this type)
     -> UPDATE bookmark to event.position
     -> pg_notify('<bookmarkTable>', '<handlerName>:<position>')
   COMMIT
   ```

5. **On abort:** The loop exits cleanly after the current event's transaction completes.

### NOTIFY on bookmark advance

After each event is processed, `runHandler` fires:

```sql
SELECT pg_notify('_handler_bookmarks', 'courseProjection:42')
```

The payload format is `<handlerName>:<position>`. This notification is consumed by `waitUntilProcessed()` for low-latency write-then-wait patterns.

### Error handling

- If the handler factory accesses the `client` during construction (before any event is processed), `runHandler` throws immediately with a diagnostic message.
- If a handler is not found in the bookmarks table, `runHandler` throws immediately.
- If any event's transaction fails, the promise rejects. The bookmark is not advanced. On restart, the handler replays from the last committed position.

---

## waitUntilProcessed()

Wait until a handler's bookmark has reached (or passed) a given position. Used after appending events to ensure a projection is up to date before reading.

```typescript
import { waitUntilProcessed } from "@dcb-es/event-store-postgres"

const position = await eventStore.append({
    events: [{ type: "CourseCreated", tags, data, metadata }]
})

await waitUntilProcessed(pool, "courseProjection", position)

// Now safe to query the courseProjection's read model
```

### Signature

```typescript
async function waitUntilProcessed(
    pool: Pool,
    handlerName: string,
    position: SequencePosition,
    options?: {
        timeoutMs?: number            // defaults to 5000
        bookmarkTableName?: string    // defaults to "_handler_bookmarks"
    }
): Promise<void>
```

Resolves when the handler's bookmark is at or past the given position. Throws `WaitTimeoutError` if the deadline is exceeded.

### Algorithm

The function uses a two-phase strategy that optimizes for the common case (handler processes the event within milliseconds) while still handling slow handlers efficiently:

**Phase 1 -- Fast poll with backoff:**

```
Check bookmark -> if reached, return
Wait 5ms  -> check -> if reached, return
Wait 15ms -> check -> if reached, return
Wait 30ms -> check -> if reached, return
Final check -> if reached, return
```

Three quick polls at 5ms, 15ms, and 30ms. Each check is a simple `SELECT last_sequence_position` query using a connection from the pool (not a dedicated connection). This avoids the overhead of establishing a LISTEN connection for the majority of cases where the handler is fast.

**Phase 2 -- LISTEN + poll (slow path):**

If the fast phase did not succeed:

1. Acquire a dedicated connection from the pool
2. `LISTEN <bookmarkTableName>` -- establish before the next check to prevent TOCTOU race
3. Loop until deadline:
   a. Check the bookmark (after LISTEN is established -- no missed notifications)
   b. If reached, return
   c. Wait for either a notification or a 100ms timeout
   d. On notification, re-check immediately
4. If deadline exceeded, throw `WaitTimeoutError`
5. `UNLISTEN` and release the connection

The LISTEN-before-check ordering prevents the race where a notification fires between the check and the LISTEN setup.

### WaitTimeoutError

```typescript
class WaitTimeoutError extends Error {
    constructor(handlerName: string, position: string, timeoutMs: number)
    name: "WaitTimeoutError"
}
```

Thrown when the handler does not reach the target position within the timeout. The error message includes the handler name, target position, and timeout value:

```
Timeout: handler "courseProjection" did not reach position 42 within 5000ms
```

---

## HandlerCatchup

```typescript
import { HandlerCatchup } from "@dcb-es/event-store-postgres"
```

> **Prefer `runHandler()` for new code.** `HandlerCatchup` is a batch-oriented, pull-based alternative. It does not subscribe to live events — it must be called explicitly to catch up.

### Constructor

```typescript
class HandlerCatchup {
    constructor(pool: Pool, eventStore: EventStore, tablePrefix?: string)
}
```

The `tablePrefix` option controls the bookmarks table name: `<tablePrefix>_handler_bookmarks` or `_handler_bookmarks` if omitted.

### ensureInstalled()

```typescript
async ensureInstalled(handlerIds: string[]): Promise<void>
```

Creates the bookmarks table and registers handler IDs. Delegates to `ensureHandlersInstalled()`.

### registerHandlers()

```typescript
async registerHandlers(handlerIds: string[]): Promise<void>
```

Registers handler IDs without creating the table (assumes it already exists). Delegates to `registerHandlers()`.

### catchupHandlers()

```typescript
async catchupHandlers(
    handlers: Record<string, EventHandler<any, any>>
): Promise<void>
```

Processes all events from each handler's last bookmark position up to the current end of the event stream.

**How it works:**

1. `BEGIN` a transaction
2. `SELECT ... FOR UPDATE NOWAIT` on all handler bookmark rows -- acquires exclusive locks, fails immediately if another process holds them (error code `55P03`)
3. For each handler sequentially:
   a. Read the last event in the store to determine the target position
   b. Read all matching events from the handler's bookmark to the target position
   c. Call `handler.when[event.type](event)` for each matching event
4. Batch-update all bookmarks in a single `UPDATE ... FROM (VALUES ...)` statement
5. `COMMIT`

### Key differences from runHandler()

| Aspect | `runHandler()` | `HandlerCatchup` |
|--------|---------------|-----------------|
| Model | Subscribe-based, continuous | Batch, must be called explicitly |
| Bookmark update | Per-event, atomic with projection | Batch, after all events processed |
| Transaction scope | One transaction per event | One transaction for entire catchup |
| NOTIFY on progress | Yes -- enables `waitUntilProcessed()` | No |
| Concurrency control | Handled by subscribe (single consumer) | `FOR UPDATE NOWAIT` on bookmark rows |
| Failure recovery | Replays from last committed bookmark | Rolls back entire batch |

---

## Pattern: write-then-wait

The combination of `runHandler()` and `waitUntilProcessed()` enables a synchronous read-after-write pattern for projections:

```typescript
// 1. Append an event
const position = await eventStore.append({
    events: [{ type: "StudentEnrolled", tags, data, metadata }],
    condition: { failIfEventsMatch: query, after: lastPosition }
})

// 2. Wait for the projection to catch up
await waitUntilProcessed(pool, "enrolmentProjection", position)

// 3. Query the read model -- guaranteed to reflect the event
const result = await pool.query(
    "SELECT * FROM enrolments WHERE course_id = $1",
    [courseId]
)
```

**How it works end-to-end:**

1. `append()` writes the event and fires `pg_notify('events', position)` on the events channel
2. `runHandler()` receives the notification, processes the event in a transaction, updates the bookmark, and fires `pg_notify('_handler_bookmarks', 'enrolmentProjection:position')`
3. `waitUntilProcessed()` detects the bookmark advance (via fast poll or LISTEN) and returns
4. The caller queries the read model, which now includes the projected event

This pattern provides the UX of synchronous writes (the API caller sees their own writes reflected immediately) while maintaining the architectural benefits of event sourcing (decoupled projections, multiple read models, replay capability).

**Timeout considerations:** The default timeout is 5 seconds. If a handler is consistently slow or the event store is under heavy load, increase `timeoutMs`. In production, monitor `WaitTimeoutError` occurrences as they indicate the projection is falling behind.

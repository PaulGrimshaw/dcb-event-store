# PostgresEventStore

API reference for `@dcb-es/event-store-postgres` -- the Postgres implementation of the [EventStore interface](../core/event-store-interface.md). For the design principles behind these choices, start with [Design](design.md).

## PostgresEventStoreOptions

```typescript
import { Pool } from "pg"
import { LockStrategy } from "@dcb-es/event-store-postgres"

interface PostgresEventStoreOptions {
    pool: Pool
    tablePrefix?: string
    copyThreshold?: number
    lockStrategy?: LockStrategy
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pool` | `Pool` | (required) | A `pg` connection pool. The store acquires and releases connections from this pool for all operations. |
| `tablePrefix` | `string` | `undefined` | When set, the events table becomes `<tablePrefix>_events` (e.g. `"myapp"` produces `myapp_events`). Must match `/^[a-z_][a-z0-9_]{0,62}$/i`. When omitted, the table is named `events`. |
| `copyThreshold` | `number` | `10` | The event count boundary between the stored-procedure append path and the COPY append path. Appends with event count at or below this threshold use the stored procedure; above it, COPY FROM STDIN. |
| `lockStrategy` | `LockStrategy` | `advisoryLocks()` | The locking strategy used during appends. See [Lock Strategies](./lock-strategies.md). |

## Constructor

```typescript
import { Pool } from "pg"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"

const pool = new Pool({ connectionString: "postgres://localhost/mydb" })
const eventStore = new PostgresEventStore({ pool })
```

The constructor derives several internal names from the table prefix:

| Derived name | Pattern | Example (no prefix) | Example (prefix `"myapp"`) |
|-------------|---------|---------------------|---------------------------|
| Table | `<prefix>_events` or `events` | `events` | `myapp_events` |
| Stored procedure | `<table>_append` | `events_append` | `myapp_events_append` |
| NOTIFY channel | `<table>` | `events` | `myapp_events` |

## ensureInstalled()

```typescript
await eventStore.ensureInstalled()
```

Creates the events table, index, and stored procedure if they do not already exist. Also calls `lockStrategy.ensureSchema()` if the strategy requires it (e.g. `rowLocks()` creates its lock-scopes table).

Idempotent -- safe to call on every application startup.

### Events table DDL

```sql
CREATE TABLE IF NOT EXISTS events (
    sequence_position BIGSERIAL PRIMARY KEY,
    type             TEXT COLLATE "C" NOT NULL,
    tags             TEXT[] NOT NULL,
    payload          TEXT NOT NULL
) WITH (
    autovacuum_freeze_min_age = 10000000,
    autovacuum_freeze_table_age = 100000000
);
```

**Design notes:**

- `sequence_position` is a `BIGSERIAL` primary key providing a gapless, monotonically increasing global order.
- `type` uses `COLLATE "C"` for byte-level equality -- faster than locale-aware collation and sufficient for event type strings.
- `tags` is a `TEXT[]` array. There is no GIN index on tags — write throughput is prioritised over tag-filtered read speed. Tag filtering uses array operators (`@>`, `&&`) with sequential scans.
- `payload` is opaque `TEXT` storing JSON with `data` and `metadata` fields. Stored as TEXT rather than JSONB because the event store never queries payload contents -- it only stores and retrieves.
- The aggressive autovacuum settings raise freeze thresholds because the events table is append-only; rows are never updated or deleted, so the default freeze-at-200M threshold wastes cycles.

### Index

```sql
CREATE INDEX IF NOT EXISTS events_type_pos_idx
ON events (type COLLATE "C", sequence_position DESC);
```

A composite B-tree index on `(type, sequence_position DESC)`. This supports:
- Filtering by event type (the most common read pattern in DCB queries)
- Ordered scans in both directions (DESC allows efficient backwards reads)
- Condition checking during appends (type + position range scans)

### Stored procedure

The `events_append` PL/pgSQL function is created as part of `ensureInstalled()`. It handles single-command appends of small event batches in a single database round-trip. See the "Stored procedure path" section below for details.

---

## Append strategies

The `append()` method accepts a single `AppendCommand` or an array of `AppendCommand[]`:

```typescript
async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition>
```

It routes to one of three internal strategies based on the shape of the input:

```
                          append(commands)
                                |
                    +-----------+-----------+
                    |                       |
              single command          multiple commands
                    |                       |
              +-----+-----+          appendBatch
              |           |          (COPY + temp table)
        count <= threshold  count > threshold
              |           |
        appendViaFunction  appendViaCopy
        (stored procedure) (COPY FROM STDIN)
```

### 1. Stored procedure path (single command, event count at or below copyThreshold)

Calls a PL/pgSQL function in a single `SELECT` — one database round-trip for lock acquisition, condition checking, event insertion, and `pg_notify`. Best for the common case of 1-5 events.

### 2. COPY FROM STDIN path (single command, event count above copyThreshold)

Uses `pg-copy-streams` to stream events via Postgres COPY protocol within a transaction. Acquires locks and checks conditions as separate steps. Best for large batches (hundreds or thousands of events).

### 3. Batch path (multiple AppendCommands)

Handles arrays of `AppendCommand[]` atomically. Each command's condition is checked independently against pre-existing events (not against events from sibling commands in the same batch). Uses a high-water mark to separate pre-existing events from newly inserted ones, and a temp table for batch condition checking.

All transactional append paths use `READ COMMITTED` isolation — scoped locking provides the necessary consistency guarantees without the overhead of `SERIALIZABLE`.

For implementation details (stored procedure SQL, COPY format, batch condition checking), see [Internals](../internals.md#append-strategy-routing).

---

## Read

```typescript
async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent>
```

Returns an async generator that yields events matching the query. Uses server-side cursors (batches of 5,000 rows) within a read-only transaction to stream large result sets with bounded memory. Multiple query items are OR'd; types and tags within a single item are AND'd. Tag filtering uses the `&&` (overlap) operator.

See [Internals -- SQL Generation](../internals.md#sql-generation) for the generated SQL structure.

---

## Subscribe

```typescript
async *subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent>
```

Returns a live event stream that yields both historical and future events. Acquires a dedicated connection for `LISTEN`, then loops: read all matching events after the current position, yield them, and if none were found, wait for a `pg_notify` notification or a poll timeout (`pollIntervalMs`, default 100ms). Stops cleanly when `signal` is aborted.

The listener connection is held for the lifetime of the subscription — size your pool accordingly.

---

## pg_notify

Every append path fires `pg_notify` after events are committed:

```sql
SELECT pg_notify('<channel>', '<last_sequence_position>')
```

- **Channel name:** Same as the events table name (e.g. `events` or `myapp_events`)
- **Payload:** The string representation of the last sequence position assigned in the append
- **Timing:** Fired within the transaction (stored procedure) or just before COMMIT (COPY/batch paths). The notification is delivered to listeners only after the transaction commits.

Subscribers and `waitUntilProcessed()` both rely on these notifications for low-latency wakeups.

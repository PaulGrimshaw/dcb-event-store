# Internals

Implementation internals and design decisions behind the Postgres adapter. This document covers how the store works under the hood -- the data model, concurrency strategy, append paths, query generation, and performance tuning.

For the public API surface, see the [Core](core/event-store-interface.md) and [Postgres](postgres/postgres-event-store.md) reference docs. For getting started, see [Getting Started](getting-started.md).

---

## Table of Contents

- [Opaque SequencePosition](#opaque-sequenceposition)
- [FNV-1a Lock Key Computation](#fnv-1a-lock-key-computation)
- [Append Strategy Routing](#append-strategy-routing)
- [Batch Condition Checking](#batch-condition-checking)
- [pg_notify Integration](#pg_notify-integration)
- [SQL Generation](#sql-generation)
- [Transaction Isolation](#transaction-isolation)
- [Performance Design](#performance-design)

---

## Opaque SequencePosition

`SequencePosition` is deliberately opaque — it exposes comparison and serialisation, but no construction or arithmetic:

```typescript
export class SequencePosition {
    isAfter(other: SequencePosition): boolean
    isBefore(other: SequencePosition): boolean
    equals(other: SequencePosition): boolean
    toString(): string

    static fromString(s: string): SequencePosition
    static compare(a: SequencePosition, b: SequencePosition): number
    static initial(): SequencePosition
}
```

The constructor is private. Positions are created via `fromString()` (deserialisation from storage or wire) or `initial()` (the zero position). Arithmetic like `value + 1` happens inside each adapter's SQL layer, never in application code.

`after` uses **exclusive semantics** — the store checks only for matching events strictly after that position. This aligns with the [dcb.events specification](https://dcb.events/specification/).

Encoding is adapter-agnostic (`position.toString()`). Decoding is adapter-specific (`SequencePosition.fromString("42")`) at the composition root, where the caller knows which adapter is in use.

---

## FNV-1a Lock Key Computation

Concurrent appends need serialisation only when they operate on overlapping scopes. The store computes a lock key for every `(type, tag)` pair in the events being appended and in the `failIfEventsMatch` condition. Two transactions acquire the same lock only when they share an exact `(type, tag)` pair.

### Hash Algorithm

Each `(type, tag)` pair is concatenated as `"${type}|${tag}"` and hashed to a 64-bit signed integer via FNV-1a:

```typescript
const FNV1A_64_OFFSET = 0xcbf29ce484222325n
const FNV1A_64_PRIME  = 0x100000001b3n
const MASK_64         = 0xffffffffffffffffn

function fnv1a64(input: string): bigint {
    let hash = FNV1A_64_OFFSET
    for (let i = 0; i < input.length; i++) {
        hash ^= BigInt(input.charCodeAt(i))
        hash = (hash * FNV1A_64_PRIME) & MASK_64
    }
    // Convert unsigned 64-bit to signed (Postgres bigint is signed)
    return hash > 0x7fffffffffffffffn
        ? hash - 0x10000000000000000n
        : hash
}
```

The unsigned-to-signed conversion at the end is required because `pg_advisory_xact_lock` accepts `bigint`, which is signed in Postgres.

### Collision Probability

With 64 bits of hash space, the probability of a collision between any two distinct `(type, tag)` pairs is approximately `1 / 2^64` (~5.4 x 10^-20). Even at millions of distinct pairs, the birthday-bound collision probability remains negligible.

### Why Not Bucketing

A bucket-based approach (hashing to a smaller range and accepting some false serialisation) would be simpler but introduces unnecessary contention. Since the hash is computed once per append for a small number of `(type, tag)` pairs, the BigInt arithmetic cost is sub-microsecond. Zero false serialisation is worth the marginally more expensive hash.

### Lock Strategies

The computed keys feed into a pluggable `LockStrategy` interface:

```typescript
export interface LockStrategy {
    computeKeys(events: DcbEvent[], condition?: AppendCondition): bigint[]
    acquire(client: PoolClient, keys: bigint[], tableName: string): Promise<void>
    generateSpLockBlock(tableName: string): string
    ensureSchema?(pool: Pool | PoolClient, tableName: string): Promise<void>
}
```

Two built-in implementations:

- **`advisoryLocks()`** -- Postgres advisory locks (`pg_advisory_xact_lock`). In-memory, fast, no schema. Default for direct Postgres connections. Causes connection pinning on RDS Proxy.
- **`rowLocks()`** -- Row-level `FOR UPDATE` locks on a companion `_lock_scopes` table. Lazy-upserts scope keys, then acquires row locks in key order. RDS Proxy and Aurora Limitless compatible.

Both strategies acquire locks in sorted order (via `ORDER BY k` in the SQL) to prevent deadlocks between concurrent transactions.

---

## Append Strategy Routing

The Postgres adapter routes each `append()` call to one of three strategies based on the shape and size of the input:

```typescript
async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition> {
    if (commands.length === 1) {
        const evts = ensureIsArray(commands[0].events)
        return evts.length <= this.copyThreshold
            ? this.appendViaFunction(evts, commands[0].condition)
            : this.appendViaCopy(evts, commands[0].condition)
    }
    return this.appendBatch(commands)
}
```

| Path | When | Round trips | Mechanism |
|---|---|---|---|
| Stored procedure | Single command, event count <= `copyThreshold` | 1 | PL/pgSQL function call |
| COPY | Single command, event count > `copyThreshold` | ~4 | `pg-copy-streams` pipeline |
| Batch | Multiple commands | ~5 | COPY + temp table for conditions |

### copyThreshold Configuration

The threshold defaults to 10 events and is configurable via `PostgresEventStoreOptions.copyThreshold`. Below the threshold, the stored procedure's single round-trip wins. Above it, COPY's streaming throughput dominates.

### Stored Procedure Path

The PL/pgSQL function `{tableName}_append` performs lock acquisition, condition checking, event insertion, and notification in a single database round-trip:

```sql
CREATE OR REPLACE FUNCTION events_append(
    p_types      text[],
    p_tags       text[],
    p_payloads   text[],
    p_lock_keys  bigint[],
    p_conditions jsonb,
    p_after_pos  bigint
) RETURNS bigint AS $fn$
BEGIN
    -- 1. Acquire locks (strategy-dependent)
    PERFORM pg_advisory_xact_lock(k)
        FROM unnest(p_lock_keys) AS k ORDER BY k;

    -- 2. Check conditions against existing events
    IF p_after_pos IS NOT NULL AND p_conditions IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM jsonb_array_elements(p_conditions) AS c
            WHERE EXISTS (
                SELECT 1 FROM events e
                WHERE e.type = ANY(ARRAY(SELECT jsonb_array_elements_text(c -> 'types')))
                  AND e.tags @> ARRAY(SELECT jsonb_array_elements_text(c -> 'tags'))
                  AND e.sequence_position > p_after_pos
            )
        ) THEN
            RAISE EXCEPTION 'APPEND_CONDITION_VIOLATED';
        END IF;
    END IF;

    -- 3. Insert events
    INSERT INTO events (type, tags, payload)
    SELECT p_types[i], string_to_array(p_tags[i], E'\x1F'), p_payloads[i]
    FROM generate_subscripts(p_types, 1) AS i;

    -- 4. Get last position and notify
    SELECT currval(pg_get_serial_sequence('events', 'sequence_position')) INTO v_pos;
    PERFORM pg_notify('events', v_pos::text);
    RETURN v_pos;
END;
$fn$ LANGUAGE plpgsql;
```

Tags are serialised with a Unit Separator (`\x1F`) delimiter on the client side and split back into an array via `string_to_array` inside the function. This avoids the need to pass a `text[][]` parameter (which `pg` does not natively support) while keeping the delimiter invisible to any realistic tag value.

The function returns the last `sequence_position` assigned, which becomes the `SequencePosition` returned from `append()`.

### COPY Path

For large single-command appends, streaming via `COPY FROM STDIN` is significantly faster than parameterised inserts:

```typescript
const copyStream = client.query(
    copyFrom(`COPY ${tableName} (type, tags, payload) FROM STDIN WITH (FORMAT text)`)
)

const source = Readable.from(function* () {
    for (const evt of events) {
        yield `${escapeCopy(evt.type)}\t${tags}\t${payload}\n`
    }
})

await pipeline(source, copyStream)
```

The COPY path runs in a client-managed transaction (not the stored procedure), so it explicitly acquires locks, checks conditions, writes events, reads the final position, and sends the `pg_notify` as separate steps. This costs more round trips but handles arbitrarily large event batches without hitting parameter limits.

The `escapeCopy` function handles COPY TEXT format escaping (backslashes, tabs, newlines, null bytes) in a single pass. When values appear inside Postgres array literals (for tags), double-quotes are also escaped.

### Batch Path

When `append()` receives multiple `AppendCommand` objects, each command may have its own `AppendCondition`. The batch path:

1. Analyses all commands in a single pass (`analyseCommands`) to extract the union of lock keys, condition rows, and a total event count
2. Acquires all locks
3. Records the current high water mark (`MAX(sequence_position)`)
4. COPYs all events from all commands into the events table
5. COPYs condition metadata into a temp table
6. Checks all conditions in a single query (see [Batch Condition Checking](#batch-condition-checking))
7. Returns the last position and sends the notification

The high water mark is critical: it separates "pre-existing events" (which conditions check against) from "events just inserted by this batch" (which must not trigger condition violations).

---

## Batch Condition Checking

When multiple commands are appended atomically, each command's condition must be checked against events that existed before the batch -- not against events inserted by sibling commands in the same batch.

### Temp Table Schema

Conditions are COPY'd into a temp table:

```sql
CREATE TEMP TABLE _conditions (
    cmd_idx    int,       -- index of the command in the batch
    cond_types text[],    -- event types to match
    cond_tags  text[],    -- tags to match (containment check)
    after_pos  bigint     -- position threshold (exclusive)
) ON COMMIT DROP
```

`ON COMMIT DROP` ensures automatic cleanup -- the table only lives for the duration of the transaction.

### The Check Query

A single query identifies the first violated condition:

```sql
SELECT cmd_idx FROM _conditions c
WHERE EXISTS (
    SELECT 1 FROM events e
    WHERE e.type = ANY(c.cond_types)
      AND e.tags @> c.cond_tags
      AND e.sequence_position > c.after_pos
      AND e.sequence_position <= $1  -- high water mark
)
LIMIT 1
```

The `<= highWaterMark` clause is what excludes events inserted by the current batch. The `LIMIT 1` short-circuits on the first violation. The `cmd_idx` in the result tells the caller which command failed, enabling precise error reporting:

```typescript
throw new AppendConditionError(commands[failedIdx].condition!, failedIdx)
```

---

## pg_notify Integration

The store uses Postgres `LISTEN`/`NOTIFY` for low-latency event delivery. Two distinct notification channels serve different purposes.

### Event Notifications

**Channel name:** The events table name (e.g. `"events"`).

**When sent:** Every append path sends a notification after events are committed:

```sql
SELECT pg_notify('events', '42')  -- payload is the last sequence_position
```

This fires once per append, regardless of how many events were written. The stored procedure path sends it inside the PL/pgSQL function; the COPY and batch paths send it as a separate statement within the same transaction.

**Consumer:** `subscribe()` uses `LISTEN` on this channel. When idle (no events returned by the last poll), it waits on a combination of `pg_notify` and a timeout:

```typescript
await new Promise<void>(resolve => {
    const timeout = setTimeout(resolve, pollInterval)
    listener.once("notification", () => {
        clearTimeout(timeout)
        resolve()
    })
})
```

When a notification arrives, `subscribe()` immediately re-polls. The poll interval (default 100ms) acts as a fallback for missed notifications (e.g. during connection recovery).

### Bookmark Notifications

**Channel name:** The bookmark table name (e.g. `"_handler_bookmarks"`).

**When sent:** `runHandler` sends a notification each time it advances a handler's bookmark:

```sql
SELECT pg_notify('_handler_bookmarks', 'myHandler:42')
```

The payload format is `handlerName:position`. The colon delimiter is why handler names must not contain colons.

**Consumer:** `waitUntilProcessed()` uses this channel to detect when a projection has caught up. It uses a two-phase strategy:

1. **Fast poll phase** -- three quick checks with backoff (5ms, 15ms, 30ms). Most events process in under 50ms, so this avoids holding a LISTEN connection for the common case.
2. **Slow path** -- establishes `LISTEN` before checking, then loops with 100ms poll intervals. The LISTEN-before-check ordering prevents a TOCTOU race where a notification fires between the check and LISTEN.

---

## SQL Generation

`readSql.ts` builds parameterised SQL from `Query` objects. Every user-supplied value goes through `ParamManager`, which assigns `$1`, `$2`, etc. and collects the parameter array.

### Query Structure

A `Query` contains one or more `QueryItem` objects, each with optional `types` (event type names) and `tags` (tag values). Items are OR'd together; within an item, types and tags are AND'd:

```sql
WHERE (type IN ($1, $2) AND tags && $3::text[])
   OR (type IN ($4) AND tags && $5::text[])
```

When `query.isAll` is true, no type/tag filtering is applied.

### Position Filtering

The `after` option generates an exclusive position filter:

```sql
-- Forward read
WHERE e.sequence_position > $1

-- Backward read
WHERE e.sequence_position < $1
```

The direction flips based on `options.backwards`.

### Tag Matching

Tags use the Postgres `&&` (overlap) operator against a `text[]` parameter:

```sql
tags && $1::text[]
```

This matches any event whose `tags` array shares at least one element with the filter array. For append condition checking, the `@>` (contains) operator is used instead, matching events whose tags contain all specified filter tags.

### Cursor Management

Reads use server-side cursors to stream large result sets without loading them into memory:

```typescript
const cursorName = `event_cursor_${Math.random().toString(36).substring(7)}`
// DECLARE "event_cursor_abc123" CURSOR FOR SELECT ...
// FETCH 5000 FROM "event_cursor_abc123"
```

The cursor name includes a random suffix to avoid collisions when multiple reads run concurrently. Results are fetched in batches of 5000 rows. The `read()` method is an `async generator`, yielding events as they arrive rather than buffering the entire result set.

---

## Transaction Isolation

### READ COMMITTED, Not SERIALIZABLE

All write transactions use `READ COMMITTED` isolation:

```typescript
await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
```

This is a deliberate choice. `SERIALIZABLE` would provide automatic conflict detection but at the cost of serialisation failures and retries across all transactions, even those that do not conflict. The DCB pattern's scope-level locking already provides the necessary guarantees:

1. **Locks** serialise only transactions with overlapping `(type, tag)` scopes
2. **Condition checks** run after locks are held, seeing a consistent snapshot
3. Non-overlapping transactions proceed fully in parallel

`READ COMMITTED` is sufficient because the locks ensure that no concurrent transaction can insert conflicting events between the condition check and the event insertion.

### Read-Only Transactions

`read()` opens a transaction purely for cursor support -- Postgres requires a transaction context for server-side cursors. The transaction is rolled back on completion (nothing was written):

```typescript
await client.query("BEGIN")
// ... declare cursor, fetch batches ...
await client.query("ROLLBACK")
```

### Lock Lifetime

Advisory locks acquired via `pg_advisory_xact_lock` are automatically released when the transaction ends (commit or rollback). Row locks acquired via `FOR UPDATE` follow the same transaction-scoped lifetime. No explicit unlock step is needed.

---

## Performance Design

### Index Strategy

The events table has two indexes:

```sql
-- Primary key (implicit B-tree index)
sequence_position BIGSERIAL PRIMARY KEY

-- Composite index for type-filtered reads
CREATE INDEX events_type_pos_idx
ON events (type COLLATE "C", sequence_position DESC);
```

The composite index covers the most common read pattern: "all events of type X after position Y". The `DESC` ordering on `sequence_position` makes backward reads (most recent first) efficient. `COLLATE "C"` uses byte-wise comparison, which is faster than locale-aware collation for event type strings that are always ASCII.

The `type` column on the table itself also uses `COLLATE "C"` for consistency.

### Autovacuum Tuning

The events table is configured with high freeze thresholds:

```sql
CREATE TABLE events (...) WITH (
    autovacuum_freeze_min_age = 10000000,
    autovacuum_freeze_table_age = 100000000
);
```

Event stores are append-only -- rows are never updated or deleted. The default autovacuum freeze thresholds would trigger unnecessary freezing passes on a table that has no dead tuples. Raising these thresholds reduces autovacuum overhead on high-throughput stores.

### Cursor Batch Size

The read path fetches 5000 rows per cursor batch:

```typescript
const READ_BATCH_SIZE = 5000
```

This balances memory usage against round-trip overhead. Each batch is a single `FETCH 5000` command. The async generator yields individual events from each batch, so the caller never holds more than one batch in memory regardless of total result set size.

### Async Generators for Memory Efficiency

Both `read()` and `subscribe()` are `AsyncGenerator` functions. This means:

- The caller pulls events one at a time via `for await...of`
- Back-pressure is automatic -- the generator pauses between yields
- A read of millions of events uses constant memory (one cursor batch at a time)
- The caller can `break` out of the loop early, triggering the `finally` block which closes the cursor and releases the connection

### COPY Streaming for Writes

Large appends use `pg-copy-streams` with a `Readable.from()` generator. Events are serialised to COPY TEXT format on the fly and streamed to Postgres without materialising the entire payload in memory. The generator pattern means even a batch of 100,000 events does not require a 100,000-element array -- an iterable suffices.

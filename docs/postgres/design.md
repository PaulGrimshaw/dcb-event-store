# Postgres Adapter Design

How the Postgres adapter implements the DCB pattern. Read this before the API reference — it explains the principles that drive the implementation choices.

---

## The Core Problem

A DCB event store must do one thing atomically: **append events only if no conflicting events have been written since the caller last read**. The caller specifies "conflicting" via a query over event types and tags, and a position marking how far they read.

In a single-writer system this is trivial. With concurrent writers it requires coordination — the store must prevent two transactions from both passing the condition check and then both writing. The Postgres adapter solves this with scoped locking, an in-database condition check, and bulk-optimised writes.

---

## Stricter Append Conditions

The [DCB specification](https://dcb.events/specification/) allows append conditions with broad queries. This implementation enforces a stricter constraint: **every query item in an append condition must specify at least one event type AND at least one tag**. `Query.all()` is not permitted in conditions.

This is deliberate. Without both types and tags, the store cannot compute scoped lock keys — it wouldn't know which concurrent transactions conflict. The constraint ensures that every append condition maps to a precise set of `(type, tag)` lock scopes, enabling fine-grained concurrency without false conflicts or global serialisation.

This does not limit what you can _read_ — `Query.all()` and partial queries work fine for `read()` and `subscribe()`. The restriction applies only to `AppendCondition.failIfEventsMatch`.

## Scoped Locking on (type, tag) Pairs

The key insight is that two concurrent appends only conflict if their consistency boundaries overlap — that is, if they share at least one `(event type, tag)` pair between their events and conditions.

Rather than serialising all appends (a global lock) or accepting false conflicts (a coarse hash), the adapter computes a lock key for every `(type, tag)` pair involved in an append — from the events being written _and_ from the condition being checked. Each key is a 64-bit hash of the `"type|tag"` string. Two transactions acquire the same lock only when they share an exact `(type, tag)` pair; everything else proceeds in parallel.

For example, subscribing student A to course X locks `(studentWasSubscribed, courseId=X)` and `(studentWasSubscribed, studentId=A)`. A concurrent append for student B to course Y locks entirely different keys — no contention. But student C subscribing to course X _would_ contend on `(studentWasSubscribed, courseId=X)`, which is correct: both affect the course's subscriber count.

Locks are acquired in sorted order to prevent deadlocks.

Two lock backends are provided:
- **Advisory locks** — in-memory Postgres locks, fast, no schema. Default for direct connections.
- **Row locks** — `SELECT ... FOR UPDATE` on a companion table. Required for RDS Proxy / Aurora where advisory locks cause connection pinning.

See [Lock Strategies](lock-strategies.md) for the full reference.

---

## Condition Check in the Database

After acquiring locks, the adapter checks the append condition entirely server-side. The check is:

> "Do any events exist where `type` matches one of the condition's types, `tags` contain the condition's tags, and `sequence_position` is after the caller's known position?"

If any such event exists, the append is rejected. Because locks are held, no concurrent transaction can insert a conflicting event between this check and the subsequent write — the check and write are effectively atomic.

For single-command appends, this check runs inside a PL/pgSQL stored procedure alongside the lock acquisition and event insertion, so the entire operation is a single database round-trip.

For multi-command batch appends, each command's condition is checked independently. A temp table holds the conditions, and a single correlated query identifies the first violated condition. A high-water mark separates pre-existing events from events just inserted by the batch, so sibling commands don't falsely trigger each other's conditions.

---

## Single Table, Append-Only

All events live in one table per bounded context. The schema is minimal:

| Column | Type | Purpose |
|--------|------|---------|
| `sequence_position` | `BIGSERIAL PK` | Global ordering — monotonic, gapless within a connection |
| `type` | `TEXT` | Event type name |
| `tags` | `TEXT[]` | Array of `"key=value"` tag strings |
| `payload` | `TEXT` | JSON blob with `data` and `metadata` — opaque to the store |

The table is append-only: rows are never updated or deleted. This simplifies vacuuming (high freeze thresholds), makes the sequence reliable for ordering, and means reads never contend with writes on row locks.

A composite index on `(type, sequence_position DESC)` covers the most common read pattern — "all events of this type after position N" — which is exactly what condition checks and query-filtered reads need. There is no GIN index on tags; write throughput is prioritised, and tag filtering falls back to sequential scan within the type-filtered subset.

---

## Append Strategy Routing

Not all appends are equal. A single event (the common case) and a bulk import of 10,000 events have very different performance profiles. The adapter routes to one of three strategies:

**Stored procedure** (single command, small batch) — A PL/pgSQL function does lock acquisition, condition check, event insertion, and `pg_notify` in a single `SELECT` call. One database round-trip. This is the fast path for the overwhelming majority of appends.

**COPY FROM STDIN** (single command, large batch) — Postgres's bulk-insert protocol, streamed via `pg-copy-streams`. Events are serialised to tab-delimited text and piped into the table without ever materialising the full batch in memory. Locks and conditions are handled as separate steps within a transaction.

**Batch with temp table** (multiple commands) — All events from all commands are COPY'd in, then a single query checks every command's condition against events that existed _before_ the batch. Atomic: all commands succeed or all fail, with per-command error identification.

The threshold between stored procedure and COPY is configurable (`copyThreshold`, default 10 events).

---

## Notifications (pg_notify)

Every append fires a `pg_notify` on commit, broadcasting the last sequence position to any listeners. This enables:

- **`subscribe()`** — a live event stream that combines `LISTEN` with polling. When idle, the subscriber waits on a notification rather than polling blindly. When a notification arrives, it immediately re-reads. The poll interval (default 100ms) is a fallback, not the primary delivery mechanism.

- **`waitUntilProcessed()`** — after appending, a command can wait for a projection handler's bookmark to reach the appended position. The handler emits its own notification on a separate channel each time it advances its bookmark. The waiter uses a fast poll (5ms, 15ms, 30ms) for the common fast case, then falls back to `LISTEN` for the slow case.

Two channels serve different purposes:
- The **events channel** (table name, e.g. `events`) — "new events were appended"
- The **bookmarks channel** (e.g. `_handler_bookmarks`) — "a handler advanced to position N"

---

## Why READ COMMITTED, Not SERIALIZABLE

The adapter uses `READ COMMITTED` isolation for all write transactions. This is a deliberate choice — scoped locking already provides the consistency guarantee. `SERIALIZABLE` would add overhead and retry complexity across _all_ transactions, even non-conflicting ones, for a guarantee that the lock strategy already delivers. `READ COMMITTED` lets non-overlapping transactions proceed with no serialisation overhead.

Read-only transactions (for cursor-based reads) use the default isolation level. They exist purely for server-side cursor lifecycle management and are rolled back on completion.

---

Next: [PostgresEventStore API](postgres-event-store.md) | [Lock Strategies](lock-strategies.md) | [Event Handling](event-handling.md)

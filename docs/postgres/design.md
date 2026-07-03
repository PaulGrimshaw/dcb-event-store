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

### Lock modes and key namespaces

Locks are taken in one of two modes — **shared (S)** or **exclusive (X)** — across three key namespaces. The compatibility rule is the standard one: any number of S holders coexist; an X holder excludes everything else (S or X).

| Namespace | Key | Purpose |
|-----------|-----|---------|
| **Leaf** | `hash("L:" + type + "\|" + tag)` | The append mutex — one per `(type, tag)`. |
| **Type intent** | `hash("T:" + type)` | Presence marker: "a write of this type is in flight". |
| **Global intent** | `hash("G")` | Presence marker: "a write is in flight" (any type). |

A **writer** acquires:

- **X on every leaf key** — from its events _and_ its condition. This is the mutex that makes the condition-check-and-write atomic against a competing writer on the same scope.
- **S on the type-intent key** of each event's type, plus **S on the global-intent key**.

The intent locks are taken in **S mode**, so writers never serialise against each other on them — any number of concurrent writers hold the same type-intent S at once. They exist purely so that a _reader_ can detect in-flight writers by taking the same key in X mode. That mechanism is [the read barrier](#the-read-barrier).

Locks are acquired in sorted key order to prevent deadlocks.

### Locking queues; it never rejects

Every lock in the append and read paths is a **blocking** acquisition (`pg_advisory_xact_lock` / `FOR UPDATE` / `FOR SHARE`): a contending transaction _queues_ for the holder to commit or roll back — it is never turned away — until the client's `lock_timeout`, if one is configured. Sorted acquisition makes that wait deadlock-free.

**Rejection comes from a different mechanism entirely** — the [condition check](#condition-check-in-the-database), which runs _after_ a writer holds its locks. So two writers on the same scope don't race to reject each other: they queue, and then each re-evaluates its append condition against the state the other left behind. The lock provides liveness (serialisation); the condition check provides safety (rejection). They are not the same thing.

Two lock backends are provided:
- **Advisory locks** — in-memory Postgres locks, fast, no schema. Default for direct connections.
- **Row locks** — `SELECT ... FOR UPDATE` / `FOR SHARE` on a companion table. Required for RDS Proxy / Aurora where advisory locks cause connection pinning.

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
| `sequence_position` | `BIGSERIAL PK` | Global ordering — monotonic. Values are allocated at INSERT and become visible at COMMIT, so concurrent writers can commit out of allocation order; the read barrier masks this from readers |
| `type` | `TEXT` | Event type name |
| `tags` | `TEXT[]` | Array of `"key=value"` tag strings |
| `payload` | `TEXT` | JSON blob with `data` and `metadata` — opaque to the store |

The table is append-only: rows are never updated or deleted. This simplifies vacuuming (high freeze thresholds), makes the sequence reliable for ordering, and means reads never contend with writes on row locks.

A composite index on `(type, sequence_position DESC)` covers the most common read pattern — "all events of this type after position N" — which is exactly what condition checks and query-filtered reads need. A GIN index on `tags` (built with `fastupdate=off`) supports tag-containment filtering within the type-filtered subset.

### The sequence position is an ordering token, not a counter

Although `sequence_position` is a `BIGSERIAL`, the design depends on only two properties of it — never on its integer-ness:

1. It imposes a **global total order** over events — any two positions are comparable.
2. Its current maximum is **cheap to read** (`pg_sequence_last_value`), which is what gives the read barrier its high-water mark.

Contiguity is _not_ required, and **gaps are normal**: a rolled-back append burns a value, and out-of-order commits leave temporary holes. They are harmless — the store only ever compares positions relationally (`isAfter` / `isBefore`), never counts or does arithmetic on them (see [opaque `SequencePosition`](../internals.md#opaque-sequenceposition)). A gap is not a missing event; it is a number that was never used.

The token could therefore be any order-preserving value — a ULID sorts correctly, for instance — **provided it is issued by a single global sequencer**. That proviso is the real constraint: the barrier's guarantee ("anything committed after the mark has a position above it") requires strictly monotonic _global_ allocation. A client-generated UUIDv7 or ULID does _not_ satisfy this on its own — independent generators drift under clock skew and can mint a "new" value that sorts below one already stored. `BIGSERIAL` is chosen because it is simply the most efficient way for Postgres to hand out exactly this: a single, globally monotonic, cheaply-readable order.

---

## The Read Barrier

The key insight is that **a reader only cares about in-flight writes that match its own query**. An append it could never return — because the event's type or tags fall outside the read's filter — cannot create a gap it would notice, so there is no reason to wait for it.

The barrier capitalises on this: it scopes each read's waiting to exactly its own query. A read for a specific `(type, tag)` waits only for writers to that scope and streams straight past writes to unrelated scopes — including ones a broad `Query.all()` read, running at the same instant, would have to block on. The narrower the query, the less it waits.

The rest of this section is how that scoping is enforced, and why it's needed.

`sequence_position` values are allocated at INSERT but only become visible at COMMIT (see [Single Table, Append-Only](#single-table-append-only)). Two consequences follow for readers:

- Concurrent writers can **commit out of allocation order** — a writer holding position 6 may commit before a writer holding position 5.
- Between those commits there is a **transient gap**: position 6 is visible while position 5 is still uncommitted, and therefore invisible.

A naive reader that just selected the highest visible position would advance past 6, and when position 5 later committed it would never re-read it. For a projection or subscription that tracks an offset, the event at position 5 would be **silently lost**.

The read barrier closes this gap. Before streaming any events, a forward read:

1. Acquires reader-side locks in its scope (the mirror of the writer's — see below),
2. Snapshots the current high-water mark (`pg_sequence_last_value`),
3. Releases the locks and returns the mark.

The read then streams only events with `sequence_position <= mark`. Because the reader's locks conflict with any in-flight writer's locks in the same scope, step 1 **blocks until those writers commit or roll back** — so by the time the mark is taken, there are no invisible holes beneath it. Anything allocated after the barrier gets a position above the mark, outside this read's window.

### Reader lock modes — the mirror of writers

Readers take the _opposite_ mode to writers on each namespace, so the two collide exactly where they must:

| Read query shape | Lock taken | Waits for |
|------------------|-----------|-----------|
| `(type, tag)` filter | **S** on each `(type, tag)` leaf | in-flight writers to that exact scope (their X on the same leaf) |
| type-only filter | **X** on each type-intent key | all in-flight writers of that type (their S on the same key) |
| `Query.all()` | **X** on the global-intent key | every in-flight writer |

A tag-scoped read only needs to wait for writers to the _same_ leaf: a concurrent write to a different tag may leave a lower-positioned gap, but that event can never match this read's filter, so skipping it is correct. Broader reads take the broader intent lock because they _can_ match events across many scopes.

### Worked example

Two writers subscribe different students to different courses (both `studentWasSubscribed`); a projector reads all `studentWasSubscribed`. Below, `intent` is `intent(studentWasSubscribed)`:

| Step | Writer A (student A → course X) | Writer B (student B → course Y) | Reader (all `studentWasSubscribed`) |
|------|---------------------------------|---------------------------------|-------------------------------------|
| 1 | X on its leaves + S `intent`; INSERT → **pos 5**; not yet committed | | |
| 2 | | X on its leaves (disjoint from A's) + S `intent` (S/S with A — no wait); INSERT → **pos 6**; **COMMIT** | |
| 3 | still in flight, holding S on `intent` | | barrier wants X `intent` → **blocks** (X vs A's S) |
| 4 | **COMMIT** → releases S on `intent` | | |
| 5 | | | X granted; snapshot **hwm = 6**; release |
| 6 | | | stream `pos <= 6` → sees 4, 5, 6 in order ✅ |

The two writers share no leaf key — different course _and_ different student — so they never serialise; their only shared lock is the type-intent S, which is exactly what the reader's X collides with. Without the barrier, the reader at step 3 would see positions 4 and 6, advance past 6, and lose the event at position 5 when it later commits.

### Cost and the read/write tradeoff

The barrier holds its lock only for **steps 1–3 — long enough to wait out in-flight writers and read a single integer**, on the order of microseconds. The event stream in step 6 runs in a _separate_ transaction holding **no lock at all**, because the high-water mark already pins a complete, gap-free prefix. A read therefore never blocks a write for the duration of the scan — only for the watermark snapshot.

The result is an asymmetric tradeoff:

- **Writes block reads** for the whole append transaction, with a radius as wide as the reader's query (a type-only read waits on every writer of that type).
- **Reads block writes** only for the microsecond watermark snapshot, with the same radius.

Concurrent reads with the same scope are coalesced by an in-process cache (`HwmCache`): they share a single barrier round-trip and reuse the mark for a short TTL, so repeated polling and fan-out reads don't each pay a barrier. Correctness is unaffected — any writer that started after the mark was taken has a position above it.

Backward reads (`{ backwards: true }`) skip the barrier entirely: scanning from the highest position downward cannot advance past an invisible gap.

### Why tag-only reads are not supported

A read filtered by tag alone (across any type) would need to wait out writers of _every_ type that could carry that tag — but there is no `(tag)` intent namespace for it to collide with. Supporting it precisely would require every writer to take an extra S lock per tag on every append, adding lock overhead to the entire write path; the cheaper alternative — falling back to the global barrier — would serialise such reads against all writers. Rather than pay either cost, tag-only queries are rejected at `Query.fromItems()`. Use `Query.all()` or add a type.

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

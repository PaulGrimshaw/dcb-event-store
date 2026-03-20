# @dcb-es/event-store-dynamodb

DynamoDB implementation of the `EventStore` interface from `@dcb-es/event-store`.

## Design Goals

- Full [DCB compliance](https://dcb.events): append conditions that reject writes when new matching events have been added since the caller last read
- No artificial batch size limits — handle 1 event or 100,000+ events transparently
- Fine-grained concurrency — unrelated writes never conflict
- Crash-safe — partial failures never produce visible corrupt state

## Adapter Constraints

This adapter enforces two constraints beyond the base `EventStore` interface. These are specific to the DynamoDB implementation and enable the fine-grained lock system that gives maximum write parallelism.

1. **Every event must have at least one tag.**
2. **Every `QueryItem` in an `AppendCondition` must have non-empty `eventTypes` AND non-empty `tags`.**

These constraints mean the adapter only needs `(eventType, tagValue)` pair locks — no type-level, tag-level, or global locks. Two writers only conflict if they share an exact (type, tag) pair, giving the finest possible granularity.

If your domain requires a type-level constraint (e.g. "count all CourseCreated events for the next course number"), model it with an explicit tag that represents the consistency boundary:

```typescript
// Every CourseCreated event carries:
{ type: "CourseCreated", tags: Tags.from(["course=CS101", "courseIndex=global"]) }

// Append condition:
{
  query: Query.fromItems([{ eventTypes: ["CourseCreated"], tags: Tags.from(["courseIndex=global"]) }]),
  expectedCeiling: lastObservedPosition
}

// Lock: _LOCK#CourseCreated:courseIndex=global
// Serializes all course creation — which is the real domain constraint.
// Does not affect AssetCreated, CourseUpdated, or any other event type.
```

**Reads are unrestricted.** `Query.all()`, type-only queries, and tag-only queries all work for reads and projections. The constraints only apply to append conditions.

```typescript
// All valid reads:
eventStore.read(Query.all())                                             // full catch-up
eventStore.read(Query.all(), { fromSequencePosition: pos })              // resume projection
eventStore.read(Query.fromItems([{ eventTypes: ["CourseCreated"] }]))     // type-only read
eventStore.read(Query.fromItems([{ tags: Tags.from(["asset=123"]) }]))   // tag-only read

// Appending without a condition — no constraints, no locks:
eventStore.append(events)
```

## Architecture Overview

### Consistency Strategy: Batch-Linked Pessimistic Locks

DynamoDB lacks Postgres's `SERIALIZABLE` isolation and SQL subqueries. Instead, we use:

1. **Fine-grained pessimistic locks** on `(eventType, tagValue)` pairs for mutual exclusion
2. **A global atomic sequence counter** for event ordering
3. **A batch commit record** for crash-safe atomic visibility

Lock items reference the batch that holds them. When a batch commits, all its locks are **implicitly released** — the next writer checks the batch status and steals the lock immediately. No explicit release step. No TTL wait on the happy path.

This gives us the DCB guarantee: the event store verifies that no new events matching the append condition's query have been added since the caller last read. If matching events are found, the append is rejected.

### Why Not Optimistic Watermarks?

We considered optimistic approaches using watermark items (tracking max sequence per event type or tag). These work but:

- **Type-level watermarks** cause false conflicts when two writers append the same event type for different entities (e.g. `AssetCreated` for asset 123 vs asset 456)
- **Fine-grained `(type, tagValue)` watermarks** require updating all relevant watermarks inside a `TransactWriteItems` call, which is limited to 100 items — problematic for large batch appends

The pessimistic lock approach avoids both issues: locks are fine-grained (no false conflicts between different entities), and the actual event writes use unlimited `BatchWriteItem` calls outside any transaction.

## Append Flow

### Step 1: Create Batch Record

```
PutItem { PK: "_BATCH#<uuid>", status: "PENDING", createdAt: now }
```

The batch record is the **source of truth** for whether a set of locks is still active. It also gates event visibility — readers ignore events from PENDING batches.

### Step 2: Compute Lock Keys

The lock key set is the **union** of keys derived from the append condition's query and the events being written. Both sources generate the same key type: `_LOCK#<eventType>:<tagValue>`.

For a query item `{ eventTypes: [A, B], tags: [x=1] }` and events of type A with tags `[x=1, y=2]`:

```
Lock keys (sorted, to prevent deadlocks):
  _LOCK#A:x=1     (from condition: type A × tag x=1)
  _LOCK#A:y=2     (from event: type A × tag y=2)
  _LOCK#B:x=1     (from condition: type B × tag x=1)
```

Lock key derivation is the **cartesian product of types × tags** from both the condition query items and the events being written, deduplicated and sorted.

### Step 3: Acquire Locks (Parallel)

All lock acquisitions fire as **parallel `UpdateItem` calls**. Each lock item stores only the `batchId` of the batch that holds it.

**Fast path — lock not held (or lock item doesn't exist yet):**

```
UpdateItem { PK: "_LOCK#AssetCreated:asset=123" }
  SET batchId = :myBatchId
  CONDITION: attribute_not_exists(batchId)
```

**Contention path — lock held by another batch:**

```
1. GetItem _LOCK#<key>              → currentBatchId
2. GetItem _BATCH#<currentBatchId>  → check status

   If COMMITTED or FAILED (or batch record missing):
     → Previous writer finished or was cleaned up. Steal immediately:
       UpdateItem SET batchId = :myBatchId
         CONDITION: batchId = :oldBatchId

   If PENDING:
     → Active writer. Backoff with jitter, retry.
```

The lock acquisition logic never reasons about timeouts or batch age. A `PENDING` batch is **always** treated as active. Only the cleanup process (see below) decides when a batch is dead and transitions it to `FAILED`.

The `CONDITION: batchId = :oldBatchId` on the steal prevents two writers from stealing the same lock simultaneously — only one wins the conditional write.

- If **all locks acquired**: proceed to step 4
- If **any lock contested**: release acquired locks (set batchId to a sentinel or delete), backoff, retry from step 3
- Locks are always acquired in **sorted key order** to prevent deadlocks

### Step 4: Read + Check Append Condition (Inside Lock)

Using **strongly consistent reads**, query events matching the append condition's query where `sequencePosition` exceeds the last position observed by the caller.

```
If any matching events found → release locks, throw AppendConditionError
```

The locks guarantee no other writer is concurrently appending events that match the same query. The strongly consistent read guarantees we see all committed events.

### Step 5: Reserve Sequence Range

```
UpdateItem { PK: "_SEQ" }
  ADD value :batchSize
  ReturnValues: UPDATED_OLD
```

Atomic increment returns the previous value. Events are assigned positions `[oldValue + 1, oldValue + batchSize]`. This is an atomic `ADD`, not a conditional check — concurrent writers on different lock sets can reserve ranges simultaneously without conflicting.

The global sequence counter is the **sole source of event ordering**. It guarantees a single total order across all events regardless of which client appended them. Gaps in the sequence (from failed batches) are expected and harmless — positions are never assumed to be contiguous. The implementation details of this counter (single item, sharded, etc.) may evolve, but the guarantee is the same: every committed event has a unique, globally comparable position.

### Step 6: Write Events (Parallel BatchWriteItem)

```
BatchWriteItem — 25 items per call, parallelised across connections
Each event written as multiple items (see Table Design for index items)
```

No transaction needed. No size limit. 100,000 events is just 4,000+ BatchWriteItem calls running in parallel. The locks prevent concurrent conflicting writes, and the PENDING batch status prevents partial visibility.

### Step 7: Commit Batch

```
UpdateItem { PK: "_BATCH#<uuid>" }
  SET status = "COMMITTED"
```

This single write atomically:
- Makes all events from this batch visible to readers
- Implicitly releases every lock held by this batch (next writer will see COMMITTED and steal)

**No explicit lock release step.** When the next writer encounters a lock pointing to this batch, it reads the batch record, sees `COMMITTED`, and immediately steals the lock.

### Optimisation: Small Appends (Transactional Path)

For appends where the total item count fits within `TransactWriteItems`' 100-item limit (~40 events depending on tag count), use a **single transaction** instead:

```
TransactWriteItems:
  ConditionCheck on relevant lock keys (no matching events with position above the caller's last observed)
  Put each event (with all index items)
  Update _SEQ
```

No locks, no batch record, no `batchId` on events. Fully atomic. This covers the common case (command handlers producing 1-5 events) with zero overhead on reads.

The adapter selects the appropriate path automatically based on payload size. The caller always just calls `append(events, condition)`.

## Read Flow

```typescript
read(query: Query, options?: ReadOptions): AsyncGenerator<EventEnvelope>
```

1. Query events matching type + tags using denormalized index items in the main table
2. For events **without** a `batchId`: yield immediately (came from a small transactional append — guaranteed complete)
3. For events **with** a `batchId`: check batch status
   - Committed batch IDs are cached in-memory (immutable once committed)
   - Unknown batch IDs: `BatchGetItem` on the `_BATCH#<id>` records
   - PENDING, FAILED, or missing → filter out those events
4. Yield matching, committed events ordered by `sequencePosition`

In practice, most reads from normal command handling encounter zero `batchId` events and need no extra lookups.

## Batch State Machine

```
PENDING  →  COMMITTED    (step 7 — happy path)
PENDING  →  FAILED       (cleanup process — crashed/stale batch detected)
```

Both `COMMITTED` and `FAILED` are terminal states. Lock acquisition treats both as "released" — the next writer can steal immediately.

## Cleanup Process

A periodic process (Lambda on a schedule, or application startup hook) handles crashed batches:

1. **Scan** for `PENDING` batches where `createdAt` is older than a threshold (e.g. 60 seconds)
2. **Transition** each to `FAILED`
3. **Optionally delete** orphaned event items written by the failed batch (identifiable by `batchId`)

This is the **only** place that reasons about batch age. The hot path (lock acquisition, reads, writes) never checks timestamps — it only checks the batch status field.

Benefits:
- **Observability**: FAILED batches can be monitored and alerted on — they represent crashes or bugs
- **Deterministic lock logic**: PENDING always means active, no guessing
- **Configurable recovery**: The cleanup threshold can be tuned independently of the append logic
- **Optional event cleanup**: Orphaned events from FAILED batches are invisible to readers (filtered out), so cleanup is a cost optimisation, not a correctness requirement

## Crash Safety Analysis

| Crash point | Batch status | Locks | Events | Recovery |
|-------------|-------------|-------|--------|----------|
| During lock acquisition | PENDING | Partial locks reference batch | None written | Cleanup marks batch FAILED → next writer steals |
| During condition check | PENDING | All locks reference batch | None written | Same — cleanup → FAILED → steal |
| During sequence reservation | PENDING | All locks held | None written | Cleanup → FAILED → steal. Sequence gap is harmless. |
| Mid-BatchWriteItem | PENDING | All locks held | Partial | **Partial events invisible** (PENDING/FAILED batch). Cleanup → FAILED → steal. |
| After all events, before commit | PENDING | All locks held | All written | Events invisible until cleanup → FAILED. Wasteful but safe. |
| After commit | COMMITTED | Locks reference committed batch | All visible | **Fully successful.** Next writer steals locks immediately. |

**Every crash scenario is safe.** Partial events from failed batches are invisible to readers because their batch status is never `COMMITTED`. The cleanup process transitions stale batches to `FAILED`, unblocking any writers waiting on those locks.

On the **happy path** (no crashes), there is never a wait for cleanup. Lock release is instantaneous via the batch commit.

## DynamoDB Table Design

### Single Table, No GSIs

All indexes are denormalized items in the main table. This is critical because **GSI reads are always eventually consistent** — they cannot be used for the strongly consistent condition check inside locks.

| Item type | PK | SK | Data |
|-----------|----|----|------|
| Event (primary) | `E#<seqPos>` | `E` | Full event: type, tags, data, metadata, timestamp, batchId? |
| Type+Tag index | `I#<eventType>#<tagValue>` | `<seqPos>` | Full event data |
| Type-only index | `IT#<eventType>` | `<seqPos>` | Full event data |
| Tag-only index | `IG#<tagValue>` | `<seqPos>` | Full event data |
| All-events bucket | `A#<bucket>` | `<seqPos>` | Full event data |
| Sequence counter | `_SEQ` | `_SEQ` | `value` (Number) |
| Batch record | `_BATCH#<uuid>` | `_BATCH#<uuid>` | `status`, `seqStart`, `seqEnd`, `createdAt` |
| Lock | `_LOCK#<key>` | `_LOCK#<key>` | `batchId` |

- `<seqPos>` is zero-padded (e.g. `00000000501`) for correct lexicographic sort order
- `<bucket>` = `floor(seqPos / 10000)` — each bucket partition holds up to 10,000 events
- Full event data is stored on every index item — no secondary lookups on read
- `IT#` and `IG#` indexes support type-only and tag-only **reads** (not used for append condition checks)

### Write Amplification

For an event of type `AssetCreated` with tags `[asset=123, location=NYC]`:

```
E#501                          | E              (primary record)
I#AssetCreated#asset=123       | 00000000501    (type+tag index)
I#AssetCreated#location=NYC    | 00000000501    (type+tag index)
IT#AssetCreated                | 00000000501    (type-only read index)
IG#asset=123                   | 00000000501    (tag-only read index)
IG#location=NYC                | 00000000501    (tag-only read index)
A#0                            | 00000000501    (all-events bucket)
```

**7 items per event** (with 2 tags). Formula: `1 + tags + 1 + tags + 1 = 2 * tags + 3`.

For a batch of 1,000 events with 3 tags average: 9,000 items = 360 BatchWriteItem calls. Running in parallel at ~5ms each from a single Fargate task, this completes in well under a second.

### Query Routing

| Query shape | DynamoDB query | Notes |
|-------------|---------------|-------|
| Types + tags | `PK = "I#<type>#<firstTag>", SK >= fromSeqPos` | Pick one tag for PK, filter remaining tags client-side |
| Types only | `PK = "IT#<type>", SK >= fromSeqPos` | One query per type, merge results |
| Tags only | `PK = "IG#<tag>", SK >= fromSeqPos` | One query per tag |
| `Query.all()` | `PK = "A#<bucket>", SK >= fromSeqPos` | Sequential bucket reads |
| Multiple QueryItems | Execute each as above, merge + deduplicate by seqPos | OR semantics, same as Postgres UNION ALL |
| Backwards | `ScanIndexForward: false`, `SK <= fromSeqPos` | Native DynamoDB reverse scan |
| Limit | Paginate results, apply limit after client-side tag filtering | DynamoDB Limit applies pre-filter |

### Condition Check Query (Inside Locks)

The append condition check uses the type+tag index items with **strongly consistent reads**:

```
Query:
  PK = "I#AssetCreated#asset=123"
  SK > "00000000500"    (last observed position)
  ConsistentRead: true
  Limit: 1              (only need to know if ANY exist)
```

Single query, returns 0 or 1 item. If 1 → matching events found, append is rejected.

## Performance Expectations

All estimates assume same-region Fargate + DynamoDB on-demand.

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Small append (1-5 events, transactional) | 5-15ms | Thousands/sec |
| Large append (1,000 events, lock-based) | 100-500ms | Hundreds/sec per lock set, unlimited across different entities |
| Very large append (100K events) | 5-15s | Bounded by BatchWriteItem parallelism |
| Sequence counter (atomic ADD) | ~5ms | ~1,000 increments/sec = up to 200K events/sec with batching |
| Read (type + tag query) | 1-5ms | Thousands/sec, scales with DynamoDB partitions |
| Lock acquisition (parallel) | 5-10ms total | Effectively unlimited for non-conflicting keys |
| Lock steal (happy path, batch committed) | 10-15ms (2 reads + 1 write) | N/A |

DynamoDB on-demand scales automatically. The primary throughput constraint is the application's ability to parallelise calls, not DynamoDB itself. 10,000+ events/sec read and write is comfortably achievable from a single Fargate task.

## Open Questions

- **Cleanup threshold**: How long a PENDING batch must exist before the cleanup process marks it FAILED. Likely 30-60 seconds. This only affects crash recovery — the hot path never checks batch age.
- **Cleanup implementation**: Options include a CloudWatch-scheduled Lambda, an application startup hook, or a DynamoDB Streams trigger. Could also run lazily — when a writer encounters a PENDING lock, it notifies the cleanup process.
- **Orphaned event deletion**: Events from FAILED batches are invisible to readers but still consume storage. Cleanup can optionally delete them (identifiable by `batchId`). Not urgent — they're inert.
- **Tag query selectivity**: When a query has multiple tags, the adapter picks one for the partition key lookup and filters the rest client-side. A heuristic for picking the most selective tag could improve performance.
- **Events with many tags**: Write amplification scales as `2 * tags + 3` items per event. Events with 10+ tags produce 23+ items. Acceptable for DynamoDB throughput but worth monitoring for cost.

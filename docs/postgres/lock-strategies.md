# Lock Strategies

Lock strategies control how `PostgresEventStore` serializes concurrent appends that target overlapping scopes. Two built-in strategies are provided; custom strategies can be implemented via the `LockStrategy` interface.

See [PostgresEventStore](./postgres-event-store.md) for how locks are used during appends.

## LockStrategy interface

```typescript
import { Pool, PoolClient } from "pg"
import { DcbEvent, AppendCondition } from "@dcb-es/event-store"

interface LockStrategy {
    computeKeys(events: DcbEvent[], condition?: AppendCondition): bigint[]
    acquire(client: PoolClient, keys: bigint[], tableName: string): Promise<void>
    generateSpLockBlock(tableName: string): string
    ensureSchema?(pool: Pool | PoolClient, tableName: string): Promise<void>
}
```

| Method | Purpose |
|--------|---------|
| `computeKeys` | Compute the set of 64-bit lock keys from the events being appended and the append condition. Each key represents a `(type, tag)` scope that must be held for the duration of the transaction. |
| `acquire` | Acquire all locks for the given keys within the current transaction. Called by the COPY and batch append paths. |
| `generateSpLockBlock` | Return a PL/pgSQL code block that acquires locks inside the stored procedure. Injected into the `events_append` function during `ensureInstalled()`. |
| `ensureSchema` | (Optional) Create any tables required by the strategy. Called by `ensureInstalled()`. |

---

## Lock key computation (shared)

Both built-in strategies use the same `computeKeys` function. Lock keys are derived from the cartesian product of event types and tags:

- From the **condition**: for each query item in `failIfEventsMatch`, compute `hash(type + "|" + tag)` for every `(type, tag)` pair
- From the **events**: for each event being appended, compute `hash(event.type + "|" + tag)` for every tag on the event

All keys are deduplicated (via `Set<bigint>`) before acquisition.

This scoping means that only transactions whose events or conditions share a `(type, tag)` pair will contend for the same lock. Transactions with non-overlapping scopes proceed in parallel.

### FNV-1a 64-bit hashing

Lock keys are computed using FNV-1a with a 64-bit hash width:

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

**Why FNV-1a 64-bit:**

- `pg_advisory_xact_lock` accepts a `bigint` (signed 64-bit integer). FNV-1a 64-bit maps directly to this without truncation.
- With 64 bits of hash space, the collision probability for any two distinct `(type, tag)` strings is approximately 1 in 2^64 (~5.4 x 10^-20). This eliminates false serialization in practice -- no bucketing is needed.
- FNV-1a is fast (simple XOR + multiply per byte) and has excellent avalanche properties. The hash is computed once per append for a small number of `(type, tag)` pairs, so performance is not a concern.
- The unsigned-to-signed conversion at the end accounts for Postgres `bigint` being a signed type.

---

## Advisory locks

```typescript
import { advisoryLocks } from "@dcb-es/event-store-postgres"

const eventStore = new PostgresEventStore({
    pool,
    lockStrategy: advisoryLocks()   // this is the default
})
```

Advisory locks are the default strategy. They use Postgres `pg_advisory_xact_lock()`, which acquires in-memory locks that are automatically released when the transaction ends.

### How they work

**Client-side (COPY / batch paths):**

```sql
SELECT pg_advisory_xact_lock(k)
FROM unnest($1::bigint[]) AS k
ORDER BY k
```

**Stored procedure (SP path):**

```sql
PERFORM pg_advisory_xact_lock(k)
FROM unnest(p_lock_keys) AS k
ORDER BY k;
```

The `ORDER BY k` clause is critical: it ensures all transactions acquire locks in a consistent numeric order, preventing deadlocks. Without ordered acquisition, two transactions locking keys `[A, B]` and `[B, A]` could each hold one and wait for the other.

### Characteristics

| Property | Value |
|----------|-------|
| Schema required | None -- advisory locks are built into Postgres |
| Lock storage | In-memory (shared memory) |
| Lock lifetime | Transaction-scoped (`xact` variant) -- released on COMMIT or ROLLBACK |
| Deadlock prevention | Sorted acquisition order |
| Performance | Fastest option -- no disk I/O, no table contention |
| `ensureSchema` | Not implemented (no schema needed) |

### RDS Proxy connection pinning caveat

Advisory locks cause **connection pinning** on Amazon RDS Proxy and similar connection-pooling proxies. When a session acquires an advisory lock, the proxy pins that session to a specific backend connection for the duration of the lock, defeating the purpose of connection pooling.

If you are deploying behind RDS Proxy, use `rowLocks()` instead.

---

## Row locks

```typescript
import { rowLocks } from "@dcb-es/event-store-postgres"

const eventStore = new PostgresEventStore({
    pool,
    lockStrategy: rowLocks()
})
```

Row locks use a dedicated table (`<tableName>_lock_scopes`) with one row per lock scope. Locking is achieved via standard `SELECT ... FOR UPDATE` row-level locks, which are compatible with connection-pooling proxies.

### How they work

**Client-side (COPY / batch paths):**

```sql
-- Step 1: Ensure rows exist for all scope keys
INSERT INTO events_lock_scopes (scope_key)
SELECT unnest($1::bigint[])
ON CONFLICT DO NOTHING;

-- Step 2: Acquire row-level locks in sorted order
SELECT 1 FROM events_lock_scopes
WHERE scope_key = ANY($1::bigint[])
ORDER BY scope_key
FOR UPDATE;
```

**Stored procedure (SP path):**

```sql
INSERT INTO events_lock_scopes (scope_key)
SELECT unnest(p_lock_keys)
ON CONFLICT DO NOTHING;

PERFORM 1 FROM events_lock_scopes
WHERE scope_key = ANY(p_lock_keys)
ORDER BY scope_key
FOR UPDATE;
```

### Lock-scopes table DDL

Created by `ensureSchema`:

```sql
CREATE TABLE IF NOT EXISTS events_lock_scopes (
    scope_key BIGINT PRIMARY KEY
);
```

The table is lazily populated -- each new `(type, tag)` scope inserts a row on first use (`INSERT ... ON CONFLICT DO NOTHING`). Rows are never deleted. Over time the table grows to contain one row per distinct scope that has been used in an append. For a typical application, this is a small table (hundreds to low thousands of rows).

### Characteristics

| Property | Value |
|----------|-------|
| Schema required | `events_lock_scopes` table (created by `ensureInstalled()`) |
| Lock storage | Row-level locks (standard Postgres row locking) |
| Lock lifetime | Transaction-scoped -- released on COMMIT or ROLLBACK |
| Deadlock prevention | Sorted acquisition order (`ORDER BY scope_key`) |
| Performance | Slightly slower than advisory locks due to disk I/O on first-use INSERT, but equivalent for subsequent locks within the same transaction |
| RDS Proxy compatible | Yes -- row locks do not cause connection pinning |
| `ensureSchema` | Creates the lock-scopes table |

---

## When to choose which

| Environment | Recommended strategy | Reason |
|-------------|---------------------|--------|
| Standalone Postgres | `advisoryLocks()` | Fastest, no schema overhead, no connection proxy to pin |
| Amazon RDS with RDS Proxy | `rowLocks()` | Advisory locks cause connection pinning, defeating the proxy |
| Amazon Aurora | `rowLocks()` | Aurora deployments typically use RDS Proxy; row locks avoid pinning |
| Aurora Limitless (distributed) | `rowLocks()` | Advisory locks are node-local and would not provide cluster-wide serialization |
| PgBouncer (transaction mode) | `advisoryLocks()` | Advisory locks are transaction-scoped, which is compatible with transaction-mode pooling |
| PgBouncer (session mode) | `advisoryLocks()` | Same as above |

**General rule:** Use advisory locks unless you have a connection-pooling proxy between your application and Postgres that pins on advisory locks. In that case, use row locks.

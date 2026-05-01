import { Pool, PoolClient } from "pg"
import { DcbEvent, Query, SequencedEvent, Tags } from "@dcb-es/event-store"
import { PostgresEventStore } from "./PostgresEventStore.js"
import { advisoryLocks, rowLocks, LockStrategy } from "./lockStrategy.js"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

// Reproduces the BIGSERIAL gap bug (issue #89). Two writers on disjoint scopes
// commit out of allocation order. A subscriber whose filter matches both writers
// sees the later-allocated, earlier-committed writer first, advances its
// bookmark past the gap, then misses the earlier-allocated writer's events when
// they finally commit.
//
// To exercise the barrier fix, writers acquire the same locks the events_append
// SP would (leaf X + intent S) inside an open transaction. Without those locks
// the barrier has nothing to wait on, so the test would not verify the fix.

const EMPTY_PAYLOAD = '{"data":{},"metadata":{}}'

const buildEvent = (type: string, tag: string): DcbEvent => ({
    type,
    tags: Tags.from([tag]),
    data: {},
    metadata: {}
})

async function appendHeldOpen(
    client: PoolClient,
    lockStrategy: LockStrategy,
    type: string,
    tag: string
): Promise<void> {
    const event = buildEvent(type, tag)
    const writerKeys = lockStrategy.computeWriterKeys([event])
    await lockStrategy.acquireWriter(client, writerKeys, "events")
    await client.query("INSERT INTO events (type, tags, payload) VALUES ($1, $2, $3)", [type, [tag], EMPTY_PAYLOAD])
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
        if (predicate()) return
        await new Promise(r => setTimeout(r, 10))
    }
    throw new Error("Timed out waiting for predicate")
}

const strategies: [string, () => LockStrategy][] = [
    ["advisory", () => advisoryLocks()],
    ["row-locks", () => rowLocks()]
]

describe.each(strategies)("Concurrent commit gap [%s]", (_name, createStrategy) => {
    let pool: Pool
    let store: PostgresEventStore
    let lockStrategy: LockStrategy

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 30 })
        lockStrategy = createStrategy()
        store = new PostgresEventStore({ pool, lockStrategy })
        await store.ensureInstalled()
    })

    beforeEach(async () => {
        // Pre-populate lock_scopes (no-op for advisory) so concurrent writers'
        // `INSERT ... ON CONFLICT DO NOTHING` against the same key is fast — without
        // pre-population, the second writer blocks until the first writer's tx
        // completes (Postgres uniqueness check waits on in-flight inserts).
        const events: DcbEvent[] = [
            buildEvent("EventA", "scope=X"),
            buildEvent("EventB", "scope=Y"),
            buildEvent("Order", "customer=A"),
            buildEvent("Order", "customer=B"),
            buildEvent("Unrelated", "scope=X"),
            buildEvent("Target", "id=42")
        ]
        const allKeys = new Set<bigint>()
        for (const ev of events) {
            const wk = lockStrategy.computeWriterKeys([ev])
            for (const k of wk.leafX) allKeys.add(k)
            for (const k of wk.intentS) allKeys.add(k)
        }
        const exists = await pool.query(
            `SELECT 1 FROM information_schema.tables WHERE table_name = 'events_lock_scopes'`
        )
        if (exists.rows.length > 0 && allKeys.size > 0) {
            await pool.query(
                `INSERT INTO events_lock_scopes (scope_key) SELECT unnest($1::bigint[]) ON CONFLICT DO NOTHING`,
                [Array.from(allKeys)]
            )
        }
    })

    afterEach(async () => {
        await pool.query("TRUNCATE table events")
        await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    test("Query.all subscriber does not lose events when commits are out-of-order", async () => {
        const slowWriter = await pool.connect()
        const fastWriter = await pool.connect()

        try {
            // Writer A: BEGIN, acquire writer locks, INSERT (allocates seq 1), DO NOT COMMIT yet.
            await slowWriter.query("BEGIN")
            await appendHeldOpen(slowWriter, lockStrategy, "EventA", "scope=X")

            // Writer B: BEGIN, acquire writer locks, INSERT (allocates seq 2), COMMIT.
            await fastWriter.query("BEGIN")
            await appendHeldOpen(fastWriter, lockStrategy, "EventB", "scope=Y")
            await fastWriter.query("COMMIT")

            // Subscriber must see B but the barrier must prevent advancing past A's slot.
            const seen: SequencedEvent[] = []
            const controller = new AbortController()
            const subPromise = (async () => {
                try {
                    for await (const ev of store.subscribe(Query.all(), {
                        pollIntervalMs: 20,
                        signal: controller.signal
                    })) {
                        seen.push(ev)
                    }
                } catch {
                    // ignore — abort can throw
                }
            })()

            // Allow time for several poll cycles.
            await new Promise(r => setTimeout(r, 200))

            // Now commit A.
            await slowWriter.query("COMMIT")

            await waitFor(() => seen.length >= 2, 2000).catch(() => {})

            controller.abort()
            await subPromise

            const types = seen.map(e => e.event.type)
            expect(types).toContain("EventA")
            expect(types).toContain("EventB")
            expect(seen.length).toBe(2)
        } finally {
            await slowWriter.query("ROLLBACK").catch(() => {})
            slowWriter.release()
            fastWriter.release()
        }
    })

    test("type-only subscriber does not lose events when commits are out-of-order", async () => {
        const slowWriter = await pool.connect()
        const fastWriter = await pool.connect()

        try {
            await slowWriter.query("BEGIN")
            await appendHeldOpen(slowWriter, lockStrategy, "Order", "customer=A")

            await fastWriter.query("BEGIN")
            await appendHeldOpen(fastWriter, lockStrategy, "Order", "customer=B")
            await fastWriter.query("COMMIT")

            const seen: SequencedEvent[] = []
            const controller = new AbortController()
            const subPromise = (async () => {
                try {
                    for await (const ev of store.subscribe(Query.fromItems([{ types: ["Order"] }]), {
                        pollIntervalMs: 20,
                        signal: controller.signal
                    })) {
                        seen.push(ev)
                    }
                } catch {
                    // ignore
                }
            })()

            await new Promise(r => setTimeout(r, 200))

            await slowWriter.query("COMMIT")

            await waitFor(() => seen.length >= 2, 2000).catch(() => {})

            controller.abort()
            await subPromise

            const tags = seen.flatMap(e => e.event.tags.values)
            expect(tags).toContain("customer=A")
            expect(tags).toContain("customer=B")
            expect(seen.length).toBe(2)
        } finally {
            await slowWriter.query("ROLLBACK").catch(() => {})
            slowWriter.release()
            fastWriter.release()
        }
    })

    test("non-overlapping (T, t) subscriber is not capped by unrelated in-flight writer", async () => {
        const slowWriter = await pool.connect()
        const fastWriter = await pool.connect()

        try {
            // Slow writer holds an event of a different type from the reader's filter.
            await slowWriter.query("BEGIN")
            await appendHeldOpen(slowWriter, lockStrategy, "Unrelated", "scope=X")

            // Fast writer commits an event matching the reader's filter.
            await fastWriter.query("BEGIN")
            await appendHeldOpen(fastWriter, lockStrategy, "Target", "id=42")
            await fastWriter.query("COMMIT")

            const seen: SequencedEvent[] = []
            const controller = new AbortController()
            const subPromise = (async () => {
                try {
                    for await (const ev of store.subscribe(
                        Query.fromItems([{ types: ["Target"], tags: Tags.from(["id=42"]) }]),
                        { pollIntervalMs: 20, signal: controller.signal }
                    )) {
                        seen.push(ev)
                    }
                } catch {
                    // ignore
                }
            })()

            // Should observe Target promptly — the barrier on K(Target, id=42) doesn't
            // conflict with the unrelated slow writer's locks.
            await waitFor(() => seen.length >= 1, 1000)
            expect(seen.length).toBe(1)
            expect(seen[0].event.type).toBe("Target")

            controller.abort()
            await subPromise
        } finally {
            await slowWriter.query("ROLLBACK").catch(() => {})
            slowWriter.release()
            fastWriter.release()
        }
    })
})

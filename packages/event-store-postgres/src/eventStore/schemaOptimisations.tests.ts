import { Pool } from "pg"
import {
    AppendConditionError,
    DcbEvent,
    Query,
    SequencePosition,
    streamAllEventsToArray,
    Tags
} from "@dcb-es/event-store"
import { PostgresEventStore } from "./PostgresEventStore.js"
import { advisoryLocks } from "./lockStrategy.js"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const event = (type: string, tags: Tags, data: unknown = {}, metadata: unknown = {}): DcbEvent => ({
    type,
    tags,
    data,
    metadata
})

describe("schema optimisations", () => {
    let pool: Pool
    let store: PostgresEventStore

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 30 })
        store = new PostgresEventStore({ pool, lockStrategy: advisoryLocks() })
        await store.ensureInstalled()
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    // ─── SCHEMA VERIFICATION ───────────────────────────────────────

    describe("schema structure", () => {
        test("type column uses C collation", async () => {
            const result = await pool.query(
                `SELECT collation_name FROM information_schema.columns
                 WHERE table_name = 'events' AND column_name = 'type'`
            )
            expect(result.rows[0].collation_name).toBe("C")
        })

        test("no GIN index on tags (write throughput prioritised)", async () => {
            const result = await pool.query(
                `SELECT indexname FROM pg_indexes
                 WHERE tablename = 'events' AND indexname = 'events_tags_gin'`
            )
            expect(result.rows.length).toBe(0)
        })

        test("btree index on (type, sequence_position DESC) exists", async () => {
            const result = await pool.query(
                `SELECT indexname, indexdef FROM pg_indexes
                 WHERE tablename = 'events' AND indexname = 'events_type_pos_idx'`
            )
            expect(result.rows.length).toBe(1)
            expect(result.rows[0].indexdef).toContain("type")
            expect(result.rows[0].indexdef).toContain("sequence_position")
        })

        test("autovacuum freeze parameters are set", async () => {
            const result = await pool.query(`SELECT reloptions FROM pg_class WHERE relname = 'events'`)
            const opts = result.rows[0].reloptions ?? []
            expect(opts).toContain("autovacuum_freeze_min_age=10000000")
            expect(opts).toContain("autovacuum_freeze_table_age=100000000")
        })
    })

    // ─── INDEX USAGE VERIFICATION ──────────────────────────────────

    describe("query plans use indexes", () => {
        beforeAll(async () => {
            // Seed enough data for the planner to prefer index scans
            const events: DcbEvent[] = []
            for (let i = 0; i < 500; i++) {
                events.push(event(`Type${i % 10}`, Tags.from([`entity=E${i}`, `batch=B${i % 5}`]), { i }))
            }
            await store.append({ events })

            // ANALYZE so the planner has accurate stats
            await pool.query("ANALYZE events")
        })

        test("type-filtered read uses btree index", async () => {
            const result = await pool.query(
                `EXPLAIN (FORMAT JSON) SELECT sequence_position, type, payload, tags
                 FROM events
                 WHERE type IN ($1) AND sequence_position > $2
                 ORDER BY sequence_position`,
                ["Type0", 0]
            )
            const plan = JSON.stringify(result.rows[0]["QUERY PLAN"])
            expect(plan).toMatch(/Index|Bitmap/)
        })

        test("condition check uses btree for type + position (tags filtered post-scan)", async () => {
            const client = await pool.connect()
            try {
                await client.query("SET enable_seqscan = off")
                const result = await client.query(
                    `EXPLAIN (FORMAT JSON) SELECT 1 FROM events
                     WHERE type = ANY($1::text[]) AND tags @> $2::text[] AND sequence_position > $3
                     LIMIT 1`,
                    [["Type0"], ["batch=B0"], 0]
                )
                const plan = JSON.stringify(result.rows[0]["QUERY PLAN"])
                // Btree on (type, sequence_position) narrows; tags filtered from heap
                expect(plan).toMatch(/Index Scan|Index Only/)
            } finally {
                await client.query("RESET enable_seqscan")
                client.release()
            }
        })
    })

    // ─── FUNCTIONAL CORRECTNESS AFTER OPTIMISATIONS ─────────────────

    describe("functional correctness", () => {
        beforeEach(async () => {
            await pool.query("TRUNCATE table events")
            await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
        })

        test("append and read round-trip", async () => {
            await store.append({
                events: event("TestEvent", Tags.from(["entity=E1"]), { foo: "bar" }, { userId: "U1" })
            })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(1)
            expect(events[0].event.type).toBe("TestEvent")
            expect(events[0].event.data).toEqual({ foo: "bar" })
            expect(events[0].event.metadata).toEqual({ userId: "U1" })
            expect(events[0].event.tags.equals(Tags.from(["entity=E1"]))).toBe(true)
            expect(events[0].position.toString()).toBe("1")
        })

        test("OR-based read query deduplicates correctly across multiple query items", async () => {
            // Event matches both query items — should appear once
            await store.append({ events: event("Order", Tags.from(["entity=E1", "region=EU"])) })
            await store.append({ events: event("Invoice", Tags.from(["entity=E2"])) })

            const events = await streamAllEventsToArray(
                store.read(
                    Query.fromItems([
                        { types: ["Order"], tags: Tags.from(["entity=E1"]) },
                        { types: ["Order"], tags: Tags.from(["region=EU"]) }
                    ])
                )
            )
            // The Order event matches both items but should only appear once
            expect(events.length).toBe(1)
            expect(events[0].event.type).toBe("Order")
        })

        test("condition check works with tag containment", async () => {
            await store.append({ events: event("Order", Tags.from(["entity=E1", "region=EU"])) })

            const condition = {
                failIfEventsMatch: Query.fromItems([{ types: ["Order"], tags: Tags.from(["entity=E1"]) }]),
                after: SequencePosition.initial()
            }
            await expect(store.append({ events: event("Order", Tags.from(["entity=E1"])), condition })).rejects.toThrow(
                AppendConditionError
            )
        })

        test("COPY path works without timestamp column", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: advisoryLocks(), copyThreshold: 1 })
            await s.ensureInstalled()

            const evts = Array.from({ length: 5 }, (_, i) => event("Bulk", Tags.from([`entity=E${i}`]), { i }))
            const pos = await s.append({ events: evts })
            expect(pos.toString()).toBe("5")

            const all = await streamAllEventsToArray(s.read(Query.all()))
            expect(all.length).toBe(5)
        })

        test("multi-command batch works without timestamp column", async () => {
            const pos = await store.append([
                {
                    events: [event("MeterCreated", Tags.from(["meterId=M1"]))],
                    condition: {
                        failIfEventsMatch: Query.fromItems([
                            { types: ["MeterCreated"], tags: Tags.from(["meterId=M1"]) }
                        ]),
                        after: SequencePosition.initial()
                    }
                },
                {
                    events: [event("MeterCreated", Tags.from(["meterId=M2"]))],
                    condition: {
                        failIfEventsMatch: Query.fromItems([
                            { types: ["MeterCreated"], tags: Tags.from(["meterId=M2"]) }
                        ]),
                        after: SequencePosition.initial()
                    }
                }
            ])
            expect(pos.toString()).toBe("2")
        })
    })

    // ─── STRESS TEST ───────────────────────────────────────────────

    describe("stress test", () => {
        beforeEach(async () => {
            await pool.query("TRUNCATE table events")
            await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
        })

        test("50 parallel conditional appends to different scopes all succeed", async () => {
            const promises = Array.from({ length: 50 }, (_, i) => {
                const condition = {
                    failIfEventsMatch: Query.fromItems([
                        { types: ["StressEvent"], tags: Tags.fromObj({ entity: `E${i}` }) }
                    ]),
                    after: SequencePosition.initial()
                }
                return store.append({
                    events: event("StressEvent", Tags.fromObj({ entity: `E${i}` })),
                    condition
                })
            })
            const results = await Promise.allSettled(promises)
            expect(results.filter(r => r.status === "fulfilled").length).toBe(50)
        })

        test("contested scope: exactly one writer wins", async () => {
            const condition = {
                failIfEventsMatch: Query.fromItems([
                    { types: ["Contested"], tags: Tags.fromObj({ entity: "SHARED" }) }
                ]),
                after: SequencePosition.initial()
            }
            const promises = Array.from({ length: 10 }, () =>
                store.append({
                    events: event("Contested", Tags.fromObj({ entity: "SHARED" })),
                    condition
                })
            )
            const results = await Promise.allSettled(promises)
            expect(results.filter(r => r.status === "fulfilled").length).toBe(1)
            expect(results.filter(r => r.status === "rejected").length).toBe(9)
        })

        test("bulk insert with condition check under load", async () => {
            // Seed baseline events
            const seedEvents = Array.from({ length: 200 }, (_, i) =>
                event("Seed", Tags.from([`entity=E${i}`, `batch=B${i % 10}`]), { i })
            )
            await store.append({ events: seedEvents })

            // Now do a conditional append that must scan through existing events
            const condition = {
                failIfEventsMatch: Query.fromItems([{ types: ["NewType"], tags: Tags.from(["batch=B0"]) }]),
                after: SequencePosition.initial()
            }
            const pos = await store.append({
                events: event("NewType", Tags.from(["batch=B0"])),
                condition
            })
            expect(parseInt(pos.toString())).toBe(201)
        })
    })

    // ─── ENSURESCHEMA IDEMPOTENCY ──────────────────────────────────

    describe("idempotency", () => {
        test("ensureInstalled can be called multiple times without error", async () => {
            await store.ensureInstalled()
            await store.ensureInstalled()
            // Verify schema is still correct
            const result = await pool.query(
                `SELECT indexname FROM pg_indexes WHERE tablename = 'events' ORDER BY indexname`
            )
            const indexes = result.rows.map(r => r.indexname)
            expect(indexes).toContain("events_pkey")
            expect(indexes).toContain("events_type_pos_idx")
        })
    })
})

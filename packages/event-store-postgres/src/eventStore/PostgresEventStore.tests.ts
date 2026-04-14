import { Pool } from "pg"
import {
    AppendCondition,
    AppendConditionError,
    DcbEvent,
    Query,
    SequencePosition,
    streamAllEventsToArray,
    Tags
} from "@dcb-es/event-store"
import { PostgresEventStore } from "./PostgresEventStore"
import { LockStrategy, advisoryLocks, rowLocks } from "./lockStrategy"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const event = (type: string, tags: Tags, data: unknown = {}, metadata: unknown = {}): DcbEvent => ({
    type,
    tags,
    data,
    metadata
})

const scopedCondition = (types: string[], tags: Tags, after: SequencePosition): AppendCondition => ({
    failIfEventsMatch: Query.fromItems([{ types, tags }]),
    after
})

const strategies: [string, () => LockStrategy][] = [
    ["advisory", () => advisoryLocks()],
    ["row-locks", () => rowLocks()]
]

describe.each(strategies)("PostgresEventStore [%s]", (_name, createStrategy) => {
    let pool: Pool
    let store: PostgresEventStore

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 30 })
        store = new PostgresEventStore({ pool, lockStrategy: createStrategy() })
        await store.ensureInstalled()
    })

    afterEach(async () => {
        await pool.query("TRUNCATE table events")
        await pool.query("ALTER SEQUENCE events_sequence_position_seq RESTART WITH 1")
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    // ─── BASIC APPEND ───────────────────────────────────────────────

    describe("basic append", () => {
        test("single event returns position 1", async () => {
            const pos = await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })) })
            expect(pos.toString()).toBe("1")
        })

        test("batch append returns position of last event", async () => {
            const pos = await store.append({
                events: [event("A", Tags.fromObj({ e: "1" })), event("B", Tags.fromObj({ e: "2" }))]
            })
            expect(pos.toString()).toBe("2")
        })

        test("sequential appends increment position", async () => {
            await store.append({ events: event("A", Tags.fromObj({ e: "1" })) })
            const pos = await store.append({ events: event("B", Tags.fromObj({ e: "2" })) })
            expect(pos.toString()).toBe("2")
        })

        test("stores and returns data, metadata, tags, type, and timestamp", async () => {
            await store.append({
                events: event("TestEvent", Tags.from(["entity=E1"]), { foo: "bar" }, { userId: "U1" })
            })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events[0].event.type).toBe("TestEvent")
            expect(events[0].event.data).toEqual({ foo: "bar" })
            expect(events[0].event.metadata).toEqual({ userId: "U1" })
            expect(events[0].event.tags.equals(Tags.from(["entity=E1"]))).toBe(true)
            expect(events[0].timestamp).toBeDefined()
        })

        test("round-trips complex nested data and metadata", async () => {
            const data = { nested: { arr: [1, null, "line\nbreak"], flag: true, obj: { deep: "value" } } }
            const metadata = { trace: "id\\with\\slashes", items: [1, 2, 3] }
            await store.append({ events: event("Complex", Tags.from(["entity=E1"]), data, metadata) })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events[0].event.data).toEqual(data)
            expect(events[0].event.metadata).toEqual(metadata)
        })

        test("rejects empty events array", async () => {
            await expect(store.append({ events: [] as DcbEvent[] })).rejects.toThrow("Cannot append zero events")
        })
    })

    // ─── CONDITIONAL APPEND ─────────────────────────────────────────

    describe("conditional append", () => {
        test("succeeds when no prior events match condition", async () => {
            const condition = scopedCondition(["TestEvent"], Tags.fromObj({ e: "1" }), SequencePosition.initial())
            const pos = await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            expect(pos.toString()).toBe("1")
        })

        test("succeeds when prior events are within ceiling", async () => {
            await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })) })
            const condition = scopedCondition(["TestEvent"], Tags.fromObj({ e: "1" }), SequencePosition.fromString("1"))
            const pos = await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            expect(pos.toString()).toBe("2")
        })

        test("fails when matching events exceed ceiling", async () => {
            await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })) })
            const condition = scopedCondition(["TestEvent"], Tags.fromObj({ e: "1" }), SequencePosition.initial())
            await expect(
                store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            ).rejects.toThrow(AppendConditionError)
        })

        test("different tag scopes do not conflict", async () => {
            await store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })) })
            const condition = scopedCondition(["TestEvent"], Tags.fromObj({ e: "2" }), SequencePosition.initial())
            await store.append({ events: event("TestEvent", Tags.fromObj({ e: "2" })), condition })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
        })

        test("different type scopes do not conflict", async () => {
            await store.append({ events: event("TypeA", Tags.fromObj({ e: "1" })) })
            const condition = scopedCondition(["TypeB"], Tags.fromObj({ e: "1" }), SequencePosition.initial())
            await store.append({ events: event("TypeB", Tags.fromObj({ e: "1" })), condition })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
        })
    })

    // ─── MULTI-TAG CONTAINMENT ──────────────────────────────────────

    describe("multi-tag containment", () => {
        test("event with superset tags IS caught by subset condition", async () => {
            await store.append({ events: event("Order", Tags.from(["entity=E1", "region=EU"])) })
            const condition = scopedCondition(["Order"], Tags.from(["entity=E1"]), SequencePosition.initial())
            await expect(
                store.append({ events: event("Order", Tags.fromObj({ entity: "E1" })), condition })
            ).rejects.toThrow(AppendConditionError)
        })

        test("event with subset tags is NOT caught by superset condition", async () => {
            await store.append({ events: event("Order", Tags.from(["entity=E1"])) })
            const condition = scopedCondition(
                ["Order"],
                Tags.from(["entity=E1", "region=EU"]),
                SequencePosition.initial()
            )
            await store.append({ events: event("Order", Tags.fromObj({ entity: "E1" })), condition })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
        })

        test("event with exact matching tags IS caught", async () => {
            await store.append({ events: event("Order", Tags.from(["entity=E1", "region=EU"])) })
            const condition = scopedCondition(
                ["Order"],
                Tags.from(["entity=E1", "region=EU"]),
                SequencePosition.initial()
            )
            await expect(
                store.append({ events: event("Order", Tags.fromObj({ entity: "E1" })), condition })
            ).rejects.toThrow(AppendConditionError)
        })
    })

    // ─── CONDITION VALIDATION ───────────────────────────────────────

    describe("condition validation", () => {
        test("rejects Query.all() conditions", async () => {
            const condition = { failIfEventsMatch: Query.all(), after: SequencePosition.initial() }
            await expect(
                store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            ).rejects.toThrow("Query.all() is not supported")
        })

        test("rejects conditions missing types", async () => {
            const condition = {
                failIfEventsMatch: Query.fromItems([{ tags: Tags.fromObj({ e: "1" }) }]),
                after: SequencePosition.initial()
            }
            await expect(
                store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            ).rejects.toThrow("at least one type and one tag")
        })

        test("rejects conditions missing tags", async () => {
            const condition = {
                failIfEventsMatch: Query.fromItems([{ types: ["TestEvent"] }]),
                after: SequencePosition.initial()
            }
            await expect(
                store.append({ events: event("TestEvent", Tags.fromObj({ e: "1" })), condition })
            ).rejects.toThrow("at least one type and one tag")
        })
    })

    // ─── HYBRID FUNCTION/COPY THRESHOLD ─────────────────────────────

    describe("hybrid function/COPY threshold", () => {
        test("small append (≤ threshold) works via function path", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 5 })
            await s.ensureInstalled()
            const pos = await s.append({
                events: [1, 2, 3].map(i => event("A", Tags.fromObj({ e: `${i}` })))
            })
            expect(pos.toString()).toBe("3")
        })

        test("large append (> threshold) works via COPY path", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 2 })
            await s.ensureInstalled()
            const pos = await s.append({
                events: [1, 2, 3, 4, 5].map(i => event("A", Tags.fromObj({ e: `${i}` })))
            })
            expect(pos.toString()).toBe("5")
        })

        test("COPY path round-trips complex data correctly", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 1 })
            await s.ensureInstalled()
            const data = { nested: { tab: "a\tb", newline: "c\nd", backslash: "e\\f" } }
            const metadata = { quote: 'say "hello"' }
            await s.append({
                events: [
                    event("Test", Tags.from(["entity=E1"]), data, metadata),
                    event("Test", Tags.from(["entity=E2"]), { simple: true })
                ]
            })
            const events = await streamAllEventsToArray(s.read(Query.all()))
            expect(events[0].event.data).toEqual(data)
            expect(events[0].event.metadata).toEqual(metadata)
            expect(events[1].event.data).toEqual({ simple: true })
        })

        test("COPY condition violation throws AppendConditionError", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 2 })
            await s.ensureInstalled()
            await s.append({ events: event("Bulk", Tags.fromObj({ batch: "B1", entity: "E0" })) })
            const condition = scopedCondition(["Bulk"], Tags.fromObj({ batch: "B1" }), SequencePosition.initial())
            await expect(
                s.append({
                    events: [1, 2, 3].map(i => event("Bulk", Tags.fromObj({ batch: "B1", entity: `E${i}` }))),
                    condition
                })
            ).rejects.toThrow(AppendConditionError)
        })

        test("failed COPY append leaves store unchanged", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 2 })
            await s.ensureInstalled()
            await s.append({ events: event("Seed", Tags.fromObj({ batch: "B1", entity: "E0" })) })
            const condition = scopedCondition(["Seed"], Tags.fromObj({ batch: "B1" }), SequencePosition.initial())
            await expect(
                s.append({
                    events: [1, 2, 3, 4, 5].map(i => event("Seed", Tags.fromObj({ batch: "B1", entity: `E${i}` }))),
                    condition
                })
            ).rejects.toThrow(AppendConditionError)
            const events = await streamAllEventsToArray(s.read(Query.all()))
            expect(events.length).toBe(1)
        })
    })

    // ─── CONCURRENCY ────────────────────────────────────────────────

    describe("concurrency", () => {
        test("parallel unconditional appends all succeed", async () => {
            const promises = Array.from({ length: 50 }, (_, i) =>
                store.append({ events: event("TestEvent", Tags.fromObj({ e: `${i}` })) })
            )
            const results = await Promise.allSettled(promises)
            expect(results.filter(r => r.status === "fulfilled").length).toBe(50)
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(50)
        })

        test("parallel conditional appends to different scopes all succeed", async () => {
            const promises = Array.from({ length: 20 }, (_, i) => {
                const condition = scopedCondition(
                    ["TestEvent"],
                    Tags.fromObj({ entity: `E${i}` }),
                    SequencePosition.initial()
                )
                return store.append({ events: event("TestEvent", Tags.fromObj({ entity: `E${i}` })), condition })
            })
            const results = await Promise.allSettled(promises)
            expect(results.filter(r => r.status === "fulfilled").length).toBe(20)
        })

        test("parallel conditional appends to same scope: exactly one wins", async () => {
            const condition = scopedCondition(
                ["TestEvent"],
                Tags.fromObj({ entity: "CONTESTED" }),
                SequencePosition.initial()
            )
            const promises = Array.from({ length: 10 }, () =>
                store.append({ events: event("TestEvent", Tags.fromObj({ entity: "CONTESTED" })), condition })
            )
            const results = await Promise.allSettled(promises)
            expect(results.filter(r => r.status === "fulfilled").length).toBe(1)
            expect(results.filter(r => r.status === "rejected").length).toBe(9)
            const events = await streamAllEventsToArray(
                store.read(Query.fromItems([{ types: ["TestEvent"], tags: Tags.fromObj({ entity: "CONTESTED" }) }]))
            )
            expect(events.length).toBe(1)
        })

        test("disjoint scopes do not block each other", async () => {
            const [resA, resB] = await Promise.allSettled([
                store.append({
                    events: event("Thing", Tags.fromObj({ thing: "ALPHA" })),
                    condition: scopedCondition(["Thing"], Tags.fromObj({ thing: "ALPHA" }), SequencePosition.initial())
                }),
                store.append({
                    events: event("Thing", Tags.fromObj({ thing: "BETA" })),
                    condition: scopedCondition(["Thing"], Tags.fromObj({ thing: "BETA" }), SequencePosition.initial())
                })
            ])
            expect(resA.status).toBe("fulfilled")
            expect(resB.status).toBe("fulfilled")
        })
    })

    // ─── READ ───────────────────────────────────────────────────────

    describe("read", () => {
        test("reads all events in order", async () => {
            await store.append({ events: event("A", Tags.fromObj({ e: "1" })) })
            await store.append({ events: event("B", Tags.fromObj({ e: "2" })) })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
            expect(events[0].event.type).toBe("A")
            expect(events[1].event.type).toBe("B")
        })

        test("reads with limit", async () => {
            for (let i = 0; i < 5; i++) await store.append({ events: event("A", Tags.fromObj({ e: `${i}` })) })
            const events = await streamAllEventsToArray(store.read(Query.all(), { limit: 3 }))
            expect(events.length).toBe(3)
        })

        test("reads backwards", async () => {
            for (let i = 0; i < 3; i++) await store.append({ events: event(`E${i}`, Tags.fromObj({ e: `${i}` })) })
            const events = await streamAllEventsToArray(store.read(Query.all(), { backwards: true }))
            expect(events[0].event.type).toBe("E2")
            expect(events[2].event.type).toBe("E0")
        })

        test("reads filtered by query", async () => {
            await store.append({ events: event("A", Tags.fromObj({ e: "1" })) })
            await store.append({ events: event("B", Tags.fromObj({ e: "2" })) })
            const events = await streamAllEventsToArray(
                store.read(Query.fromItems([{ types: ["A"], tags: Tags.fromObj({ e: "1" }) }]))
            )
            expect(events.length).toBe(1)
            expect(events[0].event.type).toBe("A")
        })

        test("reads after a specific position (exclusive)", async () => {
            for (let i = 0; i < 5; i++) await store.append({ events: event("A", Tags.fromObj({ e: `${i}` })) })
            const events = await streamAllEventsToArray(
                store.read(Query.all(), { after: SequencePosition.fromString("2") })
            )
            expect(events.length).toBe(3)
            expect(events[0].position.toString()).toBe("3")
        })

        test("reads backwards before a specific position (exclusive)", async () => {
            for (let i = 0; i < 5; i++) await store.append({ events: event("A", Tags.fromObj({ e: `${i}` })) })
            const events = await streamAllEventsToArray(
                store.read(Query.all(), { after: SequencePosition.fromString("3"), backwards: true })
            )
            expect(events.length).toBe(2)
            expect(events[0].position.toString()).toBe("2")
            expect(events[1].position.toString()).toBe("1")
        })

        test("two consecutive reads do not interfere", async () => {
            await store.append({ events: event("A", Tags.fromObj({ e: "1" })) })
            const first = await streamAllEventsToArray(store.read(Query.all()))
            const second = await streamAllEventsToArray(store.read(Query.all()))
            expect(first.length).toBe(1)
            expect(second.length).toBe(1)
        })
    })

    // ─── REALISTIC DCB SCENARIOS ────────────────────────────────────

    describe("realistic DCB scenarios", () => {
        test("course enrollment: create course then subscribe student", async () => {
            await store.append({
                events: event("CourseCreated", Tags.from(["courseId=CS101"])),
                condition: scopedCondition(["CourseCreated"], Tags.from(["courseId=CS101"]), SequencePosition.initial())
            })
            await store.append({
                events: event("StudentSubscribed", Tags.from(["courseId=CS101", "studentId=S1"])),
                condition: scopedCondition(
                    ["StudentSubscribed"],
                    Tags.from(["courseId=CS101", "studentId=S1"]),
                    SequencePosition.fromString("1")
                )
            })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
        })

        test("duplicate creation prevented", async () => {
            const condition = scopedCondition(
                ["CourseCreated"],
                Tags.from(["courseId=CS101"]),
                SequencePosition.initial()
            )
            await store.append({ events: event("CourseCreated", Tags.from(["courseId=CS101"])), condition })
            await expect(
                store.append({ events: event("CourseCreated", Tags.from(["courseId=CS101"])), condition })
            ).rejects.toThrow(AppendConditionError)
        })

        test("bulk import with condition", async () => {
            const condition = scopedCondition(["BulkEvent"], Tags.from(["batch=B1"]), SequencePosition.initial())
            const events = Array.from({ length: 100 }, (_, i) =>
                event("BulkEvent", Tags.from(["batch=B1", `entity=E${i}`]), { i })
            )
            const pos = await store.append({ events, condition })
            expect(pos.toString()).toBe("100")
            const all = await streamAllEventsToArray(store.read(Query.all()))
            expect(all.length).toBe(100)
        })
    })

    // ─── MULTI-COMMAND APPEND (via AppendCommand[]) ───────────────

    describe("append with multiple commands", () => {
        test("executes multiple commands in one transaction", async () => {
            const pos = await store.append([
                { events: [event("A", Tags.fromObj({ e: "1" }))] },
                { events: [event("B", Tags.fromObj({ e: "2" }))] }
            ])
            expect(pos.toString()).toBe("2")
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(2)
        })

        test("conditions check against pre-existing data only (not batch's own events)", async () => {
            const pos = await store.append([
                {
                    events: [event("MeterCreated", Tags.from(["meterId=M1"]))],
                    condition: scopedCondition(["MeterCreated"], Tags.from(["meterId=M1"]), SequencePosition.initial())
                },
                {
                    events: [event("MeterCreated", Tags.from(["meterId=M2"]))],
                    condition: scopedCondition(["MeterCreated"], Tags.from(["meterId=M2"]), SequencePosition.initial())
                }
            ])
            expect(pos.toString()).toBe("2")
        })

        test("detects pre-existing violation and rolls back entire batch", async () => {
            await store.append({ events: event("MeterCreated", Tags.from(["meterId=M1"])) })

            await expect(
                store.append([
                    { events: [event("MeterCreated", Tags.from(["meterId=M2"]))] },
                    {
                        events: [event("MeterCreated", Tags.from(["meterId=M1"]))],
                        condition: scopedCondition(
                            ["MeterCreated"],
                            Tags.from(["meterId=M1"]),
                            SequencePosition.initial()
                        )
                    }
                ])
            ).rejects.toThrow(AppendConditionError)

            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(1)
        })

        test("rejects empty command array", async () => {
            await expect(store.append([])).rejects.toThrow("Cannot append zero events")
        })

        test("meter import: create + field sets per meter", async () => {
            const meterCount = 100
            const fieldsPerMeter = 10

            const commands: { events: DcbEvent[]; condition?: AppendCondition }[] = []
            for (let m = 0; m < meterCount; m++) {
                const meterId = `M${String(m).padStart(4, "0")}`
                const meterTag = Tags.from([`meterId=${meterId}`])

                commands.push({
                    events: [event("MeterCreated", meterTag, { meterId })],
                    condition: scopedCondition(["MeterCreated"], meterTag, SequencePosition.initial())
                })

                for (let f = 0; f < fieldsPerMeter; f++) {
                    commands.push({
                        events: [event(`Field${f}Set`, meterTag, { field: f, value: `val-${f}` })]
                    })
                }
            }

            const expectedEvents = meterCount * (1 + fieldsPerMeter)
            const pos = await store.append(commands)
            expect(pos.toString()).toBe(String(expectedEvents))

            const allEvents = await streamAllEventsToArray(store.read(Query.all()))
            expect(allEvents.length).toBe(expectedEvents)
        })
    })

    // ─── COPY ESCAPING EDGE CASES ──────────────────────────────────

    describe("COPY escaping", () => {
        test("payload with tabs, newlines, and backslashes round-trips correctly", async () => {
            const data = { value: "line1\tcolumn2\nline2\\end\r\nwindows" }
            await store.append({ events: event("Escaped", Tags.fromObj({ e: "1" }), data) })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events[0].event.data).toEqual(data)
        })

        test("payload with quotes round-trips correctly", async () => {
            const data = { value: "he said \"hello\" and 'goodbye'" }
            await store.append({ events: event("Escaped", Tags.fromObj({ e: "1" }), data) })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events[0].event.data).toEqual(data)
        })

        test("tags with special characters round-trip via COPY path", async () => {
            const s = new PostgresEventStore({ pool, lockStrategy: createStrategy(), copyThreshold: 1 })
            await s.ensureInstalled()
            const tags = Tags.from(["key=val-with-dash", "other=has_underscore"])
            await s.append({ events: [event("A", tags), event("B", tags)] })
            const events = await streamAllEventsToArray(s.read(Query.all()))
            expect(events[0].event.tags.equals(tags)).toBe(true)
        })

        test("deeply nested JSON payload round-trips correctly", async () => {
            const data = { a: { b: { c: [1, 2, { d: "deep" }] } } }
            await store.append({ events: event("Deep", Tags.fromObj({ e: "1" }), data) })
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events[0].event.data).toEqual(data)
        })
    })

    // ─── IDEMPOTENCY ────────────────────────────────────────────────

    describe("idempotency", () => {
        test("ensureInstalled can be called twice without data loss", async () => {
            await store.append({ events: event("A", Tags.fromObj({ e: "1" })) })
            await store.ensureInstalled()
            const events = await streamAllEventsToArray(store.read(Query.all()))
            expect(events.length).toBe(1)
        })
    })
})

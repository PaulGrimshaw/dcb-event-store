import { Pool } from "pg"
import { DcbEvent, Query, SequencePosition, SequencedEvent, Tags } from "@dcb-es/event-store"
import { PostgresEventStore } from "./PostgresEventStore"
import { advisoryLocks, rowLocks } from "./lockStrategy"
import { LockStrategy } from "./lockStrategy"
import { getTestPgDatabasePool } from "@test/testPgDbPool"

const event = (type: string, tags: Tags = Tags.fromObj({ e: "1" })): DcbEvent => ({
    type,
    tags,
    data: {},
    metadata: {}
})

async function collectEvents(
    gen: AsyncGenerator<SequencedEvent>,
    count: number,
    timeoutMs = 5000
): Promise<SequencedEvent[]> {
    const events: SequencedEvent[] = []
    const deadline = Date.now() + timeoutMs
    for await (const ev of gen) {
        events.push(ev)
        if (events.length >= count) break
        if (Date.now() > deadline) throw new Error("Timed out waiting for events")
    }
    return events
}

const strategies: [string, () => LockStrategy][] = [
    ["advisory", () => advisoryLocks()],
    ["row-locks", () => rowLocks()]
]

describe.each(strategies)("PostgresEventStore.subscribe [%s]", (_name, createStrategy) => {
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

    test("delivers events appended after subscribe starts", async () => {
        const sub = store.subscribe(Query.all(), { pollIntervalMs: 20 })

        setTimeout(async () => {
            await store.append({ events: event("A") })
            await store.append({ events: event("B") })
        }, 20)

        const events = await collectEvents(sub, 2)
        expect(events[0].event.type).toBe("A")
        expect(events[1].event.type).toBe("B")
    })

    test("delivers historical + live events seamlessly", async () => {
        await store.append({ events: event("Historical") })

        const sub = store.subscribe(Query.all(), { pollIntervalMs: 20 })

        setTimeout(async () => {
            await store.append({ events: event("Live") })
        }, 50)

        const events = await collectEvents(sub, 2)
        expect(events[0].event.type).toBe("Historical")
        expect(events[1].event.type).toBe("Live")
    })

    test("after option skips earlier events", async () => {
        await store.append({ events: event("A") })
        await store.append({ events: event("B") })

        const sub = store.subscribe(Query.all(), {
            after: SequencePosition.fromString("1"),
            pollIntervalMs: 20
        })

        setTimeout(async () => {
            await store.append({ events: event("C") })
        }, 20)

        const events = await collectEvents(sub, 2)
        expect(events[0].event.type).toBe("B")
        expect(events[1].event.type).toBe("C")
    })

    test("AbortSignal terminates the generator and releases connections", async () => {
        const controller = new AbortController()
        const sub = store.subscribe(Query.all(), { signal: controller.signal, pollIntervalMs: 20 })

        await store.append({ events: event("A") })

        setTimeout(() => controller.abort(), 100)

        const events: SequencedEvent[] = []
        for await (const ev of sub) {
            events.push(ev)
        }
        expect(events.length).toBe(1)

        // Verify pool not exhausted — can still query
        const result = await pool.query("SELECT 1 as ok")
        expect(result.rows[0].ok).toBe(1)
    })

    test("consumer break releases LISTEN connection", async () => {
        await store.append({ events: event("A") })
        await store.append({ events: event("B") })

        const sub = store.subscribe(Query.all(), { pollIntervalMs: 20 })
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ev of sub) {
            break // exit after first event
        }

        // Verify pool not exhausted
        const result = await pool.query("SELECT 1 as ok")
        expect(result.rows[0].ok).toBe(1)
    })

    test("multiple concurrent subscriptions", async () => {
        const controller = new AbortController()
        const sub1 = store.subscribe(Query.all(), { signal: controller.signal, pollIntervalMs: 20 })
        const sub2 = store.subscribe(Query.all(), { signal: controller.signal, pollIntervalMs: 20 })

        setTimeout(async () => {
            await store.append({ events: event("Shared") })
            setTimeout(() => controller.abort(), 50)
        }, 20)

        const [events1, events2] = await Promise.all([collectEvents(sub1, 1), collectEvents(sub2, 1)])

        expect(events1[0].event.type).toBe("Shared")
        expect(events2[0].event.type).toBe("Shared")
    })

    test("filtered subscription delivers only matching events", async () => {
        const sub = store.subscribe(Query.fromItems([{ types: ["Target"], tags: Tags.fromObj({ e: "1" }) }]), {
            pollIntervalMs: 20
        })

        setTimeout(async () => {
            await store.append({ events: event("Noise", Tags.fromObj({ e: "1" })) })
            await store.append({ events: event("Target", Tags.fromObj({ e: "1" })) })
        }, 20)

        const events = await collectEvents(sub, 1)
        expect(events[0].event.type).toBe("Target")
    })

    test("NOTIFY wakes subscriber faster than poll interval", async () => {
        const sub = store.subscribe(Query.all(), { pollIntervalMs: 5000 })

        const start = Date.now()
        setTimeout(async () => {
            await store.append({ events: event("Fast") })
        }, 20)

        const events = await collectEvents(sub, 1, 2000)
        const elapsed = Date.now() - start

        expect(events[0].event.type).toBe("Fast")
        // Should wake via NOTIFY well before the 5s poll interval
        expect(elapsed).toBeLessThan(1000)
    })

    test("notification channel isolation with custom table prefix", async () => {
        const customStore = new PostgresEventStore({ pool, lockStrategy: createStrategy(), tablePrefix: "custom" })
        await customStore.ensureInstalled()

        const controller = new AbortController()
        const sub = customStore.subscribe(Query.all(), { signal: controller.signal, pollIntervalMs: 20 })

        setTimeout(async () => {
            // Append to default store — should NOT wake custom subscriber
            await store.append({ events: event("Wrong") })
            // Append to custom store — should wake subscriber
            await customStore.append({ events: event("Right") })
        }, 20)

        const events = await collectEvents(sub, 1)
        expect(events[0].event.type).toBe("Right")

        controller.abort()
        await pool.query("DROP TABLE IF EXISTS custom_events CASCADE")
        await pool.query("DROP FUNCTION IF EXISTS custom_events_append CASCADE")
    })

    test("position ordering is sequential", async () => {
        const sub = store.subscribe(Query.all(), { pollIntervalMs: 20 })

        setTimeout(async () => {
            for (let i = 0; i < 5; i++) {
                await store.append({ events: event(`E${i}`) })
            }
        }, 20)

        const events = await collectEvents(sub, 5)
        expect(events.map(e => e.position.toString())).toEqual(["1", "2", "3", "4", "5"])
    })
})

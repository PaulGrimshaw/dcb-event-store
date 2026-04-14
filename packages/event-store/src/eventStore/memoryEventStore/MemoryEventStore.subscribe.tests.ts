import { MemoryEventStore } from "./MemoryEventStore"
import { DcbEvent, SequencedEvent } from "../EventStore"
import { SequencePosition } from "../SequencePosition"
import { Tags } from "../Tags"
import { Query } from "../Query"

const event = (type: string, tags: Tags = Tags.fromObj({ e: "1" })): DcbEvent => ({
    type,
    tags,
    data: {},
    metadata: {}
})

async function collectEvents(
    gen: AsyncGenerator<SequencedEvent>,
    count: number,
    timeoutMs = 2000
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

describe("MemoryEventStore.subscribe", () => {
    let store: MemoryEventStore

    beforeEach(() => {
        store = new MemoryEventStore()
    })

    test("delivers historical events first", async () => {
        await store.append({ events: event("A") })
        await store.append({ events: event("B") })

        const events = await collectEvents(store.subscribe(Query.all()), 2)
        expect(events[0].event.type).toBe("A")
        expect(events[1].event.type).toBe("B")
    })

    test("delivers new events appended after subscribe starts", async () => {
        const sub = store.subscribe(Query.all())

        // Append after subscribe is running
        setTimeout(async () => {
            await store.append({ events: event("Live") })
        }, 10)

        const events = await collectEvents(sub, 1)
        expect(events[0].event.type).toBe("Live")
    })

    test("after option skips earlier events", async () => {
        await store.append({ events: event("A") })
        await store.append({ events: event("B") })

        const sub = store.subscribe(Query.all(), { after: SequencePosition.fromString("1") })

        setTimeout(async () => {
            await store.append({ events: event("C") })
        }, 10)

        const events = await collectEvents(sub, 2)
        expect(events[0].event.type).toBe("B")
        expect(events[1].event.type).toBe("C")
    })

    test("AbortSignal terminates the generator", async () => {
        const controller = new AbortController()
        const sub = store.subscribe(Query.all(), { signal: controller.signal })

        await store.append({ events: event("A") })

        setTimeout(() => controller.abort(), 50)

        const events: SequencedEvent[] = []
        for await (const ev of sub) {
            events.push(ev)
        }
        expect(events.length).toBe(1)
        expect(events[0].event.type).toBe("A")
    })

    test("empty store blocks until events arrive", async () => {
        const sub = store.subscribe(Query.all())

        setTimeout(async () => {
            await store.append({ events: event("First") })
        }, 50)

        const events = await collectEvents(sub, 1)
        expect(events[0].event.type).toBe("First")
    })

    test("delivers filtered events only", async () => {
        await store.append({ events: event("A", Tags.fromObj({ kind: "x" })) })
        await store.append({ events: event("B", Tags.fromObj({ kind: "y" })) })

        const sub = store.subscribe(Query.fromItems([{ types: ["A"], tags: Tags.fromObj({ kind: "x" }) }]))

        setTimeout(async () => {
            await store.append({ events: event("A", Tags.fromObj({ kind: "x" })) })
        }, 10)

        const events = await collectEvents(sub, 2)
        expect(events.every(e => e.event.type === "A")).toBe(true)
    })

    test("historical + live events are seamless", async () => {
        await store.append({ events: event("Historical") })

        const sub = store.subscribe(Query.all())

        setTimeout(async () => {
            await store.append({ events: event("Live") })
        }, 10)

        const events = await collectEvents(sub, 2)
        expect(events[0].event.type).toBe("Historical")
        expect(events[1].event.type).toBe("Live")
    })
})

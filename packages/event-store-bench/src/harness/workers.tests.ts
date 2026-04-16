import { describe, it, expect } from "vitest"
import { MemoryEventStore, Tags, Query, streamAllEventsToArray } from "@dcb-es/event-store"
import { conditionalRetryWorker, entityOracleWorker, spawnWorkers } from "./workers"

describe("conditionalRetryWorker", () => {
    it("writes events with conditions on isolated scope", async () => {
        const store = new MemoryEventStore()
        const query = Query.fromItems([{ types: ["TestEvent"], tags: Tags.fromObj({ scope: "W0" }) }])
        const events = () => [{
            type: "TestEvent",
            tags: Tags.fromObj({ scope: "W0" }),
            data: {},
            metadata: {},
        }]

        const result = await conditionalRetryWorker(store, 0, Date.now() + 1000, query, events)

        expect(result.operations).toBeGreaterThan(0)
        expect(result.errors).toBe(0)
    })
})

describe("entityOracleWorker", () => {
    it("creates entities without duplicates", async () => {
        const store = new MemoryEventStore()
        const readQuery = (idx: number) => Query.fromItems([{
            types: ["Created"],
            tags: Tags.fromObj({ entity: `E${idx}` }),
        }])
        const eventFactory = (_wid: number, idx: number) => [{
            type: "Created",
            tags: Tags.fromObj({ entity: `E${idx}` }),
            data: {},
            metadata: {},
        }]

        const result = await entityOracleWorker(store, 0, Date.now() + 1000, {
            entityCount: 5,
            readQuery,
            eventFactory,
        })

        expect(result.operations).toBeGreaterThan(0)
        expect(result.errors).toBe(0)

        for (let i = 1; i <= 5; i++) {
            const events = await streamAllEventsToArray(store.read(readQuery(i)))
            expect(events.length).toBeLessThanOrEqual(1)
        }
    })
})

describe("spawnWorkers", () => {
    it("creates the requested number of workers", async () => {
        const results = await spawnWorkers(3, async (id) => ({
            workerId: id, type: "write" as const, operations: 1, events: 1,
            errors: 0, conflicts: 0, latencies: [1],
        }))
        expect(results).toHaveLength(3)
        expect(results.map(r => r.workerId)).toEqual([0, 1, 2])
    })
})

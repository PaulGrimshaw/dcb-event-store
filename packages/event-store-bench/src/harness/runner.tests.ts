import { describe, it, expect } from "vitest"
import { MemoryEventStore, Tags, Query, AppendCommand } from "@dcb-es/event-store"
import { runScenario } from "./runner"
import { ScenarioConfig } from "./types"

const simpleEventFactory = (workerId: number, _iteration: number) => [{
    type: "TestEvent",
    tags: Tags.fromObj({ test: `W${workerId}` }),
    data: {},
    metadata: {},
}]

describe("runScenario", () => {
    it("single write worker completes", async () => {
        const config: ScenarioConfig = {
            name: "single-writer",
            description: "one writer",
            durationMs: 1000,
            workers: [{ type: "write", count: 1, eventFactory: simpleEventFactory }],
        }

        const result = await runScenario(config, new MemoryEventStore())

        expect(result.scenario).toBe("single-writer")
        expect(result.workers).toHaveLength(1)
        expect(result.workers[0].operations).toBeGreaterThan(0)
        expect(result.workers[0].errors).toBe(0)
    })

    it("warmup excluded from measurements", async () => {
        const config: ScenarioConfig = {
            name: "warmup-test",
            description: "warmup then measure",
            warmupMs: 500,
            durationMs: 1000,
            workers: [{ type: "write", count: 1, eventFactory: simpleEventFactory }],
        }

        const result = await runScenario(config, new MemoryEventStore())

        expect(result.durationMs).toBeGreaterThanOrEqual(900)
        expect(result.workers[0].operations).toBeGreaterThan(0)
    })

    it("multiple workers run concurrently", async () => {
        const config: ScenarioConfig = {
            name: "multi-writer",
            description: "five writers",
            durationMs: 1000,
            workers: [{ type: "write", count: 5, eventFactory: simpleEventFactory }],
        }

        const result = await runScenario(config, new MemoryEventStore())

        expect(result.workers).toHaveLength(5)
        for (const w of result.workers) {
            expect(w.operations).toBeGreaterThan(0)
        }
    })

    it("read worker works alongside write worker", async () => {
        const config: ScenarioConfig = {
            name: "read-write",
            description: "writer and reader",
            durationMs: 1000,
            workers: [
                { type: "write", count: 1, eventFactory: simpleEventFactory },
                {
                    type: "read",
                    count: 1,
                    queryFactory: () => Query.fromItems([
                        { types: ["TestEvent"], tags: Tags.fromObj({ test: "W0" }) },
                    ]),
                },
            ],
        }

        const result = await runScenario(config, new MemoryEventStore())

        expect(result.workers).toHaveLength(2)
        const readWorker = result.workers.find(w => w.type === "read")!
        expect(readWorker.operations).toBeGreaterThan(0)
        expect(readWorker.errors).toBe(0)
    })

    it("counts errors from failing append", async () => {
        const store = new MemoryEventStore()
        let appendCount = 0
        const originalAppend = store.append.bind(store)
        store.append = async (command: AppendCommand | AppendCommand[]) => {
            appendCount++
            if (appendCount % 3 === 0) throw new Error("boom")
            return originalAppend(command)
        }

        const config: ScenarioConfig = {
            name: "error-test",
            description: "errors counted",
            durationMs: 1000,
            workers: [{ type: "write", count: 1, eventFactory: simpleEventFactory }],
        }

        const result = await runScenario(config, store)

        expect(result.workers[0].errors).toBeGreaterThan(0)
        expect(result.workers[0].operations).toBeGreaterThan(0)
    })

    it("import worker completes batches", async () => {
        const config: ScenarioConfig = {
            name: "import-test",
            description: "batch import",
            durationMs: 60_000,
            workers: [{
                type: "import",
                count: 1,
                totalEvents: 100,
                batchSize: 25,
                batchFactory: (workerId, batchIndex, batchSize) =>
                    Array.from({ length: batchSize }, (_, i) => ({
                        type: "ImportEvent",
                        tags: Tags.fromObj({ batch: `${batchIndex}`, worker: `W${workerId}` }),
                        data: { index: i },
                        metadata: {},
                    })),
            }],
        }

        const result = await runScenario(config, new MemoryEventStore())

        const importWorker = result.workers[0]
        expect(importWorker.type).toBe("import")
        expect(importWorker.operations).toBe(4)
        expect(importWorker.events).toBe(100)
        expect(importWorker.errors).toBe(0)
    })
})

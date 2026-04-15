import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t4-mixed-workload"

describe("T4 — Mixed Workload", () => {
    it("completes import + small writes without errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            {
                importTotalEvents: 100,
                importBatchSize: 25,
                importConcurrency: 2,
                smallWriterCount: 3,
                smallWriteRatePerSec: 50,
                durationMs: 3_000,
            },
        )

        expect(result.id).toBe("T4")
        expect(result.scenarios).toHaveLength(3)
        expect(result.scenarios.map(s => s.scenario)).toEqual([
            "import",
            "small-writers-during",
            "small-writers-after",
        ])

        const importScenario = result.scenarios[0]
        expect(importScenario.aggregate.totalErrors).toBe(0)
        expect(importScenario.workers[0].events).toBe(100)

        const duringScenario = result.scenarios[1]
        const afterScenario = result.scenarios[2]
        expect(duringScenario.aggregate.totalErrors).toBe(0)
        expect(afterScenario.aggregate.totalErrors).toBe(0)

        expect(result.verification).toBeDefined()
        expect(result.verification!.checks.find(c => c.name === "import-completed")!.pass).toBe(true)
        expect(result.verification!.checks.find(c => c.name === "small-writer-zero-errors")!.pass).toBe(true)

        expect(result.pass).toBe(true)
    }, 15_000)
})

import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t12-batch-throughput"

describe("T12 — Batch Throughput Sweep", () => {
    it("unconditional: all batch tiers complete with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { workerCount: 2, batchSizes: [1, 10], durationPerTierMs: 1_000, conditional: false },
        )

        expect(result.id).toBe("T12")
        expect(result.scenarios).toHaveLength(2)
        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })

    it("conditional: all batch tiers complete with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { workerCount: 2, batchSizes: [1, 10], durationPerTierMs: 1_000, conditional: true },
        )

        expect(result.scenarios).toHaveLength(2)
        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })
})

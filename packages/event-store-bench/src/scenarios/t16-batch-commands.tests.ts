import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t16-batch-commands"

describe("T16 — Batch Commands Throughput", () => {
    it("completes all tiers with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { workerCount: 2, commandsPerAppend: [1, 5], durationPerTierMs: 1_000 },
        )

        expect(result.id).toBe("T16")
        expect(result.scenarios).toHaveLength(2)
        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })
})

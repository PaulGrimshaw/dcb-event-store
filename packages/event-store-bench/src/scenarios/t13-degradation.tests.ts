import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t13-degradation"

describe("T13 — Degradation Under Load", () => {
    it("completes multiple phases with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { workerCount: 2, batchSize: 10, phases: 2, durationPerPhaseMs: 1_000, conditional: true },
        )

        expect(result.id).toBe("T13")
        expect(result.scenarios).toHaveLength(2)
        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
        expect(result.pass).toBe(true)
    })
})

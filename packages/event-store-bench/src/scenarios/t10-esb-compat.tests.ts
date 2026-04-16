import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t10-esb-compat"
import { TierConfig } from "./helpers"

const FAST_TIERS: TierConfig[] = [
    { name: "1w", workers: 1, durationMs: 1_000 },
    { name: "2w", workers: 2, durationMs: 1_000 },
]

describe("T10 — ESB-compat Benchmark", () => {
    it("write phase completes with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { writeOnly: true, writeTiers: FAST_TIERS },
        )

        expect(result.id).toBe("T10")
        expect(result.scenarios.length).toBe(2)

        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })

    it("read phase completes with zero errors", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { readOnly: true, readTiers: FAST_TIERS },
        )

        expect(result.id).toBe("T10")

        for (const s of result.scenarios) {
            expect(s.aggregate.totalErrors).toBe(0)
            expect(s.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })
})

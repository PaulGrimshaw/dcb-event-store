import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t1-throughput"
import { TierConfig } from "./helpers"

const FAST_TIERS: TierConfig[] = [
    { name: "1-writer", workers: 1, durationMs: 1_000 },
    { name: "5-writers", workers: 5, durationMs: 1_000 },
]

describe("T1 — Throughput Baseline", () => {
    it("completes with correct structure and no errors", async () => {
        const result = await run(() => new MemoryEventStore(), FAST_TIERS)

        expect(result.id).toBe("T1")
        expect(result.scenarios).toHaveLength(2)

        for (const scenario of result.scenarios) {
            expect(scenario.aggregate.totalErrors).toBe(0)
            expect(scenario.workers.length).toBeGreaterThan(0)
            expect(scenario.aggregate.totalOperations).toBeGreaterThan(0)
        }
    })

    it("multi-writer tier has non-zero throughput (scaling requires real DB)", async () => {
        const result = await run(() => new MemoryEventStore(), FAST_TIERS)

        // MemoryEventStore is single-threaded — can't demonstrate scaling.
        // Just verify both tiers produced meaningful throughput.
        for (const s of result.scenarios) {
            expect(s.aggregate.eventsPerSec).toBeGreaterThan(0)
        }
    })

    it("includes verification checks", async () => {
        const result = await run(() => new MemoryEventStore(), FAST_TIERS)

        expect(result.verification).toBeDefined()
        expect(result.verification!.checks).toEqual(
            expect.arrayContaining([
                expect.objectContaining({ name: "scaling-factor" }),
                expect.objectContaining({ name: "p99-latency-all-tiers" }),
                expect.objectContaining({ name: "zero-errors" }),
            ]),
        )
    })
})

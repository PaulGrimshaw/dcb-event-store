import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t9-overlap-consistency"

describe("T9 — Overlapping Scope Consistency", () => {
    it("guarantees zero duplicates when event tags superset condition tags", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { entityCount: 10, workerCount: 3, durationMs: 2_000 },
        )

        expect(result.pass).toBe(true)

        const dupCheck = result.verification!.checks.find(c => c.name === "zero-duplicates")
        expect(dupCheck?.pass).toBe(true)
        expect(dupCheck?.actual).toBe("0")

        expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
    })
})

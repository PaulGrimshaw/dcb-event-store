import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t5-raw-throughput"

describe("T5 — Raw Bulk Throughput", () => {
    it("inserts events and verifies count", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { eventCount: 1_000 },
        )

        expect(result.id).toBe("T5")
        expect(result.scenarios).toHaveLength(1)

        const countCheck = result.verification!.checks.find(c => c.name === "event-count")
        expect(countCheck?.pass).toBe(true)

        expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
    })
})

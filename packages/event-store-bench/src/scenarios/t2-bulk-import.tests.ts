import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t2-bulk-import"

describe("T2 — Bulk Import with DCB Conditions", () => {
    it("imports meters with correct event counts and no duplicates", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { meterCount: 50, metersPerChunk: 25 },
        )

        expect(result.id).toBe("T2")
        expect(result.scenarios).toHaveLength(1)

        const countCheck = result.verification!.checks.find(c => c.name === "total-event-count")
        expect(countCheck?.pass).toBe(true)
        expect(countCheck?.expected).toBe(String(50 * 11))

        const dupCheck = result.verification!.checks.find(c => c.name === "no-duplicate-creates")
        expect(dupCheck?.pass).toBe(true)

        expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
    })
})

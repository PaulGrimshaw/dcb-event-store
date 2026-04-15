import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t6-parallel-import"

describe("T6 — Parallel Bulk Import", () => {
    it("completes parallel batches with correct counts", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { batchCount: 3, metersPerBatch: 10 },
        )

        expect(result.id).toBe("T6")
        expect(result.scenarios).toHaveLength(1)

        const batchCheck = result.verification!.checks.find(c => c.name === "all-batches-completed")
        expect(batchCheck?.pass).toBe(true)

        const countCheck = result.verification!.checks.find(c => c.name === "total-event-count")
        expect(countCheck?.pass).toBe(true)
        expect(countCheck?.expected).toBe(String(3 * 10 * 11))

        const dupCheck = result.verification!.checks.find(c => c.name === "no-duplicate-creates")
        expect(dupCheck?.pass).toBe(true)
    })
})

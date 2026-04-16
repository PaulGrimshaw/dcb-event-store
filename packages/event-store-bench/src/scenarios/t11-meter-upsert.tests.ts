import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t11-meter-upsert"

describe("T11 — Meter Upsert", () => {
    it("upserts all meters with correct state transitions", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { totalMeters: 30 },
        )

        expect(result.id).toBe("T11")
        expect(result.verification).toBeDefined()

        const aliveCheck = result.verification!.checks.find(c => c.name === "all-meters-alive")
        expect(aliveCheck?.pass).toBe(true)

        const dupCheck = result.verification!.checks.find(c => c.name === "no-duplicate-creates")
        expect(dupCheck?.pass).toBe(true)

        expect(result.pass).toBe(true)
    })
})

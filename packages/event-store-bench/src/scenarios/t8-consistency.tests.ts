import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { run } from "./t8-consistency"

describe("T8 — Consistency Oracle", () => {
    it("guarantees zero duplicates with concurrent writers", async () => {
        const result = await run(
            () => new MemoryEventStore(),
            { entityCount: 10, workerCount: 3, durationMs: 2_000 },
        )

        expect(result.pass).toBe(true)
        expect(result.verification).toBeDefined()

        const dupCheck = result.verification!.checks.find(c => c.name === "zero-duplicates")
        expect(dupCheck?.pass).toBe(true)
        expect(dupCheck?.actual).toBe("0")

        const createdCheck = result.verification!.checks.find(c => c.name === "entities-created")
        expect(createdCheck?.pass).toBe(true)

        expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
    })
})

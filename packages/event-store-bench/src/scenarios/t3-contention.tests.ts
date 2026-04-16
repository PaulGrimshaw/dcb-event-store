import { describe, it, expect } from "vitest"
import { MemoryEventStore, streamAllEventsToArray } from "@dcb-es/event-store"
import { run } from "./t3-contention"
import { t3QueryFactory } from "./factories"

describe("T3 — Contention Gauntlet", () => {
    it("verifies correctness under contention with MemoryEventStore", async () => {
        let store: MemoryEventStore | undefined
        const factory = async () => { store = new MemoryEventStore(); return store }
        const result = await run(factory, { workerCount: 3, durationMs: 2_000 })

        expect(result.verification).toBeDefined()

        const countCheck = result.verification!.checks.find(c => c.name === "event-count-matches-appends")
        expect(countCheck).toBeDefined()
        expect(countCheck!.pass).toBe(true)

        for (const scenario of result.scenarios) {
            expect(scenario.aggregate.totalErrors).toBe(0)
        }

        const totalOps = result.scenarios[0].workers.reduce((s, w) => s + w.operations, 0)
        expect(totalOps).toBeGreaterThan(0)

        const events = await streamAllEventsToArray(store!.read(t3QueryFactory(0)))
        expect(events.length).toBe(totalOps)
    })
})

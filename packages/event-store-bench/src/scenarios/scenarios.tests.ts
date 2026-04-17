import { describe, it, expect } from "vitest"
import { MemoryEventStore } from "@dcb-es/event-store"
import { standardScenarios } from "./registry"

const memoryFactory = async (_runId: string) => new MemoryEventStore()

describe("scenarios — MemoryEventStore smoke tests", () => {
    for (const scenario of standardScenarios) {
        it(`${scenario.id} completes with correct structure`, async () => {
            const result = await scenario.run(memoryFactory, scenario.presets.quick)

            expect(result.id).toBe(scenario.id)
            expect(result.scenarios.length).toBeGreaterThan(0)

            for (const s of result.scenarios) {
                expect(s.aggregate.totalErrors).toBe(0)
            }

            for (const checkName of scenario.correctnessChecks) {
                const check = result.verification?.checks.find(c => c.name === checkName)
                expect(check, `expected check "${checkName}" to exist`).toBeDefined()
                expect(check?.pass, `check "${checkName}" should pass`).toBe(true)
            }
        })
    }
})

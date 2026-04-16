import { Pool } from "pg"
import { getTestPgDatabasePool } from "@test/testPgDbPool"
import { createPgFactory } from "./pg-lifecycle"
import { runBenchTest } from "./bench-runner"
import { standardScenarios } from "./scenarios/registry"

const quickScenarios = standardScenarios.filter(s =>
    !s.id.startsWith("batch-sweep") && !s.id.startsWith("degradation") && !s.id.startsWith("batch-commands"),
)

describe("stress — postgres (testcontainers)", () => {
    let pool: Pool

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 50 })
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    for (const scenario of quickScenarios) {
        test(
            `${scenario.id} — ${scenario.description}`,
            async () => runBenchTest(scenario, "quick", createPgFactory(pool))(),
            scenario.timeout ?? 120_000,
        )
    }
})

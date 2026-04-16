import {
    pgAvailable,
    createTestDb,
    dropTestDb,
    createBenchPool,
    createPgFactory,
    createThresholdFactory,
    runBenchTest,
    runThresholdBenchTest,
} from "./test-harness"
import { standardScenarios, thresholdScenarios } from "./scenarios/registry"

const BASE_URI = process.env.PG_CONNECTION_STRING ?? "postgresql://localhost:5432/postgres"

describe("stress — local postgres", async () => {
    const available = await pgAvailable(BASE_URI)

    beforeAll(() => {
        if (!available) {
            console.log(`Skipping local PG tests — cannot connect to ${BASE_URI}`)
        }
    })

    for (const scenario of standardScenarios) {
        test.skipIf(!available)(
            `${scenario.id} — ${scenario.description}`,
            async () => {
                const { connectionString, dbName } = await createTestDb(BASE_URI)
                const pool = createBenchPool(connectionString, 50)
                try {
                    const factory = createPgFactory(pool, scenario.id.startsWith("batch-commands"))
                    await runBenchTest(scenario, "full", factory)()
                } finally {
                    await pool.end()
                    await dropTestDb(BASE_URI, dbName)
                }
            },
            scenario.timeout ?? 120_000,
        )
    }

    for (const scenario of thresholdScenarios) {
        test.skipIf(!available)(
            `${scenario.id} — ${scenario.description}`,
            async () => {
                const { connectionString, dbName } = await createTestDb(BASE_URI)
                const pool = createBenchPool(connectionString, 50)
                try {
                    await runThresholdBenchTest(scenario, createThresholdFactory(pool))()
                } finally {
                    await pool.end()
                    await dropTestDb(BASE_URI, dbName)
                }
            },
            scenario.timeout ?? 600_000,
        )
    }
})

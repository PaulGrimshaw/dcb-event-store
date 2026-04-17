import { Pool } from "pg"
import { PostgresEventStore, advisoryLocks, rowLocks } from "@dcb-es/event-store-postgres"
import { EventStoreFactory } from "./harness/types"
import { standardScenarios } from "./scenarios/registry"
import { formatScenarioResult } from "./reporters/tableReporter"
import { StressTestResult } from "./scenarios/types"

let pool: Pool | undefined

function getPool(): Pool {
    if (!pool) {
        const connectionString = process.env.PG_CONNECTION_STRING
        if (!connectionString) throw new Error("PG_CONNECTION_STRING not set")
        pool = new Pool({
            connectionString,
            max: 50,
            ssl: process.env.PG_SSL === "false" ? false : { rejectUnauthorized: false },
        })
        pool.on("connect", (client) => {
            client.query("SET synchronous_commit = off")
        })
    }
    return pool
}

function getLockStrategy() {
    return process.env.LOCK_MODE === "advisory" ? advisoryLocks() : rowLocks()
}

interface BenchEvent {
    scenarios?: string[]
    preset?: "quick" | "full"
    isolated?: boolean
}

export async function handler(event: BenchEvent = {}) {
    const { scenarios: filter, preset = "full", isolated = true } = event

    const factory: EventStoreFactory = async (runId: string) => {
        const tablePrefix = isolated ? runId.replace(/[^a-z0-9_]/gi, "_").slice(0, 40) : undefined
        const store = new PostgresEventStore({
            pool: getPool(),
            tablePrefix,
            lockStrategy: getLockStrategy(),
        })
        await store.ensureInstalled()
        return store
    }

    const selected = filter
        ? standardScenarios.filter(s => filter.includes(s.id))
        : standardScenarios

    const results: StressTestResult[] = []

    for (const scenario of selected) {
        const config = scenario.presets[preset]
        const result = await scenario.run(factory, config)
        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }
        results.push(result)
    }

    return { results }
}

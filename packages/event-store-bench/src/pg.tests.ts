import { Pool } from "pg"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"
import { EventStore } from "@dcb-es/event-store"
import { getTestPgDatabasePool } from "@test/testPgDbPool"
import { EventStoreFactory } from "./harness/types"
import { formatScenarioResult } from "./reporters/tableReporter"

import { run as runT1 } from "./scenarios/t1-throughput"
import { run as runT2 } from "./scenarios/t2-bulk-import"
import { run as runT3 } from "./scenarios/t3-contention"
import { run as runT5 } from "./scenarios/t5-raw-throughput"
import { run as runT6 } from "./scenarios/t6-parallel-import"
import { run as runT8 } from "./scenarios/t8-consistency"
import { run as runT9 } from "./scenarios/t9-overlap-consistency"
import { run as runT4 } from "./scenarios/t4-mixed-workload"
import { run as runT10 } from "./scenarios/t10-esb-compat"
import { run as runT11 } from "./scenarios/t11-meter-upsert"

function createPgFactory(pool: Pool): EventStoreFactory {
    return async (_runId: string): Promise<EventStore> => {
        const store = new PostgresEventStore({ pool })
        await store.ensureInstalled()
        return store
    }
}

describe("stress — postgres (testcontainers)", () => {
    let pool: Pool

    beforeAll(async () => {
        pool = await getTestPgDatabasePool({ max: 50 })
    })

    afterAll(async () => {
        if (pool) await pool.end()
    })

    test("T1 — throughput baseline (1/5 writers, 5s each)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT1(factory, [
            { name: "1-writer", workers: 1, durationMs: 5_000 },
            { name: "5-writers", workers: 5, durationMs: 5_000 },
        ])

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "zero-errors")?.pass).toBe(true)
    }, 120_000)

    test("T2 — bulk import 100 meters with conditions (1100 events)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT2(factory, { meterCount: 100, metersPerChunk: 50 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "total-event-count")?.pass).toBe(true)
        expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
    }, 120_000)

    test("T3 — contention (5 workers, 5s)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT3(factory, { workerCount: 5, durationMs: 5_000 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
        expect(result.verification?.checks.find(c => c.name === "event-count-matches-appends")?.pass).toBe(true)
    }, 120_000)

    test("T5 — raw bulk throughput (10K events)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT5(factory, { eventCount: 10_000 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "event-count")?.pass).toBe(true)
    }, 120_000)

    test("T8 — consistency oracle (5 workers, 20 entities, 5s)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT8(factory, { entityCount: 20, workerCount: 5, durationMs: 5_000 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "zero-duplicates")?.pass).toBe(true)
    }, 120_000)

    test("T9 — overlapping scope consistency (5 workers, 20 entities, 5s)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT9(factory, { entityCount: 20, workerCount: 5, durationMs: 5_000 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "zero-duplicates")?.pass).toBe(true)
    }, 120_000)

    test("T4 — mixed workload (import + 3 small writers, 5s)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT4(factory, {
            importTotalEvents: 1_000,
            importBatchSize: 250,
            importConcurrency: 2,
            smallWriterCount: 3,
            smallWriteRatePerSec: 10,
            durationMs: 5_000,
        })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "import-completed")?.pass).toBe(true)
        expect(result.verification?.checks.find(c => c.name === "small-writer-zero-errors")?.pass).toBe(true)
    }, 120_000)

    test("T6 — parallel bulk import (3 batches x 50 meters)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT6(factory, { batchCount: 3, metersPerBatch: 50 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "all-batches-completed")?.pass).toBe(true)
        expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
    }, 120_000)

    test("T10 — ESB-compat benchmark (1/2 writers, 1/2 readers, 3s each)", async () => {
        const factory = createPgFactory(pool)
        const tiers = [
            { name: "1", workers: 1, durationMs: 3_000 },
            { name: "2", workers: 2, durationMs: 3_000 },
        ]
        const result = await runT10(factory, { writeTiers: tiers, readTiers: tiers })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.pass).toBe(true)
    }, 120_000)

    test("T11 — meter upsert (90 meters)", async () => {
        const factory = createPgFactory(pool)
        const result = await runT11(factory, { totalMeters: 90 })

        for (const s of result.scenarios) {
            console.log(formatScenarioResult(s))
        }

        expect(result.verification?.checks.find(c => c.name === "all-meters-alive")?.pass).toBe(true)
        expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
    }, 120_000)
})

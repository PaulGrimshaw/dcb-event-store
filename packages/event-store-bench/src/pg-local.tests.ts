import { Pool, Client } from "pg"
import { v4 as uuid } from "uuid"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"
import { EventStore } from "@dcb-es/event-store"
import { EventStoreFactory, ThresholdFactory } from "./harness/types"
import { formatScenarioResult } from "./reporters/tableReporter"

import { run as runT1 } from "./scenarios/t1-throughput"
import { run as runT2 } from "./scenarios/t2-bulk-import"
import { run as runT3 } from "./scenarios/t3-contention"
import { run as runT5 } from "./scenarios/t5-raw-throughput"
import { run as runT8 } from "./scenarios/t8-consistency"
import { run as runT9 } from "./scenarios/t9-overlap-consistency"
import { run as runT4 } from "./scenarios/t4-mixed-workload"
import { run as runT6 } from "./scenarios/t6-parallel-import"
import { run as runT10 } from "./scenarios/t10-esb-compat"
import { run as runT11 } from "./scenarios/t11-meter-upsert"
import { run as runT12 } from "./scenarios/t12-batch-throughput"
import { run as runT13 } from "./scenarios/t13-degradation"
import { run as runT14 } from "./scenarios/t14-copy-threshold"
import { run as runT15 } from "./scenarios/t15-copy-crossover"
import { run as runT16 } from "./scenarios/t16-batch-commands"

const BASE_URI = process.env.PG_CONNECTION_STRING ?? "postgresql://localhost:5432/postgres"

async function pgAvailable(): Promise<boolean> {
    const client = new Client({ connectionString: BASE_URI })
    try {
        await client.connect()
        await client.end()
        return true
    } catch {
        return false
    }
}

async function createTestDb(): Promise<{ connectionString: string; dbName: string }> {
    const dbName = `bench_${uuid().split("-").join("").slice(0, 16)}`
    const client = new Client({ connectionString: BASE_URI })
    await client.connect()
    await client.query(`CREATE DATABASE "${dbName}"`)
    await client.end()

    const url = new URL(BASE_URI)
    url.pathname = `/${dbName}`
    return { connectionString: url.toString(), dbName }
}

async function dropTestDb(dbName: string): Promise<void> {
    const client = new Client({ connectionString: BASE_URI })
    await client.connect()
    await client.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`)
    await client.end()
}

function createBenchPool(connectionString: string, max: number): Pool {
    const pool = new Pool({ connectionString, max })
    // Set synchronous_commit=off on every connection for benchmark throughput.
    // Safe for event stores with replay capability — only risks last few ms on hard crash.
    pool.on("connect", (client: Client) => {
        client.query("SET synchronous_commit = off")
    })
    return pool
}

function createPgFactory(pool: Pool, isolated = false): EventStoreFactory {
    return async (runId: string): Promise<EventStore> => {
        const tablePrefix = isolated ? runId.replace(/[^a-z0-9_]/gi, "_").slice(0, 40) : undefined
        const store = new PostgresEventStore({ pool, tablePrefix })
        await store.ensureInstalled()
        return store
    }
}

describe("stress — local postgres", async () => {
    const available = await pgAvailable()

    beforeAll(() => {
        if (!available) {
            console.log(`Skipping local PG tests — cannot connect to ${BASE_URI}`)
        }
    })

    test.skipIf(!available)("T1 — throughput baseline (1/5/10 writers, 10s each)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT1(factory, [
                { name: "1-writer", workers: 1, durationMs: 10_000 },
                { name: "5-writers", workers: 5, durationMs: 10_000 },
                { name: "10-writers", workers: 10, durationMs: 10_000 },
            ])

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "zero-errors")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T2 — bulk import 10K meters (110K events)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 20)
        try {
            const factory = createPgFactory(pool)
            const result = await runT2(factory, { meterCount: 20_000, metersPerChunk: 2_500 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "total-event-count")?.pass).toBe(true)
            expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T3 — contention (20 workers, 10s)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT3(factory, { workerCount: 20, durationMs: 10_000 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.scenarios[0].aggregate.totalErrors).toBe(0)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T5 — raw bulk throughput (250K events)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 20)
        try {
            const factory = createPgFactory(pool)
            const result = await runT5(factory, { eventCount: 250_000 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "event-count")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T8 — consistency oracle (20 workers, 200 entities, 10s)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT8(factory, { entityCount: 200, workerCount: 20, durationMs: 10_000 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "zero-duplicates")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T9 — overlapping scope consistency (10 workers, 50 entities, 10s)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT9(factory, { entityCount: 50, workerCount: 10, durationMs: 10_000 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "zero-duplicates")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T4 — mixed workload (import + 5 small writers, 10s)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT4(factory, {
                importTotalEvents: 5_000,
                importBatchSize: 500,
                importConcurrency: 2,
                smallWriterCount: 5,
                smallWriteRatePerSec: 10,
                durationMs: 10_000,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "import-completed")?.pass).toBe(true)
            expect(result.verification?.checks.find(c => c.name === "small-writer-zero-errors")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T6 — parallel bulk import (5 batches x 100 meters)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT6(factory, { batchCount: 5, metersPerBatch: 100 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "all-batches-completed")?.pass).toBe(true)
            expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T10 — ESB-compat benchmark (1/2/4 writers, 1/2/4 readers, 5s each)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const tiers = [
                { name: "1", workers: 1, durationMs: 5_000 },
                { name: "2", workers: 2, durationMs: 5_000 },
                { name: "4", workers: 4, durationMs: 5_000 },
            ]
            const result = await runT10(factory, { writeTiers: tiers, readTiers: tiers })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T11 — meter upsert (300 meters)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 20)
        try {
            const factory = createPgFactory(pool)
            const result = await runT11(factory, { totalMeters: 300 })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.verification?.checks.find(c => c.name === "all-meters-alive")?.pass).toBe(true)
            expect(result.verification?.checks.find(c => c.name === "no-duplicate-creates")?.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 120_000)

    test.skipIf(!available)("T12 — batch throughput sweep (unconditional, 10 workers)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT12(factory, {
                workerCount: 10,
                batchSizes: [1, 10, 50, 100, 500],
                durationPerTierMs: 15_000,
                conditional: false,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)

    test.skipIf(!available)("T12 — batch throughput sweep (conditional, 10 workers)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT12(factory, {
                workerCount: 10,
                batchSizes: [1, 10, 50, 100, 500],
                durationPerTierMs: 15_000,
                conditional: true,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)

    test.skipIf(!available)("T13 — degradation (batch-500, conditional, 4 phases x 30s)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool)
            const result = await runT13(factory, {
                workerCount: 10,
                batchSize: 500,
                phases: 4,
                durationPerPhaseMs: 30_000,
                conditional: true,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            if (result.verification) {
                for (const c of result.verification.checks) {
                    console.log(`  ${c.name}: ${c.actual}`)
                }
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)

    test.skipIf(!available)("T14 — copy threshold sweep (batch=500, unconditional)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const thresholdFactory: ThresholdFactory = async (_runId, copyThreshold) => {
                const store = new PostgresEventStore({ pool, copyThreshold })
                await store.ensureInstalled()
                return store
            }
            const result = await runT14(thresholdFactory, {
                workerCount: 10,
                batchSize: 500,
                thresholds: [10, 100, 250, 500, 1000, 2500],
                durationPerTierMs: 15_000,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)

    test.skipIf(!available)("T15 — copy vs function crossover", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const thresholdFactory: ThresholdFactory = async (_runId, copyThreshold) => {
                const store = new PostgresEventStore({ pool, copyThreshold })
                await store.ensureInstalled()
                return store
            }
            const result = await runT15(thresholdFactory, {
                workerCount: 10,
                batchSizes: [1000, 5000, 10000, 25000, 50000],
                durationPerTierMs: 10_000,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)

    test.skipIf(!available)("T16 — batch commands (1/10/50/100/500 cmds per append, each with condition)", async () => {
        const { connectionString, dbName } = await createTestDb()
        const pool = createBenchPool(connectionString, 50)
        try {
            const factory = createPgFactory(pool, true)
            const result = await runT16(factory, {
                workerCount: 10,
                commandsPerAppend: [1, 10, 50, 100, 500],
                durationPerTierMs: 15_000,
            })

            for (const s of result.scenarios) {
                console.log(formatScenarioResult(s))
            }

            expect(result.pass).toBe(true)
        } finally {
            await pool.end()
            await dropTestDb(dbName)
        }
    }, 600_000)
})

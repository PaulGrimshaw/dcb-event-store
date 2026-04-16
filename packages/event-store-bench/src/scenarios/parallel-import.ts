import { EventStore, Query, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"
import { buildMeterCommands, EVENTS_PER_METER } from "./meter-commands"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    batchCount: number
    metersPerBatch: number
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory(`parallel-import-${Date.now()}`)
    const totalMeters = config.batchCount * config.metersPerBatch
    const totalEvents = totalMeters * EVENTS_PER_METER

    const batchLatencies: number[] = []
    const overallStart = Date.now()

    const batchPromises = Array.from({ length: config.batchCount }, async (_, idx) => {
        const start = idx * config.metersPerBatch
        const commands = buildMeterCommands(start, start + config.metersPerBatch)
        const batchStart = Date.now()
        await store.append(commands)
        const elapsed = Date.now() - batchStart
        batchLatencies.push(elapsed)
        return elapsed
    })

    const results = await Promise.allSettled(batchPromises)
    const overallElapsed = Date.now() - overallStart

    const fulfilled = results.filter(r => r.status === "fulfilled")
    const rejected = results.filter(r => r.status === "rejected")

    const verification = await verify(store, config, totalEvents, overallElapsed, fulfilled.length)

    return {
        id: "parallel-import",
        name: "Parallel Import",
        scenarios: [{
            scenario: "parallel-import",
            description: `${config.batchCount} parallel batches x ${config.metersPerBatch} meters`,
            durationMs: overallElapsed,
            workers: results.map((r, i) => ({
                workerId: i,
                type: "import" as const,
                operations: r.status === "fulfilled" ? 1 : 0,
                events: r.status === "fulfilled" ? config.metersPerBatch * EVENTS_PER_METER : 0,
                errors: r.status === "rejected" ? 1 : 0,
                conflicts: 0,
                latencies: r.status === "fulfilled" ? [r.value] : [],
            })),
            aggregate: {
                totalOperations: fulfilled.length,
                totalErrors: rejected.length,
                totalConflicts: 0,
                opsPerSec: Math.round(fulfilled.length / (overallElapsed / 1000)),
                eventsPerSec: Math.round(totalEvents / (overallElapsed / 1000)),
                latency: computeLatencyStats(batchLatencies),
            },
        }],
        verification,
        pass: verification.checks.every(c => c.pass),
    }
}

async function verify(
    store: EventStore,
    config: Config,
    expectedEvents: number,
    elapsedMs: number,
    completedBatches: number,
): Promise<VerificationResult> {
    const totalMeters = config.batchCount * config.metersPerBatch
    const all = await streamAllEventsToArray(store.read(Query.all()))
    const creates = all.filter(se => se.event.type === "MeterCreated")
    const uniqueIds = new Set(creates.map(se => (se.event.data as Record<string, unknown>).meterId))
    const evPerSec = Math.round(expectedEvents / (elapsedMs / 1000))

    return {
        checks: [
            {
                name: "all-batches-completed",
                expected: String(config.batchCount),
                actual: String(completedBatches),
                pass: completedBatches === config.batchCount,
            },
            {
                name: "total-event-count",
                expected: String(expectedEvents),
                actual: String(all.length),
                pass: all.length === expectedEvents,
            },
            {
                name: "no-duplicate-creates",
                expected: String(totalMeters),
                actual: `${uniqueIds.size} unique, ${creates.length} total`,
                pass: uniqueIds.size === creates.length && creates.length === totalMeters,
            },
            {
                name: "throughput",
                expected: ">= 10000 ev/sec",
                actual: `${evPerSec} ev/sec`,
                pass: evPerSec >= 10_000,
            },
        ],
    }
}

export const parallelImport: BenchScenario<Config> = {
    id: "parallel-import",
    name: "Parallel Import",
    description: "Multiple batch imports running simultaneously with per-entity conditions",
    presets: {
        quick: { batchCount: 3, metersPerBatch: 50 },
        full: { batchCount: 5, metersPerBatch: 100 },
    },
    correctnessChecks: ["all-batches-completed", "no-duplicate-creates"],
    run,
}

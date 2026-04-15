import {
    EventStore,
    Query,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult, VerificationResult } from "./stressTypes"
import { ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"
import { buildMeterCommands, EVENTS_PER_METER } from "./factories"

export interface T6Config {
    batchCount: number
    metersPerBatch: number
}

const DEFAULT_CONFIG: T6Config = { batchCount: 10, metersPerBatch: 2000 }

export async function run(
    factory: EventStoreFactory,
    config: T6Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const eventStore = await factory(`t6-${Date.now()}`)
    const totalMeters = config.batchCount * config.metersPerBatch
    const totalEvents = totalMeters * EVENTS_PER_METER

    const batchLatencies: number[] = []
    const overallStart = Date.now()

    const batchPromises = Array.from({ length: config.batchCount }, async (_, batchIdx) => {
        const meterStart = batchIdx * config.metersPerBatch
        const meterEnd = meterStart + config.metersPerBatch
        const commands = buildMeterCommands(meterStart, meterEnd)

        const batchStart = Date.now()
        await eventStore.append(commands)
        const elapsed = Date.now() - batchStart
        batchLatencies.push(elapsed)
        return elapsed
    })

    const batchResults = await Promise.allSettled(batchPromises)
    const overallElapsed = Date.now() - overallStart

    const fulfilled = batchResults.filter(r => r.status === "fulfilled")
    const rejected = batchResults.filter(r => r.status === "rejected")
    const overallRate = Math.round(totalEvents / (overallElapsed / 1000))

    const scenario: ScenarioResult = {
        scenario: "T6-parallel-import",
        description: `${config.batchCount} parallel batches x ${config.metersPerBatch} meters = ${totalEvents} events`,
        durationMs: overallElapsed,
        workers: batchResults.map((r, i) => ({
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
            eventsPerSec: overallRate,
            latency: computeLatencyStats(batchLatencies),
        },
    }

    const verification = await verify(eventStore, config, totalEvents, overallElapsed, fulfilled.length)
    const pass = verification.checks.every(c => c.pass)

    return { id: "T6", name: "Parallel Bulk Import", scenarios: [scenario], verification, pass }
}

async function verify(
    eventStore: EventStore,
    config: T6Config,
    expectedEvents: number,
    elapsedMs: number,
    completedBatches: number,
): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []
    const totalMeters = config.batchCount * config.metersPerBatch

    checks.push({
        name: "all-batches-completed",
        expected: String(config.batchCount),
        actual: String(completedBatches),
        pass: completedBatches === config.batchCount,
    })

    const all = await streamAllEventsToArray(eventStore.read(Query.all()))
    checks.push({
        name: "total-event-count",
        expected: String(expectedEvents),
        actual: String(all.length),
        pass: all.length === expectedEvents,
    })

    const createEvents = all.filter(se => se.event.type === "MeterCreated")
    const uniqueIds = new Set(createEvents.map(se => (se.event.data as Record<string, unknown>).meterId))
    checks.push({
        name: "no-duplicate-creates",
        expected: String(totalMeters),
        actual: `${uniqueIds.size} unique, ${createEvents.length} total`,
        pass: uniqueIds.size === createEvents.length && createEvents.length === totalMeters,
    })

    const evPerSec = Math.round(expectedEvents / (elapsedMs / 1000))
    checks.push({
        name: "throughput",
        expected: ">= 10000 ev/sec",
        actual: `${evPerSec} ev/sec`,
        pass: evPerSec >= 10_000,
    })

    return { checks }
}

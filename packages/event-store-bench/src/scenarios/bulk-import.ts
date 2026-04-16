import { EventStore, Query, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"
import { buildMeterCommands, EVENTS_PER_METER } from "./meter-commands"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    meterCount: number
    metersPerChunk: number
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory(`bulk-import-${Date.now()}`)
    const totalEvents = config.meterCount * EVENTS_PER_METER

    const prepStart = Date.now()
    const commands = buildMeterCommands(0, config.meterCount)
    const prepElapsed = Date.now() - prepStart

    const importStart = Date.now()
    await store.append(commands)
    const importElapsed = Date.now() - importStart

    const overallElapsed = prepElapsed + importElapsed
    const overallRate = Math.round(totalEvents / (overallElapsed / 1000))

    const verification = await verify(store, config, totalEvents, overallElapsed)

    return {
        id: "bulk-import",
        name: "Bulk Import",
        scenarios: [{
            scenario: "bulk-import",
            description: `${config.meterCount} meters x ${EVENTS_PER_METER} events = ${totalEvents} total`,
            durationMs: overallElapsed,
            workers: [{
                workerId: 0,
                type: "import",
                operations: 1,
                events: totalEvents,
                errors: 0,
                conflicts: 0,
                latencies: [importElapsed],
            }],
            aggregate: {
                totalOperations: 1,
                totalErrors: 0,
                totalConflicts: 0,
                opsPerSec: 1,
                eventsPerSec: overallRate,
                latency: computeLatencyStats([importElapsed]),
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
): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []
    const all = await streamAllEventsToArray(store.read(Query.all()))

    checks.push({
        name: "total-event-count",
        expected: String(expectedEvents),
        actual: String(all.length),
        pass: all.length === expectedEvents,
    })

    const meterCounts = new Map<string, number>()
    for (const se of all) {
        const meterId = (se.event.data as Record<string, unknown>).meterId as string
        meterCounts.set(meterId, (meterCounts.get(meterId) ?? 0) + 1)
    }
    const wrongCount = [...meterCounts.values()].filter(c => c !== EVENTS_PER_METER).length
    checks.push({
        name: "events-per-meter",
        expected: `all ${config.meterCount} meters have ${EVENTS_PER_METER} events`,
        actual: wrongCount === 0 ? "all correct" : `${wrongCount} meters with wrong count`,
        pass: wrongCount === 0,
    })

    const creates = all.filter(se => se.event.type === "MeterCreated")
    const createIds = creates.map(se => (se.event.data as Record<string, unknown>).meterId)
    const uniqueIds = new Set(createIds)
    checks.push({
        name: "no-duplicate-creates",
        expected: String(config.meterCount),
        actual: `${uniqueIds.size} unique, ${createIds.length} total`,
        pass: uniqueIds.size === createIds.length && createIds.length === config.meterCount,
    })

    const serials = all
        .filter(se => se.event.type === "SerialNumberSet")
        .map(se => se.event.tags.values.find(t => t.startsWith("serialNumber=")))
    const uniqueSerials = new Set(serials)
    checks.push({
        name: "no-duplicate-serials",
        expected: String(config.meterCount),
        actual: `${uniqueSerials.size} unique, ${serials.length} total`,
        pass: uniqueSerials.size === serials.length && serials.length === config.meterCount,
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

export const bulkImport: BenchScenario<Config> = {
    id: "bulk-import",
    name: "Bulk Import",
    description: "Single large append with per-entity DCB conditions, verifies event counts and no duplicates",
    presets: {
        quick: { meterCount: 100, metersPerChunk: 50 },
        full: { meterCount: 20_000, metersPerChunk: 2_500 },
    },
    correctnessChecks: ["total-event-count", "no-duplicate-creates"],
    run,
}

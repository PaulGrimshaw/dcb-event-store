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

export interface T2Config {
    meterCount: number
    metersPerChunk: number
}

const DEFAULT_CONFIG: T2Config = { meterCount: 50_000, metersPerChunk: 1000 }

export async function run(
    factory: EventStoreFactory,
    config: T2Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const eventStore = await factory(`t2-${Date.now()}`)
    const totalEvents = config.meterCount * EVENTS_PER_METER

    const prepStart = Date.now()
    const commands = buildMeterCommands(0, config.meterCount)
    const prepElapsed = Date.now() - prepStart

    const importStart = Date.now()
    await eventStore.append(commands)
    const importElapsed = Date.now() - importStart

    const overallElapsed = prepElapsed + importElapsed
    const importRate = Math.round(totalEvents / (importElapsed / 1000))
    const overallRate = Math.round(totalEvents / (overallElapsed / 1000))

    const scenario: ScenarioResult = {
        scenario: "T2-bulk-import",
        description: `${config.meterCount} meters x ${EVENTS_PER_METER} events = ${totalEvents} total, single append(commands[])`,
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
    }

    const verification = await verify(eventStore, config, totalEvents, overallElapsed)
    const pass = verification.checks.every(c => c.pass)

    return { id: "T2", name: "Bulk Import with DCB Conditions", scenarios: [scenario], verification, pass }
}

async function verify(
    eventStore: EventStore,
    config: T2Config,
    expectedEvents: number,
    elapsedMs: number,
): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []

    const all = await streamAllEventsToArray(eventStore.read(Query.all()))
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

    const createEvents = all.filter(se => se.event.type === "MeterCreated")
    const createMeterIds = createEvents.map(se => (se.event.data as Record<string, unknown>).meterId)
    const uniqueCreateIds = new Set(createMeterIds)
    checks.push({
        name: "no-duplicate-creates",
        expected: String(config.meterCount),
        actual: `${uniqueCreateIds.size} unique, ${createMeterIds.length} total`,
        pass: uniqueCreateIds.size === createMeterIds.length && createMeterIds.length === config.meterCount,
    })

    const serialEvents = all.filter(se => se.event.type === "SerialNumberSet")
    const serials = serialEvents.map(se => {
        const tags = se.event.tags.values
        return tags.find(t => t.startsWith("serialNumber="))
    })
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

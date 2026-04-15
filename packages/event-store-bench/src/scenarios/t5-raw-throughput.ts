import {
    EventStore,
    Query,
    Tags,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult, VerificationResult } from "./stressTypes"
import { ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"

export interface T5Config {
    eventCount: number
}

const DEFAULT_CONFIG: T5Config = { eventCount: 1_000_000 }

export async function run(
    factory: EventStoreFactory,
    config: T5Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const eventStore = await factory(`t5-${Date.now()}`)

    const CHUNK = 250_000
    const chunkCount = Math.ceil(config.eventCount / CHUNK)

    const importStart = Date.now()

    for (let offset = 0; offset < config.eventCount; offset += CHUNK) {
        const size = Math.min(CHUNK, config.eventCount - offset)
        const chunk = Array.from({ length: size }, (_, i) => ({
            type: "RawEvent",
            tags: Tags.fromObj({ batch: "raw", seq: `${offset + i}` }),
            data: { index: offset + i, payload: "x".repeat(80) },
            metadata: {},
        }))
        await eventStore.append({ events: chunk })
    }

    const importElapsed = Date.now() - importStart
    const importRate = Math.round(config.eventCount / (importElapsed / 1000))

    const scenario: ScenarioResult = {
        scenario: "T5-raw-throughput",
        description: `${config.eventCount} unconditional events in ${chunkCount} chunks`,
        durationMs: importElapsed,
        workers: [{
            workerId: 0,
            type: "import",
            operations: 1,
            events: config.eventCount,
            errors: 0,
            conflicts: 0,
            latencies: [importElapsed],
        }],
        aggregate: {
            totalOperations: 1,
            totalErrors: 0,
            totalConflicts: 0,
            opsPerSec: 1,
            eventsPerSec: importRate,
            latency: computeLatencyStats([importElapsed]),
        },
    }

    const verification = await verify(eventStore, config, importElapsed)
    const pass = verification.checks.every(c => c.pass)

    return { id: "T5", name: "Raw Bulk Throughput", scenarios: [scenario], verification, pass }
}

async function verify(
    eventStore: EventStore,
    config: T5Config,
    elapsedMs: number,
): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []

    const count = await streamAllEventsToArray(eventStore.read(Query.all(), { backwards: true, limit: 1 }))
    const lastPos = count.length > 0 ? Number(count[0].position.toString()) : 0

    checks.push({
        name: "event-count",
        expected: String(config.eventCount),
        actual: String(lastPos),
        pass: lastPos === config.eventCount,
    })

    const evPerSec = Math.round(config.eventCount / (elapsedMs / 1000))
    checks.push({
        name: "throughput",
        expected: ">= 50000 ev/sec",
        actual: `${evPerSec} ev/sec`,
        pass: evPerSec >= 50_000,
    })

    return { checks }
}

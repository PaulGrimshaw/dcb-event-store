import { EventStore, Tags, Query, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    eventCount: number
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory(`raw-throughput-${Date.now()}`)
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
        await store.append({ events: chunk })
    }
    const importElapsed = Date.now() - importStart
    const importRate = Math.round(config.eventCount / (importElapsed / 1000))

    const verification = await verify(store, config, importElapsed)

    return {
        id: "raw-throughput",
        name: "Raw Throughput",
        scenarios: [{
            scenario: "raw-throughput",
            description: `${config.eventCount} unconditional events in ${chunkCount} chunks`,
            durationMs: importElapsed,
            workers: [{
                workerId: 0, type: "import", operations: 1, events: config.eventCount,
                errors: 0, conflicts: 0, latencies: [importElapsed],
            }],
            aggregate: {
                totalOperations: 1, totalErrors: 0, totalConflicts: 0,
                opsPerSec: 1, eventsPerSec: importRate,
                latency: computeLatencyStats([importElapsed]),
            },
        }],
        verification,
        pass: verification.checks.every(c => c.pass),
    }
}

async function verify(store: EventStore, config: Config, elapsedMs: number): Promise<VerificationResult> {
    const count = await streamAllEventsToArray(store.read(Query.all(), { backwards: true, limit: 1 }))
    const lastPos = count.length > 0 ? Number(count[0].position.toString()) : 0
    const evPerSec = Math.round(config.eventCount / (elapsedMs / 1000))

    return {
        checks: [
            {
                name: "event-count",
                expected: String(config.eventCount),
                actual: String(lastPos),
                pass: lastPos === config.eventCount,
            },
            {
                name: "throughput",
                expected: ">= 50000 ev/sec",
                actual: `${evPerSec} ev/sec`,
                pass: evPerSec >= 50_000,
            },
        ],
    }
}

export const rawThroughput: BenchScenario<Config> = {
    id: "raw-throughput",
    name: "Raw Throughput",
    description: "Unconditional bulk append in large chunks — measures peak write speed with no conditions",
    presets: {
        quick: { eventCount: 10_000 },
        full: { eventCount: 250_000 },
    },
    correctnessChecks: ["event-count"],
    run,
}

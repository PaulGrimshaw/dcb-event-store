import { EventStore, Tags, Query, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { emptyResult, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface TierConfig { workers: number; durationMs: number }

interface Config {
    writeTiers: TierConfig[]
    readTiers: TierConfig[]
}

const PAYLOAD = "x".repeat(256)
const STREAM_ROTATE = 10
const PREPOPULATE_STREAMS = 500
const PREPOPULATE_PER_STREAM = 10
const READ_LIMIT = 10

async function writeWorker(store: EventStore, workerId: number, endTime: number) {
    const result = emptyResult(workerId, "write")
    let streamSeq = 0

    while (Date.now() < endTime) {
        const streamId = `W${workerId}-S${Math.floor(streamSeq / STREAM_ROTATE)}`
        streamSeq++

        const opStart = Date.now()
        try {
            await store.append({
                events: {
                    type: "BenchEvent",
                    tags: Tags.fromObj({ stream: streamId }),
                    data: { payload: PAYLOAD },
                    metadata: {},
                },
            })
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events++
        } catch {
            result.errors++
        }
    }

    return result
}

async function readWorker(store: EventStore, streamCount: number, workerId: number, endTime: number) {
    const result = emptyResult(workerId, "read")
    let seq = workerId

    while (Date.now() < endTime) {
        const query = Query.fromItems([{
            types: ["BenchEvent"],
            tags: Tags.fromObj({ stream: `P${seq % streamCount}` }),
        }])
        seq++

        const opStart = Date.now()
        try {
            const events = await streamAllEventsToArray(store.read(query, { limit: READ_LIMIT }))
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += events.length
        } catch {
            result.errors++
        }
    }

    return result
}

async function prepopulate(store: EventStore): Promise<void> {
    const batchSize = 500
    const total = PREPOPULATE_STREAMS * PREPOPULATE_PER_STREAM
    for (let offset = 0; offset < total; offset += batchSize) {
        const size = Math.min(batchSize, total - offset)
        const events = Array.from({ length: size }, (_, i) => {
            const idx = offset + i
            return {
                type: "BenchEvent",
                tags: Tags.fromObj({ stream: `P${idx % PREPOPULATE_STREAMS}` }),
                data: { payload: PAYLOAD },
                metadata: {},
            }
        })
        await store.append({ events })
    }
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []

    for (const tier of config.writeTiers) {
        const store = await factory(`esb-w-${tier.workers}-${Date.now()}`)
        const endTime = Date.now() + tier.durationMs

        const workers = await spawnWorkers(tier.workers, (id) => writeWorker(store, id, endTime))
        scenarios.push(buildScenarioResult(
            `write-${tier.workers}w`,
            `${tier.workers} unconditional writers`,
            tier.durationMs,
            workers,
        ))
    }

    const readStore = await factory(`esb-read-${Date.now()}`)
    await prepopulate(readStore)

    for (const tier of config.readTiers) {
        const endTime = Date.now() + tier.durationMs

        const workers = await spawnWorkers(tier.workers, (id) =>
            readWorker(readStore, PREPOPULATE_STREAMS, id, endTime),
        )
        scenarios.push(buildScenarioResult(
            `read-${tier.workers}r`,
            `${tier.workers} readers, limit=${READ_LIMIT}`,
            tier.durationMs,
            workers,
        ))
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
    return { id: "esb-compat", name: "ESB-compat Benchmark", scenarios, pass }
}

export const esbCompat: BenchScenario<Config> = {
    id: "esb-compat",
    name: "ESB-compat Benchmark",
    description: "Replicates the Event Store Benchmark pattern — separate write and read scaling tiers",
    presets: {
        quick: {
            writeTiers: [
                { workers: 1, durationMs: 3_000 },
                { workers: 2, durationMs: 3_000 },
            ],
            readTiers: [
                { workers: 1, durationMs: 3_000 },
                { workers: 2, durationMs: 3_000 },
            ],
        },
        full: {
            writeTiers: [
                { workers: 1, durationMs: 5_000 },
                { workers: 2, durationMs: 5_000 },
                { workers: 4, durationMs: 5_000 },
            ],
            readTiers: [
                { workers: 1, durationMs: 5_000 },
                { workers: 2, durationMs: 5_000 },
                { workers: 4, durationMs: 5_000 },
            ],
        },
    },
    correctnessChecks: [],
    run,
}

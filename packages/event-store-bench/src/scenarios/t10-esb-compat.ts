import {
    EventStore,
    Tags,
    Query,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"
import { TierConfig, runTieredScenario } from "./helpers"

const PAYLOAD = "x".repeat(256)
const STREAM_ROTATE = 10
const PREPOPULATE_STREAMS = 500
const PREPOPULATE_EVENTS_PER_STREAM = 10
const READ_LIMIT = 10

const WRITE_TIERS: TierConfig[] = [
    { name: "1w",  workers: 1,  durationMs: 20_000 },
    { name: "2w",  workers: 2,  durationMs: 20_000 },
    { name: "4w",  workers: 4,  durationMs: 20_000 },
    { name: "8w",  workers: 8,  durationMs: 20_000 },
    { name: "16w", workers: 16, durationMs: 20_000 },
    { name: "32w", workers: 32, durationMs: 20_000 },
]

const READ_TIERS: TierConfig[] = [
    { name: "1r",  workers: 1,  durationMs: 20_000 },
    { name: "2r",  workers: 2,  durationMs: 20_000 },
    { name: "4r",  workers: 4,  durationMs: 20_000 },
    { name: "8r",  workers: 8,  durationMs: 20_000 },
    { name: "16r", workers: 16, durationMs: 20_000 },
    { name: "32r", workers: 32, durationMs: 20_000 },
]

async function unconditionalAppendWorker(
    eventStore: EventStore,
    workerId: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    let streamSeq = 0

    while (Date.now() < endTime) {
        const streamId = `W${workerId}-S${Math.floor(streamSeq / STREAM_ROTATE)}`
        streamSeq++

        const event = {
            type: "BenchEvent",
            tags: Tags.fromObj({ stream: streamId }),
            data: { payload: PAYLOAD },
            metadata: {},
        }

        const opStart = Date.now()
        try {
            await eventStore.append({ events: event })
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events++
        } catch {
            result.errors++
        }
    }

    return result
}

async function readWorker(
    eventStore: EventStore,
    streamCount: number,
    workerId: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "read", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    let seq = workerId

    while (Date.now() < endTime) {
        const streamIdx = seq % streamCount
        seq++

        const query = Query.fromItems([{
            types: ["BenchEvent"],
            tags: Tags.fromObj({ stream: `P${streamIdx}` }),
        }])

        const opStart = Date.now()
        try {
            const events = await streamAllEventsToArray(
                eventStore.read(query, { limit: READ_LIMIT }),
            )
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += events.length
        } catch {
            result.errors++
        }
    }

    return result
}

async function prepopulate(eventStore: EventStore): Promise<void> {
    const batchSize = 500
    const total = PREPOPULATE_STREAMS * PREPOPULATE_EVENTS_PER_STREAM
    for (let offset = 0; offset < total; offset += batchSize) {
        const size = Math.min(batchSize, total - offset)
        const events = Array.from({ length: size }, (_, i) => {
            const globalIdx = offset + i
            const streamIdx = globalIdx % PREPOPULATE_STREAMS
            return {
                type: "BenchEvent",
                tags: Tags.fromObj({ stream: `P${streamIdx}` }),
                data: { payload: PAYLOAD },
                metadata: {},
            }
        })
        await eventStore.append({ events })
    }
}

export interface T10Options {
    writeOnly?: boolean
    readOnly?: boolean
    writeTiers?: TierConfig[]
    readTiers?: TierConfig[]
}

export async function run(
    factory: EventStoreFactory,
    opts?: T10Options,
): Promise<StressTestResult> {
    const writePhase = !opts?.readOnly
    const readPhase = !opts?.writeOnly
    const writeTiers = opts?.writeTiers ?? WRITE_TIERS
    const readTiers = opts?.readTiers ?? READ_TIERS

    const allScenarios: ScenarioResult[] = []

    if (writePhase) {
        const writeResult = await runTieredScenario(
            "T10-write", "ESB-compat Writes",
            writeTiers,
            async (tier) => {
                const eventStore = await factory(`t10w-${tier.name}-${Date.now()}`)
                const endTime = Date.now() + tier.durationMs

                const workers = await Promise.all(
                    Array.from({ length: tier.workers }, (_, i) =>
                        unconditionalAppendWorker(eventStore, i, endTime),
                    ),
                )

                const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
                return {
                    scenario: `T10-write-${tier.name}`,
                    description: `${tier.workers} unconditional writers, 1 event/op, 256B payload`,
                    durationMs: tier.durationMs,
                    workers,
                    aggregate: aggregateResults(workers, tier.durationMs, totalEvents),
                }
            },
        )
        allScenarios.push(...writeResult.scenarios)
    }

    if (readPhase) {
        const readStore = await factory(`t10r-${Date.now()}`)
        await prepopulate(readStore)

        const readResult = await runTieredScenario(
            "T10-read", "ESB-compat Reads",
            readTiers,
            async (tier) => {
                const endTime = Date.now() + tier.durationMs

                const workers = await Promise.all(
                    Array.from({ length: tier.workers }, (_, i) =>
                        readWorker(readStore, PREPOPULATE_STREAMS, i, endTime),
                    ),
                )

                const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
                return {
                    scenario: `T10-read-${tier.name}`,
                    description: `${tier.workers} readers, limit=${READ_LIMIT}, ${PREPOPULATE_STREAMS} streams`,
                    durationMs: tier.durationMs,
                    workers,
                    aggregate: aggregateResults(workers, tier.durationMs, totalEvents),
                }
            },
        )
        allScenarios.push(...readResult.scenarios)
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    return { id: "T10", name: "ESB-compat Benchmark", scenarios: allScenarios, pass }
}

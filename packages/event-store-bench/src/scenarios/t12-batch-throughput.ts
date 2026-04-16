import {
    EventStore,
    AppendConditionError,
    Tags,
    Query,
    SequencePosition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"

// ─── T12: Batch Insert Throughput Sweep ────────────────────────────
//
// 10 workers on isolated scopes, varying batch size (events per append).
// Tests both unconditional and conditional modes.
// Replicates the benchmark from stress-results-2026-04-10.

export interface T12Config {
    workerCount: number
    batchSizes: number[]
    durationPerTierMs: number
    conditional: boolean
}

const DEFAULT_CONFIG: T12Config = {
    workerCount: 10,
    batchSizes: [1, 10, 50, 100, 500],
    durationPerTierMs: 15_000,
    conditional: false,
}

async function batchWorker(
    eventStore: EventStore,
    workerId: number,
    batchSize: number,
    conditional: boolean,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    const scopeTag = `W${workerId}`
    let lastPosition: SequencePosition | undefined
    let iteration = 0

    while (Date.now() < endTime) {
        const events = Array.from({ length: batchSize }, (_, i) => ({
            type: "BatchEvent",
            tags: Tags.fromObj({ scope: scopeTag }),
            data: { seq: iteration * batchSize + i },
            metadata: {},
        }))

        const opStart = Date.now()
        try {
            if (conditional) {
                const scopeQuery = Query.fromItems([{ types: ["BatchEvent"], tags: Tags.fromObj({ scope: scopeTag }) }])

                if (lastPosition === undefined) {
                    const [latest] = await streamAllEventsToArray(
                        eventStore.read(scopeQuery, { backwards: true, limit: 1 }),
                    )
                    lastPosition = latest?.position ?? SequencePosition.initial()
                }

                lastPosition = await eventStore.append({
                    events,
                    condition: { failIfEventsMatch: scopeQuery, after: lastPosition },
                })
            } else {
                await eventStore.append({ events })
            }

            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += batchSize
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
                lastPosition = undefined
            } else {
                result.errors++
            }
        }

        iteration++
    }

    return result
}

export async function run(
    factory: EventStoreFactory,
    config: T12Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const mode = config.conditional ? "conditional" : "unconditional"
    const allScenarios: ScenarioResult[] = []

    for (const batchSize of config.batchSizes) {
        const eventStore = await factory(`t12-${mode}-b${batchSize}-${Date.now()}`)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await Promise.all(
            Array.from({ length: config.workerCount }, (_, i) =>
                batchWorker(eventStore, i, batchSize, config.conditional, endTime),
            ),
        )

        const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
        const aggregate = aggregateResults(workers, config.durationPerTierMs, totalEvents)

        allScenarios.push({
            scenario: `T12-batch-${batchSize}-${mode}`,
            description: `${config.workerCount} workers, batch=${batchSize}, ${mode}`,
            durationMs: config.durationPerTierMs,
            workers,
            aggregate,
        })
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        id: "T12",
        name: `Batch Throughput Sweep (${mode})`,
        scenarios: allScenarios,
        pass,
    }
}

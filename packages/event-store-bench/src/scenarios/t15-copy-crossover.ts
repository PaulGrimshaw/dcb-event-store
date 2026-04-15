import {
    EventStore,
    Tags,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult, ThresholdFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"

// ─── T15: Copy vs Function Crossover ───────────────────────────────
//
// For each batch size, runs once with function path (high threshold)
// and once with COPY path (threshold=1), so we can compare directly.

export interface T15Config {
    workerCount: number
    batchSizes: number[]
    durationPerTierMs: number
}

const DEFAULT_CONFIG: T15Config = {
    workerCount: 10,
    batchSizes: [100, 500, 1000, 2500, 5000],
    durationPerTierMs: 10_000,
}

async function worker(
    eventStore: EventStore,
    workerId: number,
    batchSize: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    const scopeTag = `W${workerId}`
    let iteration = 0

    while (Date.now() < endTime) {
        const events = Array.from({ length: batchSize }, (_, i) => ({
            type: "CrossoverEvent",
            tags: Tags.fromObj({ scope: scopeTag }),
            data: { seq: iteration * batchSize + i },
            metadata: {},
        }))

        const opStart = Date.now()
        try {
            await eventStore.append({ events })
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += batchSize
        } catch {
            result.errors++
        }

        iteration++
    }

    return result
}

export async function run(
    factory: ThresholdFactory,
    config: T15Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const allScenarios: ScenarioResult[] = []

    for (const batchSize of config.batchSizes) {
        for (const route of ["function", "COPY"] as const) {
            // threshold > batchSize → function path; threshold = 1 → COPY path
            const copyThreshold = route === "function" ? batchSize + 1 : 1
            const eventStore = await factory(`t15-b${batchSize}-${route}-${Date.now()}`, copyThreshold)
            const endTime = Date.now() + config.durationPerTierMs

            const workers = await Promise.all(
                Array.from({ length: config.workerCount }, (_, i) =>
                    worker(eventStore, i, batchSize, endTime),
                ),
            )

            const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
            const aggregate = aggregateResults(workers, config.durationPerTierMs, totalEvents)

            allScenarios.push({
                scenario: `batch-${batchSize}-${route}`,
                description: `batch=${batchSize}, route=${route}, ${config.workerCount} workers`,
                durationMs: config.durationPerTierMs,
                workers,
                aggregate,
            })
        }
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        id: "T15",
        name: "Copy vs Function Crossover",
        scenarios: allScenarios,
        pass,
    }
}

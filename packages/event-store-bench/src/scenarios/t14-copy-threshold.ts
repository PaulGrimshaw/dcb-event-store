import {
    EventStore,
    Tags,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult, ThresholdFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"

// ─── T14: Copy Threshold Sweep ─────────────────────────────────────
//
// Holds batch size constant, varies the copyThreshold setting on
// PostgresEventStore to find the crossover point where COPY FROM
// beats the stored procedure path.
//
// Requires a factory that accepts copyThreshold as a parameter.

export interface T14Config {
    workerCount: number
    batchSize: number
    thresholds: number[]
    durationPerTierMs: number
}

const DEFAULT_CONFIG: T14Config = {
    workerCount: 10,
    batchSize: 500,
    thresholds: [10, 100, 250, 500, 1000, 2500],
    durationPerTierMs: 15_000,
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
            type: "ThresholdEvent",
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
    config: T14Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const allScenarios: ScenarioResult[] = []

    for (const threshold of config.thresholds) {
        const eventStore = await factory(`t14-thresh${threshold}-${Date.now()}`, threshold)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await Promise.all(
            Array.from({ length: config.workerCount }, (_, i) =>
                worker(eventStore, i, config.batchSize, endTime),
            ),
        )

        const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
        const aggregate = aggregateResults(workers, config.durationPerTierMs, totalEvents)

        const route = config.batchSize <= threshold ? "function" : "COPY"
        allScenarios.push({
            scenario: `T14-threshold-${threshold}`,
            description: `batch=${config.batchSize}, copyThreshold=${threshold}, route=${route}, ${config.workerCount} workers`,
            durationMs: config.durationPerTierMs,
            workers,
            aggregate,
        })
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        id: "T14",
        name: `Copy Threshold Sweep (batch=${config.batchSize})`,
        scenarios: allScenarios,
        pass,
    }
}

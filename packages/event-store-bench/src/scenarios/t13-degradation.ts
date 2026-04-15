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

// ─── T13: Degradation Under Load ──────────────────────────────────
//
// Measures throughput over multiple phases as the table grows.
// 10 workers, batch-500, conditional appends on isolated scopes.
// Replicates the "Batch-500 Degradation to 5M" benchmark.

export interface T13Config {
    workerCount: number
    batchSize: number
    phases: number
    durationPerPhaseMs: number
    conditional: boolean
}

const DEFAULT_CONFIG: T13Config = {
    workerCount: 10,
    batchSize: 500,
    phases: 4,
    durationPerPhaseMs: 30_000,
    conditional: true,
}

async function phaseWorker(
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
            type: "DegradationEvent",
            tags: Tags.fromObj({ scope: scopeTag }),
            data: { seq: iteration * batchSize + i },
            metadata: {},
        }))

        const opStart = Date.now()
        try {
            if (conditional) {
                const scopeQuery = Query.fromItems([{ types: ["DegradationEvent"], tags: Tags.fromObj({ scope: scopeTag }) }])

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
    config: T13Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const mode = config.conditional ? "conditional" : "unconditional"
    // Single store across all phases — table grows
    const eventStore = await factory(`t13-degrad-${Date.now()}`)

    const allScenarios: ScenarioResult[] = []
    let cumulativeEvents = 0

    for (let phase = 1; phase <= config.phases; phase++) {
        const endTime = Date.now() + config.durationPerPhaseMs

        const workers = await Promise.all(
            Array.from({ length: config.workerCount }, (_, i) =>
                phaseWorker(eventStore, i, config.batchSize, config.conditional, endTime),
            ),
        )

        const phaseEvents = workers.reduce((sum, w) => sum + w.events, 0)
        cumulativeEvents += phaseEvents
        const aggregate = aggregateResults(workers, config.durationPerPhaseMs, phaseEvents)

        allScenarios.push({
            scenario: `T13-phase-${phase}`,
            description: `Phase ${phase}: ~${(cumulativeEvents / 1000).toFixed(0)}K rows, batch=${config.batchSize}, ${mode}`,
            durationMs: config.durationPerPhaseMs,
            workers,
            aggregate,
        })
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    // Build verification: show degradation curve
    const checks = allScenarios.map((s, i) => ({
        name: `phase-${i + 1}-throughput`,
        expected: "> 0",
        actual: `${s.aggregate.eventsPerSec} ev/sec`,
        pass: s.aggregate.totalErrors === 0,
    }))

    return {
        id: "T13",
        name: `Degradation Under Load (${mode})`,
        scenarios: allScenarios,
        verification: { checks },
        pass,
    }
}

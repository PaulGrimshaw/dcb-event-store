import {
    EventStore,
    AppendCommand,
    AppendConditionError,
    Tags,
    Query,
    SequencePosition,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"

// ─── T16: Batch Commands Throughput ────────────────────────────────
//
// Each append() call submits N AppendCommands — one event + one
// condition per command. This is the real-world pattern for bulk
// imports with per-entity uniqueness constraints.
//
// append([
//   { events: [e1], condition: c1 },
//   { events: [e2], condition: c2 },
//   ...
// ])
//
// Routes through appendBatch (COPY + temp table condition checking).

export interface T16Config {
    workerCount: number
    commandsPerAppend: number[]
    durationPerTierMs: number
}

const DEFAULT_CONFIG: T16Config = {
    workerCount: 10,
    commandsPerAppend: [1, 10, 50, 100, 500],
    durationPerTierMs: 15_000,
}

async function batchCommandWorker(
    eventStore: EventStore,
    workerId: number,
    commandsPerAppend: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    let seq = 0

    while (Date.now() < endTime) {
        const commands: AppendCommand[] = Array.from({ length: commandsPerAppend }, (_, i) => {
            const entityId = `W${workerId}-E${seq++}`
            return {
                events: [{
                    type: "EntityCreated",
                    tags: Tags.fromObj({ entity: entityId }),
                    data: { workerId, seq: seq - 1 },
                    metadata: {},
                }],
                condition: {
                    failIfEventsMatch: Query.fromItems([{
                        types: ["EntityCreated"],
                        tags: Tags.fromObj({ entity: entityId }),
                    }]),
                    after: SequencePosition.initial(),
                },
            }
        })

        const opStart = Date.now()
        try {
            await eventStore.append(commands)
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += commandsPerAppend
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
                if (result.conflicts <= 3) {
                    const idx = err.commandIndex ?? -1
                    const cmd = idx >= 0 ? commands[idx] : undefined
                    const evts = cmd ? (Array.isArray(cmd.events) ? cmd.events : [cmd.events]) : []
                    const tag = evts[0]?.tags?.values?.[0] ?? "?"
                    console.error(`  [W${workerId}] CONFLICT cmdIdx=${idx} tag=${tag}`)
                }
            } else {
                result.errors++
                if (result.errors <= 3) {
                    console.error(`  [W${workerId}] ERROR: ${(err as Error).message?.slice(0, 200)}`)
                }
            }
        }
    }

    return result
}

export async function run(
    factory: EventStoreFactory,
    config: T16Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const allScenarios: ScenarioResult[] = []

    for (const cmdsPerAppend of config.commandsPerAppend) {
        // Fresh store per tier to avoid entity ID collisions from prior tiers
        const eventStore = await factory(`t16-${cmdsPerAppend}cmd-${Date.now()}`)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await Promise.all(
            Array.from({ length: config.workerCount }, (_, i) =>
                batchCommandWorker(eventStore, i, cmdsPerAppend, endTime),
            ),
        )

        const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
        const aggregate = aggregateResults(workers, config.durationPerTierMs, totalEvents)

        allScenarios.push({
            scenario: `T16-${cmdsPerAppend}cmd`,
            description: `${config.workerCount} workers, ${cmdsPerAppend} commands/append, each 1 event + 1 condition`,
            durationMs: config.durationPerTierMs,
            workers,
            aggregate,
        })
    }

    const pass = allScenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        id: "T16",
        name: "Batch Commands Throughput (1 event + 1 condition per command)",
        scenarios: allScenarios,
        pass,
    }
}

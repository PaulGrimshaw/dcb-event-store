import {
    EventStore,
    AppendCommand,
    AppendConditionError,
    Tags,
    Query,
    SequencePosition,
} from "@dcb-es/event-store"
import { EventStoreFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { emptyResult, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface Config {
    workerCount: number
    commandsPerAppend: number[]
    durationPerTierMs: number
}

async function batchCommandWorker(
    store: EventStore,
    workerId: number,
    commandsPerAppend: number,
    endTime: number,
) {
    const result = emptyResult(workerId, "write")
    let seq = 0

    while (Date.now() < endTime) {
        const commands: AppendCommand[] = Array.from({ length: commandsPerAppend }, () => {
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
            await store.append(commands)
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += commandsPerAppend
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
            } else {
                result.errors++
            }
        }
    }

    return result
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []

    for (const cmdsPerAppend of config.commandsPerAppend) {
        const store = await factory(`batch-cmds-${cmdsPerAppend}-${Date.now()}`)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await spawnWorkers(config.workerCount, (id) =>
            batchCommandWorker(store, id, cmdsPerAppend, endTime),
        )

        scenarios.push(buildScenarioResult(
            `${cmdsPerAppend}-commands`,
            `${config.workerCount} workers, ${cmdsPerAppend} commands/append, each 1 event + 1 condition`,
            config.durationPerTierMs,
            workers,
        ))
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
    return { id: "batch-commands", name: "Batch Commands Throughput", scenarios, pass }
}

export const batchCommands: BenchScenario<Config> = {
    id: "batch-commands",
    name: "Batch Commands",
    description: "Each append submits N commands with per-entity conditions — tests the real-world bulk import pattern",
    presets: {
        quick: { workerCount: 2, commandsPerAppend: [1, 5], durationPerTierMs: 2_000 },
        full: { workerCount: 10, commandsPerAppend: [1, 10, 50, 100, 500], durationPerTierMs: 15_000 },
    },
    correctnessChecks: [],
    timeout: 600_000,
    run,
}

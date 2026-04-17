import { EventStoreFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { scopedBatchWriter, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface Config {
    workerCount: number
    batchSizes: number[]
    durationPerTierMs: number
    conditional: boolean
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const mode = config.conditional ? "conditional" : "unconditional"
    const scenarios: ScenarioResult[] = []

    for (const batchSize of config.batchSizes) {
        const store = await factory(`batch-sweep-${mode}-b${batchSize}-${Date.now()}`)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await spawnWorkers(config.workerCount, (id) =>
            scopedBatchWriter(store, id, endTime, {
                batchSize,
                eventType: "BatchEvent",
                conditional: config.conditional,
            }),
        )

        scenarios.push(buildScenarioResult(
            `batch-${batchSize}-${mode}`,
            `${config.workerCount} workers, batch=${batchSize}, ${mode}`,
            config.durationPerTierMs,
            workers,
        ))
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
    const id = config.conditional ? "batch-sweep-conditional" : "batch-sweep-unconditional"
    return { id, name: `Batch Sweep (${mode})`, scenarios, pass }
}

export const batchSweepUnconditional: BenchScenario<Config> = {
    id: "batch-sweep-unconditional",
    name: "Batch Sweep (unconditional)",
    description: "Sweeps batch sizes with unconditional appends — finds optimal batch size for raw throughput",
    presets: {
        quick: { workerCount: 3, batchSizes: [1, 10], durationPerTierMs: 2_000, conditional: false },
        full: { workerCount: 10, batchSizes: [1, 10, 50, 100, 500], durationPerTierMs: 15_000, conditional: false },
    },
    correctnessChecks: [],
    timeout: 600_000,
    run,
}

export const batchSweepConditional: BenchScenario<Config> = {
    id: "batch-sweep-conditional",
    name: "Batch Sweep (conditional)",
    description: "Sweeps batch sizes with conditional appends — measures condition-checking overhead at different scales",
    presets: {
        quick: { workerCount: 3, batchSizes: [1, 10], durationPerTierMs: 2_000, conditional: true },
        full: { workerCount: 10, batchSizes: [1, 10, 50, 100, 500], durationPerTierMs: 15_000, conditional: true },
    },
    correctnessChecks: [],
    timeout: 600_000,
    run,
}

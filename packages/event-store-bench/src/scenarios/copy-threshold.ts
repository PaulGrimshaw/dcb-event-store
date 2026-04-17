import { ThresholdFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { scopedBatchWriter, spawnWorkers } from "../harness/workers"
import { ThresholdBenchScenario, StressTestResult } from "./types"

interface Config {
    workerCount: number
    batchSize: number
    thresholds: number[]
    durationPerTierMs: number
}

async function run(factory: ThresholdFactory, config: Config): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []

    for (const threshold of config.thresholds) {
        const store = await factory(`copy-thresh-${threshold}-${Date.now()}`, threshold)
        const endTime = Date.now() + config.durationPerTierMs

        const workers = await spawnWorkers(config.workerCount, (id) =>
            scopedBatchWriter(store, id, endTime, {
                batchSize: config.batchSize,
                eventType: "ThresholdEvent",
                conditional: false,
            }),
        )

        const route = config.batchSize <= threshold ? "function" : "COPY"
        scenarios.push(buildScenarioResult(
            `threshold-${threshold}`,
            `batch=${config.batchSize}, copyThreshold=${threshold}, route=${route}`,
            config.durationPerTierMs,
            workers,
        ))
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
    return { id: "copy-threshold", name: `Copy Threshold Sweep (batch=${config.batchSize})`, scenarios, pass }
}

export const copyThreshold: ThresholdBenchScenario<Config> = {
    id: "copy-threshold",
    name: "Copy Threshold Sweep",
    description: "Holds batch size constant and varies the COPY threshold — finds the crossover where COPY FROM beats the stored procedure",
    presets: {
        full: {
            workerCount: 10,
            batchSize: 500,
            thresholds: [10, 100, 250, 500, 1000, 2500],
            durationPerTierMs: 15_000,
        },
    },
    timeout: 600_000,
    run,
}

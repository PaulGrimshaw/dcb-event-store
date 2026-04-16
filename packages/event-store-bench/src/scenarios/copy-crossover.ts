import { Tags } from "@dcb-es/event-store"
import { ThresholdFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { emptyResult, spawnWorkers } from "../harness/workers"
import { ThresholdBenchScenario, StressTestResult } from "./types"

interface Config {
    workerCount: number
    batchSizes: number[]
    durationPerTierMs: number
}

async function run(factory: ThresholdFactory, config: Config): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []

    for (const batchSize of config.batchSizes) {
        for (const route of ["function", "COPY"] as const) {
            const copyThreshold = route === "function" ? batchSize + 1 : 1
            const store = await factory(`crossover-b${batchSize}-${route}-${Date.now()}`, copyThreshold)
            const endTime = Date.now() + config.durationPerTierMs

            const workers = await spawnWorkers(config.workerCount, async (id) => {
                const result = emptyResult(id, "write")
                const scopeTag = `W${id}`
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
                        await store.append({ events })
                        result.latencies.push(Date.now() - opStart)
                        result.operations++
                        result.events += batchSize
                    } catch {
                        result.errors++
                    }
                    iteration++
                }

                return result
            })

            scenarios.push(buildScenarioResult(
                `batch-${batchSize}-${route}`,
                `batch=${batchSize}, route=${route}, ${config.workerCount} workers`,
                config.durationPerTierMs,
                workers,
            ))
        }
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
    return { id: "copy-crossover", name: "Copy vs Function Crossover", scenarios, pass }
}

export const copyCrossover: ThresholdBenchScenario<Config> = {
    id: "copy-crossover",
    name: "Copy vs Function Crossover",
    description: "For each batch size, runs both the function path and COPY path side by side — direct comparison",
    presets: {
        full: {
            workerCount: 10,
            batchSizes: [1000, 5000, 10000, 25000, 50000],
            durationPerTierMs: 10_000,
        },
    },
    timeout: 600_000,
    run,
}

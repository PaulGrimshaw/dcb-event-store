import { EventStoreFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { scopedBatchWriter, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface Config {
    workerCount: number
    batchSize: number
    phases: number
    durationPerPhaseMs: number
    conditional: boolean
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const mode = config.conditional ? "conditional" : "unconditional"
    const store = await factory(`degradation-${Date.now()}`)

    const scenarios: ScenarioResult[] = []
    let cumulativeEvents = 0

    for (let phase = 1; phase <= config.phases; phase++) {
        const endTime = Date.now() + config.durationPerPhaseMs

        const workers = await spawnWorkers(config.workerCount, (id) =>
            scopedBatchWriter(store, id, endTime, {
                batchSize: config.batchSize,
                eventType: "DegradationEvent",
                conditional: config.conditional,
            }),
        )

        const phaseEvents = workers.reduce((sum, w) => sum + w.events, 0)
        cumulativeEvents += phaseEvents

        scenarios.push(buildScenarioResult(
            `phase-${phase}`,
            `Phase ${phase}: ~${(cumulativeEvents / 1000).toFixed(0)}K rows, batch=${config.batchSize}, ${mode}`,
            config.durationPerPhaseMs,
            workers,
        ))
    }

    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        id: "degradation",
        name: `Degradation Under Load (${mode})`,
        scenarios,
        verification: {
            checks: scenarios.map((s, i) => ({
                name: `phase-${i + 1}-throughput`,
                expected: "> 0",
                actual: `${s.aggregate.eventsPerSec} ev/sec`,
                pass: s.aggregate.totalErrors === 0,
            })),
        },
        pass,
    }
}

export const degradation: BenchScenario<Config> = {
    id: "degradation",
    name: "Degradation Under Load",
    description: "Runs multiple phases on the same store — measures how throughput degrades as the table grows",
    presets: {
        quick: { workerCount: 3, batchSize: 10, phases: 2, durationPerPhaseMs: 2_000, conditional: true },
        full: { workerCount: 10, batchSize: 500, phases: 4, durationPerPhaseMs: 30_000, conditional: true },
    },
    correctnessChecks: [],
    timeout: 600_000,
    run,
}

import { StressTestResult } from "./stressTypes"
import { ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"
import { conditionalRetryWorker, TierConfig, runTieredScenario } from "./helpers"
import { t1EventFactory, t1QueryFactory } from "./factories"

const DEFAULT_TIERS: TierConfig[] = [
    { name: "1-writer", workers: 1, durationMs: 15_000 },
    { name: "5-writers", workers: 5, durationMs: 15_000 },
    { name: "10-writers", workers: 10, durationMs: 15_000 },
    { name: "20-writers", workers: 20, durationMs: 15_000 },
]

export async function run(
    factory: EventStoreFactory,
    tiers: TierConfig[] = DEFAULT_TIERS,
): Promise<StressTestResult> {
    async function runTier(tier: TierConfig): Promise<ScenarioResult> {
        const eventStore = await factory(`t1-${Date.now()}`)
        const endTime = Date.now() + tier.durationMs

        const workers = await Promise.all(
            Array.from({ length: tier.workers }, (_, i) =>
                conditionalRetryWorker(eventStore, t1QueryFactory(i), t1EventFactory, i, endTime),
            ),
        )

        const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
        return {
            scenario: `T1-${tier.name}`,
            description: `Throughput baseline: ${tier.workers} isolated writers`,
            durationMs: tier.durationMs,
            workers,
            aggregate: aggregateResults(workers, tier.durationMs, totalEvents),
        }
    }

    const result = await runTieredScenario(
        "T1",
        "Throughput Baseline & Linear Scaling",
        tiers,
        runTier,
    )

    const scenarios = result.scenarios
    const tier1 = scenarios[0].aggregate.eventsPerSec
    const tierN = scenarios[scenarios.length - 1].aggregate.eventsPerSec
    const scalingFactor = tierN / Math.max(tier1, 1)

    const pass = scalingFactor >= 2
        && scenarios.every(s => s.aggregate.latency.p99 < 25)
        && scenarios.every(s => s.aggregate.totalErrors === 0)

    return {
        ...result,
        pass,
        verification: {
            checks: [
                {
                    name: "scaling-factor",
                    expected: ">=2",
                    actual: scalingFactor.toFixed(2),
                    pass: scalingFactor >= 2,
                },
                {
                    name: "p99-latency-all-tiers",
                    expected: "<25ms",
                    actual: scenarios.map(s => `${s.scenario}=${s.aggregate.latency.p99}ms`).join(", "),
                    pass: scenarios.every(s => s.aggregate.latency.p99 < 25),
                },
                {
                    name: "zero-errors",
                    expected: "0",
                    actual: String(scenarios.reduce((sum, s) => sum + s.aggregate.totalErrors, 0)),
                    pass: scenarios.every(s => s.aggregate.totalErrors === 0),
                },
            ],
        },
    }
}

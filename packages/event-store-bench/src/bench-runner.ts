import { EventStoreFactory, ThresholdFactory } from "./harness/types"
import { BenchScenario, ThresholdBenchScenario, StressTestResult } from "./scenarios/types"
import { formatScenarioResult } from "./reporters/tableReporter"

export function runBenchTest<C>(
    scenario: BenchScenario<C>,
    preset: "quick" | "full",
    factory: EventStoreFactory,
): () => Promise<void> {
    return async () => {
        const config = scenario.presets[preset]
        const result = await scenario.run(factory, config)
        logResult(result)
        assertResult(result, scenario.correctnessChecks)
    }
}

export function runThresholdBenchTest<C>(
    scenario: ThresholdBenchScenario<C>,
    factory: ThresholdFactory,
): () => Promise<void> {
    return async () => {
        const config = scenario.presets.full
        const result = await scenario.run(factory, config)
        logResult(result)
        expect(result.pass).toBe(true)
    }
}

function logResult(result: StressTestResult): void {
    for (const s of result.scenarios) {
        console.log(formatScenarioResult(s))
    }
}

function assertResult(result: StressTestResult, correctnessChecks: string[]): void {
    if (correctnessChecks.length === 0) {
        expect(result.pass).toBe(true)
        return
    }

    for (const checkName of correctnessChecks) {
        const check = result.verification?.checks.find(c => c.name === checkName)
        expect(check?.pass, `check "${checkName}" should pass`).toBe(true)
    }
}

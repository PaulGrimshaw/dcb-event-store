import { ScenarioResult, EventStoreFactory, ThresholdFactory } from "../harness/types"

export interface StressTestResult {
    id: string
    name: string
    scenarios: ScenarioResult[]
    verification?: VerificationResult
    pass: boolean
}

export interface VerificationResult {
    checks: Array<{ name: string; expected: string; actual: string; pass: boolean }>
}

export interface BenchScenario<C = Record<string, unknown>> {
    id: string
    name: string
    description: string
    presets: { quick: C; full: C }
    correctnessChecks: string[]
    timeout?: number
    run(factory: EventStoreFactory, config: C): Promise<StressTestResult>
}

export interface ThresholdBenchScenario<C = Record<string, unknown>> {
    id: string
    name: string
    description: string
    presets: { full: C }
    timeout?: number
    run(factory: ThresholdFactory, config: C): Promise<StressTestResult>
}

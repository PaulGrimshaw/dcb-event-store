import { ScenarioResult } from "../harness/types"

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

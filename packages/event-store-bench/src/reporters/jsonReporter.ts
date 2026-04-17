import { ScenarioResult } from "../harness/types"

export function toJsonReport(results: ScenarioResult[]): string {
    const report = {
        timestamp: new Date().toISOString(),
        results: results.map(r => ({
            scenario: r.scenario,
            description: r.description,
            durationMs: r.durationMs,
            aggregate: r.aggregate,
            workers: r.workers.map(w => ({
                workerId: w.workerId,
                type: w.type,
                operations: w.operations,
                errors: w.errors,
                conflicts: w.conflicts,
                latencyCount: w.latencies.length,
            })),
        })),
    }
    return JSON.stringify(report, null, 2)
}

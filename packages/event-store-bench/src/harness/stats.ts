import { LatencyStats, WorkerResult, AggregateMetrics, ScenarioResult } from "./types"

export function computeLatencyStats(latencies: number[]): LatencyStats {
    if (latencies.length === 0) {
        return { p50: 0, p95: 0, p99: 0, p999: 0, min: 0, max: 0, mean: 0 }
    }

    const sorted = [...latencies].sort((a, b) => a - b)
    const sum = sorted.reduce((a, b) => a + b, 0)

    return {
        p50: percentile(sorted, 0.50),
        p95: percentile(sorted, 0.95),
        p99: percentile(sorted, 0.99),
        p999: percentile(sorted, 0.999),
        min: sorted[0],
        max: sorted[sorted.length - 1],
        mean: Math.round(sum / sorted.length),
    }
}

export function aggregateResults(
    workers: WorkerResult[],
    durationMs: number,
    totalEvents: number,
): AggregateMetrics {
    const allLatencies: number[] = []
    let totalOperations = 0
    let totalErrors = 0
    let totalConflicts = 0

    for (const w of workers) {
        totalOperations += w.operations
        totalErrors += w.errors
        totalConflicts += w.conflicts
        for (const l of w.latencies) allLatencies.push(l)
    }

    const durationSec = durationMs / 1000

    return {
        totalOperations,
        totalErrors,
        totalConflicts,
        opsPerSec: Math.round(totalOperations / durationSec),
        eventsPerSec: Math.round(totalEvents / durationSec),
        latency: computeLatencyStats(allLatencies),
    }
}

export function buildScenarioResult(
    scenario: string,
    description: string,
    durationMs: number,
    workers: WorkerResult[],
): ScenarioResult {
    const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
    return {
        scenario,
        description,
        durationMs,
        workers,
        aggregate: aggregateResults(workers, durationMs, totalEvents),
    }
}

function percentile(sorted: number[], p: number): number {
    const index = Math.ceil(p * sorted.length) - 1
    return sorted[Math.max(0, index)]
}

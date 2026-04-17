import { EventStore } from "@dcb-es/event-store"

export type EventStoreFactory = (runId: string) => EventStore | Promise<EventStore>

export type ThresholdFactory = (runId: string, copyThreshold: number) => EventStore | Promise<EventStore>

export interface ScenarioResult {
    scenario: string
    description: string
    durationMs: number
    workers: WorkerResult[]
    aggregate: AggregateMetrics
}

export interface WorkerResult {
    workerId: number
    type: "write" | "read" | "import"
    operations: number
    events: number
    errors: number
    conflicts: number
    latencies: number[]
}

export interface AggregateMetrics {
    totalOperations: number
    totalErrors: number
    totalConflicts: number
    opsPerSec: number
    eventsPerSec: number
    latency: LatencyStats
}

export interface LatencyStats {
    p50: number
    p95: number
    p99: number
    p999: number
    min: number
    max: number
    mean: number
}

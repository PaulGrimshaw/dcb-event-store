import { DcbEvent, AppendCondition, Query, ReadOptions, EventStore } from "@dcb-es/event-store"

// ─── Factory ───────────────────────────────────────────────────────

export type EventStoreFactory = (runId: string) => EventStore | Promise<EventStore>

export type ThresholdFactory = (runId: string, copyThreshold: number) => EventStore | Promise<EventStore>

// ─── Scenario Configuration ─────────────────────────────────────────

export interface ScenarioConfig {
    name: string
    description: string
    workers: WorkerConfig[]
    durationMs: number
    warmupMs?: number
}

export type WorkerConfig = WriteWorkerConfig | ReadWorkerConfig | ImportWorkerConfig

export interface WriteWorkerConfig {
    type: "write"
    count: number
    eventFactory: EventFactory
    conditionFactory?: ConditionFactory
    targetRatePerSec?: number
}

export interface ReadWorkerConfig {
    type: "read"
    count: number
    queryFactory: QueryFactory
    readOptions?: ReadOptions
}

export interface ImportWorkerConfig {
    type: "import"
    count: number
    batchFactory: BatchFactory
    totalEvents: number
    batchSize: number
}

// ─── Factories ──────────────────────────────────────────────────────

export type EventFactory = (workerId: number, iteration: number) => DcbEvent[]

export type ConditionFactory = (workerId: number, iteration: number) => AppendCondition | undefined

export type QueryFactory = (workerId: number, iteration: number) => Query

export type BatchFactory = (workerId: number, batchIndex: number, batchSize: number) => DcbEvent[]

// ─── Results ────────────────────────────────────────────────────────

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


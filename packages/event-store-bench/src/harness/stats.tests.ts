import { describe, it, expect } from "vitest"
import { computeLatencyStats, aggregateResults } from "./stats"

describe("computeLatencyStats", () => {
    it("returns zeros for empty array", () => {
        const stats = computeLatencyStats([])
        expect(stats).toEqual({ p50: 0, p95: 0, p99: 0, p999: 0, min: 0, max: 0, mean: 0 })
    })

    it("all percentiles equal single value", () => {
        const stats = computeLatencyStats([42])
        expect(stats.p50).toBe(42)
        expect(stats.p95).toBe(42)
        expect(stats.p99).toBe(42)
        expect(stats.min).toBe(42)
        expect(stats.max).toBe(42)
        expect(stats.mean).toBe(42)
    })

    it("known distribution 1..100", () => {
        const latencies = Array.from({ length: 100 }, (_, i) => i + 1)
        const stats = computeLatencyStats(latencies)

        expect(stats.min).toBe(1)
        expect(stats.max).toBe(100)
        expect(stats.p50).toBe(50)
        expect(stats.p95).toBe(95)
        expect(stats.p99).toBe(99)
        expect(stats.mean).toBe(51)
    })
})

describe("aggregateResults", () => {
    it("combines multiple workers correctly", () => {
        const workers = [
            { workerId: 0, type: "write" as const, operations: 100, events: 100, errors: 2, conflicts: 1, latencies: [10, 20] },
            { workerId: 1, type: "write" as const, operations: 200, events: 200, errors: 3, conflicts: 0, latencies: [15, 25] },
            { workerId: 2, type: "write" as const, operations: 50, events: 50, errors: 0, conflicts: 5, latencies: [5, 30] },
        ]

        const result = aggregateResults(workers, 10_000, 350)

        expect(result.totalOperations).toBe(350)
        expect(result.totalErrors).toBe(5)
        expect(result.totalConflicts).toBe(6)
        expect(result.opsPerSec).toBe(35)
        expect(result.eventsPerSec).toBe(35)
        expect(result.latency.min).toBe(5)
        expect(result.latency.max).toBe(30)
    })
})

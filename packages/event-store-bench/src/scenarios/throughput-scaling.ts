import { Tags, Query, DcbEvent } from "@dcb-es/event-store"
import { EventStoreFactory, ScenarioResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { conditionalRetryWorker, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface Config {
    tiers: Array<{ workers: number; durationMs: number }>
}

function events(workerId: number): DcbEvent[] {
    return [{
        type: "EntityCreated",
        tags: Tags.fromObj({ entity: `W${workerId}` }),
        data: { workerId, ts: Date.now() },
        metadata: {},
    }]
}

function query(workerId: number): Query {
    return Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: `W${workerId}` }) }])
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []

    for (const tier of config.tiers) {
        const store = await factory(`throughput-${tier.workers}w-${Date.now()}`)
        const endTime = Date.now() + tier.durationMs

        const workers = await spawnWorkers(tier.workers, (id) =>
            conditionalRetryWorker(store, id, endTime, query(id), events),
        )

        scenarios.push(buildScenarioResult(
            `${tier.workers}-writers`,
            `${tier.workers} isolated writers, conditional appends`,
            tier.durationMs,
            workers,
        ))
    }

    const tier1Rate = scenarios[0].aggregate.eventsPerSec
    const tierNRate = scenarios[scenarios.length - 1].aggregate.eventsPerSec
    const scalingFactor = tierNRate / Math.max(tier1Rate, 1)

    return {
        id: "throughput-scaling",
        name: "Throughput Scaling",
        scenarios,
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
        pass: scalingFactor >= 2
            && scenarios.every(s => s.aggregate.latency.p99 < 25)
            && scenarios.every(s => s.aggregate.totalErrors === 0),
    }
}

export const throughputScaling: BenchScenario<Config> = {
    id: "throughput-scaling",
    name: "Throughput Scaling",
    description: "Scales writers from 1 to N on isolated scopes to verify linear throughput scaling",
    presets: {
        quick: {
            tiers: [
                { workers: 1, durationMs: 3_000 },
                { workers: 5, durationMs: 3_000 },
            ],
        },
        full: {
            tiers: [
                { workers: 1, durationMs: 10_000 },
                { workers: 5, durationMs: 10_000 },
                { workers: 10, durationMs: 10_000 },
            ],
        },
    },
    correctnessChecks: ["zero-errors"],
    run,
}

import { EventStore, Tags, Query, DcbEvent, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { entityOracleWorker, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    entityCount: number
    workerCount: number
    durationMs: number
}

function entityQuery(entityIndex: number): Query {
    const padded = String(entityIndex).padStart(3, "0")
    return Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: `E${padded}` }) }])
}

function entityEvents(workerId: number, entityIndex: number): DcbEvent[] {
    const padded = String(entityIndex).padStart(3, "0")
    return [{
        type: "EntityCreated",
        tags: Tags.fromObj({ entity: `E${padded}` }),
        data: { workerId, entityIndex, ts: Date.now() },
        metadata: {},
    }]
}

async function verify(store: EventStore, entityCount: number): Promise<VerificationResult> {
    let totalCreated = 0
    let duplicates = 0

    for (let i = 1; i <= entityCount; i++) {
        const events = await streamAllEventsToArray(store.read(entityQuery(i)))
        if (events.length > 1) duplicates += events.length - 1
        if (events.length >= 1) totalCreated++
    }

    return {
        checks: [
            {
                name: "zero-duplicates",
                expected: "0",
                actual: String(duplicates),
                pass: duplicates === 0,
            },
            {
                name: "entities-created",
                expected: `<= ${entityCount}`,
                actual: String(totalCreated),
                pass: totalCreated <= entityCount,
            },
        ],
    }
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory("consistency-oracle")
    const endTime = Date.now() + config.durationMs

    const workers = await spawnWorkers(config.workerCount, (id) =>
        entityOracleWorker(store, id, endTime, {
            entityCount: config.entityCount,
            eventFactory: entityEvents,
            readQuery: entityQuery,
        }),
    )

    const scenario = buildScenarioResult("consistency-oracle", "Consistency Oracle", config.durationMs, workers)
    const verification = await verify(store, config.entityCount)
    const pass = scenario.aggregate.totalErrors === 0 && verification.checks.every(c => c.pass)

    return { id: "consistency-oracle", name: "Consistency Oracle", scenarios: [scenario], verification, pass }
}

export const consistencyOracle: BenchScenario<Config> = {
    id: "consistency-oracle",
    name: "Consistency Oracle",
    description: "Concurrent workers race to create entities — verifies DCB conditions prevent duplicates",
    presets: {
        quick: { entityCount: 20, workerCount: 5, durationMs: 3_000 },
        full: { entityCount: 200, workerCount: 20, durationMs: 10_000 },
    },
    correctnessChecks: ["zero-duplicates"],
    run,
}

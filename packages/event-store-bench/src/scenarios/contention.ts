import { EventStore, Tags, Query, DcbEvent, streamAllEventsToArray } from "@dcb-es/event-store"
import { EventStoreFactory, WorkerResult } from "../harness/types"
import { buildScenarioResult } from "../harness/stats"
import { conditionalRetryWorker, spawnWorkers } from "../harness/workers"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    workerCount: number
    durationMs: number
}

const SHARED_QUERY = Query.fromItems([{ types: ["ThingCreated"], tags: Tags.fromObj({ thing: "CONTESTED" }) }])

function events(_workerId: number): DcbEvent[] {
    return [{
        type: "ThingCreated",
        tags: Tags.fromObj({ thing: "CONTESTED" }),
        data: { ts: Date.now() },
        metadata: {},
    }]
}

async function verify(store: EventStore, workers: WorkerResult[]): Promise<VerificationResult> {
    const all = await streamAllEventsToArray(store.read(SHARED_QUERY))
    const totalSuccessful = workers.reduce((sum, w) => sum + w.operations, 0)
    const stalled = workers.filter(w => w.operations === 0)

    return {
        checks: [
            {
                name: "event-count-matches-appends",
                expected: String(totalSuccessful),
                actual: String(all.length),
                pass: all.length === totalSuccessful,
            },
            {
                name: "all-workers-progressed",
                expected: "0 stalled",
                actual: `${stalled.length} stalled`,
                pass: stalled.length === 0,
            },
        ],
    }
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory(`contention-${Date.now()}`)
    const endTime = Date.now() + config.durationMs

    const workers = await spawnWorkers(config.workerCount, (id) =>
        conditionalRetryWorker(store, id, endTime, SHARED_QUERY, events),
    )

    const scenario = buildScenarioResult(
        "contention",
        `${config.workerCount} writers on single contested scope`,
        config.durationMs,
        workers,
    )

    const verification = await verify(store, workers)
    const pass = scenario.aggregate.totalErrors === 0
        && verification.checks.every(c => c.pass)

    return { id: "contention", name: "Contention", scenarios: [scenario], verification, pass }
}

export const contention: BenchScenario<Config> = {
    id: "contention",
    name: "Contention",
    description: "All workers fight over a single shared scope — tests conflict handling and retry logic",
    presets: {
        quick: { workerCount: 5, durationMs: 3_000 },
        full: { workerCount: 20, durationMs: 10_000 },
    },
    correctnessChecks: ["event-count-matches-appends"],
    run,
}

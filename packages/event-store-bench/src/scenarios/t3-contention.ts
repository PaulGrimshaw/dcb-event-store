import { EventStore, streamAllEventsToArray } from "@dcb-es/event-store"
import { StressTestResult, VerificationResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"
import { conditionalRetryWorker, verifyAllWorkersProgressed } from "./helpers"
import { t3EventFactory, t3QueryFactory } from "./factories"

export interface T3Config {
    workerCount: number
    durationMs: number
}

const DEFAULT_CONFIG: T3Config = { workerCount: 20, durationMs: 30_000 }

export async function run(factory: EventStoreFactory, config: T3Config = DEFAULT_CONFIG): Promise<StressTestResult> {
    const eventStore = await factory(`t3-${Date.now()}`)
    const endTime = Date.now() + config.durationMs
    const sharedQuery = t3QueryFactory(0)

    const workers = await Promise.all(
        Array.from({ length: config.workerCount }, (_, i) =>
            conditionalRetryWorker(eventStore, sharedQuery, t3EventFactory, i, endTime),
        ),
    )

    const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
    const scenario: ScenarioResult = {
        scenario: "T3-contention",
        description: `${config.workerCount} writers on single contested scope`,
        durationMs: config.durationMs,
        workers,
        aggregate: aggregateResults(workers, config.durationMs, totalEvents),
    }

    const verification = await verify(eventStore, workers)
    const pass = scenario.aggregate.totalErrors === 0
        && verification.checks.every(c => c.pass)

    return { id: "T3", name: "Contention Gauntlet", scenarios: [scenario], verification, pass }
}

async function verify(eventStore: EventStore, workers: WorkerResult[]): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []

    const query = t3QueryFactory(0)
    const events = await streamAllEventsToArray(eventStore.read(query))
    const totalSuccessful = workers.reduce((sum, w) => sum + w.operations, 0)

    checks.push({
        name: "event-count-matches-appends",
        expected: String(totalSuccessful),
        actual: String(events.length),
        pass: events.length === totalSuccessful,
    })

    checks.push(verifyAllWorkersProgressed(workers))

    return { checks }
}

import {
    EventStore,
    AppendConditionError,
    SequencePosition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult, VerificationResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults } from "../harness/stats"
import { t9EventFactory, t9ConditionQuery, t9ReadQuery } from "./factories"

interface T9Config {
    entityCount: number
    workerCount: number
    durationMs: number
}

const DEFAULT_CONFIG: T9Config = { entityCount: 200, workerCount: 20, durationMs: 60_000 }

async function t9Worker(
    eventStore: EventStore,
    workerId: number,
    entityCount: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    while (Date.now() < endTime) {
        const entityIndex = Math.floor(Math.random() * entityCount) + 1
        const readQuery = t9ReadQuery(entityIndex)

        try {
            const existing = await streamAllEventsToArray(eventStore.read(readQuery))
            if (existing.length > 0) continue

            const events = t9EventFactory(workerId, entityIndex)
            const condition = { failIfEventsMatch: t9ConditionQuery(entityIndex), after: SequencePosition.initial() }

            const opStart = Date.now()
            await eventStore.append({ events, condition })
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += events.length
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
            } else {
                result.errors++
            }
        }
    }

    return result
}

async function verify(eventStore: EventStore, entityCount: number): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []
    let totalCreated = 0
    let duplicates = 0

    for (let i = 1; i <= entityCount; i++) {
        const events = await streamAllEventsToArray(eventStore.read(t9ReadQuery(i)))
        if (events.length > 1) duplicates += events.length - 1
        if (events.length >= 1) totalCreated++
    }

    checks.push({
        name: "zero-duplicates",
        expected: "0",
        actual: String(duplicates),
        pass: duplicates === 0,
    })
    checks.push({
        name: "entities-created",
        expected: `<= ${entityCount}`,
        actual: String(totalCreated),
        pass: totalCreated <= entityCount,
    })

    return { checks }
}

export async function run(factory: EventStoreFactory, config: T9Config = DEFAULT_CONFIG): Promise<StressTestResult> {
    const eventStore = await factory("t9-overlap")
    const endTime = Date.now() + config.durationMs

    const workers = await Promise.all(
        Array.from({ length: config.workerCount }, (_, i) =>
            t9Worker(eventStore, i, config.entityCount, endTime),
        ),
    )

    const totalEvents = workers.reduce((sum, w) => sum + w.events, 0)
    const aggregate = aggregateResults(workers, config.durationMs, totalEvents)

    const scenario: ScenarioResult = {
        scenario: "T9",
        description: "Overlapping Scope Consistency (event tags > condition tags)",
        durationMs: config.durationMs,
        workers,
        aggregate,
    }

    const verification = await verify(eventStore, config.entityCount)
    const pass = aggregate.totalErrors === 0 && verification.checks.every(c => c.pass)

    return { id: "T9", name: "Overlapping Scope Consistency", scenarios: [scenario], verification, pass }
}

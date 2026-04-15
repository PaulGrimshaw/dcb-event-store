import {
    EventStore,
    AppendConditionError,
    Query,
    DcbEvent,
    SequencePosition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { aggregateResults } from "../harness/stats"
import { StressTestResult, VerificationResult } from "./stressTypes"

// ─── DCB Conditional Retry Worker ───────────────────────────────────

export async function conditionalRetryWorker(
    eventStore: EventStore,
    scopeQuery: Query,
    eventFactory: (workerId: number) => DcbEvent[],
    workerId: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    let lastPosition: SequencePosition | undefined

    while (Date.now() < endTime) {
        try {
            if (lastPosition === undefined) {
                const [latest] = await streamAllEventsToArray(
                    eventStore.read(scopeQuery, { backwards: true, limit: 1 }),
                )
                lastPosition = latest?.position ?? SequencePosition.initial()
            }

            const events = eventFactory(workerId)
            const condition = { failIfEventsMatch: scopeQuery, after: lastPosition }

            const opStart = Date.now()
            lastPosition = await eventStore.append({ events, condition })
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += events.length
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
                lastPosition = undefined
            } else {
                result.errors++
            }
        }
    }

    return result
}

// ─── Verification Helpers ───────────────────────────────────────────

export async function verifyEventCount(
    eventStore: EventStore,
    query: Query,
    expected: number,
): Promise<{ name: string; expected: string; actual: string; pass: boolean }> {
    const events = await streamAllEventsToArray(eventStore.read(query))
    return {
        name: "event-count",
        expected: String(expected),
        actual: String(events.length),
        pass: events.length === expected,
    }
}

export async function verifyNoDuplicates(
    eventStore: EventStore,
    query: Query,
    keyExtractor: (event: DcbEvent) => string,
): Promise<{ name: string; expected: string; actual: string; pass: boolean }> {
    const events = await streamAllEventsToArray(eventStore.read(query))
    const keys = events.map(e => keyExtractor(e.event))
    const uniqueKeys = new Set(keys)
    const duplicateCount = keys.length - uniqueKeys.size
    return {
        name: "no-duplicates",
        expected: "0",
        actual: String(duplicateCount),
        pass: duplicateCount === 0,
    }
}

export function verifyAllWorkersProgressed(
    workers: WorkerResult[],
): { name: string; expected: string; actual: string; pass: boolean } {
    const stalled = workers.filter(w => w.operations === 0)
    return {
        name: "all-workers-progressed",
        expected: "0 stalled",
        actual: `${stalled.length} stalled`,
        pass: stalled.length === 0,
    }
}

// ─── Tiered Scenario Runner ─────────────────────────────────────────

export interface TierConfig {
    name: string
    workers: number
    durationMs: number
}

export async function runTieredScenario(
    id: string,
    name: string,
    tiers: TierConfig[],
    runTier: (tier: TierConfig) => Promise<ScenarioResult>,
    verify?: () => Promise<VerificationResult>,
): Promise<StressTestResult> {
    const scenarios: ScenarioResult[] = []
    for (const tier of tiers) {
        const result = await runTier(tier)
        scenarios.push(result)
    }
    const verification = verify ? await verify() : undefined
    const pass = scenarios.every(s => s.aggregate.totalErrors === 0)
        && (!verification || verification.checks.every(c => c.pass))
    return { id, name, scenarios, verification, pass }
}

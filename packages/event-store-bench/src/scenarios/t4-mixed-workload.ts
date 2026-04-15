import {
    EventStore,
    AppendConditionError,
    SequencePosition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult } from "./stressTypes"
import { WorkerResult, ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { aggregateResults, computeLatencyStats } from "../harness/stats"
import { t4ImportBatchFactory, t4SmallWriteEventFactory, t4SmallWriteQueryFactory } from "./factories"

export interface T4Config {
    importTotalEvents: number
    importBatchSize: number
    importConcurrency: number
    smallWriterCount: number
    smallWriteRatePerSec: number
    durationMs: number
}

const DEFAULT_CONFIG: T4Config = {
    importTotalEvents: 50_000,
    importBatchSize: 1_000,
    importConcurrency: 2,
    smallWriterCount: 50,
    smallWriteRatePerSec: 10,
    durationMs: 90_000,
}

async function importWriter(
    eventStore: EventStore,
    config: T4Config,
    importDoneResolve: () => void,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId: -1, type: "import", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }

    const totalBatches = Math.ceil(config.importTotalEvents / config.importBatchSize)
    const concurrency = config.importConcurrency ?? 2

    let nextBatch = 0
    const worker = async () => {
        while (true) {
            const i = nextBatch++
            if (i >= totalBatches) break
            const batch = t4ImportBatchFactory(0, i, config.importBatchSize)
            const opStart = Date.now()
            try {
                await eventStore.append({ events: batch })
                result.latencies.push(Date.now() - opStart)
                result.operations++
                result.events += batch.length
            } catch {
                result.errors++
            }
        }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()))

    importDoneResolve()
    return result
}

async function smallWriter(
    eventStore: EventStore,
    workerId: number,
    ratePerSec: number,
    endTime: number,
    importDonePromise: Promise<void>,
): Promise<{ worker: WorkerResult; duringLatencies: number[]; afterLatencies: number[] }> {
    const worker: WorkerResult = {
        workerId, type: "write", operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [],
    }
    const duringLatencies: number[] = []
    const afterLatencies: number[] = []
    let importDone = false

    importDonePromise.then(() => { importDone = true })

    const query = t4SmallWriteQueryFactory(workerId)
    const intervalMs = 1000 / ratePerSec
    let lastPosition: SequencePosition | undefined

    while (Date.now() < endTime) {
        try {
            if (lastPosition === undefined) {
                const [latest] = await streamAllEventsToArray(
                    eventStore.read(query, { backwards: true, limit: 1 }),
                )
                lastPosition = latest?.position ?? SequencePosition.initial()
            }

            const condition = { failIfEventsMatch: query, after: lastPosition }

            const events = t4SmallWriteEventFactory(workerId)
            const opStart = Date.now()
            lastPosition = await eventStore.append({ events, condition })
            const latency = Date.now() - opStart

            worker.latencies.push(latency)
            worker.operations++
            worker.events += events.length

            if (importDone) {
                afterLatencies.push(latency)
            } else {
                duringLatencies.push(latency)
            }
        } catch (err) {
            if (err instanceof AppendConditionError) {
                worker.conflicts++
                lastPosition = undefined
            } else {
                worker.errors++
            }
        }

        await new Promise(resolve => setTimeout(resolve, intervalMs))
    }

    return { worker, duringLatencies, afterLatencies }
}

export async function run(factory: EventStoreFactory, config: T4Config = DEFAULT_CONFIG): Promise<StressTestResult> {
    const eventStore = await factory("t4-mixed-workload")
    const endTime = Date.now() + config.durationMs

    let importDoneResolve!: () => void
    const importDonePromise = new Promise<void>(r => { importDoneResolve = r })

    const startTime = Date.now()
    const importPromise = importWriter(eventStore, config, importDoneResolve)
    const smallWriterPromises = Array.from({ length: config.smallWriterCount }, (_, i) =>
        smallWriter(eventStore, i, config.smallWriteRatePerSec, endTime, importDonePromise),
    )

    const [importResult, ...smallResults] = await Promise.all([importPromise, ...smallWriterPromises])
    const actualDurationMs = Date.now() - startTime

    const allDuringLatencies = smallResults.flatMap(r => r.duringLatencies)
    const allAfterLatencies = smallResults.flatMap(r => r.afterLatencies)
    const allSmallWorkers = smallResults.map(r => r.worker)

    const duringStats = computeLatencyStats(allDuringLatencies)
    const afterStats = computeLatencyStats(allAfterLatencies)

    const importScenario: ScenarioResult = {
        scenario: "import",
        description: `Bulk import of ${config.importTotalEvents} events in batches of ${config.importBatchSize}`,
        durationMs: actualDurationMs,
        workers: [importResult],
        aggregate: aggregateResults([importResult], actualDurationMs, importResult.events),
    }

    const duringScenario: ScenarioResult = {
        scenario: "small-writers-during",
        description: `${config.smallWriterCount} writers at ${config.smallWriteRatePerSec}/sec during import`,
        durationMs: actualDurationMs,
        workers: allSmallWorkers,
        aggregate: {
            ...aggregateResults(allSmallWorkers, actualDurationMs, allSmallWorkers.reduce((s, w) => s + w.events, 0)),
            latency: duringStats,
        },
    }

    const afterScenario: ScenarioResult = {
        scenario: "small-writers-after",
        description: `${config.smallWriterCount} writers at ${config.smallWriteRatePerSec}/sec after import`,
        durationMs: actualDurationMs,
        workers: allSmallWorkers,
        aggregate: {
            ...aggregateResults(allSmallWorkers, actualDurationMs, allSmallWorkers.reduce((s, w) => s + w.events, 0)),
            latency: afterStats,
        },
    }

    const totalSmallErrors = allSmallWorkers.reduce((s, w) => s + w.errors, 0)
    const duringP99 = duringStats.p99
    const afterP99 = afterStats.p99

    const checks = [
        {
            name: "import-completed",
            expected: String(config.importTotalEvents),
            actual: String(importResult.events),
            pass: importResult.events === config.importTotalEvents && importResult.errors === 0,
        },
        {
            name: "small-writer-zero-errors",
            expected: "0",
            actual: String(totalSmallErrors),
            pass: totalSmallErrors === 0,
        },
        {
            name: "during-import-p99-under-25ms",
            expected: "< 25ms",
            actual: `${duringP99}ms`,
            pass: duringP99 < 25,
        },
        {
            name: "during-vs-after-p99-within-20pct",
            expected: `<= ${afterP99 > 0 ? Math.round(afterP99 * 1.2) : "N/A"}ms`,
            actual: `${duringP99}ms`,
            pass: afterP99 === 0 || duringP99 <= afterP99 * 1.2,
        },
    ]

    const pass = checks.every(c => c.pass)

    return {
        id: "T4",
        name: "Mixed Workload: Import + Concurrent Small Writes",
        scenarios: [importScenario, duringScenario, afterScenario],
        verification: { checks },
        pass,
    }
}

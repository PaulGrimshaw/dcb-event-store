import {
    EventStore,
    AppendConditionError,
    DcbEvent,
    Tags,
    Query,
    SequencePosition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { EventStoreFactory, WorkerResult } from "../harness/types"
import { aggregateResults, computeLatencyStats } from "../harness/stats"
import { emptyResult } from "../harness/workers"
import { BenchScenario, StressTestResult } from "./types"

interface Config {
    importTotalEvents: number
    importBatchSize: number
    importConcurrency: number
    smallWriterCount: number
    smallWriteRatePerSec: number
    durationMs: number
}

function importBatch(_workerId: number, batchIndex: number, batchSize: number): DcbEvent[] {
    const events: DcbEvent[] = []
    const start = batchIndex * batchSize
    for (let i = 0; i < batchSize; i++) {
        events.push({
            type: "MeterCreated",
            tags: Tags.fromObj({ meter: `A${start + i + 1}` }),
            data: { meterIndex: start + i + 1 },
            metadata: {},
        })
    }
    return events
}

async function runImport(
    store: EventStore,
    config: Config,
    done: () => void,
): Promise<WorkerResult> {
    const result = emptyResult(-1, "import")
    const totalBatches = Math.ceil(config.importTotalEvents / config.importBatchSize)
    let nextBatch = 0

    const worker = async () => {
        while (true) {
            const i = nextBatch++
            if (i >= totalBatches) break
            const batch = importBatch(0, i, config.importBatchSize)
            const opStart = Date.now()
            try {
                await store.append({ events: batch })
                result.latencies.push(Date.now() - opStart)
                result.operations++
                result.events += batch.length
            } catch {
                result.errors++
            }
        }
    }

    await Promise.all(Array.from({ length: config.importConcurrency }, () => worker()))
    done()
    return result
}

async function runSmallWriter(
    store: EventStore,
    workerId: number,
    ratePerSec: number,
    endTime: number,
    importDone: Promise<void>,
): Promise<{ worker: WorkerResult; duringLatencies: number[]; afterLatencies: number[] }> {
    const worker = emptyResult(workerId, "write")
    const duringLatencies: number[] = []
    const afterLatencies: number[] = []
    let isDone = false
    importDone.then(() => { isDone = true })

    const query = Query.fromItems([{ types: ["OrderPlaced"], tags: Tags.fromObj({ order: `B${workerId}` }) }])
    const intervalMs = 1000 / ratePerSec
    let lastPosition: SequencePosition | undefined

    while (Date.now() < endTime) {
        try {
            if (lastPosition === undefined) {
                const [latest] = await streamAllEventsToArray(
                    store.read(query, { backwards: true, limit: 1 }),
                )
                lastPosition = latest?.position ?? SequencePosition.initial()
            }

            const events: DcbEvent[] = [{
                type: "OrderPlaced",
                tags: Tags.fromObj({ order: `B${workerId}` }),
                data: { workerId, ts: Date.now() },
                metadata: {},
            }]

            const opStart = Date.now()
            lastPosition = await store.append({
                events,
                condition: { failIfEventsMatch: query, after: lastPosition },
            })
            const latency = Date.now() - opStart

            worker.latencies.push(latency)
            worker.operations++
            worker.events += events.length
            ;(isDone ? afterLatencies : duringLatencies).push(latency)
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

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const store = await factory("mixed-workload")
    const endTime = Date.now() + config.durationMs

    let importDoneResolve!: () => void
    const importDonePromise = new Promise<void>(r => { importDoneResolve = r })

    const startTime = Date.now()
    const importPromise = runImport(store, config, importDoneResolve)
    const writerPromises = Array.from({ length: config.smallWriterCount }, (_, i) =>
        runSmallWriter(store, i, config.smallWriteRatePerSec, endTime, importDonePromise),
    )

    const [importResult, ...smallResults] = await Promise.all([importPromise, ...writerPromises])
    const actualDurationMs = Date.now() - startTime

    const allSmallWorkers = smallResults.map(r => r.worker)
    const duringStats = computeLatencyStats(smallResults.flatMap(r => r.duringLatencies))
    const afterStats = computeLatencyStats(smallResults.flatMap(r => r.afterLatencies))

    const totalSmallErrors = allSmallWorkers.reduce((s, w) => s + w.errors, 0)

    return {
        id: "mixed-workload",
        name: "Mixed Workload",
        scenarios: [
            {
                scenario: "import",
                description: `Bulk import of ${config.importTotalEvents} events`,
                durationMs: actualDurationMs,
                workers: [importResult],
                aggregate: aggregateResults([importResult], actualDurationMs, importResult.events),
            },
            {
                scenario: "small-writers-during",
                description: `${config.smallWriterCount} writers during import`,
                durationMs: actualDurationMs,
                workers: allSmallWorkers,
                aggregate: {
                    ...aggregateResults(allSmallWorkers, actualDurationMs, allSmallWorkers.reduce((s, w) => s + w.events, 0)),
                    latency: duringStats,
                },
            },
            {
                scenario: "small-writers-after",
                description: `${config.smallWriterCount} writers after import`,
                durationMs: actualDurationMs,
                workers: allSmallWorkers,
                aggregate: {
                    ...aggregateResults(allSmallWorkers, actualDurationMs, allSmallWorkers.reduce((s, w) => s + w.events, 0)),
                    latency: afterStats,
                },
            },
        ],
        verification: {
            checks: [
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
                    actual: `${duringStats.p99}ms`,
                    pass: duringStats.p99 < 25,
                },
                {
                    name: "during-vs-after-p99-within-20pct",
                    expected: `<= ${afterStats.p99 > 0 ? Math.round(afterStats.p99 * 1.2) : "N/A"}ms`,
                    actual: `${duringStats.p99}ms`,
                    pass: afterStats.p99 === 0 || duringStats.p99 <= afterStats.p99 * 1.2,
                },
            ],
        },
        pass: importResult.events === config.importTotalEvents && totalSmallErrors === 0,
    }
}

export const mixedWorkload: BenchScenario<Config> = {
    id: "mixed-workload",
    name: "Mixed Workload",
    description: "Bulk import running concurrently with small conditional writes — measures latency impact",
    presets: {
        quick: {
            importTotalEvents: 1_000,
            importBatchSize: 250,
            importConcurrency: 2,
            smallWriterCount: 3,
            smallWriteRatePerSec: 10,
            durationMs: 5_000,
        },
        full: {
            importTotalEvents: 5_000,
            importBatchSize: 500,
            importConcurrency: 2,
            smallWriterCount: 5,
            smallWriteRatePerSec: 10,
            durationMs: 10_000,
        },
    },
    correctnessChecks: ["import-completed", "small-writer-zero-errors"],
    run,
}

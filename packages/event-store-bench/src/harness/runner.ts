import { EventStore, AppendConditionError, streamAllEventsToArray } from "@dcb-es/event-store"
import {
    ScenarioConfig,
    ScenarioResult,
    WorkerConfig,
    WriteWorkerConfig,
    ReadWorkerConfig,
    ImportWorkerConfig,
    WorkerResult,
} from "./types"
import { aggregateResults } from "./stats"

export async function runScenario(
    config: ScenarioConfig,
    eventStore: EventStore,
): Promise<ScenarioResult> {
    const warmupMs = config.warmupMs ?? 0

    const workerTasks: Array<{ config: WorkerConfig; workerId: number }> = []
    let workerIdCounter = 0
    for (const wc of config.workers) {
        for (let i = 0; i < wc.count; i++) {
            workerTasks.push({ config: wc, workerId: workerIdCounter++ })
        }
    }

    const startTime = Date.now()
    const endTime = startTime + warmupMs + config.durationMs

    const workerPromises = workerTasks.map(({ config: wc, workerId }) =>
        runWorker(wc, workerId, eventStore, startTime + warmupMs, endTime),
    )

    const workerResults = await Promise.all(workerPromises)

    const actualDurationMs = Date.now() - startTime - warmupMs
    const totalEvents = workerResults.reduce((sum, w) => {
        return sum + (w.events > 0 ? w.events : w.operations)
    }, 0)

    return {
        scenario: config.name,
        description: config.description,
        durationMs: actualDurationMs,
        workers: workerResults,
        aggregate: aggregateResults(workerResults, actualDurationMs, totalEvents),
    }
}

async function runWorker(
    config: WorkerConfig,
    workerId: number,
    eventStore: EventStore,
    measureStart: number,
    endTime: number,
): Promise<WorkerResult> {
    switch (config.type) {
        case "write":
            return runWriteWorker(config, workerId, eventStore, measureStart, endTime)
        case "read":
            return runReadWorker(config, workerId, eventStore, measureStart, endTime)
        case "import":
            return runImportWorker(config, workerId, eventStore)
    }
}

async function runWriteWorker(
    config: WriteWorkerConfig,
    workerId: number,
    eventStore: EventStore,
    measureStart: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId,
        type: "write",
        operations: 0,
        events: 0,
        errors: 0,
        conflicts: 0,
        latencies: [],
    }

    const intervalMs = config.targetRatePerSec
        ? 1000 / config.targetRatePerSec
        : 0

    let iteration = 0
    while (Date.now() < endTime) {
        const events = config.eventFactory(workerId, iteration)
        const condition = config.conditionFactory?.(workerId, iteration)

        const opStart = Date.now()
        try {
            await eventStore.append({ events, condition })
            const latency = Date.now() - opStart

            if (Date.now() >= measureStart) {
                result.operations++
                result.events += events.length
                result.latencies.push(latency)
            }
        } catch (err) {
            if (err instanceof AppendConditionError) {
                if (Date.now() >= measureStart) {
                    result.conflicts++
                }
            } else {
                if (Date.now() >= measureStart) {
                    result.errors++
                }
            }
        }

        iteration++

        if (intervalMs > 0) {
            const elapsed = Date.now() - opStart
            const sleepTime = intervalMs - elapsed
            if (sleepTime > 0) await sleep(sleepTime)
        }
    }

    return result
}

async function runReadWorker(
    config: ReadWorkerConfig,
    workerId: number,
    eventStore: EventStore,
    measureStart: number,
    endTime: number,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId,
        type: "read",
        operations: 0,
        events: 0,
        errors: 0,
        conflicts: 0,
        latencies: [],
    }

    let iteration = 0
    while (Date.now() < endTime) {
        const query = config.queryFactory(workerId, iteration)

        const opStart = Date.now()
        try {
            await streamAllEventsToArray(eventStore.read(query, config.readOptions))
            const latency = Date.now() - opStart

            if (Date.now() >= measureStart) {
                result.operations++
                result.latencies.push(latency)
            }
        } catch {
            if (Date.now() >= measureStart) {
                result.errors++
            }
        }

        iteration++
    }

    return result
}

async function runImportWorker(
    config: ImportWorkerConfig,
    workerId: number,
    eventStore: EventStore,
): Promise<WorkerResult> {
    const result: WorkerResult = {
        workerId,
        type: "import",
        operations: 0,
        events: 0,
        errors: 0,
        conflicts: 0,
        latencies: [],
    }

    const totalBatches = Math.ceil(config.totalEvents / config.batchSize)
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const events = config.batchFactory(workerId, batchIndex, config.batchSize)

        const opStart = Date.now()
        try {
            await eventStore.append({ events })
            const latency = Date.now() - opStart
            result.operations++
            result.events += events.length
            result.latencies.push(latency)
        } catch {
            result.errors++
        }
    }

    return result
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

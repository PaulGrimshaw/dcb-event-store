import {
    EventStore,
    AppendConditionError,
    DcbEvent,
    Query,
    SequencePosition,
    Tags,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { WorkerResult } from "./types"

export function emptyResult(workerId: number, type: WorkerResult["type"]): WorkerResult {
    return { workerId, type, operations: 0, events: 0, errors: 0, conflicts: 0, latencies: [] }
}

export function spawnWorkers(
    count: number,
    fn: (workerId: number) => Promise<WorkerResult>,
): Promise<WorkerResult[]> {
    return Promise.all(Array.from({ length: count }, (_, i) => fn(i)))
}

export async function conditionalRetryWorker(
    store: EventStore,
    workerId: number,
    endTime: number,
    query: Query,
    eventFactory: (workerId: number) => DcbEvent[],
): Promise<WorkerResult> {
    const result = emptyResult(workerId, "write")
    let lastPosition: SequencePosition | undefined

    while (Date.now() < endTime) {
        try {
            if (lastPosition === undefined) {
                const [latest] = await streamAllEventsToArray(
                    store.read(query, { backwards: true, limit: 1 }),
                )
                lastPosition = latest?.position ?? SequencePosition.initial()
            }

            const events = eventFactory(workerId)
            const opStart = Date.now()
            lastPosition = await store.append({
                events,
                condition: { failIfEventsMatch: query, after: lastPosition },
            })
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

export async function scopedBatchWriter(
    store: EventStore,
    workerId: number,
    endTime: number,
    opts: {
        batchSize: number
        eventType: string
        conditional: boolean
    },
): Promise<WorkerResult> {
    const result = emptyResult(workerId, "write")
    const scopeTag = `W${workerId}`
    const scopeQuery = opts.conditional
        ? Query.fromItems([{ types: [opts.eventType], tags: Tags.fromObj({ scope: scopeTag }) }])
        : undefined
    let lastPosition: SequencePosition | undefined
    let iteration = 0

    while (Date.now() < endTime) {
        const events = Array.from({ length: opts.batchSize }, (_, i) => ({
            type: opts.eventType,
            tags: Tags.fromObj({ scope: scopeTag }),
            data: { seq: iteration * opts.batchSize + i },
            metadata: {},
        }))

        const opStart = Date.now()
        try {
            if (scopeQuery) {
                if (lastPosition === undefined) {
                    const [latest] = await streamAllEventsToArray(
                        store.read(scopeQuery, { backwards: true, limit: 1 }),
                    )
                    lastPosition = latest?.position ?? SequencePosition.initial()
                }
                lastPosition = await store.append({
                    events,
                    condition: { failIfEventsMatch: scopeQuery, after: lastPosition },
                })
            } else {
                await store.append({ events })
            }
            result.latencies.push(Date.now() - opStart)
            result.operations++
            result.events += opts.batchSize
        } catch (err) {
            if (err instanceof AppendConditionError) {
                result.conflicts++
                lastPosition = undefined
            } else {
                result.errors++
            }
        }
        iteration++
    }

    return result
}

export async function entityOracleWorker(
    store: EventStore,
    workerId: number,
    endTime: number,
    opts: {
        entityCount: number
        eventFactory: (workerId: number, entityIndex: number) => DcbEvent[]
        readQuery: (entityIndex: number) => Query
        conditionQuery?: (entityIndex: number) => Query
    },
): Promise<WorkerResult> {
    const result = emptyResult(workerId, "write")

    while (Date.now() < endTime) {
        const entityIndex = Math.floor(Math.random() * opts.entityCount) + 1
        const query = opts.readQuery(entityIndex)

        try {
            const existing = await streamAllEventsToArray(store.read(query))
            if (existing.length > 0) continue

            const events = opts.eventFactory(workerId, entityIndex)
            const condQuery = opts.conditionQuery?.(entityIndex) ?? query

            const opStart = Date.now()
            await store.append({
                events,
                condition: { failIfEventsMatch: condQuery, after: SequencePosition.initial() },
            })
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

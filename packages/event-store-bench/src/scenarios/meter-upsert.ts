import {
    EventStore,
    Tags,
    Query,
    SequencePosition,
    AppendCondition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"
import { BenchScenario, StressTestResult, VerificationResult } from "./types"

interface Config {
    totalMeters: number
}

async function run(factory: EventStoreFactory, config: Config): Promise<StressTestResult> {
    const { totalMeters } = config
    const existingCount = Math.floor(totalMeters / 3)
    const neverExistedCount = Math.floor(totalMeters / 3)
    const deletedCount = totalMeters - existingCount - neverExistedCount

    const store = await factory(`meter-upsert-${Date.now()}`)
    const BATCH = 2_500

    const metersToCreate = existingCount + deletedCount
    for (let offset = 0; offset < metersToCreate; offset += BATCH) {
        const size = Math.min(BATCH, metersToCreate - offset)
        const events = Array.from({ length: size }, (_, i) => ({
            type: "MeterCreated",
            tags: Tags.fromObj({ serial: `M${offset + i}` }),
            data: { serial: offset + i },
            metadata: {},
        }))
        await store.append({ events })
    }

    for (let offset = 0; offset < deletedCount; offset += BATCH) {
        const size = Math.min(BATCH, deletedCount - offset)
        const events = Array.from({ length: size }, (_, i) => {
            const idx = existingCount + offset + i
            return {
                type: "MeterDeleted",
                tags: Tags.fromObj({ serial: `M${idx}` }),
                data: { serial: idx },
                metadata: {},
            }
        })
        await store.append({ events })
    }

    const meterQuery = Query.fromItems([{ types: ["MeterCreated", "MeterDeleted"] }])
    const readStart = Date.now()
    const allEvents = await streamAllEventsToArray(store.read(meterQuery))
    const readPosition = allEvents.length > 0
        ? allEvents[allEvents.length - 1].position
        : SequencePosition.initial()
    const readElapsed = Date.now() - readStart

    const meterState = new Map<string, "alive" | "deleted">()
    for (const evt of allEvents) {
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        if (evt.event.type === "MeterCreated") meterState.set(serial, "alive")
        else if (evt.event.type === "MeterDeleted") meterState.set(serial, "deleted")
    }

    const metersToUpsert: number[] = []
    for (let i = 0; i < totalMeters; i++) {
        if (meterState.get(`serial=M${i}`) !== "alive") metersToUpsert.push(i)
    }

    const writeStart = Date.now()
    const commands = metersToUpsert.map(idx => {
        const serial = `M${idx}`
        const condition: AppendCondition = {
            failIfEventsMatch: Query.fromItems([{ types: ["MeterCreated"], tags: Tags.fromObj({ serial }) }]),
            after: readPosition,
        }
        return {
            events: {
                type: "MeterCreated",
                tags: Tags.fromObj({ serial }),
                data: { serial: idx },
                metadata: {},
            },
            condition,
        }
    })
    await store.append(commands)
    const writeElapsed = Date.now() - writeStart
    const totalElapsed = readElapsed + writeElapsed

    const verification = await verify(store, totalMeters)

    return {
        id: "meter-upsert",
        name: "Meter Upsert",
        scenarios: [{
            scenario: "meter-upsert",
            description: `${totalMeters} meters: ${existingCount} existing, ${neverExistedCount} new, ${deletedCount} deleted -> ${metersToUpsert.length} upserted`,
            durationMs: totalElapsed,
            workers: [
                { workerId: 0, type: "read", operations: 1, events: allEvents.length, errors: 0, conflicts: 0, latencies: [readElapsed] },
                { workerId: 1, type: "write", operations: 1, events: metersToUpsert.length, errors: 0, conflicts: 0, latencies: [writeElapsed] },
            ],
            aggregate: {
                totalOperations: 2, totalErrors: 0, totalConflicts: 0,
                opsPerSec: Math.round(2 / (totalElapsed / 1000)),
                eventsPerSec: Math.round(metersToUpsert.length / (totalElapsed / 1000)),
                latency: computeLatencyStats([readElapsed, writeElapsed]),
            },
        }],
        verification,
        pass: verification.checks.every(c => c.pass),
    }
}

async function verify(store: EventStore, totalMeters: number): Promise<VerificationResult> {
    const query = Query.fromItems([{ types: ["MeterCreated", "MeterDeleted"] }])
    const allEvents = await streamAllEventsToArray(store.read(query))

    const state = new Map<string, "alive" | "deleted">()
    for (const evt of allEvents) {
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        if (evt.event.type === "MeterCreated") state.set(serial, "alive")
        else if (evt.event.type === "MeterDeleted") state.set(serial, "deleted")
    }

    const aliveCount = [...state.values()].filter(s => s === "alive").length

    const existingCount = Math.floor(totalMeters / 3)
    const deletedCount = totalMeters - existingCount - Math.floor(totalMeters / 3)
    const createdCounts = new Map<string, number>()
    for (const evt of allEvents) {
        if (evt.event.type !== "MeterCreated") continue
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        createdCounts.set(serial, (createdCounts.get(serial) ?? 0) + 1)
    }

    let duplicateErrors = 0
    for (let i = 0; i < totalMeters; i++) {
        const count = createdCounts.get(`serial=M${i}`) ?? 0
        const isDeleted = i >= existingCount && i < existingCount + deletedCount
        if (count !== (isDeleted ? 2 : 1)) duplicateErrors++
    }

    return {
        checks: [
            { name: "all-meters-alive", expected: String(totalMeters), actual: String(aliveCount), pass: aliveCount === totalMeters },
            { name: "no-duplicate-creates", expected: "0 errors", actual: `${duplicateErrors} errors`, pass: duplicateErrors === 0 },
        ],
    }
}

export const meterUpsert: BenchScenario<Config> = {
    id: "meter-upsert",
    name: "Meter Upsert",
    description: "Read-decide-write pattern: reads all meter state, decides which need creating, batch upserts with conditions",
    presets: {
        quick: { totalMeters: 90 },
        full: { totalMeters: 300 },
    },
    correctnessChecks: ["all-meters-alive", "no-duplicate-creates"],
    run,
}

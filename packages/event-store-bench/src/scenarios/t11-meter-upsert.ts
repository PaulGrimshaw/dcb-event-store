import {
    EventStore,
    Tags,
    Query,
    SequencePosition,
    AppendCondition,
    streamAllEventsToArray,
} from "@dcb-es/event-store"
import { StressTestResult, VerificationResult } from "./stressTypes"
import { ScenarioResult } from "../harness/types"
import { EventStoreFactory } from "../harness/types"
import { computeLatencyStats } from "../harness/stats"

export interface T11Config {
    totalMeters: number
}

const DEFAULT_CONFIG: T11Config = { totalMeters: 25_000 }

export async function run(
    factory: EventStoreFactory,
    config: T11Config = DEFAULT_CONFIG,
): Promise<StressTestResult> {
    const { totalMeters } = config
    const existingCount = Math.floor(totalMeters / 3)
    const neverExistedCount = Math.floor(totalMeters / 3)
    const deletedCount = totalMeters - existingCount - neverExistedCount

    const eventStore = await factory(`t11-${Date.now()}`)

    // Phase 1: Setup
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
        await eventStore.append({ events })
    }

    for (let offset = 0; offset < deletedCount; offset += BATCH) {
        const size = Math.min(BATCH, deletedCount - offset)
        const events = Array.from({ length: size }, (_, i) => {
            const meterIdx = existingCount + offset + i
            return {
                type: "MeterDeleted",
                tags: Tags.fromObj({ serial: `M${meterIdx}` }),
                data: { serial: meterIdx },
                metadata: {},
            }
        })
        await eventStore.append({ events })
    }

    // Phase 2: Read
    const readStart = Date.now()

    const meterQuery = Query.fromItems([
        { types: ["MeterCreated", "MeterDeleted"] },
    ])

    const allEvents = await streamAllEventsToArray(eventStore.read(meterQuery))
    const readPosition = allEvents.length > 0
        ? allEvents[allEvents.length - 1].position
        : SequencePosition.initial()

    const readElapsed = Date.now() - readStart

    // Phase 3: Decide
    const decideStart = Date.now()

    const meterState = new Map<string, "alive" | "deleted">()
    for (const evt of allEvents) {
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        if (evt.event.type === "MeterCreated") {
            meterState.set(serial, "alive")
        } else if (evt.event.type === "MeterDeleted") {
            meterState.set(serial, "deleted")
        }
    }

    const metersToUpsert: number[] = []
    for (let i = 0; i < totalMeters; i++) {
        const state = meterState.get(`serial=M${i}`)
        if (state !== "alive") {
            metersToUpsert.push(i)
        }
    }

    const decideElapsed = Date.now() - decideStart

    // Phase 4: Write
    const writeStart = Date.now()

    const commands = metersToUpsert.map(meterIdx => {
        const serial = `M${meterIdx}`
        const condition: AppendCondition = {
            failIfEventsMatch: Query.fromItems([{
                types: ["MeterCreated"],
                tags: Tags.fromObj({ serial }),
            }]),
            after: readPosition,
        }
        return {
            events: {
                type: "MeterCreated",
                tags: Tags.fromObj({ serial }),
                data: { serial: meterIdx },
                metadata: {},
            },
            condition,
        }
    })

    await eventStore.append(commands)

    const writeElapsed = Date.now() - writeStart
    const totalElapsed = readElapsed + decideElapsed + writeElapsed

    const scenario: ScenarioResult = {
        scenario: "T11-meter-upsert",
        description: `${totalMeters} meters: ${existingCount} existing, ${neverExistedCount} new, ${deletedCount} deleted -> ${metersToUpsert.length} upserted`,
        durationMs: totalElapsed,
        workers: [
            {
                workerId: 0,
                type: "read" as const,
                operations: 1,
                events: allEvents.length,
                errors: 0,
                conflicts: 0,
                latencies: [readElapsed],
            },
            {
                workerId: 1,
                type: "write" as const,
                operations: 1,
                events: metersToUpsert.length,
                errors: 0,
                conflicts: 0,
                latencies: [writeElapsed],
            },
        ],
        aggregate: {
            totalOperations: 2,
            totalErrors: 0,
            totalConflicts: 0,
            opsPerSec: Math.round(2 / (totalElapsed / 1000)),
            eventsPerSec: Math.round(metersToUpsert.length / (totalElapsed / 1000)),
            latency: computeLatencyStats([readElapsed, writeElapsed]),
        },
    }

    const verification = await verify(eventStore, totalMeters)
    const pass = verification.checks.every(c => c.pass)

    return { id: "T11", name: "Meter Upsert", scenarios: [scenario], verification, pass }
}

async function verify(
    eventStore: EventStore,
    totalMeters: number,
): Promise<VerificationResult> {
    const checks: VerificationResult["checks"] = []

    const query = Query.fromItems([{ types: ["MeterCreated", "MeterDeleted"] }])
    const allEvents = await streamAllEventsToArray(eventStore.read(query))

    const state = new Map<string, "alive" | "deleted">()
    for (const evt of allEvents) {
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        if (evt.event.type === "MeterCreated") state.set(serial, "alive")
        else if (evt.event.type === "MeterDeleted") state.set(serial, "deleted")
    }

    const aliveCount = [...state.values()].filter(s => s === "alive").length

    checks.push({
        name: "all-meters-alive",
        expected: String(totalMeters),
        actual: String(aliveCount),
        pass: aliveCount === totalMeters,
    })

    const createdCounts = new Map<string, number>()
    for (const evt of allEvents) {
        if (evt.event.type !== "MeterCreated") continue
        const serial = evt.event.tags.values.find(t => t.startsWith("serial="))
        if (!serial) continue
        createdCounts.set(serial, (createdCounts.get(serial) ?? 0) + 1)
    }

    const existingCount = Math.floor(totalMeters / 3)
    const deletedCount = totalMeters - existingCount - Math.floor(totalMeters / 3)
    let duplicateErrors = 0

    for (let i = 0; i < totalMeters; i++) {
        const serial = `serial=M${i}`
        const count = createdCounts.get(serial) ?? 0
        const isDeleted = i >= existingCount && i < existingCount + deletedCount
        const expectedCreates = isDeleted ? 2 : 1
        if (count !== expectedCreates) duplicateErrors++
    }

    checks.push({
        name: "no-duplicate-creates",
        expected: "0 errors",
        actual: `${duplicateErrors} errors`,
        pass: duplicateErrors === 0,
    })

    return { checks }
}

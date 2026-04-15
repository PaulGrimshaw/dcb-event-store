import { DcbEvent, AppendCommand, Tags, Query, SequencePosition, AppendCondition } from "@dcb-es/event-store"

// ─── T1: Throughput Baseline ────────────────────────────────────────

export function t1EventFactory(workerId: number): DcbEvent[] {
    return [{
        type: "EntityCreated",
        tags: Tags.fromObj({ entity: `W${workerId}` }),
        data: { workerId, ts: Date.now() },
        metadata: {},
    }]
}

export function t1QueryFactory(workerId: number): Query {
    return Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: `W${workerId}` }) }])
}

// ─── T3: Contention Gauntlet ────────────────────────────────────────

export function t3EventFactory(_workerId: number): DcbEvent[] {
    return [{
        type: "ThingCreated",
        tags: Tags.fromObj({ thing: "CONTESTED" }),
        data: { ts: Date.now() },
        metadata: {},
    }]
}

export function t3QueryFactory(_workerId: number): Query {
    return Query.fromItems([{ types: ["ThingCreated"], tags: Tags.fromObj({ thing: "CONTESTED" }) }])
}

// ─── T4: Mixed Workload — Import phase ──────────────────────────────

export function t4ImportBatchFactory(_workerId: number, batchIndex: number, batchSize: number): DcbEvent[] {
    const events: DcbEvent[] = []
    const startIndex = batchIndex * batchSize
    for (let i = 0; i < batchSize; i++) {
        const n = startIndex + i + 1
        events.push({
            type: "MeterCreated",
            tags: Tags.fromObj({ meter: `A${n}` }),
            data: { meterIndex: n },
            metadata: {},
        })
    }
    return events
}

// ─── T4: Mixed Workload — Small writes ──────────────────────────────

export function t4SmallWriteEventFactory(workerId: number): DcbEvent[] {
    return [{
        type: "OrderPlaced",
        tags: Tags.fromObj({ order: `B${workerId}` }),
        data: { workerId, ts: Date.now() },
        metadata: {},
    }]
}

export function t4SmallWriteQueryFactory(workerId: number): Query {
    return Query.fromItems([{ types: ["OrderPlaced"], tags: Tags.fromObj({ order: `B${workerId}` }) }])
}

// ─── Meter Import (shared by T2, T6) ────────────────────────────────

export const EVENTS_PER_METER = 11

const METER_FIELD_EVENTS = [
    "FirmwareVersionSet",
    "LocationSet",
    "CommissionDateSet",
    "TariffAssigned",
    "NetworkJoined",
    "InitialReadingSet",
    "CalibrationRecorded",
    "ActivationCompleted",
    "MeterReady",
] as const

export function buildMeterCommands(meterStart: number, meterEnd: number): AppendCommand[] {
    const commands: AppendCommand[] = []

    for (let m = meterStart; m < meterEnd; m++) {
        const meterId = `M${String(m + 1).padStart(5, "0")}`
        const serialNumber = `SN${String(m + 1).padStart(8, "0")}`

        commands.push({
            events: [{
                type: "MeterCreated",
                tags: Tags.fromObj({ meterId }),
                data: { meterId, serialNumber },
                metadata: {},
            }],
            condition: {
                failIfEventsMatch: Query.fromItems([
                    { types: ["MeterCreated"], tags: Tags.fromObj({ meterId }) },
                ]),
                after: SequencePosition.initial(),
            },
        })

        commands.push({
            events: [{
                type: "SerialNumberSet",
                tags: Tags.fromObj({ meterId, serialNumber }),
                data: { meterId, serialNumber },
                metadata: {},
            }],
            condition: {
                failIfEventsMatch: Query.fromItems([
                    { types: ["SerialNumberSet"], tags: Tags.fromObj({ meterId, serialNumber }) },
                ]),
                after: SequencePosition.initial(),
            },
        })

        const fieldEvents: DcbEvent[] = METER_FIELD_EVENTS.map(type => ({
            type,
            tags: Tags.fromObj(
                type === "InitialReadingSet" ? { meterId, serialNumber } : { meterId },
            ),
            data: { meterId, serialNumber, field: type },
            metadata: {},
        }))

        commands.push({ events: fieldEvents })
    }

    return commands
}

// ─── T8: Consistency Oracle ─────────────────────────────────────────

export function t8EventFactory(workerId: number, entityIndex: number): DcbEvent[] {
    const padded = String(entityIndex).padStart(3, "0")
    return [{
        type: "EntityCreated",
        tags: Tags.fromObj({ entity: `E${padded}` }),
        data: { workerId, entityIndex, ts: Date.now() },
        metadata: {},
    }]
}

export function t8QueryFactory(entityIndex: number): Query {
    const padded = String(entityIndex).padStart(3, "0")
    return Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: `E${padded}` }) }])
}

// ─── T9: Overlapping Scope Consistency ──────────────────────────────

export function t9EventFactory(workerId: number, entityIndex: number): DcbEvent[] {
    const padded = String(entityIndex).padStart(3, "0")
    return [{
        type: "EntityRegistered",
        tags: Tags.fromObj({ entity: `E${padded}`, worker: `W${workerId}` }),
        data: { workerId, entityIndex, ts: Date.now() },
        metadata: {},
    }]
}

export function t9ConditionQuery(entityIndex: number): Query {
    const padded = String(entityIndex).padStart(3, "0")
    return Query.fromItems([{ types: ["EntityRegistered"], tags: Tags.fromObj({ entity: `E${padded}` }) }])
}

export function t9ReadQuery(entityIndex: number): Query {
    const padded = String(entityIndex).padStart(3, "0")
    return Query.fromItems([{ types: ["EntityRegistered"], tags: Tags.fromObj({ entity: `E${padded}` }) }])
}

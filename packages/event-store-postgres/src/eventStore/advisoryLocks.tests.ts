import { describe, it, expect } from "vitest"
import { Tags, Query, SequencePosition } from "@dcb-es/event-store"
import { computeLockKeys } from "./advisoryLocks.js"

describe("computeLockKeys", () => {
    it("isolated scopes produce different keys", () => {
        const event0 = { type: "EntityCreated", tags: Tags.fromObj({ entity: "W0" }), data: {}, metadata: {} }
        const event1 = { type: "EntityCreated", tags: Tags.fromObj({ entity: "W1" }), data: {}, metadata: {} }
        const cond0 = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "W0" }) }]),
            after: SequencePosition.initial()
        }
        const cond1 = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "W1" }) }]),
            after: SequencePosition.initial()
        }

        const keys0 = computeLockKeys([event0], cond0)
        const keys1 = computeLockKeys([event1], cond1)

        expect(keys0).not.toEqual(keys1)
    })

    it("same scope produces same key", () => {
        const event = { type: "ThingCreated", tags: Tags.fromObj({ thing: "CONTESTED" }), data: {}, metadata: {} }
        const cond = {
            failIfEventsMatch: Query.fromItems([
                { types: ["ThingCreated"], tags: Tags.fromObj({ thing: "CONTESTED" }) }
            ]),
            after: SequencePosition.initial()
        }

        expect(computeLockKeys([event], cond)).toEqual(computeLockKeys([event], cond))
    })

    it("overlapping tag between condition and event produces shared key", () => {
        const eventA = {
            type: "EntityCreated",
            tags: Tags.fromObj({ entity: "W1", order: "O1" }),
            data: {},
            metadata: {}
        }
        const keysA = computeLockKeys([eventA])

        const eventB = { type: "EntityCreated", tags: Tags.fromObj({ entity: "W1" }), data: {}, metadata: {} }
        const condB = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "W1" }) }]),
            after: SequencePosition.initial()
        }
        const keysB = computeLockKeys([eventB], condB)

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })

    // Validation of types+tags is now enforced globally by validateAppendCondition
    // at the store level. computeLockKeys trusts its precondition.

    it("returns unique bigint keys (no bucketing)", () => {
        const events = Array.from({ length: 100 }, (_, i) => ({
            type: "BulkEvent",
            tags: Tags.fromObj({ entity: `E${i}` }),
            data: {},
            metadata: {}
        }))

        const keys = computeLockKeys(events)

        expect(keys.length).toBe(100)
        for (const k of keys) {
            expect(typeof k).toBe("bigint")
        }
    })

    it("no false serialization — disjoint scopes produce zero shared keys", () => {
        const eventA = { type: "X", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const eventB = { type: "X", tags: Tags.fromObj({ b: "2" }), data: {}, metadata: {} }

        const keysA = computeLockKeys([eventA])
        const keysB = computeLockKeys([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared).toHaveLength(0)
    })
})

describe("per-pair locking — overlapping scopes share keys", () => {
    it("broad condition serializes with narrow-tagged event insert", () => {
        const condA = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "E1" }) }]),
            after: SequencePosition.initial()
        }
        const eventA = { type: "EntityCreated", tags: Tags.fromObj({ entity: "E1" }), data: {}, metadata: {} }
        const keysA = computeLockKeys([eventA], condA)

        const eventB = {
            type: "EntityCreated",
            tags: Tags.fromObj({ entity: "E1", category: "C1" }),
            data: {},
            metadata: {}
        }
        const keysB = computeLockKeys([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })

    it("two events with overlapping tags share a key", () => {
        const eventA = { type: "X", tags: Tags.fromObj({ a: "1", b: "2" }), data: {}, metadata: {} }
        const eventB = { type: "X", tags: Tags.fromObj({ a: "1", c: "3" }), data: {}, metadata: {} }

        const keysA = computeLockKeys([eventA])
        const keysB = computeLockKeys([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })
})

describe("FNV-1a hash properties (via computeLockKeys)", () => {
    it("is deterministic — same input always produces same key", () => {
        const evt = { type: "Evt", tags: Tags.fromObj({ id: "123" }), data: {}, metadata: {} }
        const keys1 = computeLockKeys([evt])
        const keys2 = computeLockKeys([evt])
        expect(keys1).toEqual(keys2)
    })

    it("produces signed 64-bit bigints", () => {
        const events = Array.from({ length: 200 }, (_, i) => ({
            type: `Type${i}`,
            tags: Tags.fromObj({ id: `${i}` }),
            data: {},
            metadata: {}
        }))
        const keys = computeLockKeys(events)
        for (const k of keys) {
            expect(k).toBeGreaterThanOrEqual(-9223372036854775808n)
            expect(k).toBeLessThanOrEqual(9223372036854775807n)
        }
    })

    it("has low collision rate across 1000 distinct pairs", () => {
        const events = Array.from({ length: 1000 }, (_, i) => ({
            type: "T",
            tags: Tags.fromObj({ id: `entity-${i}` }),
            data: {},
            metadata: {}
        }))
        const keys = computeLockKeys(events)
        // With 1000 keys in 2^64 space, collisions should be ~0
        expect(keys.length).toBe(1000)
    })
})

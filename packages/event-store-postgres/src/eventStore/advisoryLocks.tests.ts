import { describe, it, expect } from "vitest"
import { Tags, Query, SequencePosition } from "@dcb-es/event-store"
import { computeLockKeys } from "./advisoryLocks"

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

    it("rejects Query.all() conditions", () => {
        const event = { type: "Foo", tags: Tags.fromObj({ x: "1" }), data: {}, metadata: {} }
        const cond = { failIfEventsMatch: Query.all(), after: SequencePosition.initial() }

        expect(() => computeLockKeys([event], cond)).toThrow("Query.all() is not supported")
    })

    it("rejects condition items missing types", () => {
        const event = { type: "Foo", tags: Tags.fromObj({ x: "1" }), data: {}, metadata: {} }
        const cond = {
            failIfEventsMatch: Query.fromItems([{ tags: Tags.fromObj({ x: "1" }) }]),
            after: SequencePosition.initial()
        }

        expect(() => computeLockKeys([event], cond)).toThrow("at least one type and one tag")
    })

    it("rejects condition items missing tags", () => {
        const event = { type: "Foo", tags: Tags.fromObj({ x: "1" }), data: {}, metadata: {} }
        const cond = {
            failIfEventsMatch: Query.fromItems([{ types: ["Foo"] }]),
            after: SequencePosition.initial()
        }

        expect(() => computeLockKeys([event], cond)).toThrow("at least one type and one tag")
    })

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

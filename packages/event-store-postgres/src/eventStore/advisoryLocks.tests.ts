import { describe, it, expect } from "vitest"
import { Tags, Query, SequencePosition } from "@dcb-es/event-store"
import { computeWriterLockKeys, computeReaderLockKeys, GLOBAL_INTENT_KEY } from "./advisoryLocks.js"

const leafKeysOf = (...args: Parameters<typeof computeWriterLockKeys>) => computeWriterLockKeys(...args).leafX

describe("computeWriterLockKeys — leaf X-locks", () => {
    it("isolated scopes produce different leaf keys", () => {
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

        expect(leafKeysOf([event0], cond0)).not.toEqual(leafKeysOf([event1], cond1))
    })

    it("same scope produces same leaf keys", () => {
        const event = { type: "ThingCreated", tags: Tags.fromObj({ thing: "CONTESTED" }), data: {}, metadata: {} }
        const cond = {
            failIfEventsMatch: Query.fromItems([
                { types: ["ThingCreated"], tags: Tags.fromObj({ thing: "CONTESTED" }) }
            ]),
            after: SequencePosition.initial()
        }

        expect(leafKeysOf([event], cond)).toEqual(leafKeysOf([event], cond))
    })

    it("overlapping tag between condition and event produces shared leaf key", () => {
        const eventA = {
            type: "EntityCreated",
            tags: Tags.fromObj({ entity: "W1", order: "O1" }),
            data: {},
            metadata: {}
        }
        const keysA = leafKeysOf([eventA])

        const eventB = { type: "EntityCreated", tags: Tags.fromObj({ entity: "W1" }), data: {}, metadata: {} }
        const condB = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "W1" }) }]),
            after: SequencePosition.initial()
        }
        const keysB = leafKeysOf([eventB], condB)

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })

    it("returns unique leaf keys (no bucketing)", () => {
        const events = Array.from({ length: 100 }, (_, i) => ({
            type: "BulkEvent",
            tags: Tags.fromObj({ entity: `E${i}` }),
            data: {},
            metadata: {}
        }))

        const keys = leafKeysOf(events)

        expect(keys.length).toBe(100)
        for (const k of keys) {
            expect(typeof k).toBe("bigint")
        }
    })

    it("no false serialization — disjoint leaf scopes produce zero shared keys", () => {
        const eventA = { type: "X", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const eventB = { type: "X", tags: Tags.fromObj({ b: "2" }), data: {}, metadata: {} }

        const keysA = leafKeysOf([eventA])
        const keysB = leafKeysOf([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared).toHaveLength(0)
    })
})

describe("per-pair locking — overlapping scopes share leaf keys", () => {
    it("broad condition serializes with narrow-tagged event insert", () => {
        const condA = {
            failIfEventsMatch: Query.fromItems([{ types: ["EntityCreated"], tags: Tags.fromObj({ entity: "E1" }) }]),
            after: SequencePosition.initial()
        }
        const eventA = { type: "EntityCreated", tags: Tags.fromObj({ entity: "E1" }), data: {}, metadata: {} }
        const keysA = leafKeysOf([eventA], condA)

        const eventB = {
            type: "EntityCreated",
            tags: Tags.fromObj({ entity: "E1", category: "C1" }),
            data: {},
            metadata: {}
        }
        const keysB = leafKeysOf([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })

    it("two events with overlapping tags share a leaf key", () => {
        const eventA = { type: "X", tags: Tags.fromObj({ a: "1", b: "2" }), data: {}, metadata: {} }
        const eventB = { type: "X", tags: Tags.fromObj({ a: "1", c: "3" }), data: {}, metadata: {} }

        const keysA = leafKeysOf([eventA])
        const keysB = leafKeysOf([eventB])

        const shared = keysA.filter(k => keysB.includes(k))
        expect(shared.length).toBeGreaterThan(0)
    })
})

describe("intent S-locks", () => {
    it("every writer takes the global intent key", () => {
        const event = { type: "T", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const { intentS } = computeWriterLockKeys([event])
        expect(intentS).toContain(GLOBAL_INTENT_KEY)
    })

    it("two writers of the same type share the type-intent key", () => {
        const a = { type: "T", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const b = { type: "T", tags: Tags.fromObj({ b: "2" }), data: {}, metadata: {} }
        const ia = computeWriterLockKeys([a]).intentS
        const ib = computeWriterLockKeys([b]).intentS
        const shared = ia.filter(k => ib.includes(k))
        expect(shared.length).toBeGreaterThanOrEqual(2)
    })

    it("writers of different types share only the global intent key", () => {
        const a = { type: "T1", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const b = { type: "T2", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        const ia = computeWriterLockKeys([a]).intentS
        const ib = computeWriterLockKeys([b]).intentS
        const shared = ia.filter(k => ib.includes(k))
        expect(shared).toEqual([GLOBAL_INTENT_KEY])
    })
})

describe("computeReaderLockKeys", () => {
    it("(T, t) filter takes S on leaf only", () => {
        const q = Query.fromItems([{ types: ["T"], tags: Tags.fromObj({ a: "1" }) }])
        const { leafS, intentX } = computeReaderLockKeys(q)
        expect(leafS.length).toBe(1)
        expect(intentX.length).toBe(0)
    })

    it("type-only filter takes X on type intent only", () => {
        const q = Query.fromItems([{ types: ["T"] }])
        const { leafS, intentX } = computeReaderLockKeys(q)
        expect(leafS.length).toBe(0)
        expect(intentX.length).toBe(1)
    })

    it("Query.all takes X on global intent only", () => {
        const { leafS, intentX } = computeReaderLockKeys(Query.all())
        expect(leafS.length).toBe(0)
        expect(intentX).toEqual([GLOBAL_INTENT_KEY])
    })

    it("reader (T, t) shares the leaf key with writer", () => {
        const q = Query.fromItems([{ types: ["T"], tags: Tags.fromObj({ a: "1" }) }])
        const writerLeaf = leafKeysOf([{ type: "T", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }])
        const readerLeaf = computeReaderLockKeys(q).leafS
        expect(readerLeaf).toEqual(writerLeaf)
    })

    it("reader type-only shares the type-intent key with writer", () => {
        const q = Query.fromItems([{ types: ["T"] }])
        const writerIntent = computeWriterLockKeys([
            { type: "T", tags: Tags.fromObj({ a: "1" }), data: {}, metadata: {} }
        ]).intentS
        const readerIntent = computeReaderLockKeys(q).intentX
        // type intent must be shared between reader and writer
        expect(readerIntent.every(k => writerIntent.includes(k))).toBe(true)
    })
})

describe("FNV-1a hash properties (via computeWriterLockKeys)", () => {
    it("is deterministic — same input always produces same key", () => {
        const evt = { type: "Evt", tags: Tags.fromObj({ id: "123" }), data: {}, metadata: {} }
        expect(leafKeysOf([evt])).toEqual(leafKeysOf([evt]))
    })

    it("produces signed 64-bit bigints", () => {
        const events = Array.from({ length: 200 }, (_, i) => ({
            type: `Type${i}`,
            tags: Tags.fromObj({ id: `${i}` }),
            data: {},
            metadata: {}
        }))
        const keys = leafKeysOf(events)
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
        const keys = leafKeysOf(events)
        // With 1000 keys in 2^64 space, collisions should be ~0
        expect(keys.length).toBe(1000)
    })
})

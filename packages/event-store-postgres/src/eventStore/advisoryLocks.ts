import { DcbEvent, AppendCondition, Query } from "@dcb-es/event-store"

/**
 * FNV-1a 64-bit hash → signed bigint for pg_advisory_xact_lock.
 *
 * BigInt arithmetic is slower than Math.imul but the hash is computed once
 * per append for a small number of inputs — sub-microsecond.
 */
const FNV1A_64_OFFSET = 0xcbf29ce484222325n
const FNV1A_64_PRIME = 0x100000001b3n
const MASK_64 = 0xffffffffffffffffn

function fnv1a64(input: string): bigint {
    let hash = FNV1A_64_OFFSET
    for (let i = 0; i < input.length; i++) {
        hash ^= BigInt(input.charCodeAt(i))
        hash = (hash * FNV1A_64_PRIME) & MASK_64
    }
    return hash > 0x7fffffffffffffffn ? hash - 0x10000000000000000n : hash
}

/**
 * Lock-key taxonomy. Three disjoint namespaces — distinct prefix bytes prevent
 * accidental collisions between leaf and intent keys.
 *
 * - Leaf K(T, t) — exclusive lock taken by writers per (type, tag) pair, and
 *   shared lock taken by `(T, t)`-filtered readers. Provides writer-vs-writer
 *   mutex (existing condition-conflict semantics) and per-scope read barrier.
 * - Type intent K(T, ε) — shared lock taken by writers per event type, and
 *   exclusive lock taken by type-only-filtered readers. Read barrier for
 *   filters that span every tag of a type.
 * - Global intent K(ε, ε) — shared lock taken by every writer, and exclusive
 *   lock taken by `Query.all()` readers. Read barrier for the whole stream.
 *
 * Intent locks are taken in S-mode by writers so writers do not serialise
 * against each other; readers take them in X-mode for the brief barrier.
 */
const LEAF_PREFIX = "L:"
const TYPE_INTENT_PREFIX = "T:"
const GLOBAL_INTENT_INPUT = "G"

export const GLOBAL_INTENT_KEY: bigint = fnv1a64(GLOBAL_INTENT_INPUT)

export const leafKey = (type: string, tag: string): bigint => fnv1a64(`${LEAF_PREFIX}${type}|${tag}`)
export const typeIntentKey = (type: string): bigint => fnv1a64(`${TYPE_INTENT_PREFIX}${type}`)

/**
 * Writer keys: leaf X-locks for content mutex, intent S-locks for read barriers.
 * Intent S-locks are S/S compatible across writers so they don't serialise.
 */
export interface WriterLockKeys {
    leafX: bigint[]
    intentS: bigint[]
}

export function computeWriterLockKeys(events: DcbEvent[], condition?: AppendCondition): WriterLockKeys {
    const leaf = new Set<bigint>()
    const intent = new Set<bigint>()

    if (condition && !condition.failIfEventsMatch.isAll) {
        for (const item of condition.failIfEventsMatch.items) {
            for (const type of item.types) {
                for (const tag of item.tags?.values ?? []) {
                    leaf.add(leafKey(type, tag))
                }
            }
        }
    }

    for (const evt of events) {
        intent.add(typeIntentKey(evt.type))
        for (const tag of evt.tags.values) {
            leaf.add(leafKey(evt.type, tag))
        }
    }

    if (events.length > 0) intent.add(GLOBAL_INTENT_KEY)

    return { leafX: [...leaf], intentS: [...intent] }
}

/**
 * Reader keys derived from a Query filter. Shape determines lock mode:
 * - `(types, tags)` items take S on each leaf K(T, t) — narrow barrier.
 * - `(types, no tags)` items take X on each type intent K(T, ε).
 * - `Query.all()` takes X on the global intent K(ε, ε).
 *
 * Tag-only items are rejected at `Query.fromItems` validation.
 */
export interface ReaderLockKeys {
    leafS: bigint[]
    intentX: bigint[]
}

export function computeReaderLockKeys(query: Query): ReaderLockKeys {
    if (query.isAll) return { leafS: [], intentX: [GLOBAL_INTENT_KEY] }

    const leaf = new Set<bigint>()
    const intent = new Set<bigint>()

    for (const item of query.items) {
        const tags = item.tags?.values ?? []
        if (tags.length > 0) {
            for (const type of item.types) {
                for (const tag of tags) leaf.add(leafKey(type, tag))
            }
        } else {
            for (const type of item.types) intent.add(typeIntentKey(type))
        }
    }

    return { leafS: [...leaf], intentX: [...intent] }
}

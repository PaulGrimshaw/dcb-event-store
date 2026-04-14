import { DcbEvent, AppendCondition } from "@dcb-es/event-store"

/**
 * Compute advisory lock keys for a transaction.
 *
 * Each (type, tag) pair hashes to a signed 64-bit integer via FNV-1a.
 * pg_advisory_xact_lock accepts bigint — 64-bit gives ~1/2^64 collision
 * probability per pair, eliminating false serialisation even at very
 * high type×tag cardinalities.
 *
 * No bucketing — zero false serialisation. Only exact (type, tag)
 * overlaps between transactions cause serialisation.
 *
 * Keys are NOT sorted client-side — Postgres ORDER BY in the acquisition
 * query handles deadlock prevention.
 *
 * Precondition: condition items must have types + tags (enforced by validateAppendCondition).
 */
export function computeLockKeys(events: DcbEvent[], condition?: AppendCondition): bigint[] {
    const keys = new Set<bigint>()

    if (condition && !condition.failIfEventsMatch.isAll) {
        for (const item of condition.failIfEventsMatch.items) {
            for (const type of item.types ?? []) {
                for (const tag of item.tags?.values ?? []) {
                    keys.add(fnv1a64(`${type}|${tag}`))
                }
            }
        }
    }

    for (const evt of events) {
        for (const tag of evt.tags.values) {
            keys.add(fnv1a64(`${evt.type}|${tag}`))
        }
    }

    return [...keys]
}

/**
 * FNV-1a 64-bit hash → signed bigint for pg_advisory_xact_lock.
 *
 * BigInt arithmetic is slower than Math.imul but the hash is computed once
 * per append for a small number of (type, tag) pairs — sub-microsecond.
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
    // Convert unsigned 64-bit to signed (Postgres bigint is signed)
    return hash > 0x7fffffffffffffffn ? hash - 0x10000000000000000n : hash
}

import { DcbEvent, AppendCondition } from "@dcb-es/event-store"

/**
 * Compute advisory lock keys for a transaction.
 *
 * Each (type, tag) pair hashes to a signed 32-bit integer via FNV-1a.
 * pg_advisory_xact_lock accepts bigint but 32-bit range is sufficient —
 * collision probability is ~1/2^32 per pair, negligible for typical cardinality.
 *
 * No bucketing — zero false serialisation. Only exact (type, tag)
 * overlaps between transactions cause serialisation.
 *
 * Keys are NOT sorted client-side — Postgres ORDER BY in the acquisition
 * query handles deadlock prevention.
 */
export function computeLockKeys(events: DcbEvent[], condition?: AppendCondition): bigint[] {
    const keys = new Set<bigint>()

    if (condition) {
        const query = condition.failIfEventsMatch
        if (query.isAll) {
            throw new Error("This event store requires scoped conditions. Query.all() is not supported.")
        }
        for (const item of query.items) {
            if (!item.types?.length || !item.tags || item.tags.values.length === 0) {
                throw new Error(
                    "This event store requires every condition query item to specify at least one type and one tag."
                )
            }
            for (const type of item.types) {
                for (const tag of item.tags.values) {
                    keys.add(fnv1a32(`${type}|${tag}`))
                }
            }
        }
    }

    for (const evt of events) {
        for (const tag of evt.tags.values) {
            keys.add(fnv1a32(`${evt.type}|${tag}`))
        }
    }

    return [...keys]
}

/**
 * FNV-1a 32-bit hash → signed bigint for pg_advisory_xact_lock.
 * Uses Math.imul for fast 32-bit multiply (10-100x faster than BigInt).
 */
function fnv1a32(input: string): bigint {
    let hash = 2166136261
    for (let i = 0; i < input.length; i++) {
        hash ^= input.charCodeAt(i)
        hash = Math.imul(hash, 16777619)
    }
    return BigInt(hash | 0)
}

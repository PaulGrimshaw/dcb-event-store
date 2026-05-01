import { ReaderLockKeys } from "./advisoryLocks.js"

const DEFAULT_MAX_ENTRIES = 1024

/**
 * In-process cache for the read barrier's high-water mark, keyed by the
 * reader's (filter-derived) lock-key set. Coalesces concurrent calls with the
 * same key set into one barrier round-trip via single-flight promises and
 * serves repeat calls within `ttlMs` from memory.
 *
 * Consistency: a cached `hwm` was a valid barrier output at fill time. Any
 * writer that started after fill has `seq > hwm`, so callers scanning with
 * `sequence_position <= hwm` cannot observe its events — same prefix invariant
 * as a freshly-acquired barrier. The TTL only bounds visibility lag (callers
 * see new commits at most `ttlMs` later), not correctness.
 *
 * Cross-filter safety: the key is derived from the full sorted key set, so two
 * different filter shapes never share an entry.
 *
 * Memory bound: capped at `maxEntries` entries with FIFO eviction. Eviction of
 * a still-referenced single-flight promise is harmless — existing awaiters
 * hold their own reference and resolve normally; only the cached lookup is
 * forgotten.
 */
export class HwmCache {
    private readonly entries = new Map<string, { hwm: Promise<bigint>; expiresAt: number }>()
    private readonly ttlMs: number
    private readonly maxEntries: number

    constructor(ttlMs: number, maxEntries: number = DEFAULT_MAX_ENTRIES) {
        if (!Number.isFinite(ttlMs) || ttlMs < 0) throw new Error(`ttlMs must be non-negative finite, got ${ttlMs}`)
        if (!Number.isFinite(maxEntries) || maxEntries < 1)
            throw new Error(`maxEntries must be >= 1, got ${maxEntries}`)
        this.ttlMs = ttlMs
        this.maxEntries = maxEntries
    }

    async get(keys: ReaderLockKeys, fetcher: () => Promise<bigint>): Promise<bigint> {
        if (this.ttlMs === 0) return fetcher()

        const cacheKey = computeCacheKey(keys)
        const now = Date.now()

        const existing = this.entries.get(cacheKey)
        if (existing && existing.expiresAt > now) {
            return existing.hwm
        }

        // Sweep expired entries opportunistically when the cache fills up; this
        // amortises cleanup across normal traffic and keeps the eviction step
        // (the next branch) from throwing away still-fresh entries when the
        // workload churns through filters.
        if (this.entries.size >= this.maxEntries) {
            for (const [k, v] of this.entries) {
                if (v.expiresAt <= now) this.entries.delete(k)
            }
        }
        // Hard size cap. FIFO order = Map insertion order. A still-pending
        // single-flight promise survives eviction via the awaiters' references.
        while (this.entries.size >= this.maxEntries) {
            const oldest = this.entries.keys().next().value
            if (oldest === undefined) break
            this.entries.delete(oldest)
        }

        const promise = fetcher()
        this.entries.set(cacheKey, { hwm: promise, expiresAt: now + this.ttlMs })
        // Don't poison the cache with rejected promises.
        promise.catch(() => {
            const current = this.entries.get(cacheKey)
            if (current?.hwm === promise) this.entries.delete(cacheKey)
        })
        return promise
    }

    /**
     * Drop every cached entry. In-flight single-flight promises continue and
     * resolve their existing awaiters with whatever hwm they fetched, but
     * subsequent calls miss the cache and trigger a fresh barrier — so any
     * caller that issues a read after a known mutation (a successful local
     * append, or a NOTIFY from another writer) observes the new state.
     */
    invalidateAll(): void {
        this.entries.clear()
    }
}

function computeCacheKey({ leafS, intentX }: ReaderLockKeys): string {
    // Sorted serialisation so two equivalent key sets produce the same cache
    // key regardless of insertion order. BigInts are stringified. The `|`
    // separator is reserved — bigint stringification cannot produce it — so
    // empty sets on either side (e.g. the literal `"|"` key for two empty
    // sets) cannot collide with a non-empty set's key.
    const sortBigInt = (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0)
    const leaf = [...leafS]
        .sort(sortBigInt)
        .map(k => k.toString())
        .join(",")
    const intent = [...intentX]
        .sort(sortBigInt)
        .map(k => k.toString())
        .join(",")
    return `${leaf}|${intent}`
}

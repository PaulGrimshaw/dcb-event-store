import { describe, it, expect } from "vitest"
import { HwmCache } from "./hwmCache.js"
import { ReaderLockKeys } from "./advisoryLocks.js"

const k = (leafS: bigint[], intentX: bigint[]): ReaderLockKeys => ({ leafS, intentX })

describe("HwmCache", () => {
    it("calls fetcher once per cache key within TTL", async () => {
        const cache = new HwmCache(1000)
        let calls = 0
        const fetcher = async () => {
            calls++
            return 42n
        }
        const keys = k([1n, 2n], [3n])

        const a = await cache.get(keys, fetcher)
        const b = await cache.get(keys, fetcher)
        expect(a).toBe(42n)
        expect(b).toBe(42n)
        expect(calls).toBe(1)
    })

    it("coalesces concurrent calls into one fetcher invocation (single-flight)", async () => {
        const cache = new HwmCache(1000)
        let calls = 0
        let resolveFetch: (v: bigint) => void = () => {}
        const fetcher = async () => {
            calls++
            return new Promise<bigint>(resolve => {
                resolveFetch = resolve
            })
        }
        const keys = k([1n], [])

        const p1 = cache.get(keys, fetcher)
        const p2 = cache.get(keys, fetcher)
        const p3 = cache.get(keys, fetcher)
        expect(calls).toBe(1)

        resolveFetch(99n)
        const [a, b, c] = await Promise.all([p1, p2, p3])
        expect([a, b, c]).toEqual([99n, 99n, 99n])
        expect(calls).toBe(1)
    })

    it("re-fetches after TTL expires", async () => {
        const cache = new HwmCache(20)
        let calls = 0
        const fetcher = async () => {
            calls++
            return BigInt(calls)
        }
        const keys = k([], [7n])

        expect(await cache.get(keys, fetcher)).toBe(1n)
        await new Promise(r => setTimeout(r, 30))
        expect(await cache.get(keys, fetcher)).toBe(2n)
    })

    it("uses separate entries for different filter shapes", async () => {
        const cache = new HwmCache(1000)
        const fetcher1 = async () => 10n
        const fetcher2 = async () => 20n

        const a = await cache.get(k([1n], []), fetcher1)
        const b = await cache.get(k([1n, 2n], []), fetcher2)
        expect(a).toBe(10n)
        expect(b).toBe(20n)
    })

    it("treats key sets as equal regardless of array order", async () => {
        const cache = new HwmCache(1000)
        let calls = 0
        const fetcher = async () => {
            calls++
            return 5n
        }

        await cache.get(k([3n, 1n, 2n], [9n, 8n]), fetcher)
        await cache.get(k([2n, 1n, 3n], [8n, 9n]), fetcher)
        expect(calls).toBe(1)
    })

    it("evicts a rejected promise so the next call retries", async () => {
        const cache = new HwmCache(1000)
        let calls = 0
        const fetcher = async () => {
            calls++
            if (calls === 1) throw new Error("transient")
            return 11n
        }
        const keys = k([1n], [])

        await expect(cache.get(keys, fetcher)).rejects.toThrow("transient")
        const v = await cache.get(keys, fetcher)
        expect(v).toBe(11n)
        expect(calls).toBe(2)
    })

    it("ttl=0 disables caching", async () => {
        const cache = new HwmCache(0)
        let calls = 0
        const fetcher = async () => {
            calls++
            return 1n
        }
        const keys = k([1n], [])

        await cache.get(keys, fetcher)
        await cache.get(keys, fetcher)
        expect(calls).toBe(2)
    })

    it("rejects negative or non-finite ttl", () => {
        expect(() => new HwmCache(-1)).toThrow()
        expect(() => new HwmCache(NaN)).toThrow()
        expect(() => new HwmCache(Infinity)).toThrow()
    })

    it("rejects maxEntries < 1", () => {
        expect(() => new HwmCache(100, 0)).toThrow()
        expect(() => new HwmCache(100, -5)).toThrow()
        expect(() => new HwmCache(100, NaN)).toThrow()
    })

    it("evicts oldest entries to honour maxEntries cap", async () => {
        const cache = new HwmCache(60_000, 3)
        const seenCalls: bigint[] = []
        const fetcher = (val: bigint) => async () => {
            seenCalls.push(val)
            return val
        }

        await cache.get(k([1n], []), fetcher(1n))
        await cache.get(k([2n], []), fetcher(2n))
        await cache.get(k([3n], []), fetcher(3n))
        await cache.get(k([4n], []), fetcher(4n)) // evicts [1n]

        // [1n] was evicted; re-fetching should call its fetcher again.
        await cache.get(k([1n], []), fetcher(11n))
        // [4n] should still be cached.
        await cache.get(k([4n], []), fetcher(44n))

        expect(seenCalls).toEqual([1n, 2n, 3n, 4n, 11n])
    })

    it("invalidateAll clears all entries", async () => {
        const cache = new HwmCache(60_000)
        let calls = 0
        const fetcher = async () => {
            calls++
            return 1n
        }

        await cache.get(k([1n], []), fetcher)
        await cache.get(k([2n], []), fetcher)
        cache.invalidateAll()
        await cache.get(k([1n], []), fetcher)
        await cache.get(k([2n], []), fetcher)

        expect(calls).toBe(4)
    })

    it("under churn (many distinct keys) cache stays bounded", async () => {
        const MAX = 32
        const cache = new HwmCache(60_000, MAX)
        for (let i = 0; i < 1000; i++) {
            await cache.get(k([BigInt(i)], []), async () => BigInt(i))
        }
        // Approximate size check via behaviour: an early key should have been evicted.
        let calls = 0
        await cache.get(k([0n], []), async () => {
            calls++
            return 0n
        })
        expect(calls).toBe(1)
    })

    it("invalidateAll during an in-flight fetcher does not block existing awaiters or share the in-flight promise", async () => {
        const cache = new HwmCache(60_000)
        let resolveSlow: (v: bigint) => void = () => {}
        const slowFetcher = () =>
            new Promise<bigint>(resolve => {
                resolveSlow = resolve
            })
        const keys = k([1n], [])

        const inflight = cache.get(keys, slowFetcher)

        cache.invalidateAll()

        // After invalidation, a new get() must NOT share the prior in-flight promise.
        let secondCalls = 0
        const second = cache.get(keys, async () => {
            secondCalls++
            return 200n
        })

        expect(secondCalls).toBe(1)
        expect(await second).toBe(200n)

        // Existing awaiter still resolves cleanly when its slow fetcher completes.
        resolveSlow(100n)
        expect(await inflight).toBe(100n)
    })

    it("concurrent get()s issued immediately after invalidateAll() coalesce into one fetcher", async () => {
        const cache = new HwmCache(60_000)
        let calls = 0
        let resolveFetch: (v: bigint) => void = () => {}
        const fetcher = async () => {
            calls++
            return new Promise<bigint>(resolve => {
                resolveFetch = resolve
            })
        }
        const keys = k([1n], [])

        // Prime the cache, then invalidate.
        await cache.get(keys, async () => 1n)
        cache.invalidateAll()

        // Two concurrent gets after invalidation should share a single new fetcher.
        const p1 = cache.get(keys, fetcher)
        const p2 = cache.get(keys, fetcher)
        expect(calls).toBe(1)

        resolveFetch(42n)
        expect(await p1).toBe(42n)
        expect(await p2).toBe(42n)
        expect(calls).toBe(1)
    })

    it("empty key set on both sides is a valid, unique cache key", async () => {
        const cache = new HwmCache(60_000)
        let calls = 0
        const fetcher = async () => {
            calls++
            return 7n
        }

        await cache.get(k([], []), fetcher)
        await cache.get(k([], []), fetcher) // hit
        expect(calls).toBe(1)

        // Different key (non-empty) must not collide with the empty one.
        await cache.get(k([1n], []), async () => 99n)
        expect(calls).toBe(1) // empty-key fetcher still only ran once
    })
})

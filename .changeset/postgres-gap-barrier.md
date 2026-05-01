---
"@dcb-es/event-store": patch
"@dcb-es/event-store-postgres": patch
---

Fix: subscribers could silently lose events under concurrent out-of-order commits against the Postgres adapter (issue #89).

Postgres `BIGSERIAL` allocates `sequence_position` at INSERT time but rows only become visible at COMMIT. Two writers with disjoint scopes committing out of allocation order could cause a subscriber to advance its bookmark past an earlier-allocated, later-committed writer's events.

Fixed via a lock-based read barrier with a hierarchical key taxonomy: writers take shared intent locks in addition to exclusive content locks, and readers take a brief exclusive/shared lock matching their filter shape before snapshotting `pg_sequence_last_value()`. Reads cap at the snapshotted high-water mark, so events from writers that started after the barrier (or are still in flight) appear on the next poll instead of being skipped.

Also includes:
- An in-process hwm cache (default 50ms TTL, 1024 entries) with NOTIFY-driven invalidation — coalesces concurrent barriers and recovers the read-side overhead for stable-filter subscribers.
- `Query.fromItems()` now requires non-empty `types` on every item (tag-only filters are rejected at construction). The writer SP signature added an intent-keys parameter — `ensureInstalled` migrates safely under a session-scoped advisory mutex.

No write-throughput regression: benchmarks on native PG show the 300k events/sec peak is preserved and conditional-append workloads improve 30–45% with the cache.

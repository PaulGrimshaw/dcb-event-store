import { Pool, PoolClient } from "pg"
import { DcbEvent, AppendCondition, Query } from "@dcb-es/event-store"
import { computeWriterLockKeys, computeReaderLockKeys, WriterLockKeys, ReaderLockKeys } from "./advisoryLocks.js"

/**
 * Strategy for acquiring scope-level locks for both writers and readers.
 *
 * Two built-in implementations:
 * - `advisoryLocks()` — Postgres advisory locks (in-memory, fast).
 * - `rowLocks()` — Row-level locks on a scope table (RDS Proxy compatible,
 *   Aurora Limitless ready).
 *
 * Writers acquire X-mode on leaf keys (content mutex) plus S-mode on intent
 * keys (read barrier visibility). Readers acquire S-mode on leaf keys for
 * `(T, t)` filters, X-mode on intent keys for type-only or `Query.all`
 * filters, then snapshot the high-water mark and release.
 */
export interface LockStrategy {
    computeWriterKeys(events: DcbEvent[], condition?: AppendCondition): WriterLockKeys
    computeReaderKeys(query: Query): ReaderLockKeys
    /** Writer-side acquisition: leaf X + intent S, all in sorted order. */
    acquireWriter(client: PoolClient, keys: WriterLockKeys, tableName: string): Promise<void>
    /** Reader-side barrier acquisition: leaf S + intent X. Caller releases by COMMIT. */
    acquireReader(client: PoolClient, keys: ReaderLockKeys, tableName: string): Promise<void>
    /** Inline lock block for the writer stored procedure body. */
    generateSpLockBlock(tableName: string): string
    /** Inline barrier-acquire block for the read-barrier stored procedure body. */
    generateBarrierLockBlock(tableName: string): string
    ensureSchema?(pool: Pool | PoolClient, tableName: string): Promise<void>
}

/** Advisory locks — fast, in-memory, no schema. Default for local/direct Postgres. */
export function advisoryLocks(): LockStrategy {
    return {
        computeWriterKeys: computeWriterLockKeys,
        computeReaderKeys: computeReaderLockKeys,
        acquireWriter: async (client, keys) => {
            // Single sorted statement across both modes to prevent cross-mode deadlocks
            // between concurrent acquirers.
            await client.query(
                `SELECT CASE WHEN excl THEN pg_advisory_xact_lock(k) ELSE pg_advisory_xact_lock_shared(k) END
                 FROM (
                   SELECT k, true AS excl FROM unnest($1::bigint[]) k
                   UNION ALL
                   SELECT k, false AS excl FROM unnest($2::bigint[]) k
                 ) t
                 ORDER BY k`,
                [keys.leafX, keys.intentS]
            )
        },
        acquireReader: async (client, keys) => {
            await client.query(
                `SELECT CASE WHEN excl THEN pg_advisory_xact_lock(k) ELSE pg_advisory_xact_lock_shared(k) END
                 FROM (
                   SELECT k, false AS excl FROM unnest($1::bigint[]) k
                   UNION ALL
                   SELECT k, true AS excl FROM unnest($2::bigint[]) k
                 ) t
                 ORDER BY k`,
                [keys.leafS, keys.intentX]
            )
        },
        generateSpLockBlock: () =>
            `PERFORM CASE WHEN excl THEN pg_advisory_xact_lock(k) ELSE pg_advisory_xact_lock_shared(k) END
             FROM (
               SELECT k, true AS excl FROM unnest(p_lock_keys) k
               UNION ALL
               SELECT k, false AS excl FROM unnest(p_intent_keys) k
             ) t
             ORDER BY k;`,
        generateBarrierLockBlock: () =>
            `PERFORM CASE WHEN excl THEN pg_advisory_xact_lock(k) ELSE pg_advisory_xact_lock_shared(k) END
             FROM (
               SELECT k, false AS excl FROM unnest(p_shared_keys) k
               UNION ALL
               SELECT k, true AS excl FROM unnest(p_exclusive_keys) k
             ) t
             ORDER BY k;`
    }
}

/** Row locks — lazy-upserted scope table, RDS or PG Proxy compatible. For Aurora deployments etc. */
export function rowLocks(): LockStrategy {
    return {
        computeWriterKeys: computeWriterLockKeys,
        computeReaderKeys: computeReaderLockKeys,
        acquireWriter: async (client, keys, tableName) => {
            const lockTable = `${tableName}_lock_scopes`
            const allKeys = [...keys.leafX, ...keys.intentS]
            if (allKeys.length === 0) return
            await client.query(
                `INSERT INTO ${lockTable} (scope_key) SELECT unnest($1::bigint[]) ON CONFLICT DO NOTHING`,
                [allKeys]
            )
            // Acquire FOR UPDATE on leaf keys then FOR SHARE on intent keys, each in
            // sorted key order. This is deadlock-safe because the leaf and intent
            // keyspaces are disjoint by construction (`advisoryLocks.ts` uses distinct
            // `L:` / `T:` / `G` prefixes before hashing). No transaction can hold a lock
            // in one keyspace while waiting for a lock in the same keyspace held in the
            // opposite mode by a peer, so cross-mode cycles cannot form.
            if (keys.leafX.length > 0) {
                await client.query(
                    `SELECT 1 FROM ${lockTable} WHERE scope_key = ANY($1::bigint[]) ORDER BY scope_key FOR UPDATE`,
                    [keys.leafX]
                )
            }
            if (keys.intentS.length > 0) {
                await client.query(
                    `SELECT 1 FROM ${lockTable} WHERE scope_key = ANY($1::bigint[]) ORDER BY scope_key FOR SHARE`,
                    [keys.intentS]
                )
            }
        },
        acquireReader: async (client, keys, tableName) => {
            const lockTable = `${tableName}_lock_scopes`
            const allKeys = [...keys.leafS, ...keys.intentX]
            if (allKeys.length === 0) return
            await client.query(
                `INSERT INTO ${lockTable} (scope_key) SELECT unnest($1::bigint[]) ON CONFLICT DO NOTHING`,
                [allKeys]
            )
            // Deadlock-safety argument is the same as `acquireWriter` above.
            if (keys.leafS.length > 0) {
                await client.query(
                    `SELECT 1 FROM ${lockTable} WHERE scope_key = ANY($1::bigint[]) ORDER BY scope_key FOR SHARE`,
                    [keys.leafS]
                )
            }
            if (keys.intentX.length > 0) {
                await client.query(
                    `SELECT 1 FROM ${lockTable} WHERE scope_key = ANY($1::bigint[]) ORDER BY scope_key FOR UPDATE`,
                    [keys.intentX]
                )
            }
        },
        generateSpLockBlock: tableName => {
            const lockTable = `${tableName}_lock_scopes`
            return `
                INSERT INTO ${lockTable} (scope_key)
                  SELECT k FROM (
                    SELECT unnest(p_lock_keys) k
                    UNION SELECT unnest(p_intent_keys)
                  ) t
                  ON CONFLICT DO NOTHING;
                PERFORM 1 FROM ${lockTable} WHERE scope_key = ANY(p_lock_keys) ORDER BY scope_key FOR UPDATE;
                PERFORM 1 FROM ${lockTable} WHERE scope_key = ANY(p_intent_keys) ORDER BY scope_key FOR SHARE;
            `
        },
        generateBarrierLockBlock: tableName => {
            const lockTable = `${tableName}_lock_scopes`
            return `
                INSERT INTO ${lockTable} (scope_key)
                  SELECT k FROM (
                    SELECT unnest(p_shared_keys) k
                    UNION SELECT unnest(p_exclusive_keys)
                  ) t
                  ON CONFLICT DO NOTHING;
                PERFORM 1 FROM ${lockTable} WHERE scope_key = ANY(p_shared_keys) ORDER BY scope_key FOR SHARE;
                PERFORM 1 FROM ${lockTable} WHERE scope_key = ANY(p_exclusive_keys) ORDER BY scope_key FOR UPDATE;
            `
        },
        ensureSchema: async (pool, tableName) => {
            const lockTable = `${tableName}_lock_scopes`
            await pool.query(`CREATE TABLE IF NOT EXISTS ${lockTable} (scope_key BIGINT PRIMARY KEY)`)
        }
    }
}

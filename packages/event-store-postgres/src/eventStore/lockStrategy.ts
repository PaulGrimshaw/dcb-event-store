import { Pool, PoolClient } from "pg"
import { DcbEvent, AppendCondition } from "@dcb-es/event-store"
import { computeLockKeys } from "./advisoryLocks"

/**
 * Strategy for acquiring scope-level locks during event appends.
 *
 * Two built-in implementations:
 * - `advisoryLocks()` — Postgres advisory locks (in-memory, fast, but causes RDS Proxy pinning)
 * - `rowLocks()` — Row-level locks on a scope table (RDS Proxy compatible, Aurora Limitless ready)
 */
export interface LockStrategy {
    computeKeys(events: DcbEvent[], condition?: AppendCondition): bigint[]
    acquire(client: PoolClient, keys: bigint[], tableName: string): Promise<void>
    generateSpLockBlock(tableName: string): string
    ensureSchema?(pool: Pool | PoolClient, tableName: string): Promise<void>
}

/** Advisory locks — fast, in-memory, no schema. Default for local/direct Postgres. */
export function advisoryLocks(): LockStrategy {
    return {
        computeKeys: computeLockKeys,
        acquire: async (client, keys) => {
            await client.query("SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k ORDER BY k", [keys])
        },
        generateSpLockBlock: () => "PERFORM pg_advisory_xact_lock(k) FROM unnest(p_lock_keys) AS k ORDER BY k;"
    }
}

/** Row locks — lazy-upserted scope table, RDS or PG Proxy compatible. For Aurora deployments etc. */
export function rowLocks(): LockStrategy {
    return {
        computeKeys: computeLockKeys,
        acquire: async (client, keys, tableName) => {
            const lockTable = `${tableName}_lock_scopes`
            await client.query(
                `INSERT INTO ${lockTable} (scope_key) SELECT unnest($1::bigint[]) ON CONFLICT DO NOTHING`,
                [keys]
            )
            await client.query(
                `SELECT 1 FROM ${lockTable} WHERE scope_key = ANY($1::bigint[]) ORDER BY scope_key FOR UPDATE`,
                [keys]
            )
        },
        generateSpLockBlock: tableName => {
            const lockTable = `${tableName}_lock_scopes`
            return `
                INSERT INTO ${lockTable} (scope_key) SELECT unnest(p_lock_keys) ON CONFLICT DO NOTHING;
                PERFORM 1 FROM ${lockTable} WHERE scope_key = ANY(p_lock_keys) ORDER BY scope_key FOR UPDATE;
            `
        },
        ensureSchema: async (pool, tableName) => {
            const lockTable = `${tableName}_lock_scopes`
            await pool.query(`CREATE TABLE IF NOT EXISTS ${lockTable} (scope_key BIGINT PRIMARY KEY)`)
        }
    }
}

import { Pool } from "pg"
import { SequencePosition } from "@dcb-es/event-store"
import { WaitTimeoutError } from "./WaitTimeoutError.js"

/**
 * Wait until a handler's bookmark has reached (or passed) the given position.
 *
 * Strategy: poll with short backoff first (avoids holding a LISTEN connection
 * for the common fast case), then fall back to LISTEN+poll for the slow case.
 * LISTEN is established before the first slow-path check to prevent the
 * TOCTOU race where a notification fires between check and LISTEN.
 */
export async function waitUntilProcessed(
    pool: Pool,
    handlerName: string,
    position: SequencePosition,
    options?: { timeoutMs?: number; bookmarkTableName?: string }
): Promise<void> {
    const timeoutMs = options?.timeoutMs ?? 5000
    const tableName = options?.bookmarkTableName ?? "_handler_bookmarks"
    const deadline = Date.now() + timeoutMs

    // Fast poll phase — most events process in <50ms, so avoid holding a LISTEN
    // connection for the common case. Three quick checks with backoff.
    const pollDelays = [5, 15, 30]
    for (const delay of pollDelays) {
        if (await hasReachedPosition(pool, tableName, handlerName, position)) return
        if (Date.now() + delay > deadline) break
        await new Promise(r => setTimeout(r, delay))
    }
    if (await hasReachedPosition(pool, tableName, handlerName, position)) return

    // Slow path: LISTEN first, then check — prevents lost notifications
    const client = await pool.connect()
    try {
        await client.query(`LISTEN ${tableName}`)

        while (Date.now() < deadline) {
            // Check AFTER LISTEN is established — no TOCTOU race
            if (await hasReachedPosition(pool, tableName, handlerName, position)) return

            const remaining = deadline - Date.now()
            if (remaining <= 0) break

            await new Promise<void>(resolve => {
                const timeout = setTimeout(resolve, Math.min(100, remaining))
                client.once("notification", () => {
                    clearTimeout(timeout)
                    resolve()
                })
            })
        }

        throw new WaitTimeoutError(handlerName, position.toString(), timeoutMs)
    } finally {
        await client.query(`UNLISTEN ${tableName}`).catch(() => {})
        client.release()
    }
}

// pool.query() internally acquires and releases its own connection
async function hasReachedPosition(
    pool: Pool,
    tableName: string,
    handlerName: string,
    position: SequencePosition
): Promise<boolean> {
    const result = await pool.query(`SELECT last_sequence_position FROM ${tableName} WHERE handler_id = $1`, [
        handlerName
    ])
    if (result.rows.length === 0) return false
    const current = SequencePosition.fromString(String(result.rows[0].last_sequence_position))
    return current.isAfter(position) || current.equals(position)
}

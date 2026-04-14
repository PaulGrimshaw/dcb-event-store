import { Pool } from "pg"
import { SequencePosition } from "@dcb-es/event-store"
import { WaitTimeoutError } from "./WaitTimeoutError"

/**
 * Wait until a handler's bookmark has reached (or passed) the given position.
 *
 * Fast path: if the bookmark is already at or past the position, returns immediately.
 * Slow path: LISTEN on the bookmark table channel, poll+notify loop until reached or timeout.
 */
export async function waitUntilProcessed(
    pool: Pool,
    handlerName: string,
    position: SequencePosition,
    options?: { timeoutMs?: number; bookmarkTableName?: string }
): Promise<void> {
    const timeoutMs = options?.timeoutMs ?? 5000
    const tableName = options?.bookmarkTableName ?? "_handler_bookmarks"

    // Fast path
    if (await hasReachedPosition(pool, tableName, handlerName, position)) return

    // Slow path: LISTEN + poll
    const client = await pool.connect()
    try {
        await client.query(`LISTEN ${tableName}`)

        const deadline = Date.now() + timeoutMs
        while (Date.now() < deadline) {
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

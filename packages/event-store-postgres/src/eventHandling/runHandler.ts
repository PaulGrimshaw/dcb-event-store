import { Pool, PoolClient } from "pg"
import { EventHandler, EventStore, Query, SequencePosition, Tags } from "@dcb-es/event-store"

export interface HandlerRunnerOptions {
    pool: Pool
    eventStore: EventStore
    handlerName: string
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    handlerFactory: (client: PoolClient) => EventHandler<any, any>
    bookmarkTableName?: string
    signal?: AbortSignal
}

export interface RunningHandler {
    promise: Promise<void>
}

/**
 * Start a subscribe-based handler that processes events and atomically
 * updates a bookmark + fires NOTIFY on bookmark advance.
 *
 * Each event is processed in its own transaction: the handlerFactory
 * receives the transaction client so projection writes are atomic with
 * the bookmark update. If the transaction fails, neither advances.
 *
 * The returned promise resolves when the signal is aborted (after
 * finishing the current event) or rejects on unrecoverable error.
 * Callers decide retry policy.
 */
export function runHandler(options: HandlerRunnerOptions): RunningHandler {
    const { pool, eventStore, handlerName, handlerFactory, signal } = options
    const tableName = options.bookmarkTableName ?? "_handler_bookmarks"

    if (handlerName.includes(":")) {
        throw new Error(`Handler name "${handlerName}" must not contain ":" (used as notification delimiter)`)
    }

    const promise = (async () => {
        const bookmarkResult = await pool.query(
            `SELECT last_sequence_position FROM ${tableName} WHERE handler_id = $1`,
            [handlerName]
        )
        const row = bookmarkResult.rows[0]
        if (!row)
            throw new Error(`Handler "${handlerName}" not found in ${tableName}. Call ensureHandlersInstalled first.`)

        let position = SequencePosition.fromString(String(row.last_sequence_position))

        // Build query from handler's event types (create a temporary handler to inspect keys)
        const sampleHandler = handlerFactory(null as unknown as PoolClient)
        const types = Object.keys(sampleHandler.when) as string[]
        const query =
            types.length > 0 && sampleHandler.tagFilter
                ? Query.fromItems([{ types, tags: sampleHandler.tagFilter as Tags }])
                : Query.all()

        for await (const event of eventStore.subscribe(query, { after: position, signal })) {
            const client = await pool.connect()
            try {
                await client.query("BEGIN")

                const handler = handlerFactory(client)
                if (handler.when[event.event.type]) {
                    await handler.when[event.event.type](event)
                }

                await client.query(`UPDATE ${tableName} SET last_sequence_position = $1 WHERE handler_id = $2`, [
                    event.position.toString(),
                    handlerName
                ])
                await client.query("SELECT pg_notify($1, $2)", [
                    tableName,
                    `${handlerName}:${event.position.toString()}`
                ])

                await client.query("COMMIT")
                position = event.position
            } catch (err) {
                await client.query("ROLLBACK").catch(() => {})
                throw err
            } finally {
                client.release()
            }
        }
    })()

    return { promise }
}

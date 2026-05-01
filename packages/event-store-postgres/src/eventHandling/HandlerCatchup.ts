import { EventHandler, EventStore, Query, SequencePosition, Tags } from "@dcb-es/event-store"
import { Pool, PoolClient } from "pg"
import { ensureHandlersInstalled, registerHandlers } from "./ensureHandlersInstalled.js"

export type HandlerCheckPoints = Record<string, SequencePosition>

/**
 * @deprecated Use `runHandler()` instead — subscribe-based, atomic bookmark+projection,
 * with NOTIFY on bookmark advance for `waitUntilProcessed()` support.
 */
export class HandlerCatchup {
    private tableName: string

    constructor(
        private pool: Pool,
        private eventStore: EventStore,
        tablePrefix?: string
    ) {
        this.tableName = tablePrefix ? `${tablePrefix}_handler_bookmarks` : "_handler_bookmarks"
    }

    async ensureInstalled(handlerIds: string[]): Promise<void> {
        await ensureHandlersInstalled(this.pool, handlerIds, this.tableName)
    }

    async registerHandlers(handlerIds: string[]): Promise<void> {
        await registerHandlers(this.pool, handlerIds, this.tableName)
    }

    async catchupHandlers(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        handlers: Record<string, EventHandler<any, any>>
    ) {
        const client = await this.pool.connect()
        try {
            await client.query("BEGIN")

            const currentCheckPoints = await this.lockHandlers(client, handlers)

            // Sequential — each catchupHandler opens its own read cursor from the pool,
            // so running in parallel would require N+1 connections and risk pool exhaustion.
            for (const [handlerId, handler] of Object.entries(handlers)) {
                currentCheckPoints[handlerId] = await this.catchupHandler(handler, currentCheckPoints[handlerId])
            }

            await this.updateBookmarks(client, currentCheckPoints)
            await client.query("COMMIT")
        } catch (err) {
            await client.query("ROLLBACK").catch(() => {})
            throw err
        } finally {
            client.release()
        }
    }

    private async lockHandlers(
        client: PoolClient,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        handlers: Record<string, EventHandler<any, any>>
    ): Promise<HandlerCheckPoints> {
        try {
            const selectResult = await client.query(
                `
                SELECT handler_id, last_sequence_position
                FROM ${this.tableName}
                WHERE handler_id = ANY($1::text[])
                FOR UPDATE NOWAIT;`,
                [Object.keys(handlers)]
            )

            return Object.keys(handlers).reduce((acc, handlerId) => {
                const rawPosition = selectResult.rows.find(row => row.handler_id === handlerId)?.last_sequence_position
                if (rawPosition !== undefined) {
                    return { ...acc, [handlerId]: SequencePosition.fromString(`${rawPosition}`) }
                }
                throw new Error(`Failed to retrieve sequence number for handler ${handlerId}`)
            }, {})
        } catch (error) {
            const err = error as { code?: string }
            if (err.code === "55P03") {
                throw new Error("Could not obtain lock as it is already locked by another process")
            }
            throw error
        }
    }

    private async updateBookmarks(client: PoolClient, locks: HandlerCheckPoints): Promise<void> {
        if (Object.values(locks).some(lock => !lock)) throw new Error("Sequence number is required to commit")

        const updateValues = Object.keys(locks)
            .map((_, index) => `($${index * 2 + 1}::text, $${index * 2 + 2}::bigint)`)
            .join(", ")

        const updateParams = Object.entries(locks).flatMap(([handlerId, position]) => [handlerId, position.toString()])

        await client.query(
            `UPDATE ${this.tableName} SET last_sequence_position = v.last_sequence_position
             FROM (VALUES ${updateValues}) AS v(handler_id, last_sequence_position)
             WHERE ${this.tableName}.handler_id = v.handler_id;`,
            updateParams
        )
    }

    private async catchupHandler(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        handler: EventHandler<any, any>,
        currentPosition: SequencePosition,
        toSequencePosition?: SequencePosition
    ) {
        if (!toSequencePosition) {
            const gen = this.eventStore.read(Query.all(), { backwards: true, limit: 1 })
            try {
                const lastEventInStore = (await gen.next()).value
                toSequencePosition = lastEventInStore?.position ?? SequencePosition.initial()
            } finally {
                await gen.return(undefined).catch(() => {})
            }
        }

        const types = Object.keys(handler.when) as string[]
        if (types.length === 0) return currentPosition
        const query = Query.fromItems([{ types, tags: Tags.createEmpty() }])
        for await (const event of this.eventStore.read(query, { after: currentPosition })) {
            if (toSequencePosition && event.position.isAfter(toSequencePosition)) {
                break
            }
            if (handler.when[event.event.type]) await handler.when[event.event.type](event)

            currentPosition = event.position
        }
        return currentPosition
    }
}

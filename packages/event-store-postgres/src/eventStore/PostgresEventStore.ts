import { Pool, PoolClient, QueryResult } from "pg"
import { dbEventConverter } from "./utils"
import { readSqlWithCursor } from "./readSql"
import { appendSql as appendCommand } from "./appendCommand"
import {
    EventStore,
    DcbEvent,
    AppendCondition,
    AppendConditionError,
    SequencedEvent,
    ReadOptions,
    Query
} from "@dcb-es/event-store"
import { ensureInstalled } from "./ensureInstalled"

const BATCH_SIZE = 100

export class PostgresEventStore implements EventStore {
    private tableName: string
    constructor(
        private client: PoolClient | Pool,
        private options?: { postgresTablePrefix?: string }
    ) {
        this.tableName = options?.postgresTablePrefix ? `${options.postgresTablePrefix}_events` : "events"
    }

    async ensureInstalled(): Promise<void> {
        await ensureInstalled(this.client, this.tableName)
    }

    async append(events: DcbEvent | DcbEvent[], appendCondition?: AppendCondition): Promise<void> {
        /*  To be completely safe, we need to ensure append with transaction isolation level set to serializable */
        const isolation = (await this.client.query("SELECT current_setting('transaction_isolation') as iso")).rows[0]
            .iso
        if (isolation.toLowerCase() !== "serializable")
            throw new Error(
                "Transaction is not serializable, whihc is required for event store consistency.  You can do this by running 'BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE'"
            )

        const evts = Array.isArray(events) ? events : [events]
        const { failIfEventsMatch, after } = appendCondition ?? {}
        const { statement, params } = appendCommand(evts, failIfEventsMatch, after, this.tableName)
        const result = await this.client.query(statement, params)
        if (result.rowCount === 0) {
            if (appendCondition) throw new AppendConditionError(appendCondition)
            throw new Error("Unexpected append failure: no events were inserted.")
        }
    }

    async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent> {
        yield* this.readInternal({ query, options })
    }

    private async *readInternal({
        query,
        options
    }: {
        query: Query
        options?: ReadOptions
    }): AsyncGenerator<SequencedEvent> {
        const { sql, params, cursorName } = readSqlWithCursor(query, this.tableName, options)
        await this.client.query(sql, params)

        let result: QueryResult
        while ((result = await this.client.query(`FETCH ${BATCH_SIZE} FROM ${cursorName}`))?.rows?.length) {
            for (const ev of result.rows) {
                yield dbEventConverter.fromDb(ev)
            }
        }
    }
}

import { Pool, PoolClient, QueryResult } from "pg"
import {
    EventStore,
    DcbEvent,
    AppendCondition,
    AppendConditionError,
    AppendCommand,
    SequencedEvent,
    SequencePosition,
    ReadOptions,
    Query,
    ensureIsArray
} from "@dcb-es/event-store"
import { dbEventConverter } from "./utils"
import { readSqlWithCursor } from "./readSql"
import { ensureInstalled } from "./ensureInstalled"
import { LockStrategy, advisoryLocks } from "./lockStrategy"
import { copyEventsToTable, copyConditionsToTempTable } from "./copyWriter"
import { getHighWaterMark, getLastPosition, checkBatchConditions, isConditionViolated } from "./queries"
import { analyseCommands } from "./analyseCommands"

const VALID_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/i
const READ_BATCH_SIZE = 5000
const COPY_THRESHOLD = 10
const TAG_DELIMITER = "\x1F"
const CONDITION_VIOLATED_SIGNAL = "APPEND_CONDITION_VIOLATED"

export interface PostgresEventStoreOptions {
    pool: Pool
    tablePrefix?: string
    copyThreshold?: number
    lockStrategy?: LockStrategy
}

export class PostgresEventStore implements EventStore {
    private tableName: string
    private appendFunctionName: string
    private pool: Pool
    private copyThreshold: number
    private lockStrategy: LockStrategy

    constructor(options: PostgresEventStoreOptions) {
        this.pool = options.pool
        this.copyThreshold = options.copyThreshold ?? COPY_THRESHOLD
        this.lockStrategy = options.lockStrategy ?? advisoryLocks()
        this.tableName = options.tablePrefix ? `${options.tablePrefix}_events` : "events"
        if (!VALID_IDENTIFIER.test(this.tableName))
            throw new Error(`Invalid table name "${this.tableName}": must match ${VALID_IDENTIFIER}`)
        this.appendFunctionName = `${this.tableName}_append`
    }

    async ensureInstalled(): Promise<void> {
        await ensureInstalled(this.pool, this.tableName, this.lockStrategy)
    }

    async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition> {
        const commands = ensureIsArray(command)

        if (commands.length === 1) {
            const evts = ensureIsArray(commands[0].events)
            if (evts.length === 0) throw new Error("Cannot append zero events")

            return evts.length <= this.copyThreshold
                ? this.appendViaFunction(evts, commands[0].condition)
                : this.appendViaCopy(evts, commands[0].condition)
        }

        return this.appendBatch(commands)
    }

    async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent> {
        const client = await this.pool.connect()
        try {
            await client.query("BEGIN")
            const { sql, params, cursorName } = readSqlWithCursor(query, this.tableName, options)
            await client.query(sql, params)

            let result: QueryResult
            while ((result = await client.query(`FETCH ${READ_BATCH_SIZE} FROM ${cursorName}`))?.rows?.length) {
                for (const ev of result.rows) yield dbEventConverter.fromDb(ev)
            }
        } finally {
            await client.query("ROLLBACK").catch(() => {})
            client.release()
        }
    }

    /** Stored procedure — single round-trip for small appends (≤ copyThreshold events). */
    private async appendViaFunction(evts: DcbEvent[], condition?: AppendCondition): Promise<SequencePosition> {
        const lockKeys = this.lockStrategy.computeKeys(evts, condition)
        const types: string[] = []
        const tags: string[] = []
        const payloads: string[] = []

        for (const evt of evts) {
            types.push(evt.type)
            tags.push(evt.tags.values.join(TAG_DELIMITER))
            payloads.push(serializePayload(evt))
        }

        try {
            const result = await this.pool.query(
                `SELECT ${this.appendFunctionName}($1::text[], $2::text[], $3::text[], $4::bigint[], $5::jsonb, $6::bigint) as pos`,
                [
                    types,
                    tags,
                    payloads,
                    lockKeys,
                    condition ? serializeConditionItems(condition) : null,
                    condition?.after ? parseInt(condition.after.toString()) : null
                ]
            )
            return SequencePosition.fromString(String(result.rows[0].pos))
        } catch (err) {
            if ((err as { message?: string }).message?.includes(CONDITION_VIOLATED_SIGNAL)) {
                throw new AppendConditionError(condition!)
            }
            throw err
        }
    }

    /** COPY FROM STDIN — high throughput for large single-command appends. */
    private async appendViaCopy(evts: DcbEvent[], condition?: AppendCondition): Promise<SequencePosition> {
        const lockKeys = this.lockStrategy.computeKeys(evts, condition)
        return this.withTransaction(async client => {
            if (lockKeys.length > 0) await this.lockStrategy.acquire(client, lockKeys, this.tableName)

            if (condition && (await isConditionViolated(client, this.tableName, condition))) {
                throw new AppendConditionError(condition)
            }

            await copyEventsToTable(client, this.tableName, evts)
            return SequencePosition.fromString(String(await getLastPosition(client, this.tableName)))
        })
    }

    /** Batch COPY + temp table — multiple commands with per-command condition checking. */
    private async appendBatch(commands: AppendCommand[]): Promise<SequencePosition> {
        const { totalEvents, lockKeys, conditions, eventIterator } = analyseCommands(commands, this.lockStrategy)
        if (totalEvents === 0) throw new Error("Cannot append zero events")

        return this.withTransaction(async client => {
            if (lockKeys.length > 0) await this.lockStrategy.acquire(client, lockKeys, this.tableName)

            const highWaterMark = await getHighWaterMark(client, this.tableName)
            await copyEventsToTable(client, this.tableName, eventIterator())

            if (conditions.length > 0) {
                await copyConditionsToTempTable(client, conditions)
                const failedIdx = await checkBatchConditions(client, this.tableName, highWaterMark)
                if (failedIdx !== null) throw new AppendConditionError(commands[failedIdx].condition!, failedIdx)
            }

            return SequencePosition.fromString(String(await getLastPosition(client, this.tableName)))
        })
    }

    /** Run a callback inside a READ COMMITTED transaction, with rollback on error. */
    private async withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
        const client = await this.pool.connect()
        try {
            await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
            const result = await fn(client)
            await client.query("COMMIT")
            return result
        } catch (err) {
            await client.query("ROLLBACK").catch(() => {})
            throw err
        } finally {
            client.release()
        }
    }
}

function serializePayload(evt: DcbEvent): string {
    return `{"data":${JSON.stringify(evt.data)},"metadata":${JSON.stringify(evt.metadata)}}`
}

function serializeConditionItems(condition: AppendCondition): string {
    return JSON.stringify(
        condition.failIfEventsMatch.items.map(item => ({
            types: item.types ?? [],
            tags: item.tags?.values ?? []
        }))
    )
}

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
    SubscribeOptions,
    Query,
    ensureIsArray,
    validateAppendCondition
} from "@dcb-es/event-store"
import { dbEventConverter } from "./utils.js"
import { readSqlWithCursor } from "./readSql.js"
import { ensureInstalled } from "./ensureInstalled.js"
import { LockStrategy, advisoryLocks } from "./lockStrategy.js"
import { copyEventsToTable } from "./copyWriter.js"
import { getHighWaterMark, getLastPosition, checkConditionsCte, isConditionViolated } from "./queries.js"
import { analyseCommands } from "./analyseCommands.js"

const VALID_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/i
const READ_BATCH_SIZE = 5000
const COPY_THRESHOLD = 10_000
const SP_BATCH_LIMIT = 500
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
    private notifyChannel: string
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
        this.notifyChannel = this.tableName
    }

    async ensureInstalled(): Promise<void> {
        await ensureInstalled(this.pool, this.tableName, this.lockStrategy)
    }

    // ─── Read ───────────────────────────────────────────────────────

    async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent> {
        const client = await this.pool.connect()
        try {
            // Postgres requires a transaction for cursor-based streaming
            await client.query("BEGIN")
            const { sql, params, cursorName } = readSqlWithCursor(query, this.tableName, options)
            await client.query(sql, params)

            let result: QueryResult
            while ((result = await client.query(`FETCH ${READ_BATCH_SIZE} FROM ${cursorName}`))?.rows?.length) {
                for (const ev of result.rows) yield dbEventConverter.fromDb(ev)
            }
        } finally {
            // ROLLBACK closes the read-only transaction — nothing was written
            await client.query("ROLLBACK").catch(() => {})
            client.release()
        }
    }

    // ─── Subscribe (live event stream via poll + LISTEN/NOTIFY) ────

    async *subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent> {
        const pollInterval = options?.pollIntervalMs ?? 100
        let position = options?.after ?? SequencePosition.initial()
        const signal = options?.signal

        const listener = await this.pool.connect()
        listener.setMaxListeners(0) // subscribe uses multiple once() listeners over its lifetime
        let listenerError: Error | null = null
        listener.on("error", err => {
            listenerError = err
        })

        try {
            await listener.query(`LISTEN ${this.notifyChannel}`)

            while (!signal?.aborted) {
                let hadEvents = false
                for await (const event of this.read(query, { after: position })) {
                    yield event
                    position = event.position
                    hadEvents = true
                }

                if (hadEvents) continue
                if (listenerError) throw listenerError

                await new Promise<void>(resolve => {
                    const timeout = setTimeout(resolve, pollInterval)
                    const onNotification = () => {
                        clearTimeout(timeout)
                        signal?.removeEventListener("abort", onAbort)
                        resolve()
                    }
                    const onAbort = () => {
                        clearTimeout(timeout)
                        listener.removeListener("notification", onNotification)
                        resolve()
                    }
                    listener.once("notification", onNotification)
                    signal?.addEventListener("abort", onAbort, { once: true })
                })
            }
        } finally {
            await listener.query(`UNLISTEN ${this.notifyChannel}`).catch(() => {})
            listener.release()
        }
    }

    // ─── Append routing ─────────────────────────────────────────────

    async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition> {
        const commands = ensureIsArray(command)
        for (const cmd of commands) {
            if (cmd.condition) validateAppendCondition(cmd.condition)
        }

        if (commands.length === 1) {
            const evts = ensureIsArray(commands[0].events)
            if (evts.length === 0) throw new Error("Cannot append zero events")

            return evts.length <= this.copyThreshold
                ? this.appendViaFunction(evts, commands[0].condition)
                : this.appendViaCopy(evts, commands[0].condition)
        }

        return this.appendBatch(commands)
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

        const condTypes: string[] = []
        const condTags: string[] = []
        if (condition) {
            for (const item of condition.failIfEventsMatch.items) {
                for (const type of item.types ?? []) {
                    condTypes.push(type)
                    condTags.push(item.tags?.values.join(TAG_DELIMITER) ?? "")
                }
            }
        }

        try {
            const result = await this.pool.query(
                `SELECT ${this.appendFunctionName}($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::text[], $7::bigint) as pos`,
                [
                    types,
                    tags,
                    payloads,
                    lockKeys,
                    condition ? condTypes : null,
                    condition ? condTags : null,
                    condition ? parseInt(condition.after?.toString() ?? "0") : null
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

    /** Stored procedure — single round-trip for batch commands with ≤ copyThreshold total events. */
    private async appendBatchViaFunction(commands: AppendCommand[], lockKeys: bigint[]): Promise<SequencePosition> {
        const types: string[] = []
        const tags: string[] = []
        const payloads: string[] = []
        const condCmdIdxs: number[] = []
        const condTypes: string[] = []
        const condTags: string[] = []
        const condAfter: number[] = []

        for (let i = 0; i < commands.length; i++) {
            const cmd = commands[i]
            for (const evt of ensureIsArray(cmd.events)) {
                types.push(evt.type)
                tags.push(evt.tags.values.join(TAG_DELIMITER))
                payloads.push(serializePayload(evt))
            }
            if (cmd.condition) {
                const afterPos = parseInt(cmd.condition.after?.toString() ?? "0")
                for (const item of cmd.condition.failIfEventsMatch.items) {
                    for (const type of item.types ?? []) {
                        condCmdIdxs.push(i)
                        condTypes.push(type)
                        condTags.push(item.tags?.values.join(TAG_DELIMITER) ?? "")
                        condAfter.push(afterPos)
                    }
                }
            }
        }

        const hasConditions = condCmdIdxs.length > 0

        try {
            const result = await this.pool.query(
                `SELECT ${this.appendFunctionName}_batch($1::bigint[], $2::text[], $3::text[], $4::text[], $5::int[], $6::text[], $7::text[], $8::bigint[]) as pos`,
                [
                    lockKeys, types, tags, payloads,
                    hasConditions ? condCmdIdxs : null,
                    hasConditions ? condTypes : null,
                    hasConditions ? condTags : null,
                    hasConditions ? condAfter : null
                ]
            )
            return SequencePosition.fromString(String(result.rows[0].pos))
        } catch (err) {
            const msg = (err as { message?: string }).message ?? ""
            const match = msg.match(/APPEND_CONDITION_VIOLATED:cmd=(\d+)/)
            if (match) {
                const idx = parseInt(match[1])
                throw new AppendConditionError(commands[idx].condition!, idx)
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
            const pos = await getLastPosition(client, this.tableName)
            await client.query("SELECT pg_notify($1, $2)", [this.notifyChannel, String(pos)])
            return SequencePosition.fromString(String(pos))
        })
    }

    /** Multi-command batch — SP for small batches, COPY for larger ones. */
    private async appendBatch(commands: AppendCommand[]): Promise<SequencePosition> {
        const { totalEvents, lockKeys, conditions, eventIterator } = analyseCommands(commands, this.lockStrategy)
        if (totalEvents === 0) throw new Error("Cannot append zero events")

        if (commands.length <= SP_BATCH_LIMIT && totalEvents <= this.copyThreshold) {
            return this.appendBatchViaFunction(commands, lockKeys)
        }

        return this.withTransaction(async client => {
            if (lockKeys.length > 0) await this.lockStrategy.acquire(client, lockKeys, this.tableName)

            const highWaterMark = await getHighWaterMark(client, this.tableName)

            await copyEventsToTable(client, this.tableName, eventIterator())

            if (conditions.length > 0) {
                const condCmdIdxs: number[] = []
                const condTypes: string[] = []
                const condTags: string[] = []
                const condAfter: number[] = []
                for (const c of conditions) {
                    condCmdIdxs.push(c.cmdIdx)
                    condTypes.push(c.type)
                    condTags.push(c.tags.join(TAG_DELIMITER))
                    condAfter.push(c.afterPos)
                }
                const failedIdx = await checkConditionsCte(
                    client, this.tableName, condCmdIdxs, condTypes, condTags, condAfter,
                    highWaterMark, TAG_DELIMITER
                )
                if (failedIdx !== null) throw new AppendConditionError(commands[failedIdx].condition!, failedIdx)
            }

            const pos = await getLastPosition(client, this.tableName)
            await client.query("SELECT pg_notify($1, $2)", [this.notifyChannel, String(pos)])
            return SequencePosition.fromString(String(pos))
        })
    }

    // ─── Transaction helper ─────────────────────────────────────────

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


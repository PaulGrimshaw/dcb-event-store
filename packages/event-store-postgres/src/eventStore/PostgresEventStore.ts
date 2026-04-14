import { Pool, QueryResult } from "pg"
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
import { copyEventsToTable, copyConditionsToTempTable, ConditionRow } from "./copyWriter"
import { getHighWaterMark, getLastPosition, checkBatchConditions, isConditionViolated } from "./queries"

const VALID_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/i
const READ_BATCH_SIZE = 5000
const COPY_THRESHOLD = 10
const TAG_DELIMITER = "\x1F"
const CONDITION_VIOLATED_SIGNAL = "APPEND_CONDITION_VIOLATED"

function flattenEvents(cmd: AppendCommand): DcbEvent[] {
    return ensureIsArray(cmd.events)
}

function posToNumber(pos: SequencePosition): number {
    return parseInt(pos.toString())
}

/** Single pass over commands — extracts lock keys, conditions, and total event count. */
function analyzeCommands(
    commands: AppendCommand[],
    lockStrategy: LockStrategy
): {
    totalEvents: number
    lockKeys: bigint[]
    conditions: ConditionRow[]
    eventIterator: () => Iterable<DcbEvent>
} {
    const allKeys = new Set<bigint>()
    const conditions: ConditionRow[] = []
    let totalEvents = 0

    for (let i = 0; i < commands.length; i++) {
        const cmd = commands[i]
        const evts = flattenEvents(cmd)
        totalEvents += evts.length

        for (const k of lockStrategy.computeKeys(evts, cmd.condition)) {
            allKeys.add(k)
        }

        if (cmd.condition?.after) {
            for (const item of cmd.condition.failIfEventsMatch.items) {
                conditions.push({
                    cmdIdx: i,
                    types: item.types ?? [],
                    tags: item.tags?.values ?? [],
                    afterPos: posToNumber(cmd.condition.after)
                })
            }
        }
    }

    return {
        totalEvents,
        lockKeys: [...allKeys],
        conditions,
        eventIterator: function* () {
            for (const cmd of commands) {
                for (const evt of flattenEvents(cmd)) yield evt
            }
        }
    }
}

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
            const evts = flattenEvents(commands[0])
            if (evts.length === 0) throw new Error("Cannot append zero events")

            if (evts.length <= this.copyThreshold) {
                return this.appendViaFunction(evts, commands[0].condition)
            }
            return this.appendViaCopy(evts, commands[0].condition)
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
                for (const ev of result.rows) {
                    yield dbEventConverter.fromDb(ev)
                }
            }
        } finally {
            await client.query("ROLLBACK").catch(() => {})
            client.release()
        }
    }

    // ─── Stored procedure path (1 round-trip for small appends) ──

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

        const conditionItems = condition ? serializeConditionItems(condition) : null
        const afterPos = condition?.after ? posToNumber(condition.after) : null

        try {
            const result = await this.pool.query(
                `SELECT ${this.appendFunctionName}($1::text[], $2::text[], $3::text[], $4::bigint[], $5::jsonb, $6::bigint) as pos`,
                [types, tags, payloads, lockKeys, conditionItems, afterPos]
            )
            return SequencePosition.fromString(String(result.rows[0].pos))
        } catch (err) {
            if ((err as { message?: string }).message?.includes(CONDITION_VIOLATED_SIGNAL)) {
                throw new AppendConditionError(condition!)
            }
            throw err
        }
    }

    // ─── COPY path (single command, large event count) ───────────

    private async appendViaCopy(evts: DcbEvent[], condition?: AppendCondition): Promise<SequencePosition> {
        const lockKeys = this.lockStrategy.computeKeys(evts, condition)

        const client = await this.pool.connect()
        try {
            await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")

            if (lockKeys.length > 0) {
                await this.lockStrategy.acquire(client, lockKeys, this.tableName)
            }

            if (condition) {
                if (await isConditionViolated(client, this.tableName, condition)) {
                    await client.query("ROLLBACK")
                    throw new AppendConditionError(condition)
                }
            }

            await copyEventsToTable(client, this.tableName, evts)

            const pos = await getLastPosition(client, this.tableName)
            await client.query("COMMIT")
            return SequencePosition.fromString(String(pos))
        } catch (err) {
            if (err instanceof AppendConditionError) throw err
            await client.query("ROLLBACK").catch(() => {})
            throw err
        } finally {
            client.release()
        }
    }

    // ─── Batch path (multiple commands, COPY + temp table) ───────

    private async appendBatch(commands: AppendCommand[]): Promise<SequencePosition> {
        const { totalEvents, lockKeys, conditions, eventIterator } = analyzeCommands(commands, this.lockStrategy)
        if (totalEvents === 0) throw new Error("Cannot append zero events")

        const client = await this.pool.connect()
        try {
            await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")

            if (lockKeys.length > 0) {
                await this.lockStrategy.acquire(client, lockKeys, this.tableName)
            }

            const highWaterMark = await getHighWaterMark(client, this.tableName)

            await copyEventsToTable(client, this.tableName, eventIterator())

            if (conditions.length > 0) {
                await copyConditionsToTempTable(client, conditions)

                const failedIdx = await checkBatchConditions(client, this.tableName, highWaterMark)
                if (failedIdx !== null) {
                    await client.query("ROLLBACK")
                    throw new AppendConditionError(commands[failedIdx].condition!, failedIdx)
                }
            }

            const pos = await getLastPosition(client, this.tableName)
            await client.query("COMMIT")
            return SequencePosition.fromString(String(pos))
        } catch (err) {
            if (err instanceof AppendConditionError) throw err
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

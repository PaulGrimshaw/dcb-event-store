import { Pool, PoolClient, QueryResult } from "pg"
import {
    EventStore,
    DcbEvent,
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
import { getHighWaterMark, getLastPosition, checkConditions } from "./queries.js"
import { analyseCommands } from "./analyseCommands.js"
import { HwmCache } from "./hwmCache.js"

const VALID_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/i
const READ_BATCH_SIZE = 5000
const COPY_THRESHOLD = 10_000
const TAG_DELIMITER = "\x1F"
const CONDITION_VIOLATED_SIGNAL = "APPEND_CONDITION_VIOLATED"
const DEFAULT_HWM_CACHE_TTL_MS = 50

export interface PostgresEventStoreOptions {
    pool: Pool
    tablePrefix?: string
    copyThreshold?: number
    lockStrategy?: LockStrategy
    /**
     * Time-to-live for the in-process read-barrier hwm cache. Coalesces concurrent
     * `read()` / `subscribe()` calls with the same filter into a single barrier
     * round-trip. Set to 0 to disable. Default: 50ms — readers can lag new commits
     * by up to this many ms; correctness is unaffected.
     */
    hwmCacheTtlMs?: number
    /**
     * Hard cap on cached hwm entries (FIFO eviction). Default: 1024. Bounds the
     * cache memory footprint when filters churn (per-entity reads, distinct
     * tag values per request, etc.).
     */
    hwmCacheMaxEntries?: number
}

export class PostgresEventStore implements EventStore {
    private tableName: string
    private appendFunctionName: string
    private barrierFunctionName: string
    private notifyChannel: string
    private pool: Pool
    private copyThreshold: number
    private lockStrategy: LockStrategy
    private hwmCache: HwmCache

    constructor(options: PostgresEventStoreOptions) {
        this.pool = options.pool
        this.copyThreshold = options.copyThreshold ?? COPY_THRESHOLD
        this.lockStrategy = options.lockStrategy ?? advisoryLocks()
        this.hwmCache = new HwmCache(options.hwmCacheTtlMs ?? DEFAULT_HWM_CACHE_TTL_MS, options.hwmCacheMaxEntries)
        this.tableName = options.tablePrefix ? `${options.tablePrefix}_events` : "events"
        if (!VALID_IDENTIFIER.test(this.tableName))
            throw new Error(`Invalid table name "${this.tableName}": must match ${VALID_IDENTIFIER}`)
        this.appendFunctionName = `${this.tableName}_append`
        this.barrierFunctionName = `${this.tableName}_barrier_hwm`
        this.notifyChannel = this.tableName
    }

    async ensureInstalled(): Promise<void> {
        await ensureInstalled(this.pool, this.tableName, this.lockStrategy)
    }

    // ─── Read ───────────────────────────────────────────────────────

    async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent> {
        // Backwards reads have no gap problem: they scan from highest seq downwards,
        // and the danger of skipping past in-flight allocations doesn't apply.
        const upperBound = options?.backwards ? undefined : await this.barrierSnapshot(query)

        const client = await this.pool.connect()
        try {
            await client.query("BEGIN")
            const { sql, params, cursorName } = readSqlWithCursor(query, this.tableName, { ...options, upperBound })
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

    /**
     * Acquire reader-side barrier locks via the per-table SP, snapshot
     * pg_sequence_last_value(), release. Returns the safe high-water mark.
     *
     * Implemented as a single autocommit function call so the barrier locks
     * are held only for the duration of the function — they don't span the
     * subsequent cursor scan, which keeps writers unblocked. Concurrent calls
     * with the same filter are coalesced through `hwmCache`; the TTL bounds
     * how stale a cached hwm may be (correctness is unaffected — see HwmCache).
     */
    private async barrierSnapshot(query: Query): Promise<bigint> {
        const keys = this.lockStrategy.computeReaderKeys(query)
        return this.hwmCache.get(keys, async () => {
            const result = await this.pool.query(
                `SELECT ${this.barrierFunctionName}($1::bigint[], $2::bigint[]) AS hwm`,
                [keys.leafS, keys.intentX]
            )
            return BigInt(String(result.rows[0].hwm ?? "0"))
        })
    }

    // ─── Subscribe ──────────────────────────────────────────────────

    async *subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent> {
        const pollInterval = options?.pollIntervalMs ?? 100
        let position = options?.after ?? SequencePosition.initial()
        const signal = options?.signal

        const listener = await this.pool.connect()
        listener.setMaxListeners(0)
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
                        // A NOTIFY means a writer (here or on another instance) committed.
                        // Invalidate so the next iteration's barrier picks up the new state.
                        this.hwmCache.invalidateAll()
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

    // ─── Append ─────────────────────────────────────────────────────

    async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition> {
        const commands = ensureIsArray(command)
        for (const cmd of commands) {
            if (cmd.condition) validateAppendCondition(cmd.condition)
        }

        const { totalEvents, leafLockKeys, intentLockKeys, conditions, eventIterator } = analyseCommands(
            commands,
            this.lockStrategy
        )
        if (totalEvents === 0) throw new Error("Cannot append zero events")

        const result = await (totalEvents <= this.copyThreshold
            ? this.appendViaFunction(commands, leafLockKeys, intentLockKeys)
            : this.appendViaCopy(commands, leafLockKeys, intentLockKeys, conditions, eventIterator))
        // We just committed new events; any cached hwm in this process is now stale.
        // Cross-instance staleness is handled by subscribers invalidating on NOTIFY.
        this.hwmCache.invalidateAll()
        return result
    }

    /** Stored procedure — single round-trip for ≤ copyThreshold total events. */
    private async appendViaFunction(
        commands: AppendCommand[],
        leafLockKeys: bigint[],
        intentLockKeys: bigint[]
    ): Promise<SequencePosition> {
        const { types, tags, payloads, condCmdIdxs, condTypes, condTags, condAfter } = serializeCommands(commands)
        const hasConditions = condCmdIdxs.length > 0

        try {
            const result = await this.pool.query(
                `SELECT ${this.appendFunctionName}($1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::int[], $7::text[], $8::text[], $9::bigint[]) as pos`,
                [
                    leafLockKeys,
                    intentLockKeys,
                    types,
                    tags,
                    payloads,
                    hasConditions ? condCmdIdxs : null,
                    hasConditions ? condTypes : null,
                    hasConditions ? condTags : null,
                    hasConditions ? condAfter : null
                ]
            )
            return SequencePosition.fromString(String(result.rows[0].pos))
        } catch (err) {
            const msg = (err as { message?: string }).message ?? ""
            if (msg.includes(CONDITION_VIOLATED_SIGNAL)) {
                const match = msg.match(/APPEND_CONDITION_VIOLATED:cmd=(\d+)/)
                const idx = match ? parseInt(match[1]) : 0
                throw new AppendConditionError(commands[idx].condition!, idx)
            }
            throw err
        }
    }

    /** COPY FROM STDIN — high throughput for > copyThreshold total events. */
    private async appendViaCopy(
        commands: AppendCommand[],
        leafLockKeys: bigint[],
        intentLockKeys: bigint[],
        conditions: { cmdIdx: number; type: string; tags: string[]; afterPos: number }[],
        eventIterator: () => Iterable<DcbEvent>
    ): Promise<SequencePosition> {
        return this.withTransaction(async client => {
            // Lock-then-allocate invariant: acquire leaf X + intent S BEFORE INSERT.
            await this.lockStrategy.acquireWriter(
                client,
                { leafX: leafLockKeys, intentS: intentLockKeys },
                this.tableName
            )

            const highWaterMark = await getHighWaterMark(client, this.tableName)
            await copyEventsToTable(client, this.tableName, eventIterator())

            if (conditions.length > 0) {
                const { condCmdIdxs, condTypes, condTags, condAfter } = flattenConditionRows(conditions)
                const failedIdx = await checkConditions(
                    client,
                    this.tableName,
                    condCmdIdxs,
                    condTypes,
                    condTags,
                    condAfter,
                    highWaterMark,
                    TAG_DELIMITER
                )
                if (failedIdx !== null) throw new AppendConditionError(commands[failedIdx].condition!, failedIdx)
            }

            return this.notifyAndReturnPosition(client)
        })
    }

    private async notifyAndReturnPosition(client: PoolClient): Promise<SequencePosition> {
        const pos = await getLastPosition(client, this.tableName)
        await client.query("SELECT pg_notify($1, $2)", [this.notifyChannel, String(pos)])
        return SequencePosition.fromString(String(pos))
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

function serializeCommands(commands: AppendCommand[]) {
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
                for (const type of item.types) {
                    condCmdIdxs.push(i)
                    condTypes.push(type)
                    condTags.push(item.tags?.values.join(TAG_DELIMITER) ?? "")
                    condAfter.push(afterPos)
                }
            }
        }
    }

    return { types, tags, payloads, condCmdIdxs, condTypes, condTags, condAfter }
}

function flattenConditionRows(conditions: { cmdIdx: number; type: string; tags: string[]; afterPos: number }[]) {
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
    return { condCmdIdxs, condTypes, condTags, condAfter }
}

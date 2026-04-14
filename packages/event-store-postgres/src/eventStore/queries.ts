import { PoolClient } from "pg"
import { AppendCondition } from "@dcb-es/event-store"

/** MAX on BIGSERIAL PK is an index-only reverse scan — O(1). */
export async function getHighWaterMark(client: PoolClient, tableName: string): Promise<number> {
    const result = await client.query(`SELECT COALESCE(MAX(sequence_position), 0) as hwm FROM ${tableName}`)
    return Number(result.rows[0].hwm)
}

/** Get the last sequence_position assigned in this session. */
export async function getLastPosition(client: PoolClient, tableName: string): Promise<number> {
    const result = await client.query(`SELECT currval(pg_get_serial_sequence($1, 'sequence_position')) as pos`, [
        tableName
    ])
    return Number(result.rows[0].pos)
}

/**
 * Check if any condition is violated by pre-existing events (before highWaterMark).
 * Uses the _conditions temp table populated by copyConditionsToTempTable.
 * Returns the command index of the first violation, or null if all pass.
 */
export async function checkBatchConditions(
    client: PoolClient,
    tableName: string,
    highWaterMark: number
): Promise<number | null> {
    const result = await client.query(
        `SELECT cmd_idx FROM _conditions c
         WHERE EXISTS (
             SELECT 1 FROM ${tableName} e
             WHERE e.type = ANY(c.cond_types)
               AND e.tags @> c.cond_tags
               AND e.sequence_position > c.after_pos
               AND e.sequence_position <= $1
         )
         LIMIT 1`,
        [highWaterMark]
    )
    return (result.rowCount ?? 0) > 0 ? result.rows[0].cmd_idx : null
}

/**
 * Check if a single condition is violated (used by the COPY path for single-command appends).
 * Returns true if violated.
 */
export async function isConditionViolated(
    client: PoolClient,
    tableName: string,
    condition: AppendCondition
): Promise<boolean> {
    const { failIfEventsMatch, after } = condition
    const afterPos = after ? parseInt(after.toString()) : 0

    for (const item of failIfEventsMatch.items) {
        const clauses = [`sequence_position > $1`]
        const params: unknown[] = [afterPos]
        let idx = 2

        if (item.types?.length) {
            clauses.push(`type = ANY($${idx++}::text[])`)
            params.push(item.types)
        }
        if (item.tags && item.tags.values.length > 0) {
            clauses.push(`tags @> $${idx++}::text[]`)
            params.push(item.tags.values)
        }

        const result = await client.query(`SELECT 1 FROM ${tableName} WHERE ${clauses.join(" AND ")} LIMIT 1`, params)
        if ((result.rowCount ?? 0) > 0) return true
    }

    return false
}

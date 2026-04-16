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
 * Check batch conditions using MATERIALIZED CTE + forced nested loop for GIN.
 * Accepts flat parallel arrays (same encoding as the batch SP).
 * Returns the command index of the first violation, or null if all pass.
 */
export async function checkConditionsCte(
    client: PoolClient,
    tableName: string,
    condCmdIdxs: number[],
    condTypes: string[],
    condTags: string[],
    condAfter: number[],
    highWaterMark: number,
    tagDelimiter: string
): Promise<number | null> {
    const fnName = `${tableName}_check_conditions`
    const result = await client.query(
        `SELECT ${fnName}($1::int[], $2::text[], $3::text[], $4::bigint[], $5::bigint, $6::text) AS failed_idx`,
        [condCmdIdxs, condTypes, condTags, condAfter, highWaterMark, tagDelimiter]
    )
    const idx = result.rows[0].failed_idx
    return idx !== null ? idx : null
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

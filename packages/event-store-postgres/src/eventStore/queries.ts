import { PoolClient } from "pg"

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
 * Check conditions via the _check_conditions stored function.
 * Returns the command index of the first violation, or null if all pass.
 */
export async function checkConditions(
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

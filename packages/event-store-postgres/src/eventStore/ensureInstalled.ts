import { Pool, PoolClient } from "pg"
import { LockStrategy } from "./lockStrategy.js"

const VALID_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/i

export const ensureInstalled = async (pool: Pool | PoolClient, tableName: string, lockStrategy: LockStrategy) => {
    if (!VALID_IDENTIFIER.test(tableName))
        throw new Error(`Invalid table name "${tableName}": must match ${VALID_IDENTIFIER}`)

    await pool.query(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          sequence_position BIGSERIAL PRIMARY KEY,
          type             TEXT COLLATE "C" NOT NULL,
          tags             TEXT[] NOT NULL,
          payload          TEXT NOT NULL
        ) WITH (
          autovacuum_freeze_min_age = 10000000,
          autovacuum_freeze_table_age = 100000000
        );

        CREATE INDEX IF NOT EXISTS ${tableName}_type_pos_idx
        ON ${tableName} (type COLLATE "C", sequence_position DESC);

        CREATE OR REPLACE FUNCTION ${tableName}_append(
            p_types      text[],
            p_tags       text[],
            p_payloads   text[],
            p_lock_keys  bigint[],
            p_conditions jsonb,
            p_after_pos  bigint
        ) RETURNS bigint AS $fn$
        DECLARE
            v_pos bigint;
        BEGIN
            IF p_lock_keys IS NOT NULL AND array_length(p_lock_keys, 1) > 0 THEN
                ${lockStrategy.generateSpLockBlock(tableName)}
            END IF;

            IF p_after_pos IS NOT NULL AND p_conditions IS NOT NULL THEN
                IF EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(p_conditions) AS c
                    WHERE EXISTS (
                        SELECT 1 FROM ${tableName} e
                        WHERE e.type = ANY(ARRAY(SELECT jsonb_array_elements_text(c -> 'types')))
                          AND e.tags @> ARRAY(SELECT jsonb_array_elements_text(c -> 'tags'))
                          AND e.sequence_position > p_after_pos
                    )
                ) THEN
                    RAISE EXCEPTION 'APPEND_CONDITION_VIOLATED';
                END IF;
            END IF;

            INSERT INTO ${tableName} (type, tags, payload)
            SELECT p_types[i], string_to_array(p_tags[i], E'\\x1F'), p_payloads[i]
            FROM generate_subscripts(p_types, 1) AS i;

            SELECT currval(pg_get_serial_sequence('${tableName}', 'sequence_position')) INTO v_pos;
            PERFORM pg_notify('${tableName}', v_pos::text);
            RETURN v_pos;
        END;
        $fn$ LANGUAGE plpgsql;
    `)

    if (lockStrategy.ensureSchema) {
        await lockStrategy.ensureSchema(pool, tableName)
    }
}

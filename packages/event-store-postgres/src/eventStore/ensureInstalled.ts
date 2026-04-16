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

        DROP INDEX IF EXISTS ${tableName}_type_tags_gin;

        CREATE INDEX IF NOT EXISTS ${tableName}_tags_gin
        ON ${tableName} USING GIN(tags) WITH (fastupdate=off);

        CREATE OR REPLACE FUNCTION ${tableName}_append(
            p_lock_keys      bigint[],
            p_types          text[],
            p_tags           text[],
            p_payloads       text[],
            p_cond_cmd_idxs  int[],
            p_cond_types     text[],
            p_cond_tags      text[],
            p_cond_after     bigint[]
        ) RETURNS bigint AS $fn$
        DECLARE
            v_hwm    bigint;
            v_pos    bigint;
            v_failed int;
        BEGIN
            IF p_lock_keys IS NOT NULL AND array_length(p_lock_keys, 1) > 0 THEN
                ${lockStrategy.generateSpLockBlock(tableName)}
            END IF;

            SELECT COALESCE(pg_sequence_last_value(pg_get_serial_sequence('${tableName}', 'sequence_position')), 0)
            INTO v_hwm;

            IF p_cond_cmd_idxs IS NOT NULL AND array_length(p_cond_cmd_idxs, 1) > 0 THEN
                SET LOCAL enable_hashjoin = off;
                SET LOCAL enable_mergejoin = off;
                SET LOCAL plan_cache_mode = force_generic_plan;

                WITH conds AS MATERIALIZED (
                    SELECT c.cmd_idx, c.ctype,
                           CASE WHEN c.ctags_str = '' THEN ARRAY[]::text[]
                                ELSE string_to_array(c.ctags_str, E'\\x1F') END AS ctags,
                           c.after_pos
                    FROM unnest(p_cond_cmd_idxs, p_cond_types, p_cond_tags, p_cond_after)
                         AS c(cmd_idx, ctype, ctags_str, after_pos)
                )
                SELECT c.cmd_idx INTO v_failed
                FROM conds c
                WHERE EXISTS (
                    SELECT 1 FROM ${tableName} e
                    WHERE e.tags @> c.ctags
                      AND e.type = c.ctype
                      AND e.sequence_position > c.after_pos
                      AND e.sequence_position <= v_hwm
                )
                ORDER BY c.cmd_idx
                LIMIT 1;

                SET LOCAL enable_hashjoin = on;
                SET LOCAL enable_mergejoin = on;
                SET LOCAL plan_cache_mode = auto;

                IF v_failed IS NOT NULL THEN
                    RAISE EXCEPTION 'APPEND_CONDITION_VIOLATED:cmd=%', v_failed;
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

    await pool.query(`
        CREATE OR REPLACE FUNCTION ${tableName}_check_conditions(
            p_cmd_idxs   int[],
            p_types      text[],
            p_tags       text[],
            p_after      bigint[],
            p_hwm        bigint,
            p_delim      text
        ) RETURNS int AS $cc$
        DECLARE
            v_failed int;
        BEGIN
            SET LOCAL enable_hashjoin = off;
            SET LOCAL enable_mergejoin = off;
            SET LOCAL plan_cache_mode = force_generic_plan;

            WITH conds AS MATERIALIZED (
                SELECT c.cmd_idx, c.ctype,
                       CASE WHEN c.ctags_str = '' THEN ARRAY[]::text[]
                            ELSE string_to_array(c.ctags_str, p_delim) END AS ctags,
                       c.after_pos
                FROM unnest(p_cmd_idxs, p_types, p_tags, p_after)
                     AS c(cmd_idx, ctype, ctags_str, after_pos)
            )
            SELECT c.cmd_idx INTO v_failed
            FROM conds c
            WHERE EXISTS (
                SELECT 1 FROM ${tableName} e
                WHERE e.tags @> c.ctags
                  AND e.type = c.ctype
                  AND e.sequence_position > c.after_pos
                  AND e.sequence_position <= p_hwm
            )
            ORDER BY c.cmd_idx
            LIMIT 1;

            SET LOCAL enable_hashjoin = on;
            SET LOCAL enable_mergejoin = on;
            SET LOCAL plan_cache_mode = auto;

            RETURN v_failed;
        END;
        $cc$ LANGUAGE plpgsql;
    `)

    if (lockStrategy.ensureSchema) {
        await lockStrategy.ensureSchema(pool, tableName)
    }
}

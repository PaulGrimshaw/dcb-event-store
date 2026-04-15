import { Query, QueryItem, ReadOptions } from "@dcb-es/event-store"
import { ParamManager } from "./utils.js"

export const readSqlWithCursor = (query: Query, tableName: string, options?: ReadOptions) => {
    const { sql, params } = readSql(query, tableName, options)
    const cursorName = `event_cursor_${Math.random().toString(36).substring(7)}`
    return {
        sql: `DECLARE "${cursorName}" CURSOR FOR ${sql}`,
        params,
        cursorName: `"${cursorName}"`
    }
}

const readSql = (query: Query, tableName: string, options?: ReadOptions) => {
    const pm = new ParamManager()

    const filters = [positionFilterClause(pm, options)]

    if (!query.isAll) {
        filters.push(criteriaClause(query, pm))
    }

    const sql = `
    SELECT
      e.sequence_position,
      e.type,
      e.payload,
      e.tags
    FROM ${tableName} e
    ${whereClause(filters)}
    ORDER BY e.sequence_position ${options?.backwards ? "DESC" : ""}
    ${options?.limit ? `LIMIT ${pm.add(options.limit)}` : ""};
  `
    return { sql, params: pm.params }
}

const notEmpty = (s: string): boolean => s !== null && s.trim() !== ""

const tagsFilterClause = (pm: ParamManager, c: QueryItem): string =>
    c.tags && c.tags.length ? `tags && ${pm.add(c.tags.values)}::text[]` : ""

const positionFilterClause = (pm: ParamManager, options?: ReadOptions): string =>
    options?.after ? `e.sequence_position ${options.backwards ? "<" : ">"} ${pm.add(options.after.toString())}` : ""

const typesFilterClause = (c: QueryItem, pm: ParamManager): string =>
    c.types?.length ? `type IN (${c.types.map(t => pm.add(t)).join(", ")})` : ""

const itemFilterClause = (c: QueryItem, pm: ParamManager): string => {
    const parts = [typesFilterClause(c, pm), tagsFilterClause(pm, c)].filter(notEmpty)
    return parts.length ? `(${parts.join(" AND ")})` : ""
}

const criteriaClause = (query: Query, pm: ParamManager): string => {
    if (query.isAll) return ""
    const clauses = query.items.map(c => itemFilterClause(c, pm)).filter(notEmpty)
    if (clauses.length === 0) return ""
    if (clauses.length === 1) return clauses[0]
    return `(${clauses.join(" OR ")})`
}

const whereClause = (queryParts: string[]): string => {
    const parts = queryParts.filter(notEmpty)
    return parts.length ? `WHERE ${parts.join(" AND ")}` : ""
}

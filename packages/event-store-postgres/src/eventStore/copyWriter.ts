import { PoolClient } from "pg"
import { from as copyFrom } from "pg-copy-streams"
import { pipeline } from "stream/promises"
import { Readable } from "stream"
import { DcbEvent } from "@dcb-es/event-store"

export interface ConditionRow {
    cmdIdx: number
    type: string
    tags: string[]
    afterPos: number
}

/**
 * Stream events into the events table via COPY FROM STDIN.
 * Accepts an iterable to avoid materialising large arrays.
 * Postgres never parses the payload — it's stored as opaque TEXT.
 */
export async function copyEventsToTable(
    client: PoolClient,
    tableName: string,
    events: Iterable<DcbEvent>
): Promise<void> {
    const copyStream = client.query(copyFrom(`COPY ${tableName} (type, tags, payload) FROM STDIN WITH (FORMAT text)`))

    const source = Readable.from(
        (function* () {
            for (const evt of events) {
                const tags = formatTagsForCopy(evt.tags.values)
                const payload = escapeCopy(serializePayload(evt))
                yield `${escapeCopy(evt.type)}\t${tags}\t${payload}\n`
            }
        })()
    )

    await pipeline(source, copyStream)
}

function serializePayload(evt: DcbEvent): string {
    return `{"data":${JSON.stringify(evt.data)},"metadata":${JSON.stringify(evt.metadata)}}`
}

function formatTagsForCopy(tags: string[]): string {
    return `{${tags.map(t => `"${escapeCopy(t, true)}"`).join(",")}}`
}

/**
 * Single-pass escape for COPY TEXT format.
 * When `quoted` is true, also escapes double-quotes for use inside
 * double-quoted array elements (e.g. tag values within Postgres arrays).
 */
function escapeCopy(value: string, quoted = false): string {
    let result = ""
    for (let i = 0; i < value.length; i++) {
        switch (value.charCodeAt(i)) {
            case 92:
                result += "\\\\"
                break // backslash
            case 0:
                result += "\\0"
                break // null
            case 9:
                result += "\\t"
                break // tab
            case 10:
                result += "\\n"
                break // newline
            case 13:
                result += "\\r"
                break // carriage return
            case 34:
                result += quoted ? '\\"' : '"'
                break // double quote — only escape inside array elements
            default:
                result += value[i]
        }
    }
    return result
}

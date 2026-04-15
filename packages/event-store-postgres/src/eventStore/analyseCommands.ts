import { AppendCommand, DcbEvent, ensureIsArray } from "@dcb-es/event-store"
import { LockStrategy } from "./lockStrategy.js"
import { ConditionRow } from "./copyWriter.js"

/** Single pass over commands — extracts lock keys, conditions, and total event count. */
export function analyseCommands(
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
        const evts = ensureIsArray(cmd.events)
        totalEvents += evts.length

        for (const k of lockStrategy.computeKeys(evts, cmd.condition)) {
            allKeys.add(k)
        }

        if (cmd.condition) {
            for (const item of cmd.condition.failIfEventsMatch.items) {
                conditions.push({
                    cmdIdx: i,
                    types: item.types ?? [],
                    tags: item.tags?.values ?? [],
                    afterPos: cmd.condition.after ? parseInt(cmd.condition.after.toString()) : 0
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
                for (const evt of ensureIsArray(cmd.events)) yield evt
            }
        }
    }
}

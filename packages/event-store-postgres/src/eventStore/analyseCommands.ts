import { AppendCommand, DcbEvent, ensureIsArray } from "@dcb-es/event-store"
import { LockStrategy } from "./lockStrategy.js"
import { ConditionRow } from "./copyWriter.js"

/** Single pass over commands — extracts lock keys, conditions, and total event count. */
export function analyseCommands(
    commands: AppendCommand[],
    lockStrategy: LockStrategy
): {
    totalEvents: number
    leafLockKeys: bigint[]
    intentLockKeys: bigint[]
    conditions: ConditionRow[]
    eventIterator: () => Iterable<DcbEvent>
} {
    const leafSet = new Set<bigint>()
    const intentSet = new Set<bigint>()
    const conditions: ConditionRow[] = []
    let totalEvents = 0

    for (let i = 0; i < commands.length; i++) {
        const cmd = commands[i]
        const evts = ensureIsArray(cmd.events)
        totalEvents += evts.length

        const { leafX, intentS } = lockStrategy.computeWriterKeys(evts, cmd.condition)
        for (const k of leafX) leafSet.add(k)
        for (const k of intentS) intentSet.add(k)

        if (cmd.condition) {
            const afterPos = cmd.condition.after ? parseInt(cmd.condition.after.toString()) : 0
            for (const item of cmd.condition.failIfEventsMatch.items) {
                for (const type of item.types) {
                    conditions.push({
                        cmdIdx: i,
                        type,
                        tags: item.tags?.values ?? [],
                        afterPos
                    })
                }
            }
        }
    }

    return {
        totalEvents,
        leafLockKeys: [...leafSet],
        intentLockKeys: [...intentSet],
        conditions,
        eventIterator: function* () {
            for (const cmd of commands) {
                for (const evt of ensureIsArray(cmd.events)) yield evt
            }
        }
    }
}

import { SequencedEvent, EventStore, AppendCommand, ReadOptions, validateAppendCondition } from "../EventStore"
import { AppendConditionError } from "../AppendConditionError"
import { SequencePosition } from "../SequencePosition"
import { Timestamp } from "../Timestamp"
import { isInRange, matchesQueryItem, deduplicateEvents } from "./utils"
import { Query } from "../Query"
import { ensureIsArray } from "../../ensureIsArray"

const offsetPosition = (pos: SequencePosition, n: number) =>
    SequencePosition.fromString(String(parseInt(pos.toString()) + n))

const lastPosition = (events: SequencedEvent[]) =>
    events
        .map(event => event.position)
        .filter(pos => pos !== undefined)
        .pop() || SequencePosition.initial()

export class MemoryEventStore implements EventStore {
    private testListenerRegistry: { read: () => void; append: () => void } = {
        read: () => null,
        append: () => null
    }

    private events: Array<SequencedEvent> = []

    constructor(initialEvents: Array<SequencedEvent> = []) {
        this.events = [...initialEvents]
    }

    public on(ev: "read" | "append", fn: () => void) {
        this.testListenerRegistry[ev] = fn
    }

    async *read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent> {
        if (this.testListenerRegistry.read) this.testListenerRegistry.read()

        let yieldedCount = 0

        const filterByPosition = (event: SequencedEvent): boolean => {
            if (!options?.after) return true
            return isInRange(event.position, options.after, options?.backwards)
        }

        const allMatchedEvents = !query.isAll
            ? query.items.flatMap((criterion, index) =>
                  this.events
                      .filter(event => filterByPosition(event) && matchesQueryItem(criterion, event))
                      .map(event => ({ ...event, matchedCriteria: [index.toString()] }))
                      .sort((a, b) => SequencePosition.compare(a.position, b.position))
              )
            : this.events.filter(filterByPosition)

        const uniqueEvents = deduplicateEvents(allMatchedEvents).sort((a, b) =>
            options?.backwards
                ? SequencePosition.compare(b.position, a.position)
                : SequencePosition.compare(a.position, b.position)
        )

        for (const event of uniqueEvents) {
            yield event
            yieldedCount++
            if (options?.limit && yieldedCount >= options.limit) {
                break
            }
        }
    }

    async append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition> {
        if (this.testListenerRegistry.append) this.testListenerRegistry.append()

        const commands = ensureIsArray(command)
        const snapshot = [...this.events]
        const allNewEvents: SequencedEvent[] = []
        let eventIndex = 0

        for (let i = 0; i < commands.length; i++) {
            const cmd = commands[i]
            const evts = ensureIsArray(cmd.events)

            if (cmd.condition) {
                validateAppendCondition(cmd.condition)
                const { failIfEventsMatch, after } = cmd.condition
                const matchingEvents = getMatchingEvents(failIfEventsMatch, after, snapshot)
                if (matchingEvents.length > 0) {
                    throw new AppendConditionError(cmd.condition, commands.length > 1 ? i : undefined)
                }
            }

            for (const ev of evts) {
                allNewEvents.push({
                    event: ev,
                    timestamp: Timestamp.now(),
                    position: offsetPosition(lastPosition(this.events), ++eventIndex)
                })
            }
        }

        if (allNewEvents.length === 0) throw new Error("Cannot append zero events")

        this.events.push(...allNewEvents)
        return allNewEvents[allNewEvents.length - 1].position
    }
}

const getMatchingEvents = (query: Query, after: SequencePosition | undefined, events: SequencedEvent[]) => {
    const filterByPosition = (event: SequencedEvent): boolean => {
        if (!after) return true
        return event.position.isAfter(after)
    }

    if (query.isAll) return events.filter(filterByPosition)

    return query.items.flatMap(queryItem =>
        events.filter(event => filterByPosition(event) && matchesQueryItem(queryItem, event))
    )
}

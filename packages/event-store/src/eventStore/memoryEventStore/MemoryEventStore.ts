import { SequencedEvent, EventStore, AppendCondition, DcbEvent, ReadOptions } from "../EventStore"
import { AppendConditionError } from "../AppendConditionError"
import { SequencePosition } from "../SequencePosition"
import { Timestamp } from "../Timestamp"
import { isSeqOutOfRange, matchesQueryItem as matchesQueryItem, deduplicateEvents } from "./utils"
import { Query } from "../Query"

export const ensureIsArray = (events: DcbEvent | DcbEvent[]) => (Array.isArray(events) ? events : [events])

const maxSeqNo = (events: SequencedEvent[]) =>
    events
        .map(event => event.position)
        .filter(seqNum => seqNum !== undefined)
        .pop() || SequencePosition.zero()

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
        yield* this.#read({ query, options })
    }
    async *#read({ query, options }: { query: Query; options?: ReadOptions }): AsyncGenerator<SequencedEvent> {
        if (this.testListenerRegistry.read) this.testListenerRegistry.read()

        const step = options?.backwards ? -1 : 1
        const maxPosition = maxSeqNo(this.events)
        const defaultPosition = options?.backwards ? maxPosition : SequencePosition.zero()
        let currentPosition = options?.fromPosition ?? defaultPosition
        let yieldedCount = 0

        const allMatchedEvents = !query.isAll
            ? query.items.flatMap((criterion, index) => {
                  const matchedEvents = this.events
                      .filter(
                          event =>
                              !isSeqOutOfRange(event.position, currentPosition, options?.backwards) &&
                              matchesQueryItem(criterion, event)
                      )
                      .map(event => ({ ...event, matchedCriteria: [index.toString()] }))
                      .sort((a, b) => a.position.value - b.position.value)

                  return matchedEvents
              })
            : this.events.filter(ev => !isSeqOutOfRange(ev.position, currentPosition, options?.backwards))

        const uniqueEvents = deduplicateEvents(allMatchedEvents)
            .sort((a, b) => a.position.value - b.position.value)
            .sort((a, b) =>
                options?.backwards ? b.position.value - a.position.value : a.position.value - b.position.value
            )

        for (const event of uniqueEvents) {
            yield event
            yieldedCount++
            if (options?.limit && yieldedCount >= options.limit) {
                break
            }
            currentPosition = event.position.plus(step)
        }
    }

    async append(events: DcbEvent | DcbEvent[], appendCondition?: AppendCondition): Promise<void> {
        if (this.testListenerRegistry.append) this.testListenerRegistry.append()
        const nextPosition = maxSeqNo(this.events).inc()
        const sequencedEvents: Array<SequencedEvent> = ensureIsArray(events).map((ev, i) => ({
            event: ev,
            timestamp: Timestamp.now(),
            position: nextPosition.plus(i)
        }))

        if (appendCondition) {
            const { failIfEventsMatch, after } = appendCondition

            const matchingEvents = getMatchingEvents(failIfEventsMatch, after, this.events)

            if (matchingEvents.length > 0) throw new AppendConditionError(appendCondition)
        }

        this.events.push(...sequencedEvents)
    }
}

const getMatchingEvents = (query: Query, maxSeqNo: SequencePosition, events: SequencedEvent[]) => {
    if (query.isAll) return events.filter(event => !isSeqOutOfRange(event.position, maxSeqNo.plus(1), false))

    return (query ?? []).items.flatMap(queryItem =>
        events.filter(
            event => !isSeqOutOfRange(event.position, maxSeqNo.plus(1), false) && matchesQueryItem(queryItem, event)
        )
    )
}

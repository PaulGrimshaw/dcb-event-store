import {
    SequencedEvent,
    EventStore,
    AppendCommand,
    ReadOptions,
    SubscribeOptions,
    validateAppendCondition
} from "../EventStore"
import { AppendConditionError } from "../AppendConditionError"
import { SequencePosition } from "../SequencePosition"
import { Timestamp } from "../Timestamp"
import { isInRange, matchesQueryItem, deduplicateEvents } from "./utils"
import { Query } from "../Query"
import { ensureIsArray } from "../../ensureIsArray"
import { EventEmitter } from "events"

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

    private emitter = new EventEmitter()
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
        this.emitter.emit("append")
        return allNewEvents[allNewEvents.length - 1].position
    }
    async *subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent> {
        let position = options?.after ?? SequencePosition.initial()
        const signal = options?.signal

        try {
            while (!signal?.aborted) {
                let hadEvents = false
                for await (const event of this.read(query, { after: position })) {
                    yield event
                    position = event.position
                    hadEvents = true
                }
                if (hadEvents) continue

                // Wait for append or abort
                await new Promise<void>(resolve => {
                    const onAppend = () => {
                        signal?.removeEventListener("abort", onAbort)
                        resolve()
                    }
                    const onAbort = () => {
                        this.emitter.removeListener("append", onAppend)
                        resolve()
                    }
                    this.emitter.once("append", onAppend)
                    signal?.addEventListener("abort", onAbort, { once: true })
                })
            }
        } finally {
            // Generator closed — nothing to clean up for in-memory store
        }
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

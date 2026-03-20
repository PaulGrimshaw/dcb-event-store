import { SequencedEvent } from "../EventStore"
import { SequencePosition } from "../SequencePosition"
import { matchTags } from "../../eventHandling/matchTags"
import { QueryItem } from "../Query"

export const isSeqOutOfRange = (
    sequencePosition: SequencePosition,
    fromPosition: SequencePosition,
    backwards: boolean | undefined
) => (backwards ? sequencePosition > fromPosition : sequencePosition < fromPosition)

export const deduplicateEvents = (events: SequencedEvent[]): SequencedEvent[] => {
    const uniqueEventsMap = new Map<number, SequencedEvent>()

    for (const event of events) {
        if (!uniqueEventsMap.has(event.position.value)) {
            uniqueEventsMap.set(event.position.value, event)
        }
    }

    return Array.from(uniqueEventsMap.values())
}

export const matchesQueryItem = (queryItem: QueryItem, { event }: SequencedEvent) => {
    if (queryItem.types && queryItem.types.length > 0 && !queryItem.types.includes(event.type)) return false

    return matchTags({ tagFilter: queryItem.tags, tags: event.tags })
}

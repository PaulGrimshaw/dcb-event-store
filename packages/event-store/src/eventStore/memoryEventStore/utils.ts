import { SequencedEvent } from "../EventStore.js"
import { SequencePosition } from "../SequencePosition.js"
import { matchTags } from "../../eventHandling/matchTags.js"
import { QueryItem } from "../Query.js"

export const isInRange = (
    sequencePosition: SequencePosition,
    after: SequencePosition,
    backwards: boolean | undefined
) => (backwards ? sequencePosition.isBefore(after) : sequencePosition.isAfter(after))

export const deduplicateEvents = (events: SequencedEvent[]): SequencedEvent[] => {
    const uniqueEventsMap = new Map<string, SequencedEvent>()

    for (const event of events) {
        const key = event.position.toString()
        if (!uniqueEventsMap.has(key)) {
            uniqueEventsMap.set(key, event)
        }
    }

    return Array.from(uniqueEventsMap.values())
}

export const matchesQueryItem = (queryItem: QueryItem, { event }: SequencedEvent) => {
    if (queryItem.types && queryItem.types.length > 0 && !queryItem.types.includes(event.type)) return false

    return matchTags({ tagFilter: queryItem.tags, tags: event.tags })
}

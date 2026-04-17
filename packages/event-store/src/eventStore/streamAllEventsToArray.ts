import { SequencedEvent } from "../eventStore/EventStore.js"

export const streamAllEventsToArray = async (generator: AsyncGenerator<SequencedEvent>): Promise<SequencedEvent[]> => {
    const results: SequencedEvent[] = []
    let done: boolean | undefined = false
    while (!done) {
        const next = await generator.next()
        if (next.value) {
            results.push(next.value)
        }
        done = next.done
    }
    return results
}

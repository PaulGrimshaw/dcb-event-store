import { DcbEvent, SequencedEvent } from "../eventStore/EventStore.js"
import { Tags } from "../eventStore/Tags.js"

export interface EventHandler<TEvents extends DcbEvent<string, Tags, unknown, unknown>, TTags extends Tags = Tags> {
    tagFilter?: Partial<TTags>
    onlyLastEvent?: boolean
    when: {
        [E in TEvents as E["type"]]: (
            sequencedEvent: SequencedEvent<Extract<TEvents, { type: E["type"] }>>
        ) => void | Promise<void>
    }
}

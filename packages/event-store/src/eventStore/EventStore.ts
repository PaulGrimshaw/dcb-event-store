import { Query } from "./Query"
import { SequencePosition } from "./SequencePosition"
import { Tags } from "./Tags"
import { Timestamp } from "./Timestamp"

export interface DcbEvent<Tpe extends string = string, Tgs = Tags, Dta = unknown, Mtdta = unknown> {
    type: Tpe
    tags: Tgs
    data: Dta
    metadata: Mtdta
}

export interface SequencedEvent<T extends DcbEvent = DcbEvent> {
    event: T
    timestamp: Timestamp
    position: SequencePosition
}

export type AppendCondition = {
    failIfEventsMatch: Query
    after?: SequencePosition
}

export interface ReadOptions {
    backwards?: boolean
    after?: SequencePosition
    limit?: number
}

export interface AppendCommand {
    events: DcbEvent | DcbEvent[]
    condition?: AppendCondition
}

export interface EventStore {
    append: (command: AppendCommand | AppendCommand[]) => Promise<SequencePosition>
    read: (query: Query, options?: ReadOptions) => AsyncGenerator<SequencedEvent>
}

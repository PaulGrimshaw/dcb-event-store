import { Query } from "./Query.js"
import { SequencePosition } from "./SequencePosition.js"
import { Tags } from "./Tags.js"

export interface DcbEvent<Tpe extends string = string, Tgs = Tags, Dta = unknown, Mtdta = unknown> {
    type: Tpe
    tags: Tgs
    data: Dta
    metadata: Mtdta
}

export interface SequencedEvent<T extends DcbEvent = DcbEvent> {
    event: T
    position: SequencePosition
}

/**
 * Every query item in failIfEventsMatch must specify at least one type AND one tag.
 * This enables scoped locking — without it, the store cannot determine which
 * concurrent transactions conflict.
 */
export type AppendCondition = {
    failIfEventsMatch: Query
    after?: SequencePosition
}

export function validateAppendCondition(condition: AppendCondition): void {
    if (condition.failIfEventsMatch.isAll) {
        throw new Error("AppendCondition requires scoped conditions. Query.all() is not supported.")
    }
    for (const item of condition.failIfEventsMatch.items) {
        if (!item.types?.length || !item.tags || item.tags.values.length === 0) {
            throw new Error("AppendCondition requires every query item to specify at least one type and one tag.")
        }
    }
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

export interface SubscribeOptions {
    after?: SequencePosition
    pollIntervalMs?: number
    signal?: AbortSignal
}

export interface EventStore {
    append: (command: AppendCommand | AppendCommand[]) => Promise<SequencePosition>
    read: (query: Query, options?: ReadOptions) => AsyncGenerator<SequencedEvent>
    subscribe: (query: Query, options?: SubscribeOptions) => AsyncGenerator<SequencedEvent>
}

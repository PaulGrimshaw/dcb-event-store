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
 * Conditions are a strict subset of read queries. `Query.fromItems` already
 * enforces non-empty `types` on every item; conditions add the requirement that
 * `tags` is also non-empty. This is checked at append time by
 * `validateAppendCondition`, not at `Query` construction — the same `Query`
 * value can be valid for `read()`/`subscribe()` (tags optional) but invalid for
 * `failIfEventsMatch` (tags required).
 */
export type AppendCondition = {
    failIfEventsMatch: Query
    after?: SequencePosition
}

export function validateAppendCondition(condition: AppendCondition): void {
    if (condition.failIfEventsMatch.isAll) {
        throw new Error("AppendCondition requires scoped conditions. Query.all() is not supported.")
    }
    // `types` non-emptiness is already guaranteed by `Query.fromItems`, but we
    // re-check here for defence-in-depth (a `QueryItem` could be constructed
    // outside the validated path). The extra requirement on tags is what makes
    // conditions stricter than reads — the writer's lock-key derivation needs a
    // (type, tag) pair to scope mutual exclusion against other writers.
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

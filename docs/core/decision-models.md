# Decision Models and Event Handling

API reference for building decision models -- the mechanism that reads events, derives state, and produces an [AppendCondition](event-store-interface.md#appendcondition) for optimistic concurrency. This is the core of the DCB pattern: the consistency boundary is composed dynamically from the event handlers you pass in.

See the [DCB specification](https://dcb.events/specification/) for the theoretical foundation.

> **Note:** The event handling layer is entirely optional. You can use the [EventStore](event-store-interface.md) directly -- calling `read()` to query events, building state however you like, and passing your own `AppendCondition` to `append()`. The types and functions below are a convenience for structuring the read-decide-append loop, not a requirement.

---

## EventHandler

```ts
interface EventHandler<
  TEvents extends DcbEvent<string, Tags, unknown, unknown>,
  TTags extends Tags = Tags
> {
  tagFilter?: Partial<TTags>
  onlyLastEvent?: boolean
  when: {
    [E in TEvents as E["type"]]: (
      sequencedEvent: SequencedEvent<Extract<TEvents, { type: E["type"] }>>
    ) => void | Promise<void>
  }
}
```

A stateless event handler. Declares which event types it handles and an optional tag filter. The `when` object maps event type names to callbacks that receive the typed `SequencedEvent`.

| Field | Description |
|-------|-------------|
| `tagFilter` | Optional [Tags](tags-and-queries.md#tags) filter. Only events whose tags are a superset of this filter are dispatched to `when` callbacks. |
| `onlyLastEvent` | When `true`, only the most recent matching event is processed. |
| `when` | Object keyed by event type name. Each value is a callback receiving the strongly-typed `SequencedEvent`. |

Use `EventHandler` for side-effect-only handling (projections, process managers). For decision-making with accumulated state, use [EventHandlerWithState](#eventhandlerwithstate).

---

## EventHandlerWithState

```ts
interface EventHandlerWithState<
  TEvents extends DcbEvent<string, Tags, unknown, unknown>,
  TState,
  TTags extends Tags = Tags
> {
  tagFilter?: Partial<TTags>
  onlyLastEvent?: boolean
  init: TState
  when: {
    [E in TEvents as E["type"]]: (
      sequencedEvent: SequencedEvent<Extract<TEvents, { type: E["type"] }>>,
      state: TState
    ) => TState | Promise<TState>
  }
}
```

A stateful event handler that folds events into a state value. This is what `buildDecisionModel` consumes.

| Field | Description |
|-------|-------------|
| `tagFilter` | Optional [Tags](tags-and-queries.md#tags) filter. Scopes which events are dispatched. |
| `onlyLastEvent` | When `true`, only the most recent matching event is processed. |
| `init` | The initial state before any events are applied. |
| `when` | Object keyed by event type name. Each callback receives the typed `SequencedEvent` and the current state, and returns the new state. |

### Example

```ts
const CourseCapacity = (
  courseId: string
): EventHandlerWithState<
  CourseWasRegisteredEvent | CourseCapacityWasChangedEvent |
  StudentWasSubscribedEvent | StudentWasUnsubscribedEvent,
  { subscriberCount: number; capacity: number }
> => ({
  tagFilter: Tags.fromObj({ courseId }),
  init: { subscriberCount: 0, capacity: 0 },
  when: {
    courseWasRegistered: ({ event }) => ({
      capacity: event.data.capacity,
      subscriberCount: 0
    }),
    courseCapacityWasChanged: ({ event }, { subscriberCount }) => ({
      subscriberCount,
      capacity: event.data.newCapacity
    }),
    studentWasSubscribed: (_ev, { capacity, subscriberCount }) => ({
      subscriberCount: subscriberCount + 1,
      capacity
    }),
    studentWasUnsubscribed: (_ev, { capacity, subscriberCount }) => ({
      subscriberCount: subscriberCount - 1,
      capacity
    })
  }
})
```

---

## buildDecisionModel

```ts
async function buildDecisionModel<T extends Record<string, EventHandlerWithState<any, any>>>(
  eventStore: EventStore,
  eventHandlers: T
): Promise<{
  state: { [K in keyof T]: T[K]["init"] }
  appendCondition: AppendCondition
}>
```

Reads the event store and folds events through the provided handlers to produce both the current state and a matching [AppendCondition](event-store-interface.md#appendcondition). The append condition captures the *exact* consistency boundary needed for the command being executed.

### Parameters

| Parameter | Description |
|-----------|-------------|
| `eventStore` | Any [EventStore](event-store-interface.md#eventstore) implementation. |
| `eventHandlers` | An object where each key names a handler and each value is an `EventHandlerWithState`. |

### Return value

| Field | Type | Description |
|-------|------|-------------|
| `state` | `{ [K in keyof T]: T[K]["init"] }` | A map from handler key to that handler's accumulated state. The type of each value matches the handler's `init` type. |
| `appendCondition` | `AppendCondition` | A condition built from the union of all handlers' queries, positioned after the last event read. |

### Algorithm

1. **Build the query.** For each handler, create a [QueryItem](tags-and-queries.md#queryitem) from `Object.keys(handler.when)` (the event types) and `handler.tagFilter` (the tags). Combine all items into a single `Query.fromItems(...)`.

2. **Initialize state.** Clone each handler's `init` value into a state map keyed by handler name.

3. **Read and fold.** Stream all matching events from the store (via `eventStore.read(query)`). For each event:
   - For each handler, check whether the event is relevant: the handler must have a `when` entry for the event's type, **and** the event's tags must be a superset of the handler's `tagFilter`.
   - If relevant, call the handler's `when` callback with the event and current state. Update the state.
   - Track the highest `SequencePosition` seen.

4. **Return.** Return the state map and an `AppendCondition` where `failIfEventsMatch` is the combined query and `after` is the last position read.

### How the consistency boundary is composed

The key insight of the DCB pattern is that the consistency boundary is not fixed to an aggregate -- it is derived from the handlers you pass in. Each handler contributes a `QueryItem` (types + tags) to the condition. When you compose multiple handlers that span different entities, the resulting condition protects all the invariants simultaneously.

For example, subscribing a student to a course requires checking:
- The course exists (scoped to `courseId`)
- The course has capacity (scoped to `courseId`)
- The student is not already subscribed (scoped to `courseId + studentId`)
- The student has not exceeded the subscription limit (scoped to `studentId`)

By passing all four handlers to `buildDecisionModel`, the resulting `AppendCondition` covers all four concerns in a single optimistic concurrency check.

---

## Full example: course subscription

The `course-manager-cli` example demonstrates how multiple handlers compose to enforce cross-entity invariants. The `subscribeStudentToCourse` command composes four decision models:

- `CourseExists(courseId)` -- scoped to `courseId`
- `CourseCapacity(courseId)` -- scoped to `courseId`
- `StudentAlreadySubscribed({ courseId, studentId })` -- scoped to both
- `StudentSubscriptions(studentId)` -- scoped to `studentId`

Each handler has a different `tagFilter`, so the combined query covers exactly the events relevant to the four invariants being enforced. The `appendCondition` returned by `buildDecisionModel` contains a query item per handler. If a concurrent writer appends a matching event (e.g. another student subscribes to the same course), the condition check fails and the caller retries with fresh state.

This is the DCB pattern in action: the consistency boundary is exactly as wide as the invariants require, and no wider.

See [`DecisionModels.ts`](../../examples/course-manager-cli/src/api/DecisionModels.ts) and [`Api.ts`](../../examples/course-manager-cli/src/api/Api.ts) for the full implementation, and [Examples](../examples.md) for a walkthrough.

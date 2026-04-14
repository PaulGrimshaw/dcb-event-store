# EventStore Interface

API reference for the `EventStore` interface and its associated types. This is the primary abstraction in `@dcb-es/event-store` -- all implementations ([MemoryEventStore](memory-event-store.md), [PostgresEventStore](../postgres/postgres-event-store.md)) conform to this interface.

See the [DCB specification](https://dcb.events/specification/) for the theoretical foundation.

---

## EventStore

```ts
interface EventStore {
  append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition>
  read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent>
  subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent>
}
```

### `append`

Appends one or more events to the store. Accepts a single `AppendCommand` or an array of commands.

- When given an array of commands, all events across all commands are appended atomically. Each command's condition is checked independently against the store state *before* any events from the batch are written.
- Returns the `SequencePosition` of the last appended event.
- Throws `AppendConditionError` if any command's condition is violated.
- Throws if zero events would be appended.

### `read`

Returns an `AsyncGenerator` that yields `SequencedEvent` items matching the given [Query](tags-and-queries.md#query). Events are yielded in position order (ascending by default).

The generator can be consumed with `for await...of` or collected with [`streamAllEventsToArray()`](memory-event-store.md#streamalleventstoarray).

### `subscribe`

Returns a long-lived `AsyncGenerator` that yields matching events and then waits for new events to be appended. Unlike `read`, the generator does not terminate when it reaches the end of the stream -- it polls or listens for new appends.

Control the subscription with `SubscribeOptions.signal` (an `AbortSignal`) to stop the generator.

---

## DcbEvent

```ts
interface DcbEvent<
  Tpe extends string = string,
  Tgs = Tags,
  Dta = unknown,
  Mtdta = unknown
> {
  type: Tpe
  tags: Tgs
  data: Dta
  metadata: Mtdta
}
```

The core event structure in the DCB pattern.

| Parameter | Description |
|-----------|-------------|
| `Tpe` | String literal type for the event name (e.g. `"courseWasRegistered"`). Defaults to `string`. |
| `Tgs` | The [Tags](tags-and-queries.md#tags) type. Defaults to `Tags`. |
| `Dta` | The event payload type. Defaults to `unknown`. |
| `Mtdta` | Metadata type (correlation IDs, timestamps, etc.). Defaults to `unknown`. |

All four fields are required. Concrete event classes typically implement `DcbEvent` directly. See [`Events.ts`](../../examples/course-manager-cli/src/api/Events.ts) for examples.

---

## SequencedEvent

```ts
interface SequencedEvent<T extends DcbEvent = DcbEvent> {
  event: T
  position: SequencePosition
}
```

An event together with its global [SequencePosition](tags-and-queries.md#sequenceposition) in the store. This is what `read` and `subscribe` yield, and what event handler `when` callbacks receive.

---

## AppendCondition

```ts
type AppendCondition = {
  failIfEventsMatch: Query
  after?: SequencePosition
}
```

The optimistic concurrency mechanism. When provided on an `AppendCommand`, the store checks whether any events matching `failIfEventsMatch` exist *after* the given position. If they do, the append is rejected with an `AppendConditionError`.

- `failIfEventsMatch` -- a [Query](tags-and-queries.md#query) describing the conflict boundary. Must not be `Query.all()`. Every `QueryItem` must specify at least one type and one tag.
- `after` -- optional. Only events after this position are checked. When omitted, the entire stream is checked.

This is the core of the Dynamic Consistency Boundary pattern: the condition is scoped to exactly the event types and tags that matter for the invariant being enforced, rather than locking an entire aggregate or stream.

See `validateAppendCondition()` below for the validation rules.

---

## AppendCommand

```ts
interface AppendCommand {
  events: DcbEvent | DcbEvent[]
  condition?: AppendCondition
}
```

A unit of work for `append()`. Contains one or more events and an optional condition.

- `events` -- a single `DcbEvent` or an array. Events are appended in order.
- `condition` -- if provided, the store validates it before writing. See [AppendCondition](#appendcondition).

When `append()` receives an array of `AppendCommand`, all commands are processed atomically.

---

## ReadOptions

```ts
interface ReadOptions {
  backwards?: boolean
  after?: SequencePosition
  limit?: number
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `backwards` | `false` | Yield events in reverse position order (newest first). |
| `after` | -- | Only yield events after (or before, if `backwards`) this position. |
| `limit` | -- | Maximum number of events to yield. |

---

## SubscribeOptions

```ts
interface SubscribeOptions {
  after?: SequencePosition
  pollIntervalMs?: number
  signal?: AbortSignal
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `after` | `SequencePosition.initial()` | Start yielding events after this position. Events at or before this position are skipped. |
| `pollIntervalMs` | -- | Polling interval for implementations that poll. The Postgres implementation uses `pg_notify` and does not poll by default. |
| `signal` | -- | An `AbortSignal` to stop the subscription. When aborted, the generator returns cleanly. |

---

## AppendConditionError

```ts
class AppendConditionError extends Error {
  readonly appendCondition: AppendCondition
  readonly commandIndex?: number

  constructor(appendCondition: AppendCondition, commandIndex?: number)
}
```

Thrown by `append()` when the append condition is violated -- i.e., events matching `failIfEventsMatch` exist after the specified position.

| Property | Description |
|----------|-------------|
| `appendCondition` | The condition that was violated. |
| `commandIndex` | Present only when `append()` received an array of commands. Identifies which command (zero-indexed) failed. |
| `name` | Always `"AppendConditionError"`. |
| `message` | `"Expected Version fail: New events matching appendCondition found."` with an optional `(command N)` suffix. |

### Recovery pattern

The standard recovery for an `AppendConditionError` is to re-read the decision model and retry. The [buildDecisionModel](decision-models.md#builddecisionmodel) function returns a fresh `appendCondition` on each call, so a retry loop naturally picks up the latest state:

```ts
while (true) {
  const { state, appendCondition } = await buildDecisionModel(eventStore, {
    courseExists: CourseExists(courseId)
  })
  if (state.courseExists) throw new Error("Course already exists")

  try {
    await eventStore.append({
      events: new CourseWasRegisteredEvent({ courseId, title, capacity }),
      condition: appendCondition
    })
    break
  } catch (e) {
    if (e instanceof AppendConditionError) continue // retry
    throw e
  }
}
```

---

## validateAppendCondition

```ts
function validateAppendCondition(condition: AppendCondition): void
```

Validates that an `AppendCondition` meets the requirements for scoped locking:

1. `failIfEventsMatch` must not be `Query.all()`.
2. Every `QueryItem` in the query must specify at least one type **and** at least one tag.

Throws a plain `Error` if validation fails. Called internally by event store implementations before processing an append. Also available as a public export for custom implementations.

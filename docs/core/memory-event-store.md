# MemoryEventStore and Utilities

API reference for the in-memory [EventStore](event-store-interface.md#eventstore) implementation and helper functions exported from `@dcb-es/event-store`.

---

## MemoryEventStore

```ts
class MemoryEventStore implements EventStore {
  constructor(initialEvents?: Array<SequencedEvent>)

  append(command: AppendCommand | AppendCommand[]): Promise<SequencePosition>
  read(query: Query, options?: ReadOptions): AsyncGenerator<SequencedEvent>
  subscribe(query: Query, options?: SubscribeOptions): AsyncGenerator<SequencedEvent>

  on(ev: "read" | "append", fn: () => void): void
}
```

A fully-featured in-memory implementation of the `EventStore` interface. All events are stored in an array in memory -- nothing is persisted.

### Constructor

```ts
new MemoryEventStore(initialEvents?: Array<SequencedEvent>)
```

Optionally accepts pre-seeded events for test scenarios. If omitted, the store starts empty.

```ts
// Empty store
const store = new MemoryEventStore()

// Pre-seeded store
const store = new MemoryEventStore([
  {
    event: { type: "courseWasRegistered", tags: Tags.fromObj({ courseId: "cs101" }), data: { title: "CS 101", capacity: 30, courseId: "cs101" }, metadata: {} },
    position: SequencePosition.fromString("1")
  }
])
```

### EventStore methods

`append`, `read`, and `subscribe` behave as specified by the [EventStore interface](event-store-interface.md#eventstore).

- **append** validates conditions (via `validateAppendCondition`), assigns sequential positions starting after the last existing event, and emits an internal `"append"` event.
- **read** yields events matching the query, respecting `ReadOptions` (position filtering, backwards ordering, limit).
- **subscribe** yields matching events, then waits for new appends via an internal `EventEmitter`. Stops when `options.signal` is aborted.

### Test helpers

#### `on(ev: "read" | "append", fn: () => void)`

Registers a callback that fires whenever `read()` or `append()` is called. Useful for test assertions such as verifying that a read was performed, or for injecting delays.

```ts
const store = new MemoryEventStore()

let readCount = 0
store.on("read", () => readCount++)

for await (const event of store.read(Query.all())) { /* ... */ }
expect(readCount).toBe(1)
```

### When to use

- **Unit tests** -- fast, no dependencies, deterministic.
- **Prototyping** -- get a working system before choosing a persistence backend.
- **Integration tests** -- when testing command handlers, decision models, or event handlers in isolation from the database.

For production use, see [PostgresEventStore](../postgres/postgres-event-store.md).

---

## streamAllEventsToArray

```ts
function streamAllEventsToArray(
  generator: AsyncGenerator<SequencedEvent>
): Promise<SequencedEvent[]>
```

Collects all events from an `AsyncGenerator<SequencedEvent>` into an array. A convenience wrapper over `for await...of` that is useful in tests and scripts.

```ts
const events = await streamAllEventsToArray(
  eventStore.read(Query.all())
)
```

**Note:** Do not use this with `subscribe()` -- that generator does not terminate on its own, so the promise would never resolve.

---

## ensureIsArray

```ts
function ensureIsArray<T>(input: T | T[]): T[]
```

Returns the input wrapped in an array if it is not already an array. Used internally by `append()` to normalize `AppendCommand | AppendCommand[]` and `DcbEvent | DcbEvent[]`. Exported as a general utility.

```ts
ensureIsArray("hello")        // ["hello"]
ensureIsArray(["a", "b"])     // ["a", "b"]
```

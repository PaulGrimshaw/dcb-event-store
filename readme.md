# Kraken DCB EventStore

A TypeScript implementation of the [Dynamic Consistency Boundary (DCB)](https://dcb.events) pattern for event sourcing, as specified by Sara Pellegrini and Bastian Waidelich at [dcb.events](https://dcb.events).

In traditional event sourcing, aggregates define fixed consistency boundaries. When a business rule spans two aggregates — like limiting course enrolment while also capping how many courses a student can join — you're stuck choosing between large aggregates, sagas, or eventual consistency. DCBs solve this by defining the consistency boundary at runtime via a query, not a stream name. The scope is exactly as wide as the invariant requires.

> See the full [DCB specification](https://dcb.events/specification/) for the pattern definition.

## Install

```bash
npm install @dcb-es/event-store                # core abstractions + in-memory store
npm install @dcb-es/event-store-postgres       # production Postgres adapter
```

## Usage

### Define events

Events carry **tags** — key-value references to the domain concepts they involve. Tags determine which consistency boundaries the event participates in. They are separate from the event's data payload: `data` is the business content, `tags` are the references the event store uses for filtering and scoped locking.

```typescript
import { DcbEvent, Tags } from "@dcb-es/event-store"

class StudentWasSubscribed implements DcbEvent {
    type = "studentWasSubscribed" as const
    tags: Tags
    data: { courseId: string; studentId: string }
    metadata = {}

    constructor(courseId: string, studentId: string) {
        // Tags reference the domain concepts involved in consistency rules.
        // This event is tagged with both courseId and studentId because it
        // participates in two boundaries: course capacity AND student limits.
        this.tags = Tags.fromObj({ courseId, studentId })
        this.data = { courseId, studentId }
    }
}
```

### Define decision models

Decision models are reducers scoped by one or more tags. They derive the state needed to validate a command.

```typescript
import { EventHandlerWithState, Tags } from "@dcb-es/event-store"

// Scoped to a single courseId — only sees events tagged with this course
const CourseCapacity = (courseId: string): EventHandlerWithState<any,
    { subscriberCount: number; capacity: number }
> => ({
    tagFilter: Tags.fromObj({ courseId }),
    init: { subscriberCount: 0, capacity: 0 },
    when: {
        courseWasRegistered: ({ event }) => ({ capacity: event.data.capacity, subscriberCount: 0 }),
        studentWasSubscribed: (_, s) => ({ ...s, subscriberCount: s.subscriberCount + 1 }),
        studentWasUnsubscribed: (_, s) => ({ ...s, subscriberCount: s.subscriberCount - 1 }),
    }
})

// Scoped to a single studentId — sees all subscriptions for this student
const StudentSubscriptions = (studentId: string): EventHandlerWithState<any,
    { count: number }
> => ({
    tagFilter: Tags.fromObj({ studentId }),
    init: { count: 0 },
    when: {
        studentWasSubscribed: (_, s) => ({ count: s.count + 1 }),
        studentWasUnsubscribed: (_, s) => ({ count: s.count - 1 }),
    }
})
```

### Handle a command

Compose decision models with `buildDecisionModel`. It reads matching events, folds them through each handler, and returns the derived state plus an `AppendCondition` that protects the combined consistency boundary.

```typescript
import { buildDecisionModel } from "@dcb-es/event-store"

async function subscribeToCourse(eventStore, courseId: string, studentId: string) {
    const { state, appendCondition } = await buildDecisionModel(eventStore, {
        capacity: CourseCapacity(courseId),
        subscriptions: StudentSubscriptions(studentId),
    })

    if (state.capacity.subscriberCount >= state.capacity.capacity)
        throw new Error("Course is full")
    if (state.subscriptions.count >= 5)
        throw new Error("Student subscription limit reached")

    await eventStore.append({
        events: new StudentWasSubscribed(courseId, studentId),
        condition: appendCondition,  // fails if any relevant event was added concurrently
    })
}
```

The consistency boundary spans both the course _and_ the student — no aggregates, no sagas. If a concurrent write conflicts, `append` throws `AppendConditionError` and you retry from the top with fresh state.

### Use the store directly

The event handling layer (`buildDecisionModel`, `EventHandler`, etc.) is optional. The `EventStore` interface is three methods:

```typescript
const eventStore = new PostgresEventStore({ pool })
await eventStore.ensureInstalled()

// Append
const position = await eventStore.append({
    events: { type: "courseWasRegistered", tags: Tags.fromObj({ courseId: "cs101" }), data: { title: "CS 101", capacity: 30 }, metadata: {} }
})

// Read
for await (const { event, position } of eventStore.read(Query.all())) {
    console.log(event.type, position.toString())
}

// Subscribe (live stream via pg_notify)
const controller = new AbortController()
for await (const { event } of eventStore.subscribe(Query.all(), { signal: controller.signal })) {
    console.log("New event:", event.type)
}
```

### Projections

Build read models with `runHandler` — a subscribe-based loop that atomically updates projections and bookmarks:

```typescript
import { runHandler, waitUntilProcessed, ensureHandlersInstalled } from "@dcb-es/event-store-postgres"

await ensureHandlersInstalled(pool, ["courseProjection"], "_handler_bookmarks")

const { promise } = runHandler({
    pool, eventStore,
    handlerName: "courseProjection",
    handlerFactory: (client) => ({
        when: {
            courseWasRegistered: async ({ event }) => {
                await client.query("INSERT INTO courses (id, title) VALUES ($1, $2)",
                    [event.data.courseId, event.data.title])
            }
        }
    }),
    signal: controller.signal,
})

// After appending, wait for the projection to catch up before querying
const position = await eventStore.append({ events: newEvent, condition })
await waitUntilProcessed(pool, "courseProjection", position)
// Read model now reflects the event
```

## Packages

| Package | Description |
|---------|-------------|
| [`@dcb-es/event-store`](https://www.npmjs.com/package/@dcb-es/event-store) | Core abstractions, decision model helpers, in-memory `MemoryEventStore` |
| [`@dcb-es/event-store-postgres`](https://www.npmjs.com/package/@dcb-es/event-store-postgres) | Postgres adapter — optimised append strategies, advisory/row lock strategies, `pg_notify` subscriptions, handler infrastructure |

## Documentation

Full reference in [`docs/`](docs/index.md):

- [Overview](docs/overview.md) — DCB pattern, architecture, data flow
- [Getting Started](docs/getting-started.md) — setup, running examples, debugging
- [Core API](docs/core/event-store-interface.md) — EventStore, Tags, Query, decision models
- [Postgres Design](docs/postgres/design.md) — how the adapter implements DCBs: scoped locking, condition checking, append strategies
- [Postgres API](docs/postgres/postgres-event-store.md) — PostgresEventStore, lock strategies, event handling
- [Examples](docs/examples.md) — walkthrough of both CLI example apps
- [Internals](docs/internals.md) — implementation details and design decisions

## Development

```bash
yarn install && npm run build && npm test
```

Docker required for Postgres tests. See [Getting Started](docs/getting-started.md).

## License

[MIT](./LICENSE.md)

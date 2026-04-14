# DCB Event Store

A TypeScript/Node.js implementation of the [Dynamic Consistency Boundary](https://dcb.events) (DCB) pattern for event sourcing, as specified by Sara Pellegrini and Bastian Waidelich at [dcb.events](https://dcb.events).

DCBs replace the traditional aggregate with a more flexible consistency model: instead of locking an entire aggregate root, you define the exact subset of events (by type and tags) that a command depends on, and the event store enforces consistency only within that boundary. This eliminates false conflicts and allows independent commands to execute concurrently even when they share a stream.

See the full [DCB specification](https://dcb.events/specification/) for the pattern definition.

---

## Packages

| Package | npm | Description |
|---------|-----|-------------|
| `@dcb-es/event-store` | [@dcb-es/event-store](https://www.npmjs.com/package/@dcb-es/event-store) | Core abstractions -- EventStore interface, tags, queries, decision models, in-memory implementation |
| `@dcb-es/event-store-postgres` | [@dcb-es/event-store-postgres](https://www.npmjs.com/package/@dcb-es/event-store-postgres) | Production Postgres adapter with advisory locking, pg_notify subscriptions, and handler infrastructure |

---

## Documentation

### Getting Started

- [Overview](overview.md) -- The DCB pattern, how this library models it, and high-level architecture
- [Getting Started](getting-started.md) -- Installation, project setup, running tests, and debugging

### Core (`@dcb-es/event-store`)

- [EventStore Interface](core/event-store-interface.md) -- `EventStore`, `DcbEvent`, `AppendCondition`, `AppendConditionError`, and the read/append contract
- [Tags and Queries](core/tags-and-queries.md) -- `Tags`, `Query`, `SequencePosition`, and how tag-based filtering drives the consistency boundary
- [Decision Models](core/decision-models.md) -- `EventHandler`, `EventHandlerWithState`, `buildDecisionModel`, and the pattern for deriving command decisions from event history
- [MemoryEventStore](core/memory-event-store.md) -- In-memory implementation, test utilities, and `streamAllEventsToArray`

### Postgres (`@dcb-es/event-store-postgres`)

- [Design](postgres/design.md) -- How the Postgres adapter implements DCBs: scoped locking, condition checking, append strategies, notifications
- [PostgresEventStore](postgres/postgres-event-store.md) -- Configuration, schema, append strategies, `pg_notify` subscriptions, and `subscribe()`
- [Lock Strategies](postgres/lock-strategies.md) -- Advisory locks, row-level locks, FNV-1a hashing, and choosing a strategy
- [Event Handling](postgres/event-handling.md) -- `runHandler`, `waitUntilProcessed`, bookmark management, and building projections

### Reference

- [Examples](examples.md) -- Walkthrough of `course-manager-cli` and `course-manager-cli-with-readmodel`
- [Internals](internals.md) -- Implementation details, design decisions, and schema layout

---

## Quick Example

```typescript
import { buildDecisionModel, EventHandlerWithState, Tags } from "@dcb-es/event-store"
import { PostgresEventStore } from "@dcb-es/event-store-postgres"

const CourseExists = (courseId: string): EventHandlerWithState<any, boolean> => ({
  tagFilter: Tags.fromObj({ courseId }),
  init: false,
  when: { courseWasRegistered: () => true }
})

// Read events, derive state, get an append condition
const { state, appendCondition } = await buildDecisionModel(eventStore, {
  courseExists: CourseExists("cs101")
})

if (state.courseExists) throw new Error("Course already exists")

// Append with the condition -- the store rejects if conflicting events
// were written since the decision was made
await eventStore.append({
  events: { type: "courseWasRegistered", tags: Tags.fromObj({ courseId: "cs101" }), data: { title: "CS 101", capacity: 30 }, metadata: {} },
  condition: appendCondition
})
```

---

## License

MIT

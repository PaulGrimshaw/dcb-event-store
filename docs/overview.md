# Overview

## The Problem with Aggregates

In traditional event sourcing, aggregates define the consistency boundary: all commands targeting the same aggregate are serialised against its stream. This works until it doesn't.

When a business invariant spans two aggregates, you have two options -- both bad. Make a larger aggregate that encompasses both (increasing contention, reducing throughput) or introduce a saga that coordinates across aggregates (accepting eventual consistency and compensating actions for what should be a simple rule).

The root cause is that aggregate boundaries are **static**. They are defined at design time by stream name, not by what a command actually needs. Two commands on the same aggregate that touch entirely different data still contend. Two commands on different aggregates that share a constraint cannot be checked atomically.

## Dynamic Consistency Boundaries

The [Dynamic Consistency Boundary](https://dcb.events) (DCB) pattern, specified by Sara Pellegrini and Bastian Waidelich at [dcb.events](https://dcb.events), replaces the aggregate with a consistency boundary defined at runtime by the command itself.

Instead of reading a single aggregate stream, a command declares a **query** -- the set of event types and tags it depends on. The event store reads those events, the command makes its decision, and on append it provides an **append condition**: "fail if any new events matching this query have been written since position N."

This is query-based optimistic locking. The boundary is exactly as wide as the invariant requires -- no wider, no narrower. Two commands that depend on different tags never conflict, even in the same bounded context. A command that spans what would have been two aggregates simply queries across both tag sets.

For the full specification, see [dcb.events/specification](https://dcb.events/specification/).

## Key Concepts

This section introduces the core concepts briefly. Each has dedicated documentation linked below.

### Single Shared Stream

DCBs use a single event stream per bounded context. There are no per-aggregate streams. All events land in one append-only log, ordered by a global [sequence position](core/tags-and-queries.md). Partitioning happens at query time via tags, not at write time via stream names.

### Tags

[Tags](core/tags-and-queries.md) are key-value pairs attached to each event (e.g., `courseId=CS101`, `studentId=42`). They serve the same role that stream names serve in aggregate-per-stream designs -- they define which events belong to which logical grouping -- but a single event can carry multiple tags, enabling it to participate in multiple consistency boundaries.

### Queries

A [Query](core/tags-and-queries.md) is a list of query items, each specifying event types and tag filters. It defines which events to read from the store. The query drives both reads and the append condition.

### AppendCondition

The `AppendCondition` is the optimistic lock. It contains a query (`failIfEventsMatch`) and a position (`after`). On append, the store checks whether any events matching the query exist after the given position. If they do, the append fails with an `AppendConditionError` and the caller retries.

### Decision Models

A [decision model](core/decision-models.md) composes one or more event handlers into a single read pass. `buildDecisionModel` reads the matching events, folds them into state, and returns both the derived state and the `AppendCondition` for the append. The caller validates business rules against the state, then appends with the condition.

Decision models are composable. A command that needs to enforce two independent invariants combines two event handlers into one decision model. The resulting append condition covers both.

## Architecture

### Monorepo Structure

The library is split into two packages:

**`@dcb-es/event-store`** -- Core abstractions and in-memory implementation. Defines the `EventStore` interface, `DcbEvent`, `Tags`, `Query`, `SequencePosition`, `AppendCondition`, `AppendConditionError`, `buildDecisionModel`, and the event handler types. The in-memory `MemoryEventStore` implements the full interface for unit testing. This package has no infrastructure dependencies.

**`@dcb-es/event-store-postgres`** -- Production Postgres adapter. `PostgresEventStore` implements `EventStore` with:

- Multiple append strategies (stored-procedure for small appends, `COPY FROM` for bulk writes, batched multi-command appends)
- Pluggable [lock strategies](postgres/lock-strategies.md) -- advisory locks (default) or row-level locks (RDS Proxy / Aurora compatible)
- `pg_notify`-driven subscriptions via `subscribe()`
- Handler infrastructure: `runHandler` for subscribe-based projections with atomic bookmark management, `waitUntilProcessed` for synchronous read-after-write

Two example applications demonstrate the pattern end-to-end:

- `examples/course-manager-cli` -- Simple CLI with course and student management
- `examples/course-manager-cli-with-readmodel` -- Extended version with subscribe-based projections and read models

### Event Handling is Optional

The event handling layer -- `buildDecisionModel`, `EventHandler`, `EventHandlerWithState`, `runHandler`, `waitUntilProcessed` -- is a convenience built on top of the core `EventStore` interface. You can use `append()`, `read()`, and `subscribe()` directly without any of the event handling abstractions. The decision model machinery is one way to structure the read-decide-append loop, but the store itself imposes no requirements on how you build state or manage projections.

### Core is Adapter-Agnostic

The `@dcb-es/event-store` package defines the contract. The `EventStore` interface has three methods:

- `append(command)` -- Append events, optionally with a condition
- `read(query, options?)` -- Read matching events as an async generator
- `subscribe(query, options?)` -- Live event stream (poll + notification)

Any adapter that implements this interface is a valid event store. The decision model machinery, event handlers, and tag matching all operate against the interface, not a concrete implementation.

## Data Flow

### Command Handling

1. **Build the decision model.** Call `buildDecisionModel(eventStore, handlers)`. This reads all events matching the handlers' combined query, folds them through each handler's `when` reducers, and returns the derived `state` and the `appendCondition`.

2. **Validate business rules.** Inspect the state. If the command violates an invariant, reject it. No events are written.

3. **Append with the condition.** Call `eventStore.append({ events, condition: appendCondition })`. The store atomically checks whether any events matching the condition's query have been written after the recorded position. If none have, the events are appended. If any have, the store throws `AppendConditionError`.

4. **Retry on conflict.** Catch `AppendConditionError`, re-run from step 1 (re-read events, rebuild state, re-validate, re-attempt append). The retry loop is the caller's responsibility.

### Projection (Subscribe-Based)

1. **Start a handler.** Call `runHandler({ pool, eventStore, handlerName, handlerFactory })`. The runner reads the handler's bookmark from Postgres, subscribes to the event store from that position, and begins processing events.

2. **Process each event.** For each event, the runner opens a transaction, calls the handler's `when` function (which writes to read model tables using the transaction client), updates the bookmark, and commits. The projection write and bookmark advance are atomic -- if either fails, both roll back.

3. **Wait for processing.** Command handlers that need read-after-write consistency call `waitUntilProcessed(pool, handlerName, position)` after appending. This blocks until the named handler's bookmark reaches the given position, using fast polling with a LISTEN/NOTIFY fallback.

---

Next: [Getting Started](getting-started.md) | [EventStore Interface](core/event-store-interface.md) | [Tags and Queries](core/tags-and-queries.md)

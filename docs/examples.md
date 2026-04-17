# Examples

The repository includes two CLI applications that implement the [course subscriptions](https://dcb.events/examples/course-subscriptions/) example from dcb.events. Both manage the same domain -- courses, students, and subscriptions -- but differ in how they handle reads.

| Example | Reads from | Key concepts |
|---------|-----------|--------------|
| `course-manager-cli` | Event stream (on-the-fly) | Core DCB pattern, decision models, command handling |
| `course-manager-cli-with-readmodel` | PostgreSQL read model | Projections, `runHandler`, `waitUntilProcessed` |

Both require a running PostgreSQL instance and use [`PostgresEventStore`](postgres/postgres-event-store.md) as the write-side store.

---

## course-manager-cli

**Location:** [`examples/course-manager-cli/`](../examples/course-manager-cli/)

This example demonstrates the core DCB pattern with no read model. Every command derives its state on-the-fly from the event stream using `buildDecisionModel`, validates business rules against that state, and appends events with the returned append condition. There is no separate query side -- the event stream is the only source of truth for both reads and writes.

### Events

Six event types model the domain. Each implements [`DcbEvent`](core/event-store-interface.md#dcbevent) with a literal `type`, typed `data` payload, and [`Tags`](core/tags-and-queries.md#tags) referencing the domain concepts involved in the event.

| Event | Tags | Data |
|-------|------|------|
| `courseWasRegistered` | `courseId` | `courseId`, `title`, `capacity` |
| `courseCapacityWasChanged` | `courseId` | `courseId`, `newCapacity` |
| `courseTitleWasChanged` | `courseId` | `courseId`, `newTitle` |
| `studentWasRegistered` | `studentId`, `studentNumberIndex=global` | `studentId`, `name`, `studentNumber` |
| `studentWasSubscribed` | `studentId`, `courseId` | `courseId`, `studentId` |
| `studentWasUnsubscribed` | `studentId`, `courseId` | `courseId`, `studentId` |

Tag selection is deliberate. `studentWasSubscribed` carries both `courseId` and `studentId` because it participates in two distinct consistency boundaries: course capacity enforcement and per-student subscription limits. The `studentNumberIndex=global` tag on `studentWasRegistered` exists to give the `NextStudentNumber` decision model a tag to scope its query on -- without it, there would be no tag to lock against for globally-sequenced student numbers.

### Decision models

Each decision model is a factory function returning an [`EventHandlerWithState`](core/decision-models.md#eventhandlerwithstate). It declares which event types it handles, a `tagFilter` scoping it to a specific entity instance, an initial state, and reducer functions in `when` that fold events into accumulated state.

| Model | Scoped by | State | Description |
|-------|-----------|-------|-------------|
| `CourseExists` | `courseId` | `boolean` | Has the course been registered? |
| `CourseCapacity` | `courseId` | `{ subscriberCount, capacity }` | Tracks capacity and current subscriber count across four event types |
| `CourseTitle` | `courseId` | `string` | Current title (registration + changes) |
| `StudentAlreadyRegistered` | `studentId` | `boolean` | Has the student been registered? |
| `StudentAlreadySubscribed` | `courseId + studentId` | `boolean` | Is this student subscribed to this course? |
| `StudentSubscriptions` | `studentId` | `{ subscriptionCount }` | Total courses this student is subscribed to |
| `NextStudentNumber` | `studentNumberIndex=global` | `number` | Next sequential student number |

For the full implementation, see [`examples/course-manager-cli/src/api/DecisionModels.ts`](../examples/course-manager-cli/src/api/DecisionModels.ts). For how `EventHandlerWithState` works, see [Decision Models](core/decision-models.md).

### Command handling

Every command follows the same three-step pattern:

1. **Build decision model** -- call `buildDecisionModel` with the event store and a map of named decision models. This issues a single combined query against the event store, routes matching events to each handler, and returns the accumulated `state` and an `appendCondition`.

2. **Validate** -- check business rules against the derived state. If any rule is violated, throw an error.

3. **Append** -- persist the new event(s) with the `appendCondition`. The store atomically rejects the append if any events matching the combined query were added since the state was read.

The `subscribeStudentToCourse` command (see [`Api.ts`](../examples/course-manager-cli/src/api/Api.ts)) is the most illustrative because it composes four decision models spanning two entity instances (a course and a student) into a single dynamic consistency boundary. It calls `buildDecisionModel` with `CourseExists`, `CourseCapacity`, `StudentAlreadySubscribed`, and `StudentSubscriptions`, validates four business rules against the derived state, then appends with the returned `appendCondition`.

The four decision models produce a consistency boundary that covers:

- Events tagged with `courseId` (course existence, capacity, and subscription counts for that course)
- Events tagged with both `courseId` and `studentId` (whether this student is already subscribed to this course)
- Events tagged with `studentId` (total subscription count for the student across all courses)

If a concurrent process appends any event matching this combined query -- another student subscribing to the same course, or the same student subscribing to a different course -- the append fails and the caller retries from step 1. This is query-based optimistic locking in action: no aggregates, no sagas, and no eventual consistency compromise.

### CLI

The interactive CLI uses `inquirer` to present a menu of operations:

- Register course / Register student
- Update course capacity / Update course title
- Subscribe / Unsubscribe student from course
- Exit

Each menu action prompts for the relevant inputs (course ID, student ID, etc.) and calls the corresponding `Api` method. Errors from business rule violations are caught and displayed in the terminal.

---

## course-manager-cli-with-readmodel

**Location:** [`examples/course-manager-cli-with-readmodel/`](../examples/course-manager-cli-with-readmodel/)

This example extends the basic CLI with a PostgreSQL read model. The write side is identical -- the same events, decision models, and `buildDecisionModel` command handling pattern. The difference is on the read side: instead of deriving state from the event stream on every query, a background projection handler maintains denormalized tables that the CLI reads from directly.

### What it adds

- A **projection handler** (`PostgresCourseSubscriptionsProjection`) that reacts to events and writes to three PostgreSQL tables
- A **read model repository** (`PostgresCourseSubscriptionsRepository`) providing CRUD queries against those tables
- The **write-then-wait pattern** using `waitUntilProcessed` to guarantee read-your-writes consistency
- Background handler lifecycle management with `runHandler` and `AbortController`

### Entry point wiring

The [`index.ts`](../examples/course-manager-cli-with-readmodel/index.ts) entry point wires everything together. Three setup steps happen before the CLI starts:

1. `eventStore.ensureInstalled()` -- creates the events table (idempotent)
2. `ensureHandlersInstalled(pool, [PROJECTION_NAME], "_handler_bookmarks")` -- creates the bookmark table and registers the handler with a starting position of 0
3. `installPostgresCourseSubscriptionsRepository(pool)` -- creates the three read model tables (`courses`, `students`, `subscriptions`)

`runHandler` starts a subscribe-based loop in the background. It uses the event store's `subscribe()` method (backed by `pg_notify`) to receive events as they are appended, processes each one in its own transaction, and atomically advances the handler's bookmark. The `AbortController` provides clean shutdown -- aborting the signal causes the handler to finish its current event and resolve its promise.

### Projection handler

`PostgresCourseSubscriptionsProjection` is an [`EventHandler`](core/decision-models.md#eventhandler) that handles all six event types, translating each into a repository write (INSERT, UPDATE, or DELETE against the read model tables). The `handlerFactory` callback in `runHandler` receives a `PoolClient` already inside a transaction, so the projection writes and bookmark advance are atomic -- if either fails, neither commits.

See [`PostgresCourseSubscriptionsProjection.ts`](../examples/course-manager-cli-with-readmodel/src/api/PostgresCourseSubscriptionsProjection.ts) for the full implementation.

### Read model repository

The repository is a plain CRUD layer over three PostgreSQL tables:

| Table | Columns | Purpose |
|-------|---------|---------|
| `courses` | `id`, `title`, `capacity` | Course registrations and updates |
| `students` | `id`, `name`, `student_number` | Student registrations |
| `subscriptions` | `course_id`, `student_id` | Active course-student subscriptions |

The repository exposes two query methods used by the CLI:

- `findCourseById(courseId)` -- returns the course with its subscribed students (joined through `subscriptions`)
- `findStudentById(studentId)` -- returns the student with their subscribed courses

And write methods called by the projection handler: `registerCourse`, `registerStudent`, `updateCourseCapacity`, `updateCourseTitle`, `subscribeStudentToCourse`, `unsubscribeStudentFromCourse`.

This repository has nothing to do with event sourcing itself -- it is a conventional repository pattern with SQL queries, used here to demonstrate how a projection handler might persist denormalized state to a database.

### Write-then-wait pattern

The key difference from the basic example is what happens after `append`. In the basic example, the command is done after the append succeeds. In this example, every command captures the returned `SequencePosition` and calls `waitUntilProcessed` before returning. See [`Api.ts`](../examples/course-manager-cli-with-readmodel/src/api/Api.ts) for the implementation.

The flow is:

1. `append` returns the `SequencePosition` of the newly written event(s)
2. `waitUntilProcessed` polls the handler's bookmark row, then falls back to `LISTEN`/`NOTIFY` if needed, waiting until the bookmark reaches or passes that position
3. Only then does the command return, guaranteeing that subsequent reads from the read model will reflect the just-appended event

This gives the CLI read-your-writes consistency without coupling the write path to the projection. The projection runs independently in the background; `waitUntilProcessed` is just a synchronization point that blocks the caller until the projection has caught up to a known position. The default timeout is 5 seconds.

### How it differs from the basic example

The **write side is identical** -- same events, same decision models, same `buildDecisionModel` pattern. The differences are all on the read side:

| Aspect | Basic example | Read model example |
|--------|--------------|-------------------|
| Read path | Derives state from event stream on every command | Queries denormalized PostgreSQL tables |
| Query operations | Not available | `findCourseById`, `findStudentById` return rich objects with joins |
| Background work | None | `runHandler` runs a projection handler continuously |
| After append | Done | Waits for projection via `waitUntilProcessed` |
| CLI menu | Write operations only | Adds "Find course" and "Find student" |
| Lifecycle | Start store, run CLI | Start store, install handlers, install read model tables, start handler, run CLI, shut down handler |

The basic example is appropriate when all interactions are commands that need only the state derivable from decision models. The read model example is appropriate when you need rich query capabilities -- listing, searching, joining -- that would be expensive or awkward to derive from the event stream on every request.

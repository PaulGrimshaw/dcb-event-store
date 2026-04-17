# Getting Started

## Using DCB Event Store as a library

### Prerequisites

- Node.js 18+
- TypeScript 5+
- PostgreSQL 14+ (for the Postgres adapter)

### Installation

Install the core event store abstractions:

```bash
npm install @dcb-es/event-store
```

For the Postgres-backed implementation:

```bash
npm install @dcb-es/event-store-postgres
```

`@dcb-es/event-store` provides the `EventStore` interface, tag-based querying, `buildDecisionModel`, and an in-memory implementation suitable for testing. `@dcb-es/event-store-postgres` provides a production-ready Postgres implementation and utilities such as `runHandler`, `subscribe`, and `waitUntilProcessed`.

---

## Contributing / working with the codebase

### Prerequisites

- Node.js 18+
- Yarn (classic, v1)
- Docker (required for Postgres tests via testcontainers)

### Clone and install

```bash
git clone git@github.com:sennentech/dcb-event-store.git
cd dcb-event-store
yarn install
```

This is a Lerna monorepo with Yarn workspaces. `yarn install` at the root resolves all inter-package dependencies.

### Build

```bash
npm run build
```

Runs `tsc` in each package via Lerna. You must build before running tests in packages that depend on other packages (e.g. `event-store-postgres` depends on `event-store`).

### Test

```bash
npm test
```

Runs `vitest run` in every package and example via Lerna.

To run tests for a single package:

```bash
cd packages/event-store && npm test
cd packages/event-store-postgres && npm test
```

Docker must be running for any package that uses Postgres tests (`event-store-postgres`, both examples). The test suite uses testcontainers to start a Postgres 17 container automatically.

### Lint

```bash
npm run lint
npm run lint-fix
```

ESLint + Prettier across all packages.

### Shared test infrastructure

Packages that need Postgres share two pieces of test infrastructure in the root `test/` directory:

**`test/vitest.globalSetup.ts`** -- Starts a single Postgres 17 container (via `@testcontainers/postgresql`) before all tests in the package and tears it down after. The connection URI is exported as `process.env.__PG_CONNECTION_URI`.

**`test/testPgDbPool.ts`** -- Creates a fresh, isolated database per test suite. Each call to `getTestPgDatabasePool()` creates a new database on the shared container and returns a `pg.Pool` connected to it. This means test suites run in parallel without interfering with each other.

These are imported using a Vite resolve alias. Each package's `vitest.config.ts` maps `@test` to the root `test/` directory:

```ts
resolve: {
    alias: {
        "@test": path.resolve(__dirname, "../../test")
    }
}
```

So test files import with:

```ts
import { getTestPgDatabasePool } from "@test/testPgDbPool"
```

---

## Running the examples

Both examples implement the [DCB course-subscriptions example](https://dcb.events/examples/course-subscriptions/) -- a domain where students subscribe to courses with capacity limits and subscription caps.

Both require a running PostgreSQL instance. The simplest way:

```bash
docker run -d \
  --name dcb-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:17
```

Then create the database the examples expect:

```bash
docker exec -it dcb-postgres psql -U postgres -c "CREATE DATABASE dcb_test_1;"
```

Both examples connect to `localhost:5432` with user `postgres`, password `postgres`, database `dcb_test_1`.

### course-manager-cli

An interactive CLI for managing courses and student subscriptions. All reads go through the event store directly using `buildDecisionModel` -- there is no separate read model.

```bash
npm run build
cd examples/course-manager-cli
npx ts-node index.ts
```

The CLI presents a menu for registering courses, registering students, subscribing/unsubscribing students, and updating course capacity and titles.

### course-manager-cli-with-readmodel

Extends the basic example with a Postgres-backed read model projection. Commands still use `buildDecisionModel` for decision-making, but queries (find course, find student) read from a projected Postgres table. The projection runs as a background handler using `runHandler` and `subscribe`, processing events via `pg_notify` as they are appended.

```bash
npm run build
cd examples/course-manager-cli-with-readmodel
npx ts-node index.ts
```

On startup, this example:

1. Initialises the event store schema (`ensureInstalled`)
2. Installs the handler bookmarks table (`ensureHandlersInstalled`)
3. Installs the course-subscriptions read model tables
4. Starts the projection handler in the background
5. Launches the interactive CLI

---

## Debugging

### Running individual tests

Run a single test file with vitest:

```bash
cd packages/event-store-postgres
npx vitest run src/PostgresEventStore.tests.ts
```

For verbose output:

```bash
npx vitest run --reporter=verbose src/PostgresEventStore.tests.ts
```

Watch mode re-runs on file changes:

```bash
npx vitest src/PostgresEventStore.tests.ts
```

### Common issues

**Tests fail with "PG container not started"**
Docker is not running, or the testcontainers Postgres container failed to start. Make sure Docker Desktop (or the Docker daemon) is running before running tests.

**Module not found errors in dependent packages**
Run `npm run build` from the repo root before testing. `event-store-postgres` and the examples import the compiled output of `event-store` -- if `packages/event-store/dist/` does not exist, imports will fail.

**Tests hang or time out**
Testcontainers pulls the `postgres:17` image on first run, which can take a while on slow connections. Subsequent runs use the cached image. If tests hang indefinitely, check Docker container logs:

```bash
docker ps -a | grep testcontainers
docker logs <container-id>
```

**Port 5432 already in use (examples only)**
The examples connect to `localhost:5432`. If another Postgres instance is using that port, stop it or change the port in the example's `index.ts`. This does not affect the test suite -- tests use testcontainers on a random port.

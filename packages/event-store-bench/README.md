# @dcb-es/event-store-bench

Stress test and benchmark suite for all event store adapters.

## Architecture

```
src/
├── harness/           Reusable infrastructure
│   ├── types.ts       Result types (WorkerResult, ScenarioResult, AggregateMetrics)
│   ├── stats.ts       Latency percentiles and result aggregation
│   └── workers.ts     Four composable worker primitives (see below)
├── scenarios/         One file per scenario, self-contained
│   ├── registry.ts    Scenario list used by test files
│   └── types.ts       BenchScenario / ThresholdBenchScenario interfaces
├── reporters/         Console table and JSON output formatters
├── test-harness.ts    Shared Postgres lifecycle (pool, DB create/drop, factories)
├── pg.tests.ts        Runs quick presets on testcontainers Postgres
└── pg-local.tests.ts  Runs full presets on a local Postgres instance
```

### Worker primitives

Every scenario is built from one of four worker functions in `harness/workers.ts`:

| Worker | What it does |
|---|---|
| `conditionalRetryWorker` | Appends events with a DCB condition on a scoped query, retries on conflict |
| `scopedBatchWriter` | Appends N events per operation to an isolated scope, optionally with conditions |
| `entityOracleWorker` | Picks a random entity, checks if it exists, creates it with a condition if not |
| `emptyResult` + inline | For scenarios with custom logic (import, read, etc.) |

### Scenario structure

Each scenario exports a `BenchScenario` (or `ThresholdBenchScenario`) object with:

- **`id`** — unique identifier, used as the test name
- **`name`** — human-readable title
- **`description`** — one sentence explaining what the test measures
- **`presets.quick`** — fast config for CI / testcontainers (seconds, small data)
- **`presets.full`** — production-scale config for local Postgres benchmarking
- **`correctnessChecks`** — which verification checks are asserted in tests
- **`run(factory, config)`** — executes the scenario, returns `StressTestResult`

## Scenarios

| Scenario | What it tests |
|---|---|
| **throughput-scaling** | Scales from 1 to N isolated writers — does throughput grow linearly with concurrency? |
| **bulk-import** | Single massive `append(commands[])` with per-entity DCB conditions — can it import thousands of entities in one call without duplicates? |
| **contention** | All workers write to the same scope simultaneously — do conditions and retries handle conflicts correctly under heavy contention? |
| **mixed-workload** | A bulk import runs while small conditional writers operate concurrently — does the import degrade latency for other writers? |
| **raw-throughput** | Unconditional bulk append with no conditions at all — what is the absolute peak write speed? |
| **parallel-import** | Multiple batches import different entity ranges in parallel — do concurrent batch appends with conditions stay correct? |
| **consistency-oracle** | Workers race to create random entities (check-then-write pattern) — does DCB prevent any duplicate entity creation? |
| **overlap-consistency** | Same as consistency-oracle but events carry more tags than the condition query — do conditions still hold when event scope is wider than condition scope? |
| **esb-compat** | Replicates the Event Store Benchmark pattern — separate write and read scaling tiers to measure both paths independently |
| **meter-upsert** | Full read-decide-write cycle: reads all events, decides which meters to create, batch upserts with conditions — does the decision model work end-to-end? |
| **batch-sweep (unconditional)** | Sweeps batch sizes (1, 10, 50, 100, 500) with unconditional appends — which batch size gives the best throughput? |
| **batch-sweep (conditional)** | Same sweep but with conditional appends — how much overhead do conditions add at different batch sizes? |
| **degradation** | Runs multiple phases on the same store as the table grows — how much does throughput degrade as event count increases? |
| **batch-commands** | Each `append()` submits N `AppendCommand` objects (1 event + 1 condition each) — tests the real-world bulk import pattern with per-entity uniqueness |
| **copy-threshold** *(pg-local only)* | Holds batch size constant, varies the Postgres COPY threshold — finds the crossover point where `COPY FROM` beats the stored procedure |
| **copy-crossover** *(pg-local only)* | For each batch size, runs both function path and COPY path back-to-back — direct side-by-side comparison |

## Running

```bash
# Unit tests (MemoryEventStore, fast)
npm test

# Testcontainers Postgres (requires Docker)
npm run test:pg

# Local Postgres (requires PG_CONNECTION_STRING or localhost:5432)
npm run test:pg-local
```

Set `PG_CONNECTION_STRING` to point at a local Postgres instance for `test:pg-local`. The test harness creates and drops an isolated database per test.

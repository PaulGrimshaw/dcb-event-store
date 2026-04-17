import { BenchScenario, ThresholdBenchScenario } from "./types"
import { throughputScaling } from "./throughput-scaling"
import { bulkImport } from "./bulk-import"
import { contention } from "./contention"
import { mixedWorkload } from "./mixed-workload"
import { rawThroughput } from "./raw-throughput"
import { parallelImport } from "./parallel-import"
import { consistencyOracle } from "./consistency-oracle"
import { overlapConsistency } from "./overlap-consistency"
import { esbCompat } from "./esb-compat"
import { meterUpsert } from "./meter-upsert"
import { batchSweepUnconditional, batchSweepConditional } from "./batch-sweep"
import { degradation } from "./degradation"
import { batchCommands } from "./batch-commands"
import { copyThreshold } from "./copy-threshold"
import { copyCrossover } from "./copy-crossover"

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const standardScenarios: BenchScenario<any>[] = [
    throughputScaling,
    bulkImport,
    contention,
    mixedWorkload,
    rawThroughput,
    parallelImport,
    consistencyOracle,
    overlapConsistency,
    esbCompat,
    meterUpsert,
    batchSweepUnconditional,
    batchSweepConditional,
    degradation,
    batchCommands,
]

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const thresholdScenarios: ThresholdBenchScenario<any>[] = [
    copyThreshold,
    copyCrossover,
]

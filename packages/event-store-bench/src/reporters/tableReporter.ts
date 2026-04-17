import { ScenarioResult, LatencyStats } from "../harness/types"

export function formatScenarioResult(result: ScenarioResult): string {
    const lines: string[] = []

    lines.push("")
    lines.push("=".repeat(72))
    lines.push(`  ${result.scenario}`)
    lines.push(`  ${result.description}`)
    lines.push("=".repeat(72))
    lines.push("")

    const a = result.aggregate
    lines.push("  Aggregate Metrics")
    lines.push("  " + "-".repeat(50))
    lines.push(formatRow("Duration", `${(result.durationMs / 1000).toFixed(1)}s`))
    lines.push(formatRow("Total operations", String(a.totalOperations)))
    lines.push(formatRow("Ops/sec", String(a.opsPerSec)))
    lines.push(formatRow("Events/sec", String(a.eventsPerSec)))
    lines.push(formatRow("Errors", String(a.totalErrors)))
    lines.push(formatRow("Conflicts", String(a.totalConflicts)))
    lines.push("")

    lines.push("  Latency (ms)")
    lines.push("  " + "-".repeat(50))
    lines.push(formatLatencyTable(a.latency))
    lines.push("")

    if (result.workers.length > 1) {
        lines.push("  Per-Worker Breakdown")
        lines.push("  " + "-".repeat(50))
        lines.push(formatWorkerHeader())
        for (const w of result.workers) {
            lines.push(formatWorkerRow(w))
        }
        lines.push("")
    }

    return lines.join("\n")
}

export function formatMultipleResults(results: ScenarioResult[]): string {
    return results.map(formatScenarioResult).join("\n")
}

function formatRow(label: string, value: string): string {
    return `  ${label.padEnd(24)} ${value}`
}

function formatLatencyTable(stats: LatencyStats): string {
    const lines: string[] = []
    lines.push(formatRow("min", `${stats.min}ms`))
    lines.push(formatRow("p50", `${stats.p50}ms`))
    lines.push(formatRow("p95", `${stats.p95}ms`))
    lines.push(formatRow("p99", `${stats.p99}ms`))
    lines.push(formatRow("p99.9", `${stats.p999}ms`))
    lines.push(formatRow("max", `${stats.max}ms`))
    lines.push(formatRow("mean", `${stats.mean}ms`))
    return lines.join("\n")
}

function formatWorkerHeader(): string {
    return `  ${"ID".padEnd(6)}${"Type".padEnd(10)}${"Ops".padEnd(10)}${"Err".padEnd(8)}${"Conf".padEnd(8)}${"p50".padEnd(8)}${"p99".padEnd(8)}`
}

function formatWorkerRow(w: { workerId: number; type: string; operations: number; errors: number; conflicts: number; latencies: number[] }): string {
    const sorted = [...w.latencies].sort((a, b) => a - b)
    const p50 = sorted.length > 0 ? sorted[Math.ceil(0.5 * sorted.length) - 1] : 0
    const p99 = sorted.length > 0 ? sorted[Math.ceil(0.99 * sorted.length) - 1] : 0
    return `  ${String(w.workerId).padEnd(6)}${w.type.padEnd(10)}${String(w.operations).padEnd(10)}${String(w.errors).padEnd(8)}${String(w.conflicts).padEnd(8)}${String(p50).padEnd(8)}${String(p99).padEnd(8)}`
}

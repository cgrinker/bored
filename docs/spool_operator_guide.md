# Spool Worktable Operator Guide

This guide outlines how materialize spools behave inside the `bored` executor, how to monitor them, and what operators should verify during crash recovery drills.

_Last updated: 2025-11-04_

## When the Planner Inserts a Spool
- The planner emits `PhysicalOperatorType::Materialize` whenever a pipeline has to buffer its child rows (SELECT projections that need rewind, UPDATE/DELETE statements that revisit qualifying rows, or uniqueness enforcement that spills snapshot data).
- Shell pipelines translate those materialize nodes into `SpoolExecutor` instances automatically. The executor chain reported by the shell (for example, `executor.pipeline.UPDATE=Spool -> Projection -> Filter -> SeqScan`) confirms when a spool is active.
- Spools inherit the transaction snapshot from the surrounding `ExecutorContext`. A snapshot change (new statement or new transaction) forces the spool to rematerialise its child.

## Worktable Registry and Reuse
- `SpoolExecutor::Config` accepts an optional `WorkTableRegistry` pointer plus a `worktable_id`. When both are provided, the spool publishes its materialised rows into the registry and attempts to reuse them for future executions that share the same snapshot.
- Registry lookups require an exact snapshot match (`txn::snapshots_equal`). If the snapshot changes, the cache is bypassed and the spool replays its child before refreshing the registry entry.
- Operators embedding custom pipelines should call `WorkTableRegistry::clear` during transaction teardown to drop stale entries.
- Crash drills in `tests/wal_replay_tests.cpp` validate that WAL replay restores cached worktables for SELECT, UPDATE, and DELETE pipelines and that recovered runtimes can reuse them without re-reading the child.

## Telemetry Surfaces
- `ExecutorTelemetry` tracks a dedicated `spool_latency` histogram. Each call to `SpoolExecutor::next` records latency and row counts alongside existing scan/filter metrics.
- Shell integrations tag spool samplers using telemetry identifiers:
  - `shell.select.materialize`
  - `shell.update.materialize`
  - `shell.delete.materialize`
- Register samplers with `StorageTelemetryRegistry` via `ExecutorTelemetrySampler` to surface spool latency and invocation counts in storage diagnostics JSON.
- Pair spool telemetry with WAL retention metrics when investigating snapshot reuse, because excessive rematerialisation often correlates with snapshot churn or long-lived readers.

## Diagnostics Checklist
- Inspect shell plan detail lines after `SELECT`, `UPDATE`, or `DELETE` statements; ensure the reported executor pipeline lists `Spool` whenever replay is expected.
- Capture storage diagnostics before and after workload changes to monitor `spool_latency.invocations` and `spool_latency.total_duration_ns` trends.
- Run the spool crash drills (`ctest -R "Wal crash drill rehydrates spool"`) after changes to recovery code to confirm worktables survive restart scenarios.
- When custom registries are supplied, make sure each logical worktable uses a stable `worktable_id` so cached rows can be located across executor instances.

## Recursive Spool Playbook
- `WorkTableRegistry::RecursiveCursor` exposes seed rows and delta staging for recursive CTE consumers. After a spool materialises its child, call `SpoolExecutor::recursive_cursor()` to obtain the cursor, iterate seeds via `next_seed`, and append delta tuples between rounds with `append_delta`/`mark_delta_processed`.
- The storage benchmark harness now exercises this flow. Run `./build/bored_benchmarks --samples=10 --json` to capture the `spool_worktable_recovery` timings; the run replays 64 seed rows, appends one delta per round, and confirms the cursor clears processed deltas before the next iteration.
- Use the recorded metrics in `benchmarks/baseline_results.json` to flag regressions. The baseline currently tracks mean and p95 timings for the recursive spool drill alongside FSM refresh and WAL retention/replay workloads.
- Training checklist for operators exploring recursive workloads:
  1. Inspect shell plans for `RecursiveMaterialize`/`Spool` pairs and confirm worktable IDs remain stable across statements.
  2. Verify registry snapshots using the shell telemetry surface (`storage diagnostics spoolLatency`), checking that delta invocations align with expected recursion depth.
  3. Re-run the benchmark command above after tuning spool heuristics or WAL retention knobs, comparing results to the baseline JSON and capturing deviations in release notes.

Keeping the playbook handy ensures teams validate recursive spool behaviour (seed reuse, delta drains, and latency budgets) before enabling recursive CTE plans in production pipelines.

Spool instrumentation now makes it straightforward to confirm when materialised worktables are reused, how much time is spent buffering, and whether crash recovery continues to honour cached intermediate results.

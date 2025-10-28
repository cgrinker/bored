# Query Executor Milestones

## Objectives
- Execute physical plans produced by the planner using an iterator-first architecture that can evolve toward pipelines or vectorized batches.
- Coordinate transactional visibility, latching, and WAL emission for read/write operators without regressing storage invariants.
- Provide sufficient instrumentation (telemetry, diagnostics, explain integration) so operators can trace execution behaviour and performance.

## Milestone 0: Execution Framework Scaffolding (0.5 sprint)
- [x] Define `ExecutorContext` carrying transaction snapshot, catalog/cardinality helpers, and operator scratch allocators.
- [x] Specify the executor interface (`open()`, `next()`, `close()`) and base `ExecutorNode` abstraction.
- [x] Implement runtime tuple/materialization buffer utilities shared across operators.
- [x] Add Catch2 scaffolding tests that exercise a stub executor tree and validate lifecycle semantics.
- [x] Document extension points and conventions (state ownership, spill behaviour) in this milestone doc.

### Milestone 0 Notes
- `ExecutorContext` centralises access to the active transaction snapshot, catalog hooks, and a default polymorphic allocator. Operators can request a custom scratch resource via the constructor to feed pipeline-local arenas.
- `ExecutorNode` establishes the lifecycle contract (open/next/close) and provides helpers for child traversal. Derived executors own their children and must forward lifecycle calls in order, ensuring downstream cleanup even when a node reports end-of-stream or throws.
- `TupleBuffer` offers aligned storage for tuple materialisation. Callers reset the buffer between batches; out-of-line payloads (e.g., overflow tuples) remain the caller's responsibility.
- Tests in `tests/executor_scaffolding_tests.cpp` validate context propagation, child call ordering, and tuple buffer resizing so later operators inherit stable semantics.

## Milestone 1: Read Operator Foundations (1 sprint)
- [x] Implement Sequential Scan executor that reads via `PageManager`/`StorageReader`, respecting MVCC visibility.
- [x] Introduce Projection and Filter executors that compose atop child iterators and reuse expression evaluation helpers.
- [x] Add telemetry counters for rows scanned/filtered/projected and integrate with `StorageTelemetryRegistry`.
- [x] Expand planner lowering to annotate physical plans with executor operator IDs and provide construction hooks.
- [x] Create integration tests using frozen table fixtures to validate result sets and diagnostics output.

### Milestone 1 Notes
- Added a `StorageReader` abstraction with frozen-table cursors for unit validation while the real page-backed reader is under construction.
- Sequential scan now enforces MVCC visibility using snapshot metadata from `ExecutorContext`, feeding projection/filter operators built on the iterator contract.
- Per-operator telemetry (scan/filter/projection) flows into `StorageTelemetryRegistry` and surfaces through storage diagnostics JSON.
- Planner physical nodes allocate stable executor operator identifiers so execution plans can materialize concrete operator trees.
- Catch2 suites (`tests/executor_milestone1_tests.cpp`) exercise scan/filter/projection pipelines and confirm telemetry counters against expected row sets.

## Milestone 2: Join & Aggregation Operators (1–1.5 sprints)
- [x] Implement Nested Loop Join executor supporting parameterized probe inputs and basic predicate evaluation.
- [x] Provide Hash Join skeleton with in-memory hash table, build/probe phases, and spill TODO markers.
- [x] Add Aggregation executor (hash/group aggregate) with accumulator guards for overflow and null semantics.
- [x] Extend planner cost model/explain output to emit executor-specific metadata (batch sizes, join strategy hints).
- [x] Cover new operators with integration tests that compare against known-good query outputs and verify telemetry (join rows, aggregation groups).

### Milestone 2 Notes
- Nested loop join reuses the iterator contract with optional probe rebinding callbacks so parameterized scans (e.g., index lookups) can react to the outer tuple prior to each probe.
- Join output defaults to concatenating child columns, with projection hooks for downstream shape control; telemetry tracks comparisons, matches, and emitted rows.
- Hash join materialises build-side tuple buffers and exposes projection hooks alongside telemetry for build/probe/match counts; spill integration remains TODO.
- Aggregation executor groups by caller-provided keys, maintains per-group aggregate state via callback-defined accumulators, and reports input/group counts through executor telemetry.
- Explain output now surfaces per-operator batch heuristics (sourced from the cost model) and join strategy hints consumed by diagnostics tooling.
- Added an integration pipeline that joins customer/order fixtures, rolls up counts, and asserts executor telemetry snapshots for both operators.
- Remaining follow-ups: (1) calibrate cost-model batch heuristics against benchmark workloads, (2) exercise join+aggregate pipelines with filter/projection chains, and (3) prototype aggregation spill/reclaim policies once temporary storage is available.

## Milestone 3: DML & WAL Coordination (1 sprint)
- [x] Implement Insert executor that consumes child rows, allocates heap tuples, and emits WAL via `PageManager` hooks.
- [x] Implement Update executor handling before-image capture, overflow modifications, and commit visibility updates.
- [x] Implement Delete executor that coordinates lock/undo logging and free space reclamation.
- [x] Wire executors into transaction manager callbacks to ensure commit/abort flows flush staged writes.
- [x] Add regression tests covering crash-recovery of DML operations and verifying WAL segments via existing readers.

### Milestone 3 Notes
- Insert executor drains child pipelines into a pluggable storage target that currently wraps `PageManager`, stamps tuple headers with the running transaction id, and flushes WAL as part of close semantics.
- Executor telemetry now tracks insert attempts/successes plus payload and WAL byte counters so diagnostics can surface DML impact.
- New unit coverage exercises end-to-end insert flows against a single-page heap, verifying tuple materialisation, WAL emission, and telemetry snapshots.
- Latest validation: `cmake --build build-macos --target bored_tests` followed by `./build-macos/bored_tests "InsertExecutor writes tuples via PageManager"`.
- Update executor captures before-images via `PageManager` pin hooks, routes WAL undo payloads through `WalUndoWalker`, and extends telemetry with update attempt/success counters.
- Update executor now drains row id/slot index/new payload tuples, pins `PageManager` slots for before-image accounting, tracks update telemetry (attempt/success, payload bytes, WAL bytes), and flushes WAL on close.
- Latest validation: `./build-macos/bored_tests "UpdateExecutor applies updates via PageManager"`.
- Delete executor now consumes row id/slot index tuples, issues `PageManager` deletes that capture before-images for undo, records reclaimed bytes/WAL metrics, and flushes WAL on close.
- Latest validation: `./build-macos/bored_tests "DeleteExecutor removes tuples via PageManager"`.
- Added `tests/executor_dml_recovery_tests.cpp`, which drives insert/update/delete executors against `PageManager`, flushes WAL through the commit pipeline, and replays crash recovery via `WalReader`, `WalRecoveryDriver`, and `WalReplayer` to confirm page state and record sequencing.
- Next steps: wire insert/update/delete executors into transaction manager commit/abort callbacks so staged WAL flushes at the right horizon, and extend tests to multi-page crash/recovery scenarios once undo walk plumbing is ready.

## Milestone 4: Execution Services & Diagnostics (0.5 sprint)
- [x] Introduce executor telemetry samplers (rows produced, latency per operator) and register with diagnostics JSON.
- [x] Provide tracing hooks so `explain_plan` can attach executor runtime stats post-execution.
- [x] Build micro-benchmark harness that exercises representative executor pipelines with configurable workloads.
- [x] Document troubleshooting steps in an operator addendum (extension of `planner_operator_guide.md`).
- [ ] Review retention/checkpoint integration to ensure executor-managed temp resources are cleaned during recovery.

### Milestone 4 Notes
- Executor telemetry now publishes per-operator row counters and latency samples via `ExecutorTelemetrySampler`. Plans can register samplers with `StorageTelemetryRegistry`, and diagnostics JSON surfaces per-operator latency totals alongside existing row counters.
- `planner::ExplainOptions::runtime_stats` accepts per-operator execution traces so rendered plans can embed runtime loops/rows/latency when available.
- `bored_executor_benchmarks` drives scan, filter, projection, join, and aggregation pipelines with configurable table sizes, selectivity, and join match rates, reporting throughput in text or JSON.
- The operator guide now includes an executor troubleshooting addendum covering telemetry capture, runtime explain traces, micro-benchmark workflows, and WAL retention checks.

## Milestone 4.5: Retention & Recovery Cleanup (0.5 sprint)
- [x] Wire production `CheckpointScheduler` instances with a retention hook that invokes `WalWriter::apply_retention` and hands executor temp cleanup to a shared manager.
- [x] Implement an executor temp resource manager (spill directories, scratch segments) that registers with the retention hook and exposes crash-safe purge routines.
- [x] Extend crash recovery (`WalRecoveryDriver`/`WalReplayer`) to invoke the temp resource purge after redo/undo completes so orphaned artifacts are removed before restart.
- [x] Add integration tests that stage executor spill files, trigger checkpoints, and simulate crash recovery to verify deletion on resume.
- [x] Surface retention/cleanup telemetry (counts, duration, failure tally) through `StorageTelemetryRegistry` so diagnostics JSON reflects temp resource hygiene.

### Milestone 4.5 Notes
- The retention hook currently exists only in tests; production wiring will ensure checkpoints consistently drive archival pruning and temp cleanup flows.
- Temp resource purging should tolerate missing files/directories and log non-fatal errors so recovery continues while still surfacing diagnostics.
- `ExecutorTempResourceManager` creates per-executor spill directories, registers scratch segments with the shared temp registry, and exposes a purge helper so checkpoints and crash recovery invoke the same cleanup path.
- `WalReplayer::apply_undo` now finalises recovery by purging registered temp artifacts once redo/undo completes, ensuring restart surfaces start clean even after crash spills.
- `tests/executor_spill_recovery_integration_tests.cpp` runs spill staging through checkpoint retention and crash recovery replay to confirm temp artifacts disappear both during steady-state checkpoints and restart.
- `WalWriter` registers temp cleanup telemetry samplers with `StorageTelemetryRegistry`, and storage diagnostics JSON now emits aggregate and per-writer temp cleanup counters, durations, and failure tallies.
- Integration coverage should include both clean shutdown and crash scenarios to confirm checkpoints scrub artifacts promptly and recovery catches any stragglers.

## Milestone 5: Pipeline & Vectorization Exploration (stretch)
- [ ] Prototype batch-oriented executor API that can wrap iterator operators without breaking compatibility.
- [ ] Evaluate adaptive scheduling (pipeline breakers, concurrency) and produce design recommendations.
- [ ] Capture performance findings and next-step proposals in a follow-up design memo.

## Dependencies & Tracking
- Pre-reqs: Transaction Manager Milestone 1 (snapshot plumbing), Planner Milestone 4 (telemetry/explain), storage WAL invariants.
- Update `docs/relational_layer_design.md` as milestones complete to reflect executor progress.

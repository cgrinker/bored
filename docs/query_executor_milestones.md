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
- [ ] Implement Update executor handling before-image capture, overflow modifications, and commit visibility updates.
- [ ] Implement Delete executor that coordinates lock/undo logging and free space reclamation.
- [ ] Wire executors into transaction manager callbacks to ensure commit/abort flows flush staged writes.
- [ ] Add regression tests covering crash-recovery of DML operations and verifying WAL segments via existing readers.

### Milestone 3 Notes
- Insert executor drains child pipelines into a pluggable storage target that currently wraps `PageManager`, stamps tuple headers with the running transaction id, and flushes WAL as part of close semantics.
- Executor telemetry now tracks insert attempts/successes plus payload and WAL byte counters so diagnostics can surface DML impact.
- New unit coverage exercises end-to-end insert flows against a single-page heap, verifying tuple materialisation, WAL emission, and telemetry snapshots.
- Latest validation: `cmake --build build-macos --target bored_tests` followed by `./build-macos/bored_tests "InsertExecutor writes tuples via PageManager"`.
- Next steps: design update/delete executors with undo logging hooks, ensure mutation targets coordinate with transaction abort callbacks, and extend tests to crash/recovery scenarios once undo walk plumbing is ready.

## Milestone 4: Execution Services & Diagnostics (0.5 sprint)
- [ ] Introduce executor telemetry samplers (rows produced, latency per operator) and register with diagnostics JSON.
- [ ] Provide tracing hooks so `explain_plan` can attach executor runtime stats post-execution.
- [ ] Build micro-benchmark harness that exercises representative executor pipelines with configurable workloads.
- [ ] Document troubleshooting steps in an operator addendum (extension of `planner_operator_guide.md`).
- [ ] Review retention/checkpoint integration to ensure executor-managed temp resources are cleaned during recovery.

## Milestone 5: Pipeline & Vectorization Exploration (stretch)
- [ ] Prototype batch-oriented executor API that can wrap iterator operators without breaking compatibility.
- [ ] Evaluate adaptive scheduling (pipeline breakers, concurrency) and produce design recommendations.
- [ ] Capture performance findings and next-step proposals in a follow-up design memo.

## Dependencies & Tracking
- Pre-reqs: Transaction Manager Milestone 1 (snapshot plumbing), Planner Milestone 4 (telemetry/explain), storage WAL invariants.
- Update `docs/relational_layer_design.md` as milestones complete to reflect executor progress.

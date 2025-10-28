# Query Executor Milestones

## Objectives
- Execute physical plans produced by the planner using an iterator-first architecture that can evolve toward pipelines or vectorized batches.
- Coordinate transactional visibility, latching, and WAL emission for read/write operators without regressing storage invariants.
- Provide sufficient instrumentation (telemetry, diagnostics, explain integration) so operators can trace execution behaviour and performance.

## Milestone 0: Execution Framework Scaffolding (0.5 sprint)
- [ ] Define `ExecutorContext` carrying transaction snapshot, catalog/cardinality helpers, and operator scratch allocators.
- [ ] Specify the executor interface (`open()`, `next()`, `close()`) and base `ExecutorNode` abstraction.
- [ ] Implement runtime tuple/materialization buffer utilities shared across operators.
- [ ] Add Catch2 scaffolding tests that exercise a stub executor tree and validate lifecycle semantics.
- [ ] Document extension points and conventions (state ownership, spill behaviour) in this milestone doc.

## Milestone 1: Read Operator Foundations (1 sprint)
- [ ] Implement Sequential Scan executor that reads via `PageManager`/`StorageReader`, respecting MVCC visibility.
- [ ] Introduce Projection and Filter executors that compose atop child iterators and reuse expression evaluation helpers.
- [ ] Add telemetry counters for rows scanned/filtered/projected and integrate with `StorageTelemetryRegistry`.
- [ ] Expand planner lowering to annotate physical plans with executor operator IDs and provide construction hooks.
- [ ] Create integration tests using frozen table fixtures to validate result sets and diagnostics output.

## Milestone 2: Join & Aggregation Operators (1–1.5 sprints)
- [ ] Implement Nested Loop Join executor supporting parameterized probe inputs and basic predicate evaluation.
- [ ] Provide Hash Join skeleton with in-memory hash table, build/probe phases, and spill TODO markers.
- [ ] Add Aggregation executor (hash/group aggregate) with accumulator guards for overflow and null semantics.
- [ ] Extend planner cost model/explain output to emit executor-specific metadata (batch sizes, join strategy hints).
- [ ] Cover new operators with integration tests that compare against known-good query outputs and verify telemetry (join rows, aggregation groups).

## Milestone 3: DML & WAL Coordination (1 sprint)
- [ ] Implement Insert executor that consumes child rows, allocates heap tuples, and emits WAL via `PageManager` hooks.
- [ ] Implement Update executor handling before-image capture, overflow modifications, and commit visibility updates.
- [ ] Implement Delete executor that coordinates lock/undo logging and free space reclamation.
- [ ] Wire executors into transaction manager callbacks to ensure commit/abort flows flush staged writes.
- [ ] Add regression tests covering crash-recovery of DML operations and verifying WAL segments via existing readers.

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

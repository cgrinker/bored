# Relational Feature Progress Report

This ticket-level report summarizes the relational engine roadmap, capturing what has shipped, what remains, and which code paths we expect to touch next so the team can quickly regain context when switching threads.

_Last updated: 2025-11-04_

Latest validation: `build/bored_tests` (438/438) on 2025-11-04 after aligning overflow WAL ownership so crash drills collapse to single undo spans and landing recursive cursor coverage for spool crash/restart workflows; `build/bored_benchmarks --samples=10 --json` captured updated baselines (including `spool_worktable_recovery`) the same day.

## Current Capabilities

- **Storage Durability**: WAL writer/reader/replayer pipeline complete with retention, checkpoint scheduling, crash recovery, and page manager integration; overflow WAL descriptors now inherit the owning table page/transaction so recovery emits a single undo span per owner while crash drills validate overflow chain undo paths with cached payloads stripped of stub headers.
- **Catalog & DDL**: Persistent catalog with fully wired DDL handlers (create/alter/drop) for schemas, tables, and indexes, including restart-safe catalog bootstrap.
- **Sequence Allocation (foundation)**: Transactional sequence allocator stages `next_value` updates via catalog mutator hooks, dispatcher now wires allocators into DDL handlers, and Catch2 regression coverage remains; planner/executor wiring is still pending.
- **Transaction Lifecycle (partial)**: Transaction manager handles ID allocation, snapshots, commit metadata emission, and integrates with WAL commit pipeline; bored_shell now supports BEGIN/COMMIT/ROLLBACK so INSERT/UPDATE/DELETE/SELECT flows can share session-scoped transactions with executor snapshots; catalog accessor caches refresh on snapshot changes and planner/executor pipelines share the same transaction snapshot; snapshot-aware retention guard now propagates oldest reader LSNs while cross-session isolation and deadlock handling remain on the roadmap.
- **Parser, Binder, and Normalizer**: PEGTL-based SQL parser covering core DDL/DML verbs (including `WITH RECURSIVE` anchor and recursive members); binder resolves identifiers, enforces CTE column/type compatibility, and scopes recursive references; lowering currently inlines non-recursive CTE definitions into the logical tree, and normalization stages generate logical plans for select/join queries.
- **Planner & Executor (core path)**: Logical-to-physical planning for scans, projections, filters, joins, insert/update/delete; executor framework supports sequential scans, nested loop and hash joins, basic aggregations, and WAL-aware DML operators.
- **Constraint Enforcement**: Planner lowers unique/foreign key operators and executor now performs index-backed uniqueness checks plus referential integrity probes with MVCC-aware visibility.
- **Index Infrastructure**: B+Tree page formats, insertion/deletion/update routines, retention hooks, and executor-side probes are implemented; background pruning/retention and telemetry wired up.
- **Observability & Tooling**: Storage diagnostics, WAL/retention telemetry, checkpoint metrics, and shell integration for disk-backed catalogs and WAL configuration.

## Feature Checklist

| Capability | Status | Notes |
|------------|--------|-------|
| Secondary indexes | **Available (foundation)** | B+Tree manager, index retention, recovery, and executor probes implemented; integration tests cover index operations. |
| Key/foreign constraints | **Available (shell integration)** | Catalog metadata, planner & executor enforcement, and bored_shell INSERT/UPDATE pipelines enforcing PRIMARY KEY/UNIQUE/FOREIGN KEY checks. |
| Auto-incrementing primary keys | **In progress** | Sequence allocator now stages transactional `next_value` updates with tests; planner/executor integration remains. |
| Join execution | **Available** | Logical lowering, planner, and executor support nested-loop and hash joins with tests covering join predicates and pipelines. |
| Common table expressions (CTEs) | **In progress** | Parser/AST and binder now understand `WITH RECURSIVE`, enforcing anchor/recursive column and type compatibility; lowering continues to inline non-recursive producers with regression coverage; planner memo deduplicates reusable producers and injects materialize/spool alternatives. Recursive spools now surface worktable ids and cursors through planner → shell, leaving logical lowering/planner wiring and end-to-end tests for recursive execution as the remaining work. |

## Gaps to Close

1. **Constraint Enforcement**: Finish catalog DDL plumbing for constraint creation/drop and surface richer shell diagnostics for violations now that enforcement is active.
2. **Sequence/Identity Support**: Wire planner/executor and DDL verbs to consume the transactional sequence allocator so auto-increment columns surface to users.
3. **CTE Parsing & Execution**: Planner memo now deduplicates non-recursive CTE producers; spool executor is available but needs worktable integration with snapshot-aware iterators and shell wiring.
4. **Transaction Visibility**: Finish Transaction & Concurrency Milestone 1 to provide consistent snapshots across planner/executor and integrate with retention manager for snapshot retirement.
5. **Optimizer Enhancements**: Broaden rule set, add cost-based join order selection, and integrate catalog statistics for selectivity estimates.
6. **Index DDL Integration**: Surface index creation options (unique, covering, partial) and ensure planner/executor leverage indexes for predicates and joins.

## Roadmap to Full Relational Coverage

1. **Finalize Concurrency Milestone 1 (Completed)**
   - [x] Shell DML and SELECT pipelines now share live TransactionManager contexts with catalog cache refreshes and planner/executor visibility checks.
   - [x] Added regression coverage verifying catalog accessor tuples are re-evaluated when snapshots advance without extra scans.
   - [x] Wired snapshot-aware retention guard through the commit pipeline so WAL retention honors active reader horizons.
   - [x] Landed multi-page crash drill verifying committed pages replay while in-flight pages roll back with fragment-aware undo.
   - [x] Extend undo walker crash drills to cover overflow chains with fragment-aware assertions and sanitized cached overflow payloads to remove stub headers.
   - [x] Add regression coverage for session rollback edge conditions, including catalog and overflow interactions.

2. **Constraint & Sequence Foundations (Completed)**
   - [x] Extend catalog metadata for constraints and sequences; persist via WAL and recovery hooks.
   - [x] Expose catalog accessor support for sequence descriptors across schema and relation scopes.
   - [x] Implement sequence allocator with transactional semantics for auto-increment columns (validated by `catalog_sequence_allocator_tests`).
   - [x] Update DDL verbs (`CREATE TABLE`, `ALTER TABLE`, `CREATE SEQUENCE`) to request transactional sequence allocators; planner/executor consumption tracked under Constraint Enforcement Pipeline.

3. **Constraint Enforcement Pipeline (Completed)**
   - [x] Catalog accessor now exposes constraint descriptors for planner/executor consumption.
   - [x] Planner: Recognize unique/primary keys and foreign keys; generate enforcement operators.
   - [x] Executor: Implement uniqueness checks (indexes + deferred validation) and referential integrity probes with transactional awareness.
   - [x] Shell: Apply constraint enforcement to INSERT/UPDATE pipelines using planner metadata and simulated index probes.

4. **CTE Enablement (In progress)**
   - Parser/AST: ✅ `WITH`/`WITH RECURSIVE` grammar and AST nodes emit anchor plus recursive members.
   - Planner: ✅ Memo deduplicates non-recursive CTE producers, tracks reusable groups, and injects materialize/spool alternatives so the cost model can compare inline versus reuse plans.
   - Binder: ✅ Binding layer registers CTE definitions, scopes, and column aliases, and now validates recursive members for column count/type compatibility while exposing recursive references during binding.
   - Executor: ✅ Spool executor in place and bored_shell SELECT/UPDATE/DELETE pipelines now wrap planner materialize nodes with spool-backed iterators; shell diagnostics surface executor pipeline chains, spool telemetry tests account for terminal reads, the worktable registry exposes snapshot-aware reuse, and crash/restart drills in `tests/wal_replay_tests.cpp` now verify worktables rehydrate across recovery.
   - Remaining tasks:
    - [ ] Teach logical lowering/planner to emit recursive spool-capable plans so recursive WITH clauses schedule cursors and delta propagation alongside memo reuse. _(Lowering now produces recursive spool-ready logical trees; planner memo/executor wiring remains.)_
     - [ ] Extend integration coverage for recursive spool consumers (multi-reader registry reuse, delta draining across statements) once planner wiring lands.
   - Source files to update next: src/planner/memo.cpp, src/planner/planner.cpp, src/planner/rules/, src/executor/spool_executor.cpp, src/executor/executor_node.cpp, tests/planner_integration_tests.cpp, tests/planner_rule_tests.cpp, tests/executor_integration_tests.cpp, tests/shell_backend_tests.cpp, docs/spool_operator_guide.md
   - Next work item: Wire planner memo alternatives to recognise recursive spool requirements emitted by lowering, then add integration tests exercising recursive spool cursors end-to-end.

5. **Advanced Indexing & Optimization (Planned)**
   - Support unique indexes tied to constraint metadata; expose covering/partial index options.
   - Enhance optimizer to choose index scans based on statistics and predicates; add cost model refinements.
   - Expand join optimization (multi-join reordering, bushy plans) once statistics available.
   - Source files to update: src/storage/index_btree_manager.cpp, src/storage/index_retention.cpp, src/planner/cost_model.cpp, src/planner/statistics_catalog.cpp, src/planner/rules/, src/planner/rule.cpp, tests/index_btree_manager_tests.cpp, tests/planner_cost_model_tests.cpp, tests/planner_rule_tests.cpp

6. **Comprehensive Transactions & Isolation Levels (Planned)**
   - Implement lock manager integration for key-range locking where needed for uniqueness.
   - Add multi-version concurrency control (MVCC) visibility rules across executor operators.
   - Provide configurable isolation levels and conflict resolution.
   - Source files to update: src/storage/lock_manager.cpp, src/storage/lock_introspection.cpp, src/executor/mvcc_visibility.cpp, src/txn/transaction_manager.cpp, src/txn/wal_commit_pipeline.cpp, tests/lock_manager_tests.cpp, tests/transaction_manager_tests.cpp, tests/transaction_crash_recovery_integration_tests.cpp

7. **Extended SQL Surface & Tooling (Planned)**
   - Broaden parser/executor to cover CTEs, window functions, analytic aggregates, and advanced DDL (constraints, sequences, views).
   - Enhance shell tooling and diagnostics to expose new capabilities, sequence states, constraint violations, and query plans.
   - Source files to update: src/parser/grammar.cpp, src/parser/relational_binder.cpp, src/parser/relational_logical_normalization.cpp, src/executor/aggregation_executor.cpp, src/executor/projection_executor.cpp, src/shell/shell_engine.cpp, src/shell/shell_backend.cpp, tests/parser_select_tests.cpp, tests/executor_aggregation_tests.cpp, tests/shell_integration_tests.cpp

8. **Readiness Validation (Planned)**
   - Develop regression suites covering constraint enforcement, sequence allocation, CTE behavior, transaction isolation, and index-aware query plans.
   - Run performance and crash/recovery drills to validate durability and consistency with new features.
   - Source files to update: tests/executor_milestone1_tests.cpp, tests/planner_integration_tests.cpp, tests/transaction_crash_recovery_integration_tests.cpp, tests/end_to_end/e2e_smoke.sql, docs/checkpoint_recovery_milestones.md, docs/storage_benchmark_telemetry_backlog.md, benchmarks/storage_benchmarks.cpp

Delivering these milestones will transition the project from a WAL-driven storage core with baseline relational operators to a full-featured relational database supporting constraints, rich SQL constructs, and mature concurrency control.

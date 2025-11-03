# Relational Feature Progress Report

_Last updated: 2025-11-03_

Latest validation: Release `ctest` (420/420) on 2025-11-03.

## Current Capabilities

- **Storage Durability**: WAL writer/reader/replayer pipeline complete with retention, checkpoint scheduling, crash recovery, and page manager integration.
- **Catalog & DDL**: Persistent catalog with fully wired DDL handlers (create/alter/drop) for schemas, tables, and indexes, including restart-safe catalog bootstrap.
- **Sequence Allocation (foundation)**: Transactional sequence allocator stages `next_value` updates via catalog mutator hooks, dispatcher now wires allocators into DDL handlers, and Catch2 regression coverage remains; planner/executor wiring is still pending.
- **Transaction Lifecycle (partial)**: Transaction manager handles ID allocation, snapshots, commit metadata emission, and integrates with WAL commit pipeline; bored_shell now supports BEGIN/COMMIT/ROLLBACK so INSERT/UPDATE/DELETE/SELECT flows can share session-scoped transactions with executor snapshots; catalog accessor caches refresh on snapshot changes and planner/executor pipelines share the same transaction snapshot; snapshot-aware retention guard now propagates oldest reader LSNs while cross-session isolation and deadlock handling remain on the roadmap.
- **Parser, Binder, and Normalizer**: PEGTL-based SQL parser covering core DDL/DML verbs; binder resolves identifiers and types; lowering and normalization stages generate logical plans for select/join queries.
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
| Common table expressions (CTEs) | **Missing** | Parser and planner lack WITH clause support; no recursive/non-recursive CTE execution pipeline. |

## Gaps to Close

1. **Constraint Enforcement**: Finish catalog DDL plumbing for constraint creation/drop and surface richer shell diagnostics for violations now that enforcement is active.
2. **Sequence/Identity Support**: Wire planner/executor and DDL verbs to consume the transactional sequence allocator so auto-increment columns surface to users.
3. **CTE Parsing & Execution**: Expand grammar, AST, and planner memo to represent WITH clauses; add execution support (materialized or pipelined) with snapshot-aware iterators.
4. **Transaction Visibility**: Finish Transaction & Concurrency Milestone 1 to provide consistent snapshots across planner/executor and integrate with retention manager for snapshot retirement.
5. **Optimizer Enhancements**: Broaden rule set, add cost-based join order selection, and integrate catalog statistics for selectivity estimates.
6. **Index DDL Integration**: Surface index creation options (unique, covering, partial) and ensure planner/executor leverage indexes for predicates and joins.

## Roadmap to Full Relational Coverage

1. **Finalize Concurrency Milestone 1 (In Progress)**
   - [x] Shell DML and SELECT pipelines now share live TransactionManager contexts with catalog cache refreshes and planner/executor visibility checks.
   - [x] Added regression coverage verifying catalog accessor tuples are re-evaluated when snapshots advance without extra scans.
   - [x] Wired snapshot-aware retention guard through the commit pipeline so WAL retention honors active reader horizons.
   - [x] Landed multi-page crash drill verifying committed pages replay while in-flight pages roll back with fragment-aware undo.
   - [x] Extend undo walker crash drills to cover overflow chains with fragment-aware assertions.
   - [x] Add regression coverage for session rollback edge conditions, including catalog and overflow interactions.

2. **Constraint & Sequence Foundations**
   - [x] Extend catalog metadata for constraints and sequences; persist via WAL and recovery hooks.
   - [x] Expose catalog accessor support for sequence descriptors across schema and relation scopes.
   - [x] Implement sequence allocator with transactional semantics for auto-increment columns (validated by `catalog_sequence_allocator_tests`).
   - [x] Update DDL verbs (`CREATE TABLE`, `ALTER TABLE`, `CREATE SEQUENCE`) to request transactional sequence allocators; planner/executor consumption tracked under Constraint Enforcement Pipeline.

3. **Constraint Enforcement Pipeline**
   - [x] Catalog accessor now exposes constraint descriptors for planner/executor consumption.
   - [x] Planner: Recognize unique/primary keys and foreign keys; generate enforcement operators.
   - [x] Executor: Implement uniqueness checks (indexes + deferred validation) and referential integrity probes with transactional awareness.
   - [x] Shell: Apply constraint enforcement to INSERT/UPDATE pipelines using planner metadata and simulated index probes.

4. **CTE Enablement**
   - Parser/AST: Add WITH clause grammar and nodes (non-recursive first, recursive second).
   - Planner: Introduce memo entries for CTE producers/consumers and support inlining or materialization strategies.
   - Executor: Provide worktable infrastructure for recursive CTEs and integrate with snapshot visibility.

5. **Advanced Indexing & Optimization**
   - Support unique indexes tied to constraint metadata; expose covering/partial index options.
  - Enhance optimizer to choose index scans based on statistics and predicates; add cost model refinements.
   - Expand join optimization (multi-join reordering, bushy plans) once statistics available.

6. **Comprehensive Transactions & Isolation Levels**
   - Implement lock manager integration for key-range locking where needed for uniqueness.
   - Add multi-version concurrency control (MVCC) visibility rules across executor operators.
   - Provide configurable isolation levels and conflict resolution.

7. **Extended SQL Surface & Tooling**
   - Broaden parser/executor to cover CTEs, window functions, analytic aggregates, and advanced DDL (constraints, sequences, views).
   - Enhance shell tooling and diagnostics to expose new capabilities, sequence states, constraint violations, and query plans.

8. **Readiness Validation**
   - Develop regression suites covering constraint enforcement, sequence allocation, CTE behavior, transaction isolation, and index-aware query plans.
   - Run performance and crash/recovery drills to validate durability and consistency with new features.

Delivering these milestones will transition the project from a WAL-driven storage core with baseline relational operators to a full-featured relational database supporting constraints, rich SQL constructs, and mature concurrency control.

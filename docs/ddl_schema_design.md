# DDL & Schema Management Design

## Objectives
- Provide transactional create/alter/drop flows for schemas, tables, and indexes that persist metadata through the catalog subsystem.
- Enforce dependency and constraint validation to prevent inconsistent catalog state.
- Integrate DDL execution with WAL, checkpointing, and crash recovery so partially applied operations can be rolled back safely.
- Surface diagnostics and telemetry that reflect DDL throughput, contention, and failure scenarios.

## Scope
- DDL verbs: `CREATE/DROP DATABASE`, `SCHEMA`, `TABLE`, `INDEX`, plus `ALTER TABLE` (rename column/table, add/drop column) in the initial scope.
- Command execution path from high-level DDL request structures (or AST nodes) down to catalog mutations and storage coordination.
- Crash drills and undo coverage for mid-flight catalog changes, identifier allocation, and index metadata updates.
- Operator-facing feedback (success/failure messages, warnings) and telemetry hooks.

## Out of Scope (Deferred)
- Constraint DDL beyond primary key and not-null enforcement (e.g., foreign keys, check constraints).
- Online/parallel index builds; only single-threaded builds assumed.
- Partitioning, materialized views, and advanced index types.
- Privilege management and security-related DDL.

## Dependencies
- Completed catalog subsystem (see `catalog_design.md`) with WAL-integrated mutation path.
- Storage WAL/replay pipeline, including catalog crash drills.
- Transaction ID allocator and snapshot infrastructure for catalog visibility.
- Planned parser/AST layers to supply structured DDL requests (temporary stubs acceptable for initial milestones).

## Architectural Pillars
- **Command Dispatcher:** Routes parsed DDL requests to concrete executors, supplies transactional context, and coordinates commit/abort hooks.
- **Validation Layer:** Resolves existing catalog entries, checks dependencies (schemas, indexes, columns), and enforces naming rules.
- **Mutation Engine:** Uses `CatalogMutator` to stage catalog changes, allocates identifiers, and persists WAL fragments.
- **Execution Hooks:** Triggers page manager operations (e.g., table root page allocation), checkpoint scheduler notifications, and retention updates.
- **Diagnostics:** Emits telemetry counters (per-verb success/failure, latency, retries) and structured errors for client consumption.

## Milestones

- **Milestone 0: DDL Infrastructure Bootstrap (1 sprint)**
  - [x] Define DDL request/response structs covering planned verbs with extensible metadata (options, IF EXISTS, etc.).
  - [x] Implement DDL command dispatcher that acquires a `CatalogTransaction`, invokes verb-specific executors, and translates errors to client responses.
  - [x] Provide validation helpers for common checks (name syntax, schema existence, duplicate detection) returning `std::error_code` flavors aligned with storage errors.
  - [x] Integrate dispatcher with telemetry registry (basic counters for each verb and failure mode).
  - Tests: unit tests for dispatcher plumbing, validation helpers, and telemetry counter increments.

- **Milestone 1: Schema & Table Lifecycle (1-2 sprints)**
  - [x] Implement `CREATE SCHEMA`/`DROP SCHEMA` with dependency checks ensuring schemas cannot be dropped while containing tables.
  - [x] Implement `CREATE TABLE` with column definitions, type validation (primitive types), and catalog identifier allocation.
  - [ ] Support `ALTER TABLE` operations: rename table, add column (appends column metadata), drop column (marks metadata as dropped, no physical reclaim yet).
  - [ ] Implement `DROP TABLE` with retention-aware cleanup hooks (signal retention manager, flag table metadata for pruning).
  - [ ] Extend checkpoint scheduler integration to account for DDL-driven dirty catalog pages.
  - Tests: integration tests using `CatalogMutator` stubs to verify catalog state after each verb, crash drills for mid-flight table creation with undo coverage.

- **Milestone 2: Index Definition & Dependency Enforcement (1-2 sprints)**
  - [ ] Implement `CREATE INDEX` for single-column btree stubs, validating target table/column existence and type compatibility.
  - [ ] Implement `DROP INDEX` ensuring no dependent constraints (future-proof hook) and cleaning index metadata.
  - [ ] Wire index creation into storage hooks: reserve index root pages, seed WAL records for index metadata, register with retention manager.
  - [ ] Introduce dependency graph helper to prevent dropping schemas/tables while indexes depend on them.
  - [ ] Emit index-focused telemetry (build duration, failure counts) and add crash drills for interrupted index creation (before-image coverage).
  - Tests: end-to-end DDL sequences for index create/drop with catalog replay validation and recovery drills.

- **Milestone 3: Hardening, Rollback, and Diagnostics (1 sprint)**
  - [ ] Implement savepoint-friendly DDL rollback (ensure staged catalog mutations can be unwound before commit).
  - [ ] Expand crash drills to cover identifier allocator updates during concurrent DDL, ensuring WAL replay restores counters and metadata consistently.
  - [ ] Integrate DDL activity telemetry into `storage_diagnostics.hpp` for operator surfaces (per-verb latency distribution, failure taxonomy).
  - [ ] Enhance error propagation with structured diagnostics (error code, severity, remediation hints) surfaced via CLI/API.
  - [ ] Document DDL verb semantics, failure modes, and recovery behaviors.
  - Tests: negative-path suites (duplicate names, missing dependencies), concurrency simulations for conflicting DDL, and documentation lint checks.

## Open Questions
- Should identifier allocation for schemas/tables/indexes move to dedicated counter pages or remain catalog-driven?
- What minimal constraint support (e.g., NOT NULL enforcement) is required for initial ALTER TABLE semantics?
- How should long-running index builds report progress through telemetry or diagnostics surfaces?

## Next Actions
- Align milestone sequencing with overall relational roadmap (update `relational_layer_design.md` accordingly).
- Prepare engineering tickets for Milestone 0 tasks, including parser stub integration and telemetry wiring.
- Schedule design review to confirm dependency handling strategy and rollback expectations before implementation starts.

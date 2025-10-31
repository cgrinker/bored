# Shell Frontend Integration Roadmap

The goal is to evolve `bored_shell` from a demo harness into a thin client over the production relational stack. We progress layer by layer so each abstraction (parser/AST, planner/executor, storage) is verified before moving on. This document records status, code touch-points, and iteration-sized action items so work can resume quickly even after context switches.

**Roadmap summary**
- Milestone 0 keeps the shell aligned with shared DDL parsing/dispatch (done).
- Milestone 1 replaces handwritten DML parsing with the PEGTL parser + AST/IR pipeline.
- Milestone 2 routes shell DML through the planner/executor layer to eliminate bespoke row handling.
- Milestone 3 swaps the in-memory catalog cache for the WAL/page-backed storage engine.

## Milestone 0 — DDL Dispatch & Script Runner _(Status: Complete)_
- **Relevant code**: `src/shell/shell_backend.cpp` (dispatcher wiring, script helpers), `src/tools/bored_shell_main.cpp` (command runner), `src/parser/ddl_script_executor.cpp`.
- **What shipped**
	- Shell constructs `parser::DdlScriptExecutor` and `ddl::DdlCommandDispatcher`, so DDL shares diagnostics and catalog mutations with the rest of the system.
	- Script/batch execution (`--file`) and SQL comment stripping let `tests/end_to_end/e2e_smoke.sql` run unattended.
- No further work required; retain as regression reference.

## Milestone 1 — Parser/AST DML Adoption _(Status: Planned)_
- **Action items**
 1. [x] Add a shell entry point that invokes the PEGTL parser for single statements (`parser::parse_relational_statement` shim if needed) and returns AST diagnostics. ✓ _2025-10-30: `execute_select` now parses via `parser::parse_select` and rejects unsupported constructs with parser-style diagnostics._
 2. [x] Replace `ShellBackend::execute_insert|update|delete|select` with logic that lowers the AST to the logical IR (reusing the binder) and converts semantic/parse failures into `CommandMetrics`.
	 - 2025-10-30: `execute_insert|update|delete` now parse and bind through the shared PEGTL pipeline; executor handoff remains deferred to Milestone 2.
	 - 2025-10-30: `execute_insert` builds a placeholder planner `LogicalPlan`/`plan_query` result before mutating in-memory tables so planner diagnostics flow into shell metrics.
	 - 2025-10-31: `execute_update` and `execute_delete` now construct placeholder planner plans (Update/Delete + SeqScan) so planner diagnostics surface before in-memory mutations.
	 - 2025-10-31: `execute_select` now binds against the catalog and constructs a Projection + SeqScan logical plan so planner diagnostics accompany result rendering.
 	- 2025-10-31: `execute_select` rendering now consumes the planner-projected column set so column order mirrors the planner pipeline.
	- 2025-10-30: Shared planner helpers (`collect_table_columns`, `plan_scan_operation`, `plan_select_operation`) now centralize DML scaffolding and diagnostics across INSERT/UPDATE/DELETE/SELECT.
	- 2025-10-30: Executor stub consumes the shared physical plans and appends simulated executor diagnostics for SELECT/UPDATE/DELETE metrics.
	- 2025-10-31: INSERT now emits executor stub diagnostics and Catch2 coverage asserts planner/executor detail lines.
	 - 2025-10-31: UPDATE and DELETE now invoke the executor stub once per command and thread the rendered plan lines into command metrics.
	 - 2025-10-31: All DML routes lower through the logical plan stage; `ShellBackend` records logical root/plan diagnostics (via `parser::relational::lower_select` for SELECT and binder metadata for INSERT/UPDATE/DELETE) before planner/executor stubs, surfacing lowering errors as `CommandMetrics` failures.
	- **Next task**: Add focused tests (e.g., Catch2 unit coverage) that assert planner and executor stub detail lines, covering Milestone 1 action item 3.
	
 3. [ ] Update or add tests (`tests/end_to_end/e2e_smoke.sql` + unit coverage under `tests/parser` or new Catch2 cases) to confirm comment handling and DML success via the parser.
- **Iteration guidance**: each numbered item should be achievable in a single Codex iteration; if an item grows (for example, needing extensive binder changes), break it into subtasks before starting.

## Milestone 2 — Executor-Backed DML _(Status: Planned)_
- **Relevant code**: `src/planner` (logical → physical planning), `src/executor` (operator pipeline), `src/shell/shell_backend.cpp` (row cache), telemetry hooks in `include/bored/storage/storage_telemetry_registry.hpp`.
- **Action items**
	1. Introduce a planner/executor call path in `ShellBackend::execute_dml` that takes the logical plan from Milestone 1 and produces results via the executor (likely through `planner::plan_query` and `executor::run_query`).
	2. Remove the in-memory `TableData::rows` mutation logic once executor pipelines write/read from catalog-backed storage or executor buffers.
	3. Validate telemetry propagation by asserting `CommandMetrics.rows_touched`/`wal_bytes` reflect executor metrics in updated end-to-end tests.
- **Iteration guidance**: if integrating planner and executor together is too large, split into subtasks (e.g., planning integration first, executor wiring second) and track them explicitly.

## Milestone 3 — Persistent Storage Backend _(Status: Planned)_
- **Relevant code**: `src/storage/page_manager.cpp`, `src/storage/wal_writer.cpp`, `src/storage/wal_replayer.cpp`, IO configuration in `include/bored/storage/storage_configuration.hpp`, shell bootstrap in `src/shell/shell_backend.cpp`.
- **Action items**
	1. Replace `InMemoryCatalogStorage` with a bootstrap path that instantiates the page manager and WAL writer (mirroring the server runtime) and seeds catalog state via `catalog::CatalogBootstrap`.
	2. Extend CLI/config (`src/tools/bored_shell_main.cpp`) to accept database directories, WAL retention knobs, and IO_uring settings, and pass them to the backend.
	3. Add integration tests (Catch2 or scripted) that start the shell on disk, execute the smoke script, restart the process, and verify catalog/data persistence.
- **Iteration guidance**: each step must have verifiable outputs (e.g., config flag recognized, test cases added). When a task cannot finish in one iteration, decompose it into labelled subtasks before committing code.

## Verification Checklist Template
- Confirm updated tests (unit, Catch2, or smoke script runs) cover the new behavior.
- Ensure documentation updates reference concrete file paths and CLI switches.
- When work exceeds a single iteration, record follow-up subtasks in this doc (or the issue tracker) before switching context so progress stays recoverable.

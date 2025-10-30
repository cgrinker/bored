# Project Backlog

## Storage WAL Resilience
- **Crash/restart drills for undo walker chains** — design crash/restart scenarios that stress overflow chains and validate before-image consistency during recovery replay. Tracks the drill orchestration work outlined in [page_wal_design.md](./page_wal_design.md).
- **Benchmark FSM refresh, retention pruning, and overflow replay** — capture baseline runtimes for key storage maintenance loops to set regression thresholds before the next release. Benchmark expectations and telemetry hooks live in [storage.md](./storage.md).
- **Operator tooling for retention and recovery** — finalize CLI/docs for retention knobs, checkpoint scheduling, and recovery workflows so SRE playbooks stay consistent with current WAL lifecycle. Operator experience requirements are documented in [storage.md](./storage.md).

## Parser Logical IR
- **Benchmark binder/lowering stages** — capture baseline timings for `parse_select` + `lower_select` using representative workloads so we can flag regressions via `benchmarks/parser_benchmarks.cpp`.
- **Document AST/IR extension conventions** — codify naming, ownership, and testing guidelines for future logical operator additions, expanding on the expectations outlined in [ast_logical_ir_design.md](./ast_logical_ir_design.md).

## Transaction & Concurrency Control
- **MVCC design review** — Completed via `docs/transaction_concurrency_design.md` (2025-10-26); milestone follow-ups tracked in `docs/transaction_concurrency_milestones.md`.

## End-to-End Database Validation
- **Interactive shell smoke tests** — drive `bored_shell` through scenarios that create databases and schemas, issue `CREATE TABLE` statements, insert representative tuples, and verify `SELECT` output matches expectations. Capture scripts under `tests/end_to_end/` so they can run locally and in CI.
- **Shell script runner** — extend `bored_shell` CLI to accept SQL script files (with `@file` or `--file` semantics), stream commands to the engine, and surface results through the existing console renderer so automation can validate behavior without the interactive REPL.
- **Integration harness for catalog + storage** — author a Catch2 suite that spins up the catalog, WAL, and storage subsystems together, executes the smoke script programmatically, and asserts catalog/WAL side effects (segment counts, retention stats) alongside query results.
- **Scenario coverage and diagnostics documentation** — document the end-to-end workflows, required configuration, and troubleshooting steps in `docs/storage_shell_recipes.md`, including how to surface parser/telemetry diagnostics when a step fails.

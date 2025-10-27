# Project Backlog

## Storage WAL Resilience
- **Crash/restart drills for undo walker chains** — design crash/restart scenarios that stress overflow chains and validate before-image consistency during recovery replay. Tracks the drill orchestration work outlined in [page_wal_design.md](./page_wal_design.md).
- **Benchmark FSM refresh, retention pruning, and overflow replay** — capture baseline runtimes for key storage maintenance loops to set regression thresholds before the next release. Benchmark expectations and telemetry hooks live in [storage.md](./storage.md).
- **Operator tooling for retention and recovery** — finalize CLI/docs for retention knobs, checkpoint scheduling, and recovery workflows so SRE playbooks stay consistent with current WAL lifecycle. Operator experience requirements are documented in [storage.md](./storage.md).

## Parser Logical IR
- **Join-aware logical plan dumps** — extend `dump_select_plan` to surface join normalization stages once join lowering lands, keeping plan traces aligned with the milestones in [ast_logical_ir_design.md](./ast_logical_ir_design.md).

# User-Facing Tooling & Diagnostics Milestones

## Milestone 0: Telemetry & Diagnostics Foundations
- **Goals:** Establish a reliable operator-facing diagnostics contract and baseline CLI primitives.
- **Key Tasks:**
  - [x] Define JSON schema versioning for `storage_diagnostics_to_json` payloads.
  - [x] Implement `boredctl diagnostics capture` CLI command that shells out to diagnostics collector.
  - [x] Surface `checkpoint`, `recovery`, and `transaction` subsections with formatting rules in CLI.
- **Exit Criteria:**
  - [x] CLI command emits JSON snapshot and human-readable summary without requiring manual script glue.

## Milestone 1: Interactive SQL Shell
- **Goals:** Provide a readline-capable shell with SQL parsing, execution, and telemetry overlays.
- **Key Tasks:**
  - [x] Initialize shell binary (`bored_shell`) with basic REPL loop and command dispatch.
  - [x] Integrate parser front-end for statement tokenization and error reporting.
  - [x] Wire execution pipeline to route DDL/DML statements into the executor framework when enabled.
  - [x] Surface per-command timing, rows touched, and WAL bytes via telemetry registry hooks.
- **Progress (Oct 29, 2025):** Shell REPL now buffers multi-line statements, persists history between sessions, and exercises catalog-backed DDL paths end-to-end in integration tests.
- **Exit Criteria:**
  - [x] Shell supports multi-line statements, command history, and executes DDL operations end-to-end against the engine in integration tests.

## Milestone 2: Catalog Inspection & System Views
- **Goals:** Equip operators with introspection commands and system metadata views.
- **Key Tasks:**
  - [x] Define SQL system views (`bored_catalog.*`, `bored_storage.*`) backed by catalog metadata snapshots.
  - [x] Implement shell commands `\dt`, `\di`, `\dv`, and `\dl` that render table/index/view/lock summaries.
  - [x] Add JSON export endpoints for catalog and telemetry snapshots to the diagnostics CLI.
- **Progress (Oct 29, 2025):** `bored_catalog` and `bored_storage` virtual views now back the shell's `\d*` commands, and `boredctl diagnostics catalog|locks` emit JSON snapshots sourced from the shared introspection samplers.
- **Exit Criteria:**
  - [x] System views and shell commands expose schema state and lock holder snapshots, validated via regression tests.

## Milestone 3: Observability Integrations
- **Goals:** Bridge storage/relational telemetry into external observability stacks.
- **Key Tasks:**
  - [ ] Implement metrics emitter for Prometheus/OpenMetrics (checkpoint lag, WAL backlog, query latency).
  - [ ] Provide structured logging sink with correlation identifiers for SQL statements.
  - [ ] Document configuration for alerting thresholds (checkpoint lag, retention backlog, recovery duration).
- **Exit Criteria:**
  - [ ] Telemetry surfaces scrape-able metrics set and emit structured logs consumed by integration tests.

## Milestone 4: Operator Runbooks & Dashboards
- **Goals:** Deliver actionable runbooks and reference dashboards for common incident workflows.
- **Key Tasks:**
  - [ ] Expand `docs/storage.md` and new operator guides with SQL shell recipes for diagnostics.
  - [ ] Ship Grafana dashboard JSON templates covering checkpoint, recovery, retention, and lock contention panels.
  - [ ] Create incident playbooks for checkpoint failures, recovery stalls, and executor regressions.
- **Exit Criteria:**
  - [ ] Documentation and dashboard assets are published alongside CLI tooling, and incident drills reference them end-to-end.

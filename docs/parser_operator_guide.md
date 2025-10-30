# Parser Diagnostics Operator Guide

This guide explains how parser diagnostics surface through `bored` tooling and how operators can react to them during day-to-day database management.

## Diagnostic Pipeline
- The PEGTL-based parser produces `ParserDiagnostic` records with `severity`, `message`, and optional remediation hints for each statement in a script.
- `parser::build_ddl_commands` propagates successful AST translations while preserving every diagnostic raised during parsing or semantic validation.
- `parser::DdlScriptExecutor` packages script parsing, command translation, and dispatcher invocation. It returns:
  - The original `ScriptParseResult` (per-statement AST and diagnostics).
  - A `DdlCommandBuilderResult` containing translated commands and any additional diagnostics emitted during catalog resolution.
  - Dispatcher responses, so operators can correlate parser warnings/errors with downstream execution failures.
- `parser::ParserTelemetry` counts script attempts, successes, diagnostic severities, and aggregate parse durations. When `StorageTelemetryRegistry` registers the executor, these metrics flow into storage diagnostics exports.

## Severity Levels
- **Info** – Informational notices (e.g., "statement skipped" when the parser intentionally ignores a benign construct). These do not block execution.
- **Warning** – The parser accepted the statement but detected unsupported options (for example, PRIMARY KEY clauses that are not enforced yet). The executor still dispatches the command; operators should note the remediation hints and plan follow-up actions.
- **Error** – The parser could not understand the statement or map it to a supported catalog operation (missing schema, unsupported data type, etc.). The executor skips dispatching those statements, and the surrounding script is considered unsuccessful.

## Common Diagnostic Categories
| Category | Example Message | Operator Action |
|----------|-----------------|-----------------|
| Missing catalog object | `Schema 'analytics' not found` | Verify database/schema defaults, ensure the object exists, or adjust the script to reference the correct schema. |
| Unsupported type | `Unsupported column type 'GEOMETRY'` | Replace the column type with a supported alternative (`INT`, `SMALLINT`, `UINT32`, `TEXT` variants) or delay the change until type support lands. |
| Ignored constraints | `PRIMARY KEY constraints are not currently enforced; column 'id' will be created without a key.` | Plan to create the constraint via catalog APIs or supply an alternate enforcement mechanism after table creation. |
| Parser recovery | `Unsupported DDL statement; only CREATE or DROP commands ...` | Remove unsupported statements from the script or extend the parser to understand them before retrying. |

Each diagnostic ships with remediation hints; surface them to developers or automation to maintain clarity about required follow-up actions.

## Monitoring & Telemetry
- `StorageDiagnostics` JSON now includes a `parser` section with totals and per-identifier breakdowns. Look for:
  - `scripts_attempted` / `scripts_succeeded` – Gauge parsing success rate per workload.
  - `diagnostics_warning` / `diagnostics_error` – Track trends in warnings or failures after deployments.
  - `total_parse_duration_ns` / `last_parse_duration_ns` – Detect performance regressions in the parser front-end.
- Use the diagnostics collector to capture snapshots before and after configuration changes. Pair parser telemetry with DDL dispatcher telemetry to quickly isolate whether failures arise from parsing or catalog execution.

## Operational Checklist
1. **Review diagnostics before execution** – When running `DdlScriptExecutor`, inspect the returned diagnostics collection. Surface warnings to developers even when commands succeed.
2. **Automate monitoring** – Hook storage diagnostics exports into your observability pipeline to alert on rising parser error counts or unusual duration spikes.
3. **Act on hints** – Remediation hints are tailored to the failure; use them to script guardrails (e.g., reject migrations with unsupported column types).
4. **Feed improvements back** – Capture recurring warnings/errors and extend parser grammar or catalog support accordingly to keep scripts clean.

Keeping parser diagnostics visible ensures DDL migrations remain predictable and that parser limitations are addressed before they impact production workloads.

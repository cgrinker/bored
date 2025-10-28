# Planner Operator Guide

## Overview
The query planner turns relational logical plans into executable physical plans. Operators interact with it indirectly through session APIs but remain responsible for enabling telemetry, consuming diagnostics, and verifying latency targets. This guide documents the tooling added in Milestone 4 so on-call operators can observe planner health and triage regressions quickly.

## Operating the Planner
- **Plan diagnostics** – The `planner::plan_query` result now includes `PlanDiagnostics`, exposing the chosen logical alternative, total rule attempts/applies, and costing counters. Capture these snapshots when logging slow or anomalous plans.
- **Telemetry registration** – Planners register samplers with `StorageTelemetryRegistry::register_planner`. Ensure each running planner instance calls `register_planner` and `unregister_planner` during its lifecycle so aggregated snapshots appear in storage diagnostics and operator dashboards.
- **Planner benchmarks** – Run `bored_planner_benchmarks` during deployments to verify latency budgets. Use `--json` for machine ingestion or `--baseline=<path>` plus `--tolerance=<fraction>` to enforce regression thresholds in CI.
- **Explain printer** – Invoke `planner::explain_plan` on `PlannerResult::plan` to produce a textual tree. Toggle `ExplainOptions::include_properties` or `ExplainOptions::include_snapshot` when collecting evidence for support tickets.

## Observability Surfaces
- **Storage diagnostics JSON** – `collect_storage_diagnostics` now emits a `planner` section mirroring the planner telemetry snapshot. Feed this JSON into your existing observability pipeline to chart rule attempts, successes, and costing cadence over time.
- **Command-line workflows** –
  1. Run the application workload with tracing enabled (`PlannerOptions::enable_rule_tracing = true`) to record rule-level events in `PlanDiagnostics::rule_trace`.
  2. Capture `explain_plan` output for the same query to visualise the operator tree that was actually executed.
  3. Compare benchmark output to established baselines to confirm there are no systemic latency regressions.
- **Integration with storage dashboards** – Aggregated planner metrics live alongside WAL and checkpoint telemetry. Alerting rules should trigger if `plans_failed` or `rules_attempted` spikes beyond baseline windows.

## Troubleshooting Checklist
1. **Check planner telemetry**: Use `StorageTelemetryRegistry::aggregate_planner()` or the diagnostics JSON to confirm plans are still being attempted and succeeding. A jump in `plans_failed` usually indicates rule misconfiguration or catalog drift.
2. **Collect plan diagnostics**: Retrieve `PlanDiagnostics` from the failing session to inspect alternative costs, chosen logical plan, and rule traces. Look for dramatic swings in `chosen_plan_cost` or missing alternatives.
3. **Render the plan**: Call `explain_plan` with snapshots enabled to ensure visibility requirements and ordering semantics are being preserved. Differences in partitioning or ordering often explain executor regressions.
4. **Validate statistics**: Rebuild or refresh statistics using `StatisticsCatalog` helpers if costing numbers seem off. Stale row counts will skew join ordering and cardinality estimates.
5. **Run benchmarks**: Execute `bored_planner_benchmarks --baseline=<file>` to detect performance regressions. Failures with the JSON baseline typically correlate with CPU-intensive rule loops or new cost weights.
6. **Inspect configuration**: Verify `PlannerOptions` flags, ensuring rule tracing or experimental toggles match expectations, and confirm planner instances remain registered with `StorageTelemetryRegistry`.

Documenting incidents using the steps above will keep the planner telemetry actionable and drastically reduce the time required to root-cause planning regressions.

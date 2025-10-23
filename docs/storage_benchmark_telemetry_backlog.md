# Storage Benchmark & Telemetry Backlog

This document tracks deferred work related to benchmarking, telemetry, and operator-facing observability for the storage layer. These items are intentionally postponed so the core WAL and page flows can stabilise before adding more surface area.

## Deferred Benchmark Tasks

- **CI baseline enforcement**: Wire `bored_benchmarks` into continuous integration, including a stable invocation that records overflow tuple parameters and exits non-zero on regressions via `--baseline` and `--tolerance`.
- **Fixture hardening**: Resolve the large-overflow tuple allocation limits so we can re-enable the default 16&nbsp;KiB payload in benchmarks before refreshing the persisted baselines.
- **Workload coverage**: Expand the harness with representative retention pruning, free-space refresh, and overflow replay scenarios that reflect production duty cycles.
- **Threshold governance**: Document how tolerance windows are chosen, who maintains the baselines, and how to regenerate them when hardware or configuration changes occur.

## Deferred Telemetry & Observability Tasks

- **Diagnostics surfacing**: Extend `storage_diagnostics.hpp` outputs with latch contention, checkpoint latency, retention pruning metrics, and overflow replay counters suitable for dashboards.
- **Operator runbooks**: Produce operator-facing guides covering retention knobs, checkpoint scheduling, crash recovery workflows, and how to interpret the emitted telemetry.
- **Alerting hooks**: Define thresholds and delivery mechanisms for alerting on retention backlog, checkpoint starvation, or sustained WAL replay slowdowns.
- **Historical baselines**: Decide on storage and rotation policies for historical benchmark/telemetry snapshots so trend analysis is possible without bloating repos.

## Exit Criteria

The backlog is considered cleared once CI enforcement is live, the harness exercises stable workloads with published thresholds, telemetry surfaces expose actionable metrics, and operator runbooks are in place to consume that data.

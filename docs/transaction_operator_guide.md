# Transaction Operator Guide

This guide aggregates the operator-facing procedures for keeping the transaction subsystem healthy. It focuses on runtime telemetry, the most impactful tuning knobs, and the drills required to confirm crash recovery remains reliable as the system evolves.

## Runtime Concepts

- **Transaction states:** `TransactionState` values progress from `Active` through `Preparing`, `FlushingWal`, `Durable`, and `Publishing` before reaching `Committed`. Aborts transition the state machine to `AbortCleanup` and finally `Aborted`.
- **Durability horizon:** The shared `WalDurabilityHorizon` advances once the commit pipeline flushes WAL and updates the `durable_commit_lsn`. Retention and checkpoint schedulers read this horizon when deciding whether it is safe to prune segments or emit new checkpoints.
- **Undo linkages:** Tuple headers record `created_transaction_id`, `deleted_transaction_id`, and `undo_next_lsn` so crash recovery can assemble MVCC chains. `WalUndoWalker` groups undo spans by owner page, keeping overflow chains intact during replay.

## Instrumentation & Dashboards

- **Storage diagnostics JSON:** Invoke `storage::collect_storage_diagnostics()` to capture aggregate telemetry. Transaction-specific fields include `active_transactions`, `committed_transactions`, `aborted_transactions`, and `durable_commit_lsn`. Persist samples during incident response and scheduled crash drills.
- **Commit pipeline metrics:** Expose `CommitPipeline` counters (prepare/flush/confirm durations) via your telemetry sink. Sustained increases in `flush_duration_ns` or retries indicate WAL backpressure.
- **Lock manager gauges:** `StorageTelemetryRegistry` aggregates latch wait counts. Track `page_exclusive_wait_ns` and `page_shared_wait_ns` to spot contention long before user-facing timeouts appear.
- **Snapshot lag monitors:** Record the difference between `Snapshot::xmax` and `Snapshot::xmin` returned to new transactions. Growth suggests either snapshot consumers are lingering or the transaction allocator is starved.

## Daily Operations Checklist

1. **Review telemetry snapshots** at the start of each shift. Ensure `durable_commit_lsn` trails the latest `WalWriter` LSN by less than the agreed SLA (default: < 250 ms at peak load).
2. **Verify checkpoint cadence** by confirming `CheckpointScheduler` has emitted at least one checkpoint within the last hour and the `lsn_gap_since_last_checkpoint` is shrinking.
3. **Confirm retention progress** by checking that `WalRetentionManager` pruned or archived segments within the past duty cycle. The backlog should never exceed `retention_segments + 2` unless a long-running transaction is purposely holding WAL.
4. **Spot check abort ratios** by comparing `committed_transactions` vs. `aborted_transactions`. A sudden spike in aborts often correlates with lock thrash or constraint validation failures.
5. **Run the crash-replay smoke test** (`bored_tests "Crash recovery preserves committed changes and rolls back aborting transactions"`) weekly to confirm WAL archives and recovery tooling stay aligned.

## Tuning Knobs

| Component | Setting | Impact | Notes |
|-----------|---------|--------|-------|
| TransactionManager | `commit_pipeline` configuration | Controls batching and fsync cadence for commits | Scale pipeline threads or throttle queue depth when commit latency spikes |
| TransactionManager | `idle_snapshot_ttl` (future hook) | Governs how long read-only snapshots remain cached | Lower when snapshot churn pressure is high |
| WalWriter | `buffer_size`, `size_flush_threshold`, `time_flush_threshold` | Balances throughput vs. commit latency | Confirm `durable_commit_lsn` lag after adjustments |
| WalRetentionManager | `retention_segments`, `retention_hours`, `archive_path` | Determines WAL recycling windows | Increase during CDC export rollouts to avoid premature pruning |
| CheckpointScheduler | `dirty_page_limit`, `lsn_gap_limit`, `tick_interval` | Sets checkpoint cadence | Lower limits shrink recovery time but increase IO load |
| VacuumBackgroundLoop | `tick_interval`, `force_run` triggers | Keeps fragmentation in check | Coordinate with retention so extra cleanup doesn't exhaust WAL buffers |

## Incident Playbooks

### Commit Latency Spike

1. Capture live diagnostics and isolate whether `prepare_commit` or `flush_commit` is slow.
2. If flush time dominates, temporarily raise `WalWriterConfig::buffer_size` and confirm IO subsystem health (fsync throughput, disk queue depth).
3. Inspect retention backlog; if the active segment has grown large, trigger a checkpoint to advance pruning and reduce fsync pressure.
4. After mitigation, schedule a post-incident crash drill to ensure WAL remains consistent.

### Aborted Transaction Storm

1. Compare abort counts against lock telemetry to determine whether conflicts or validation errors are the source.
2. If locks dominate, use `LockManager::snapshot_waiters()` tooling (planned) or logs to identify hot pages. Consider increasing `VacuumWorker` batch size to reduce fragmentation.
3. If validation errors dominate, enable parser/catalog diagnostics via `catalog_operator_guide.md` to trace failing DDL/DML requests.
4. Once resolved, archive the telemetry snapshot for regression detection.

### WAL Retention Backlog

1. Inspect `oldest_active_transaction_id`; if non-zero, coordinate with application owners to terminate or conclude the blocking session.
2. If backlog persists with no active transactions, verify that checkpoint scheduling continues. Trigger a manual checkpoint via operator tooling.
3. If archives accumulate faster than expected, enlarge `retention_segments` and adjust archival cron jobs to flush the staging directory.

## Crash & Recovery Drills

- **Frequency:** Run at least monthly, or immediately after WAL, retention, or checkpoint code changes.
- **Procedure:**
  1. Snapshot the data directory and WAL archives.
  2. Execute the replay harness against the snapshot (`bored_tests "Crash recovery preserves committed changes and rolls back aborting transactions"`).
  3. Validate that MVCC headers and fragment counts match expectations.
  4. Record the drill duration and any anomalies.
- **Success criteria:** No replay errors, no unexpected fragment growth, and `WalRecoveryPlan` reports zero truncated tails.

## References

- `docs/storage.md` &mdash; storage architecture, WAL details, retention/vacuum tuning.
- `docs/transaction_concurrency_milestones.md` &mdash; milestone checklist and historical context.
- `docs/catalog_operator_guide.md` &mdash; catalog-specific operational practices that often correlate with transaction incidents.
- `tests/transaction_abort_integration_tests.cpp` and `tests/transaction_crash_recovery_integration_tests.cpp` &mdash; reference scenarios for local verification and incident reproduction.

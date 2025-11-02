# Storage Diagnostics Shell Recipes

These SQL shell recipes help operators triage storage incidents with the interactive `bored_shell` REPL and the `boredctl` CLI. Pair them with the structured logging and OpenMetrics exporters introduced in Milestone 3.

## Prerequisites
- Start the shell with JSON logging when correlating with telemetry:
  ```bash
  bored_shell --log-json /var/log/bored/sql.log
  ```
- When scraping metrics, run the storage engine or mocks with `boredctl diagnostics metrics --mock` in development environments.

## Manual Storage Control Hooks
1. Ensure `boredctl` is executing inside the process that hosts `StorageRuntime::initialize`; otherwise the control handlers are absent and the CLI returns `checkpoint request failed: operation not permitted`.
2. Inspect available verbs:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control --help
   ```
3. Request a dry-run checkpoint to sample scheduler state without emitting WAL:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control checkpoint --dry-run true
   ```
   Each invocation records an attempt/failure/duration sample in the storage telemetry registry under the configured control telemetry identifier (for example `runtime_control`). Surface those counters from `StorageControlTelemetrySnapshot` to dashboards or ad-hoc diagnostics.
4. Trigger retention maintenance when backlog thresholds are exceeded:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control retention --index true
   ```
   Disable the index sweep with `--index false` when validating WAL pruning only.
5. Exercise crash-recovery planning paths:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control recovery --redo true --undo true
   ```
   Use `--cleanup-only true` to purge scratch state after testing. Redo/undo attempts and failures surface in the control telemetry snapshot alongside checkpoint and retention counters.

## Identify Checkpoint Lag
1. List recent checkpoints and retention metadata:
   ```sql
   \dt bored_storage.checkpoints
   ```
2. Inspect scheduler telemetry for queue depth and emit failures:
   ```sql
   SELECT scheduler_id,
          queue_depth,
          emit_failures,
          last_emit_duration_ms
     FROM bored_storage.checkpoint_scheduler_stats
    ORDER BY scheduler_id;
   ```
3. Confirm backlog in metrics:
   ```bash
   boredctl diagnostics metrics | grep bored_checkpoint_lag_seconds
   ```
4. Kick the scheduler if lag exceeds policy targets:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control checkpoint --force true
   ```
   Record the resulting run duration in incident notes by sampling the control telemetry snapshot (`StorageControlTelemetrySnapshot::checkpoint`).

## Track WAL Retention Backlog
1. Summarise WAL backlog buckets:
   ```sql
   SELECT retention_bucket,
          total_segments,
          oldest_transaction_id
     FROM bored_storage.wal_retention_window
    ORDER BY retention_bucket;
   ```
2. Compare backlog size against capacity thresholds:
   ```bash
   boredctl diagnostics metrics | grep bored_wal_replay_backlog_bytes
   ```
3. If backlog exceeds two active segments, schedule maintenance directly through the CLI:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control retention --index true
   ```
   Follow up with a forced checkpoint when latency budgets allow:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control checkpoint --force true --dry-run false
   ```
   Retention attempts, failures, and last-run durations surface alongside checkpoint counters in the control telemetry snapshot.

## Lock Contention Snapshot
1. Collect long-running locks:
   ```sql
   \dl
   ```
2. Join shell output with diagnostics JSON:
   ```bash
   boredctl diagnostics capture --summary > /tmp/diagnostics.json
   jq '.locks | map(select(.holders > 0))' /tmp/diagnostics.json
   ```
3. Correlate with structured command logs using correlation ids emitted by the shell.

## Crash-Replay Drill Checklist
1. Freeze workload and record durable LSN:
   ```bash
   boredctl diagnostics capture --summary | jq '.checkpoint.last_checkpoint_lsn'
   ```
2. Export WAL backlog snapshot:
   ```bash
   boredctl diagnostics metrics | grep backlog
   ```
3. Run recovery dry-runs against archived WAL:
   ```bash
   boredctl diagnostics repl --script scripts/recovery_drill.sql
   ```
4. Validate runtime recovery handlers without replaying changes:
   ```powershell
   .\build\RelWithDebInfo\boredctl.exe control recovery --redo false --undo false --cleanup-only true
   ```
   Capture the resulting cleanup duration from the control telemetry snapshot to compare against historical baselines.
5. Review shell logs for commands exceeding 1s by filtering the JSON log:
   ```bash
   jq 'select(.duration_ms > 1000)' /var/log/bored/sql.log
   ```

## Executor Latency Investigation
1. Surface top execution latencies over the last minute:
   ```sql
   SELECT plan_id,
          rolling_latency_ms,
          rows_emitted,
          wal_bytes
     FROM bored_storage.executor_runtime_stats
    ORDER BY rolling_latency_ms DESC
    LIMIT 10;
   ```
2. Capture current summaries for dashboards:
   ```bash
   boredctl diagnostics metrics | grep bored_query_latency
   ```
3. Re-run the query in verbose mode while streaming JSON logs for correlation:
   ```bash
   bored_shell --log-json -c "EXPLAIN ANALYZE SELECT * FROM ..."
   ```

## Post-Maintenance Validation
1. Confirm no active backlog remains:
   ```bash
   boredctl diagnostics metrics | grep -E 'checkpoint|backlog'
   ```
2. Ensure retention archives stayed within budget:
   ```sql
   SELECT archive_path,
          archived_segments,
          last_archive_duration_ms
     FROM bored_storage.retention_archive_activity;
   ```
3. Clean up maintenance logs or archive them with the maintenance ticket.

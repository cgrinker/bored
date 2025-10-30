# Storage Diagnostics Shell Recipes

These SQL shell recipes help operators triage storage incidents with the interactive `bored_shell` REPL and the `boredctl` CLI. Pair them with the structured logging and OpenMetrics exporters introduced in Milestone 3.

## Prerequisites
- Start the shell with JSON logging when correlating with telemetry:
  ```bash
  bored_shell --log-json /var/log/bored/sql.log
  ```
- When scraping metrics, run the storage engine or mocks with `boredctl diagnostics metrics --mock` in development environments.

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
3. If backlog exceeds two active segments, schedule a checkpoint:
   ```sql
   SELECT bored_storage.request_checkpoint('force_retention');
   ```

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
3. Run recovery dry-run against archived WAL:
   ```bash
   boredctl diagnostics repl --script scripts/recovery_drill.sql
   ```
4. Review shell logs for commands exceeding 1s by filtering the JSON log:
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

# Storage Incident Playbooks

The following playbooks formalise the operator response for common storage incidents. Each section pairs first-response validation with diagnostic captures and escalation criteria.

## Checkpoint Failure or Lagging Horizon
1. **Acknowledge Alert** – Confirm `bored_checkpoint_lag_seconds` breached 300s or checkpoint failures were logged.
2. **Capture State**
   - `boredctl diagnostics capture --summary > /var/tmp/checkpoint_snapshot.json`
   - `boredctl diagnostics metrics | tee /var/tmp/checkpoint_metrics.prom`
3. **Triage Queue Depth**
   - Shell: `\dt bored_storage.checkpoint_scheduler_stats`
   - Note `queue_depth`, `emit_failures`, and scheduler identifier.
4. **Mitigate**
   - Request a scheduler run with telemetry recording:
     ```powershell
     .\build\RelWithDebInfo\boredctl.exe control checkpoint --force true --dry-run false
     ```
     Record the resulting duration sample from the control telemetry snapshot for incident reporting.
   - If dirty-page pressure persists: `SELECT bored_storage.request_checkpoint('force_emit');`
   - If WAL retention is blocking, bump `WalRetentionConfig::retention_segments` by one segment and reopen.
5. **Verify Recovery Point Objective**
   - Ensure `durable_commit_lsn - last_checkpoint_lsn < policy_gap`.
6. **Escalate if**
   - Two forced checkpoints still fail.
   - `recovery.total.last_redo_duration_ns` exceeds 60s post-mitigation.

## Recovery Stall or Replay Regression
1. **Detection** – Alert fires on `recovery_total_last_redo_duration_ns / 1e9 > 30` or stalled replay logs.
2. **Immediate Actions**
   - Freeze retention pruning: `SELECT bored_storage.pause_retention();`
   - Capture `boredctl diagnostics metrics` and backlog snapshot.
3. **Investigate**
   - Shell: `SELECT * FROM bored_storage.recovery_queue ORDER BY started_at DESC LIMIT 10;`
   - Validate WAL continuity with `boredctl diagnostics capture --include wal`.
4. **Run Crash Drill**
   - Execute replay dry-run on replica hardware: `boredctl diagnostics repl --script scripts/recovery_validation.sql`.
   - Exercise runtime handlers without mutating data to validate telemetry plumbing:
     ```powershell
     .\build\RelWithDebInfo\boredctl.exe control recovery --redo true --undo false
     ```
     Dry-run combinations (`--cleanup-only true`) provide cleanup timings and failure counts that surface in the recovery portion of the control telemetry snapshot.
   - Compare output against pre-incident snapshot.
5. **Mitigate**
   - If truncated tail detected, archive offending WAL for R&D, reset writer, and initiate controlled restart.
   - If replay work backlog > retention max, force checkpoint once stall cleared.
6. **Escalate if**
   - Replay run remains stalled beyond 15 minutes despite mitigation.
   - WAL integrity check fails or truncated segments reappear.

## Executor Regression or Latency Spike
1. **Trigger** – Dashboard panel `bored_query_latency_last_seconds` turns orange/red or shell logs highlight queries >1s.
2. **Capture Evidence**
   - `boredctl diagnostics metrics | grep bored_query_latency`
   - Filter command logs: `jq 'select(.duration_ms > 500)' /var/log/bored/sql.log > /var/tmp/slow_queries.json`
3. **Identify Culprit Plans**
   - Shell query:
     ```sql
     SELECT plan_id,
            rolling_latency_ms,
            rows_emitted,
            wal_bytes
       FROM bored_storage.executor_runtime_stats
      ORDER BY rolling_latency_ms DESC
      LIMIT 5;
     ```
4. **Mitigation Options**
   - Refresh statistics via `bored_catalog.refresh_statistics('<schema>.<table>')`.
   - Pause offending session using session management tooling. Document decisions.
   - When executor load spills into retention or checkpoint pressure, request maintenance via `boredctl control checkpoint --dry-run true` to gather a timing sample before forcing a full run.
5. **Regression Validation**
   - Re-run representative workload with `EXPLAIN ANALYZE` captured in logs.
   - Compare runtime metrics before/after mitigation.
6. **Escalation if**
   - Latency remains >2x baseline for 10 minutes.
   - Executor telemetry shows sustained WAL spikes with no plan changes.

## Post-Incident Checklist
- Archive telemetry JSON, metrics snapshots, and shell log excerpts alongside ticket.
- Capture control telemetry deltas for the incident window via the `StorageControlTelemetrySnapshot` exported by the active runtime (for example through diagnostics capture tooling or bespoke dashboards).
- Update the Grafana dashboard annotation with incident summary and correlation IDs.
- File follow-up tasks for permanent configuration or code fixes if manual intervention was required.

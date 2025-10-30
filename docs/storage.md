# Storage Format Design

This document captures the first pass at the on-disk layout for the experimental relational database that lives in this repository. Two foundational building blocks are described: the fixed-size page format used by the main storage engine and the write-ahead log (WAL) format that guarantees durability and supports crash recovery.

### Progress Snapshot (Oct 23, 2025)

- **WAL pipeline 100%**: Writer, reader, recovery planning/replay, telemetry registry, checkpoint scheduler, retention manager, and compaction metadata logging are covered by Catch2 suites. Checkpoint cadence, retention pruning metrics, and transaction manager gauges (active count, commit/abort totals, and snapshot age) now flow through the storage telemetry registry for diagnostics callers, and the diagnostics collector exports aggregated snapshots for operators.
- **Vacuum scaffolding live**: `VacuumWorker` batches MVCC pages behind safe-horizon gates, telemetry flows through the storage registry, and the new `VacuumBackgroundLoop` drives dispatch on a configurable tick with force-run pokes for checkpoint checkpoints or operator overrides.
- **Commit pipeline WAL-backed**: `TransactionManager` now stages commits through `WalCommitPipeline`, encodes commit payloads with `WalWriter::stage_commit_record`, flushes them durably, and advances a shared `WalDurabilityHorizon` so snapshots, retention policies, and checkpoint schedulers observe the newest safe commit window while durability telemetry flows through `StorageTelemetryRegistry` for operator dashboards.
- **MVCC visibility coverage tightened**: Catalog accessor suites now simulate concurrent inserts, updates, deletes, and vacuum-safe pruning so snapshot readers observe the correct version set when transactions straddle commit boundaries.
- **Storage pages ~99%**: Core page operations, compaction with WAL slot relocation metadata, free-space persistence, overflow tuple WAL emission, cached before-image logging for overflow chains, undo-driven crash drills now verify before-image recovery across single and multi-owner overflow spans, and the new `bored_benchmarks` harness exercises FSM refresh, retention pruning, and overflow replay loops. Baseline runs (`benchmarks/baseline_results.json`) capture current timings until real workloads exist; follow-on benchmark and observability work is documented separately in `docs/storage_benchmark_telemetry_backlog.md`. The `IndexRetentionExecutor` now walks catalog descriptors, loads index leaves, and rewrites heap tuple pointers based on compaction metadata so retention runs repair offsets before checkpoints advance, and integration suites compact table pages, schedule retention, and simulate crash/restart drills to assert rewritten index leaves persist across recovery.

## Page Format

- **Page size:** 8 KiB (`kPageSize`). This size balances I/O efficiency with memory utilisation and aligns with common database implementations.
- **Layout:**
  1. A 40-byte `PageHeader` at the beginning of every page.
  2. Tuple data grows upward from `free_start`.
  3. The slot directory grows downward from the end of the page; each `SlotPointer` is 4 bytes.
- **Header fields:**
  - `magic` & `version` identify the page format and allow future upgrades.
  - `type` encodes the page role (table data, index node, free list, etc.).
  - `page_id` is the logical identifier; `lsn` captures the last WAL record applied.
  - `checksum` stores the full 32-bit page CRC32C so corrupt pages are detected quickly.
  - `flags` holds bits such as `Dirty` and `HasOverflow`.
  - `free_start` / `free_end` mark the current boundaries of the free space region.
  - `tuple_count` and `fragment_count` track occupancy and fragmentation statistics.
- **Slot directory:** `SlotPointer` entries contain an offset/length pair. Offsets always reference the start of a tuple payload counted from the beginning of the page, enabling relocations without rewriting referencing indices.
- **Max tuples:** With the default sizing, a page can host up to 2,038 slots before either free space or the slot array is exhausted.
- **Validation helpers:** Inline helpers (`is_valid`, `compute_free_bytes`, etc.) provide lightweight sanity checks without forcing an allocator implementation.
- **Mutable helpers:** `page_operations.hpp` offers routines to initialise a page buffer, append tuples, reclaim slots, and read payloads while tracking the header metadata. These helpers detect overflow tuples, shape stub payloads, and cooperate with the new latch callbacks so concurrent readers and mutators coordinate via lightweight locks.
- **Free space map:** `FreeSpaceMap` maintains bucketed page candidates keyed by contiguous free bytes and prefers unfragmented buffers during allocation. Snapshots persist via `FreeSpaceMapPersistence` and prime replay contexts before WAL redo.
- **Page compaction:** `compact_page` rewrites surviving tuples to eliminate gaps, resets contiguous free space, drops `fragment_count`, and pushes the refreshed measurements back into the free-space map.

## Write-Ahead Log (WAL) Format

- **Segment size:** 16 MiB files partition the WAL, giving manageable archival / recycling boundaries.
- **Block size:** Records are arranged on 4 KiB aligned blocks so that direct I/O remains efficient.
- **Segment header:** Each segment begins with a 32-byte `WalSegmentHeader` containing `magic`, `version`, `segment_id`, and LSN range metadata.
- **Record header:** Every change is written as a `WalRecordHeader` followed by an optional payload. Key fields include:
  - `total_length`: total bytes occupied by the record (header + payload + padding).
  - `type`: enumerated `WalRecordType` (page image, delta, commit, abort, checkpoint, tuple insert/delete/update).
  - `flags`: bitmask describing payload properties (`HasPayload`, `Compressed`).
  - `lsn` / `prev_lsn`: establish the log chain for recovery / roll-forward.
  - `page_id`: the page impacted by the change (0 for meta records).
  - `checksum`: CRC32C that covers the header and payload.
- **Alignment:** `align_up_to_block` rounds record lengths to the 4 KiB boundary so records never straddle pages unexpectedly.
- **Validation helpers:** `is_valid_segment_header` and `is_valid_record_header` allow quick filtering of corrupt or stale WAL data before deeper parsing.
- **Tuple payloads:** `wal_payloads.hpp` defines aligned layouts for tuple inserts, deletes, and updates to simplify serialisation/deserialisation when building records.
- **Async persistence abstraction:** The `AsyncIo` interface hides platform-specific queues (Windows IORing, Linux io_uring) behind a unified submission/completion API for page and WAL traffic, with a portable thread-pool fallback selected by `create_async_io()` today.
- **WAL writer runtime:** `WalWriter` maintains an aligned in-memory buffer, allocates monotonically increasing LSNs, rotates 16 MiB segments when full, and persists segment headers + records through the shared `AsyncIo` dispatchers. Exposes size/time/commit-driven flush hooks layered on `flush()` for commit coordination.
- **Telemetry registry:** `WalTelemetryRegistry` collects `WalWriterTelemetrySnapshot` samplers so admin tooling and diagnostics endpoints can surface append/flush metrics without coupling to writer internals. Configure `WalWriterConfig::telemetry_registry` + `telemetry_identifier` to auto-register writers and tear them down safely. `StorageTelemetryRegistry` now feeds a diagnostics collector that emits JSON for page managers, checkpoint schedulers, retention workers, the durability horizon published by `WalCommitPipeline`, and per-writer temp cleanup runs (invocations, purged entries, duration, failures) when `WalWriterConfig::storage_telemetry_registry` + `temp_cleanup_telemetry_identifier` are populated.
- **Retention manager:** `WalRetentionManager` enforces `WalRetentionConfig` knobs (`retention_segments`, `retention_hours`, `archive_path`) after durable flushes, pruning or archiving old segments without ever touching the active writer segment. It now consults the shared `WalDurabilityHorizon` so segments that still cover the oldest-active commit LSN remain protected while pruning proceeds, and retention runs continue to report scan/prune statistics through storage telemetry samplers when invoked by the scheduler.
- **Checkpoint manager:** `CheckpointManager` encodes dirty page tables and active transaction snapshots into `WalRecordType::Checkpoint` payloads so recovery can bootstrap redo horizons quickly while keeping the WAL append path consistent.
- **Page compaction record:** `WalRecordType::PageCompaction` captures slot relocation metadata, refreshed free space boundaries, and index maintenance flags so both online replay and background tooling can reconcile tuple layout changes without full page images.
- **Checkpoint scheduler:** `CheckpointScheduler` coordinates periodic checkpoint emission based on dirty-page/transaction pressure, flushes the WAL writer, and kicks retention policies once records are durable. It now samples the shared `WalDurabilityHorizon` when evaluating LSN gap triggers so checkpoint cadence reflects the true durable horizon. Trigger counts, forced runs, and emission/flush/retention latencies remain tracked per scheduler and exposed through `StorageTelemetryRegistry` samplers and diagnostics exports.
- **Vacuum scheduling:** `VacuumWorker` batches candidate page ids with per-page safe horizons, deduplicates queue entries, and tracks totals/runs/failures through `StorageTelemetryRegistry`. The `VacuumBackgroundLoop` invokes the worker every `tick_interval` (default 10 ms) or immediately when `request_force_run()` is issued, letting operators tune cadence and wire custom dispatchers for table/index vacuum routines.

- **Diagnostics collector:** `storage_diagnostics.hpp` provides a lightweight collector that aggregates storage telemetry snapshots, optional per-component breakdowns, and renders them as JSON so HTTP endpoints or CLI tooling can respond without reimplementing aggregation logic. Transaction manager telemetry (active/commit/abort counters plus snapshot windows) alongside parser front-end metrics now rides through the same registry so diagnostics callers can correlate concurrency pressure, DDL parse failures, and downstream storage activity.
- **WAL reader runtime:** `WalReader` enumerates segment files, validates CRC32C checksums, and streams records across segment boundaries for recovery and tooling consumers while honouring on-disk alignment rules.
- **Recovery planning:** `WalRecoveryDriver` consumes `WalReader` streams, groups records by provisional transaction identifier, emits REDO/UNDO plans, and flags truncated tails so replay can halt cleanly on partial segments.
- **Recovery replay:** `WalReplayer` hydrates page images from `WalRecoveryPlan` redo entries, applies tuple inserts/updates/deletes idempotently, and prepares the buffer cache for crash restart simulations while refreshing free-space hints.
- **Page manager integration:** `PageManager` plans tuple inserts/deletes/updates, emits the corresponding WAL records first, then applies the in-memory mutation so LSNs stay chained, free-space tracking stays coherent, and page headers capture the latest LSN. Large tuples now spill across overflow pages with stub tuples, per-chunk WAL records, and page flags that guide recovery; replay refreshes `HasOverflow` automatically during redo/undo.

## Operational Guidance

### Diagnostics Entry Points

- **Telemetry snapshots:** Call `storage::collect_storage_diagnostics()` (see `storage_diagnostics.hpp`) to obtain a JSON blob containing WAL writer throughput, checkpoint cadence, retention pruning counts, vacuum activity, and transaction horizon gauges. The export now promotes `last_checkpoint_lsn` and `outstanding_replay_backlog_bytes` to top-level fields so operators can gauge restart readiness at a glance. Surface this through an HTTP endpoint or CLI command; consumers should treat missing fields as component-disabled rather than failure.

### Alert thresholds

Expose the OpenMetrics endpoint via `boredctl diagnostics metrics` to make the storage engine scrape-able. The following gauges/summary values are considered critical for day-to-day operations:

| Metric | Recommended threshold | Primary knobs |
| --- | --- | --- |
| `bored_checkpoint_lag_seconds` | Warn at ≥120s, critical at ≥300s. | Tighten `CheckpointScheduler::Config::min_interval`, lower `dirty_page_trigger`, or enable the `lsn_gap_trigger` to force checkpoints sooner. |
| `bored_wal_replay_backlog_bytes` | Warn once backlog exceeds two active segments, critical once it exceeds retention capacity. | Increase `WalRetentionConfig::retention_segments` or `retention_hours`, or force a checkpoint to advance pruning. |
| `recovery.total.last_redo_duration_ns` / `last_undo_duration_ns` (from `boredctl diagnostics capture`) | Alert when either exceeds expected RPO (e.g., >30s). | Reduce replay window by increasing checkpoint frequency, or provision faster storage for WAL/replay staging. |

Pair the metrics scrape with structured logs by launching the shell with `bored_shell --log-json /var/log/bored/sql.log`. Each command log entry carries a correlation id that can be joined with telemetry samples and the diagnostics export to trace long-running statements.
- **CLI capture:** Invoke `boredctl diagnostics capture --format json --summary` to emit a fresh snapshot to stdout; pass `--mock` during development builds to exercise formatting without a running storage runtime. The command writes JSON and a checkpoint/recovery/transaction summary suitable for incident tickets.
- **Point-in-time probes:** The `bored_tests` harness exercises `WalRecoveryDriver`, `WalReplayer`, and `WalUndoWalker` end-to-end. Re-run `ctest -R wal_.*` during incident response to validate replay primitives after code or configuration changes.
- **Log sampling:** Set `WalWriterConfig::telemetry_identifier` plus `WalWriterConfig::telemetry_registry` to register append/fail counters automatically. Pair this with `StorageTelemetryRegistry::snapshot()` to feed dashboards tracking write latency spikes and retention lag.
- **Filesystem health:** When retention stalls, invoke `WalRetentionManager::collect_metrics()` (exposed through the diagnostics collector) to inspect archived segment counts, prune durations, and oldest-active transaction ids that are blocking reclamation.

### Tuning Knobs

- **WAL durability:** Adjust `WalWriterConfig::buffer_size`, `size_flush_threshold`, and `time_flush_threshold` to balance commit latency vs. throughput. Use telemetry snapshots to confirm `durable_commit_lsn` keeps pace with peak workloads before tightening thresholds.
- **Retention policy:** Configure `WalRetentionConfig::retention_segments` for size-based pruning and `retention_hours` for time-based fallbacks. Operators running change-data-capture tooling should widen both windows until downstream lag consistently stays within their SLA. Archive staging paths are controlled via `archive_path` and can be rotated with daily cron jobs.
- **Checkpoint cadence:** `CheckpointSchedulerConfig::dirty_page_limit`, `lsn_gap_limit`, and `tick_interval` govern checkpoint frequency. When recovery time objectives (RTO) shrink, lower the LSN gap or tick interval while monitoring `checkpoint_scheduler_tests` derived metrics for increased flush pressure.
- **Vacuum pressure:** Tweak `VacuumWorkerConfig::batch_limit` and `VacuumBackgroundLoopConfig::tick_interval` when fragment counts remain high. Larger batches reduce amortised latch overhead, while shorter ticks favour latency-sensitive workloads at the cost of extra WAL churn.
- **Crash drills:** Schedule nightly invocations of the crash-replay harness (mirroring `tests/transaction_crash_recovery_integration_tests.cpp`) against production WAL archives. Combine with `WalRecoveryDriver::build_plan()` logging to catch truncated segments or mis-matched transaction horizons before they impact actual restart scenarios.

### Common Runbooks

1. **Investigate retention backlog:**
  1. Capture `storage_diagnostics` output and look for `oldest_active_transaction_id` covers that exceed SLA.
  2. If backlog stems from stalled checkpoints, increase the scheduler's `lsn_gap_limit` or request a forced checkpoint through the operator tooling.
  3. If backlog stems from long-running transactions, use the transaction operator guide to engage the owning sessions.
2. **Mitigate WAL flush latency spikes:**
  1. Inspect `WalWriter` telemetry for `flush_duration_ns` outliers.
  2. Temporarily raise `buffer_size` and `size_flush_threshold` to absorb bursty workloads, then monitor `durable_commit_lsn` lag.
  3. If macOS full-fsync is enabled, evaluate disabling `AsyncIoConfig::use_full_fsync` during low-risk windows to validate whether the platform layer is the bottleneck.
3. **Validate crash readiness:**
  1. Snapshot the active data directory and WAL segment set.
  2. Run the replay harness offline; the final fragment/tuple assertions must match the latest durable state.
  3. Archive the diagnostics artefacts alongside the drill ticket for regression tracking.

### Checkpoint & Recovery Troubleshooting Playbook

- **Confirm checkpoint health**
  1. Pull `storage::collect_storage_diagnostics()` and record `last_checkpoint_lsn`, `checkpoint_queue_depth`, `blocked_transactions`, and `outstanding_replay_backlog_bytes`.
  2. Compare `last_checkpoint_lsn` with the current durable commit horizon; if drift exceeds policy, flag the window in the incident ticket.
- **Diagnose failed checkpoints**
  1. Inspect checkpoint scheduler logs for recent `emit_failures`, `flush_failures`, or coordinator phase errors; correlate timestamps with telemetry counters.
  2. Re-run `ctest --output-on-failure -R checkpoint_scheduler` to confirm the control flow did not regress after recent changes.
  3. If failures persist, invoke `CheckpointScheduler::request_force_checkpoint()` (via operator tooling) and capture fresh telemetry snapshots plus WAL writer stats for escalation.
- **Triaging slow or stalled recovery**
  1. Review diagnostics for rising `outstanding_replay_backlog_bytes` or repeated `plan_failures`; if backlog grows, inspect retention directories for missing or corrupt segments.
  2. Execute `ctest --output-on-failure -R wal_recovery` and `-R wal_replay` locally to ensure the binaries can enumerate segments and replay plans with the current build.
  3. Run `bored_tests "Wal crash drill"` filters to validate crash drills against production WAL archives if available; store outputs with the incident record.
- **Escalation checklist**
  1. Attach telemetry JSON before and after mitigation, plus the most recent `CheckpointScheduler` and `WalRecoveryDriver` logs.
  2. Document operator actions (forced checkpoints, retention overrides, replay dry-runs) so follow-up teams can audit recovery state transitions.

### Redo/Undo State Flow

```mermaid
stateDiagram-v2
  [*] --> EnumerateSegments
  EnumerateSegments --> BuildPlan: WalReader -> WalRecoveryDriver
  BuildPlan -->{TruncatedTail}
  {TruncatedTail} --> HaltReplay: Flag truncated tail
  {TruncatedTail} --> ApplyRedo: No truncation detected
  ApplyRedo --> ApplyRecord: For each redo record
  ApplyRecord --> ApplyRecord: Next record
  ApplyRecord --> PrepareUndo: Redo complete
  PrepareUndo --> ApplyUndo: Active transactions remain
  PrepareUndo --> Finalize: No undo work
  ApplyUndo --> ApplyUndo: Next undo record
  ApplyUndo --> Finalize
  HaltReplay --> Finalize
  Finalize --> [*]
```

Redo records always run in log order to rebuild page images, while undo records (if any) walk surviving transactions in reverse order so partially applied changes are rolled back before restart.

### WAL Retention & Archival Policies

- **Checkpoint-based pruning:** After each checkpoint, retain all WAL segments newer than the persisted dirty-page table and the oldest active transaction.
- **Segment archival tiers:** Configure an archival directory for immutable segments; rotate files older than the retention window while keeping a sliding time-based cache onsite.
- **Crash drill cadence:** Schedule periodic recovery exercises that replay archived WAL onto a snapshot to validate retention assumptions and detect corrupt segments early.
- **Retention configuration:** `WalRetentionConfig` exposes `retention_segments`, `retention_hours`, and `archive_path` knobs so retention or archival behaviour is opt-in and automated after each flush.

## Asynchronous I/O Architecture

- **Interface skeleton:**
  ```cpp
  struct IoDescriptor {
    NativeHandle handle;
    std::uint64_t offset;
    FileClass file_class;
  };

  struct ReadRequest : IoDescriptor {
    std::byte* data;
    std::size_t size;
  };

  struct WriteRequest : IoDescriptor {
    const std::byte* data;
    std::size_t size;
    IoFlag flags;
  };

  struct IoResult {
    std::size_t bytes_transferred;
    std::error_code status;
  };

  class AsyncIo {
  public:
    virtual ~AsyncIo() = default;
    virtual std::future<IoResult> submit_read(ReadRequest request) = 0;
    virtual std::future<IoResult> submit_write(WriteRequest request) = 0;
    virtual std::future<IoResult> flush(FileClass target) = 0;  // fsync-equivalent for WAL/data files
  };
  ```
  Concrete `IoRingDispatcher` (Windows) and `IoUringDispatcher` (Linux) implementations translate descriptors into native SQE submissions and monitor CQEs to fulfil the returned futures/promises.
- **macOS backend:** `DispatchAsyncIo` binds the same interface to Grand Central Dispatch queues, throttles concurrency with semaphores, performs `pread`/`pwrite` under the covers, and elevates durability guarantees with either `fsync` or `fcntl(F_FULLFSYNC)` depending on `AsyncIoConfig::use_full_fsync` (default controllable via `BORED_STORAGE_PREFER_FULL_FSYNC`).
- **Factory selection:** `create_async_io()` inspects the `AsyncIoBackend` hint in `AsyncIoConfig` (`Auto`, `ThreadPool`, `WindowsIoRing`, `LinuxIoUring`). It instantiates the requested backend when available, otherwise falls back to the portable thread-pool implementation so the storage layer stays asynchronous on every platform today.
- **Threading model:** A dedicated dispatcher thread owns the submission/completion queues. Storage components (buffer manager, WAL writer, checkpoint worker) run on separate threads and await their futures. This enables non-blocking prefetching, batched WAL writes, and overlapping flushes.
- **Scheduling policy:**
  - Buffer manager issues asynchronous reads ahead of demand and tracks outstanding futures to maintain pin counts.
  - WAL writer coalesces sequential writes, requests durability after commit batches, and lets the dispatcher handle fsync semantics per `flush` calls.
  - Checkpointer and background cleaners enqueue page flushes without monopolising submission slots by respecting dispatcher-issued backpressure tokens.
- **Error propagation:** All completions map platform-specific status codes into `std::error_code`. Fatal errors surface through the associated futures, allowing higher layers to trigger crash-recovery sequences while ensuring the dispatcher drains outstanding I/O safely.

## Next Steps (Prioritised Backlog)

Deferred items covering benchmark automation and telemetry surfaces are tracked in `docs/storage_benchmark_telemetry_backlog.md`.

### Roadmap to 100% Feature Completeness (Updated Oct 23, 2025)

1. **Concurrency & Lock Manager Integration** (✅ Oct 23, 2025)
  - RAII latch callbacks now delegate to the storage lock manager; contention and propagation paths are verified through new unit tests.
2. **Checkpoint Scheduling & Retention Wiring** (✅ Oct 23, 2025)
  - `CheckpointScheduler` orchestrates WAL checkpoints from dirty page snapshots, flushes the writer, and invokes retention hooks after successful emits.
3. **Compaction & Index Metadata Logging** (✅ Oct 23, 2025)
  - `PageManager::compact_page` now emits `PageCompaction` records with relocation metadata and index refresh hints; `WalReplayer` replays and records the events for downstream index maintenance.
4. **Undo Walkers & Crash Drills** (✅ Oct 23, 2025)
  - ✅ Implement undo walker coverage for in-flight transactions, grouping overflow chains under the owning table page and exposing walk spans.
  - ✅ Add WAL-only restart tests that inspect page/header correctness using the new walker.
5. **Observability & Telemetry** (Following)
  - Capture and publish latch contention, checkpoint latency, and retention activity metrics for operators and regression dashboards.
6. **Benchmarking & Tooling** (Later)
  - Establish reproducible benchmarks for FSM refresh, overflow replay, and retention throughput; finalise operational runbooks and visual documentation.
- **Async IO backlog: macOS descriptor cache**
  - Extend `DispatchAsyncIo` with a small LRU of open descriptors/dispatch channels so repeated WAL appends skip `open`/`close`, defer `fsync`/`F_FULLFSYNC` to flush boundaries, and expose cache telemetry (hits/misses/evictions).
- **Deferred: Linux io_uring backend roadmap**
  - Target liburing 2.x to mirror the Windows IoRing dispatcher with queue-depth aware submission, per-file-class backpressure, and dsync-aware flush handling.
  - Translate Linux `io_uring_cqe::res` results into `std::error_code` values using `std::system_category()` while preserving errno semantics.
  - Provide runtime detection and a configuration guard (`AsyncIoBackend::LinuxIoUring`) so tests and production builds can opt-in explicitly.
  - Stub integration tests that are skipped unless the backend is available; this keeps CI green while the implementation is deferred.
  - Status: planning documented here; implementation intentionally deferred while the Windows IoRing backend stabilises.

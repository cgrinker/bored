# Storage Format Design

This document captures the first pass at the on-disk layout for the experimental relational database that lives in this repository. Two foundational building blocks are described: the fixed-size page format used by the main storage engine and the write-ahead log (WAL) format that guarantees durability and supports crash recovery.

### Progress Snapshot (Oct 23, 2025)

- **WAL pipeline 100%**: Writer, reader, recovery planning/replay, telemetry registry, checkpoint scheduler, retention manager, and compaction metadata logging are covered by Catch2 suites. Checkpoint cadence and retention pruning metrics now flow through the storage telemetry registry for diagnostics callers, and the new diagnostics collector exports aggregated snapshots for operators.
- **Storage pages ~99%**: Core page operations, compaction with WAL slot relocation metadata, free-space persistence, overflow tuple WAL emission, cached before-image logging for overflow chains, and the undo walker now feeds the replay path; remaining work targets crash drills, observability surface wiring, and benchmarking.

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
- **Telemetry registry:** `WalTelemetryRegistry` collects `WalWriterTelemetrySnapshot` samplers so admin tooling and diagnostics endpoints can surface append/flush metrics without coupling to writer internals. Configure `WalWriterConfig::telemetry_registry` + `telemetry_identifier` to auto-register writers and tear them down safely. `StorageTelemetryRegistry` now feeds a diagnostics collector that emits JSON for page managers, checkpoint schedulers, and retention workers.
- **Retention manager:** `WalRetentionManager` enforces `WalRetentionConfig` knobs (`retention_segments`, `retention_hours`, `archive_path`) after durable flushes, pruning or archiving old segments without ever touching the active writer segment. Retention pruning runs now report scan/prune statistics back through storage telemetry samplers when invoked by the scheduler.
- **Checkpoint manager:** `CheckpointManager` encodes dirty page tables and active transaction snapshots into `WalRecordType::Checkpoint` payloads so recovery can bootstrap redo horizons quickly while keeping the WAL append path consistent.
- **Page compaction record:** `WalRecordType::PageCompaction` captures slot relocation metadata, refreshed free space boundaries, and index maintenance flags so both online replay and background tooling can reconcile tuple layout changes without full page images.
- **Checkpoint scheduler:** `CheckpointScheduler` coordinates periodic checkpoint emission based on dirty-page/transaction pressure, flushes the WAL writer, and kicks retention policies once records are durable. Trigger counts, forced runs, and emission/flush/retention latencies are tracked per scheduler and exposed through `StorageTelemetryRegistry` samplers and diagnostics exports.

- **Diagnostics collector:** `storage_diagnostics.hpp` provides a lightweight collector that aggregates storage telemetry snapshots, optional per-component breakdowns, and renders them as JSON so HTTP endpoints or CLI tooling can respond without reimplementing aggregation logic.
- **WAL reader runtime:** `WalReader` enumerates segment files, validates CRC32C checksums, and streams records across segment boundaries for recovery and tooling consumers while honouring on-disk alignment rules.
- **Recovery planning:** `WalRecoveryDriver` consumes `WalReader` streams, groups records by provisional transaction identifier, emits REDO/UNDO plans, and flags truncated tails so replay can halt cleanly on partial segments.
- **Recovery replay:** `WalReplayer` hydrates page images from `WalRecoveryPlan` redo entries, applies tuple inserts/updates/deletes idempotently, and prepares the buffer cache for crash restart simulations while refreshing free-space hints.
- **Page manager integration:** `PageManager` plans tuple inserts/deletes/updates, emits the corresponding WAL records first, then applies the in-memory mutation so LSNs stay chained, free-space tracking stays coherent, and page headers capture the latest LSN. Large tuples now spill across overflow pages with stub tuples, per-chunk WAL records, and page flags that guide recovery; replay refreshes `HasOverflow` automatically during redo/undo.

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
- **Factory selection:** `create_async_io()` inspects the `AsyncIoBackend` hint in `AsyncIoConfig` (`Auto`, `ThreadPool`, `WindowsIoRing`, `LinuxIoUring`). It instantiates the requested backend when available, otherwise falls back to the portable thread-pool implementation so the storage layer stays asynchronous on every platform today.
- **Threading model:** A dedicated dispatcher thread owns the submission/completion queues. Storage components (buffer manager, WAL writer, checkpoint worker) run on separate threads and await their futures. This enables non-blocking prefetching, batched WAL writes, and overlapping flushes.
- **Scheduling policy:**
  - Buffer manager issues asynchronous reads ahead of demand and tracks outstanding futures to maintain pin counts.
  - WAL writer coalesces sequential writes, requests durability after commit batches, and lets the dispatcher handle fsync semantics per `flush` calls.
  - Checkpointer and background cleaners enqueue page flushes without monopolising submission slots by respecting dispatcher-issued backpressure tokens.
- **Error propagation:** All completions map platform-specific status codes into `std::error_code`. Fatal errors surface through the associated futures, allowing higher layers to trigger crash-recovery sequences while ensuring the dispatcher drains outstanding I/O safely.

## Next Steps (Prioritised Backlog)

1. Build crash/restart drills that exercise the new undo walker across overflow chains and validate before-image consistency on restart.
2. Benchmark FSM refresh, retention pruning, and overflow replay using representative workloads to establish performance baselines and regression thresholds.
3. Finalise operator-facing tooling and documentation for retention, checkpoint scheduling, and recovery workflows.

### Roadmap to 100% Feature Completeness (Updated Oct 23, 2025)

1. **Concurrency & Lock Manager Integration** (✅ Oct 23, 2025)
  - RAII latch callbacks now delegate to the storage lock manager; contention and propagation paths are verified through new unit tests.
2. **Checkpoint Scheduling & Retention Wiring** (✅ Oct 23, 2025)
  - `CheckpointScheduler` orchestrates WAL checkpoints from dirty page snapshots, flushes the writer, and invokes retention hooks after successful emits.
3. **Compaction & Index Metadata Logging** (✅ Oct 23, 2025)
  - `PageManager::compact_page` now emits `PageCompaction` records with relocation metadata and index refresh hints; `WalReplayer` replays and records the events for downstream index maintenance.
4. **Undo Walkers & Crash Drills** (In Progress)
  - ✅ Implement undo walker coverage for in-flight transactions, grouping overflow chains under the owning table page and exposing walk spans.
  - ☐ Add WAL-only restart tests that inspect page/header correctness using the new walker.
5. **Observability & Telemetry** (Following)
  - Capture and publish latch contention, checkpoint latency, and retention activity metrics for operators and regression dashboards.
6. **Benchmarking & Tooling** (Later)
  - Establish reproducible benchmarks for FSM refresh, overflow replay, and retention throughput; finalise operational runbooks and visual documentation.
- **Deferred: Linux io_uring backend roadmap**
  - Target liburing 2.x to mirror the Windows IoRing dispatcher with queue-depth aware submission, per-file-class backpressure, and dsync-aware flush handling.
  - Translate Linux `io_uring_cqe::res` results into `std::error_code` values using `std::system_category()` while preserving errno semantics.
  - Provide runtime detection and a configuration guard (`AsyncIoBackend::LinuxIoUring`) so tests and production builds can opt-in explicitly.
  - Stub integration tests that are skipped unless the backend is available; this keeps CI green while the implementation is deferred.
  - Status: planning documented here; implementation intentionally deferred while the Windows IoRing backend stabilises.

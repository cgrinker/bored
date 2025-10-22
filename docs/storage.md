# Storage Format Design

This document captures the first pass at the on-disk layout for the experimental relational database that lives in this repository. Two foundational building blocks are described: the fixed-size page format used by the main storage engine and the write-ahead log (WAL) format that guarantees durability and supports crash recovery.

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
- **Mutable helpers:** `page_operations.hpp` offers routines to initialise a page buffer, append tuples, reclaim slots, and read payloads while tracking the header metadata. These helpers will become latch-aware so concurrent readers and mutators can coordinate via lightweight locks.
- **Free space map:** `FreeSpaceMap` maintains bucketed page candidates keyed by contiguous free bytes and prefers unfragmented buffers during allocation.
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
- **WAL reader runtime:** `WalReader` enumerates segment files, validates CRC32C checksums, and streams records across segment boundaries for recovery and tooling consumers while honouring on-disk alignment rules.
- **Page manager integration:** `PageManager` plans tuple inserts/deletes/updates, emits the corresponding WAL records first, then applies the in-memory mutation so LSNs stay chained, free-space tracking stays coherent, and page headers capture the latest LSN.

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

## Next Steps

- Extend page compaction to emit slot relocation metadata for indexes, integrate with WAL archival / recycling processes, and communicate flushes through `AsyncIo`.
- Implement on-disk free-space map management to speed page allocation decisions and route reads/writes through the async dispatcher.
- Design index logging payloads (B-Tree page splits/merges) using the same WAL infrastructure and schedule their persistence via `AsyncIo` implementations.
- Prototype REDO/UNDO recovery drivers on top of `WalReader`, including truncated-tail handling and reapplication ordering semantics.
- **Deferred: Linux io_uring backend roadmap**
  - Target liburing 2.x to mirror the Windows IoRing dispatcher with queue-depth aware submission, per-file-class backpressure, and dsync-aware flush handling.
  - Translate Linux `io_uring_cqe::res` results into `std::error_code` values using `std::system_category()` while preserving errno semantics.
  - Provide runtime detection and a configuration guard (`AsyncIoBackend::LinuxIoUring`) so tests and production builds can opt-in explicitly.
  - Stub integration tests that are skipped unless the backend is available; this keeps CI green while the implementation is deferred.
  - Status: planning documented here; implementation intentionally deferred while the Windows IoRing backend stabilises.

# Page and WAL First-Pass Roadmap

This document tracks the remaining work required to take the current page manager and write-ahead log (WAL) scaffolding to a minimally viable state. Tasks are grouped by subsystem and roughly sequenced.

## Page Manager

1. **Free Space Map (FSM)**
   - ✅ Track free space at the page granularity (per relation) with bucketed lookups.
   - ✅ Provide O(1) lookup for pages with sufficient contiguous bytes while preferring unfragmented candidates.
   - ☐ Persist the FSM to disk and ensure it survives crash recovery.
   - ☐ Record WAL metadata for FSM updates so crash recovery can rebuild hints.

2. **Page Compaction / Defragmentation**
   - ✅ Implement vacuum/defragment routines that coalesce free space within a page by rewriting tuples and updating slots via `compact_page`.
   - ☐ Capture slot relocation metadata and feed it into index/WAL machinery when compaction runs.
   - ☐ Integrate with WAL to log page images or logical redo data for compaction.

3. **Overflow / Large Tuple Handling**
   - Support tuples that exceed a single page by chaining overflow pages.
   - Extend page flags and tuple metadata accordingly.
   - ☐ Provide WAL formats for overflow chain creation/truncation.

4. **Concurrency Hooks**
   - Define latching protocol for page access (shared/exclusive) compatible with asynchronous I/O completions.
   - Stub out lock manager interactions to prepare for multi-transaction workloads and cooperative scheduling.

5. **Diagnostic Instrumentation**
   - Add page dump utilities for debugging (hexdump + interpreted view of slots/tuples).
   - Track fragmentation and average free-space utilisation metrics.
   - ☐ Emit WAL append latency/throughput counters for tuning.

## Write-Ahead Log

1. **Log Sequencing & Buffers**
   - ✅ Maintain an in-memory log buffer with LSN allocation and latency-aware batching (`WalWriter`).
   - Implement flush policy hooks (interval, commit-driven, size-based) and feed durable writes through `AsyncIo`.

2. **Physical Page Records**
   - Finalise the `PageImage` and `PageDelta` record payloads.
   - Ensure page CRC is recomputed after applying WAL during recovery.
   - Integrate asynchronous write submission paths so redo operations reuse `AsyncIo` dispatchers.

3. **Logical Tuple Records**
   - ✅ Integrate tuple insert/delete payloads via `PageManager` so WAL commits precede page mutations.
   - ✅ Integrate tuple update payloads into the WAL writer and manager path.
   - Specify redo/undo semantics so logical operations can reproduce page changes.

4. **Checkpointing**
   - Define checkpoint record payload (snapshot of dirty page table + active transactions).
   - Implement a basic checkpoint writer that truncates/compresses WAL segments once durable.

5. **Segment Management**
   - ✅ Handle WAL segment creation, rollover, and alignment enforcement in `WalWriter`.
   - Add archival/deletion policies plus retention knobs.

6. **Recovery Workflow**
   - Outline REDO/UNDO passes using the log records defined above.
   - Prototype a recovery driver that replays committed transactions and rolls back incomplete ones while coordinating asynchronous reads/writes.
   - Introduce a WAL reader utility to iterate segment files and validate checksums before replay.

## Asynchronous I/O Layer

The `AsyncIo` abstraction now routes work through a portable thread-pool backend while platform adapters are being implemented.

1. **Unified Interface**
   - ✅ Design an `AsyncIo` abstraction that submits reads/writes and flushes via futures/promises (thread-pool fallback in place) with explicit `AsyncIoBackend` selection.
   - ✅ Ship the Windows `IoRingDispatcher` with queue-depth aware backpressure, dsync flushing, and graceful shutdown semantics.
   - ☐ *(Deferred)* Build the Linux `IoUringDispatcher` once the pending plan in `docs/storage.md` is ready to execute.

2. **Dispatcher Runtime**
   - ✅ Run submission/completion processing on dedicated threads; integrate with buffer manager, WAL writer, and checkpoint workers.
   - ✅ Implement backpressure so producers respect queue depth limits (queue-capacity blocking in thread-pool fallback).

3. **Error Handling & Telemetry**
   - Normalise error codes from both platforms and surface them through futures.
   - Collect per-operation latency/throughput metrics for tuning.

## Cross-Cutting Concerns

1. **CRC32C Hardware Detection Improvements**
   - ☐ Cache CPU feature detection results to avoid repeated checks.
   - ☐ Provide configuration knob to force software fallback for testing.

2. **Testing Strategy**
   - ☐ Build fuzz tests around page encoding/decoding and WAL payload parsing.
   - ☐ Create integration tests simulating crash/restart with a small dataset.

3. **Performance Benchmarks**
   - ☐ Micro-bench tuple append/delete and WAL append throughput.
   - ☐ Compare FSM lookup performance against naive linear search as features land.

4. **Documentation & Tooling**
   - ☐ Expand `docs/storage.md` with concrete record diagrams and state transition flows.
   - ☐ Generate schema diagrams for WAL record types using plantuml/mermaid for quick reference.

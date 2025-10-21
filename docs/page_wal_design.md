# Page and WAL First-Pass Roadmap

This document tracks the remaining work required to take the current page manager and write-ahead log (WAL) scaffolding to a minimally viable state. Tasks are grouped by subsystem and roughly sequenced.

## Page Manager

1. **Free Space Map (FSM)**
   - Track free space at the page granularity (per relation).
   - Provide O(1) lookup for pages with sufficient free bytes when inserting tuples.
   - Persist the FSM to disk and ensure it survives crash recovery.

2. **Page Compaction / Defragmentation**
   - Implement vacuum/defragment routines that coalesce free space within a page by rewriting tuples and updating slots. _(Prototype landed via `compact_page`; remaining work captures relocation metadata and WAL logging.)_
   - Integrate with WAL to log page images or logical redo data for compaction.

3. **Overflow / Large Tuple Handling**
   - Support tuples that exceed a single page by chaining overflow pages.
   - Extend page flags and tuple metadata accordingly.

4. **Concurrency Hooks**
   - Define latching protocol for page access (shared/exclusive).
   - Stub out lock manager interactions to prepare for multi-transaction workloads.

5. **Diagnostic Instrumentation**
   - Add page dump utilities for debugging (hexdump + interpreted view of slots/tuples).
   - Track fragmentation and average free-space utilisation metrics.

## Write-Ahead Log

1. **Log Sequencing & Buffers**
   - Maintain an in-memory log buffer with LSN allocation.
   - Implement flush policy hooks (interval, commit-driven, size-based).

2. **Physical Page Records**
   - Finalise the `PageImage` and `PageDelta` record payloads.
   - Ensure page CRC is recomputed after applying WAL during recovery.

3. **Logical Tuple Records**
   - Integrate the current tuple insert/delete/update payloads into the WAL writer.
   - Specify redo/undo semantics so logical operations can reproduce page changes.

4. **Checkpointing**
   - Define checkpoint record payload (snapshot of dirty page table + active transactions).
   - Implement a basic checkpoint writer that truncates/compresses WAL segments once durable.

5. **Segment Management**
   - Handle WAL segment creation, rollover, and archival/deletion policies.
   - Verify segment headers and enforce alignment when streaming to disk.

6. **Recovery Workflow**
   - Outline REDO/UNDO passes using the log records defined above.
   - Prototype a recovery driver that replays committed transactions and rolls back incomplete ones.

## Cross-Cutting Concerns

1. **CRC32C Hardware Detection Improvements**
   - Cache CPU feature detection results to avoid repeated checks.
   - Provide configuration knob to force software fallback for testing.

2. **Testing Strategy**
   - Build fuzz tests around page encoding/decoding and WAL payload parsing.
   - Create integration tests simulating crash/restart with a small dataset.

3. **Performance Benchmarks**
   - Micro-bench tuple append/delete and WAL append throughput.
   - Compare FSM lookup performance against naive linear search as features land.

4. **Documentation & Tooling**
   - Expand `docs/storage.md` with concrete record diagrams and state transition flows.
   - Generate schema diagrams for WAL record types using plantuml/mermaid for quick reference.

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
- **Mutable helpers:** `page_operations.hpp` offers routines to initialise a page buffer, append tuples, reclaim slots, and read payloads while tracking the header metadata.
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

## Next Steps

- Extend page compaction to emit slot relocation metadata for indexes and integrate with WAL archival / recycling processes.
- Implement on-disk free-space map management to speed page allocation decisions.
- Design index logging payloads (B-Tree page splits/merges) using the same WAL infrastructure.

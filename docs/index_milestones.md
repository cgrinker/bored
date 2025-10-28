# Index Management Milestones

## Milestone 0: Storage Formats & Catalog Plumbing
- [x] Finalise B+Tree page header/layout schema and version gates. (_Spec drafted in [index_btree_layout.md](index_btree_layout.md); structs live in `include/bored/storage/index_btree_page.hpp`_) 
- [x] Add index metadata descriptors (root page, fanout, comparator) to catalog bootstrap and DDL flows.
- [x] Introduce WAL record shapes for index page splits/merges and bulk build checkpoints.

## Milestone 1: Core CRUD & Recovery
- [ ] Implement index insert/delete/update operations with latch discipline and TempResourceRegistry hooks for scratch space.
- [ ] Extend executor scan operators with index probes and fallback to heap when predicates miss.
- [ ] Teach `WalRecoveryDriver`/`WalReplayer` to reconstruct B+Tree structure, including split replay and orphan cleanup.

## Milestone 2: Maintenance & Telemetry
- [ ] Build background compaction/prune routines plus checkpoint integration for index retention windows.
- [ ] Surface per-index telemetry (build durations, probe latency, split rate) through `StorageTelemetryRegistry` and diagnostics JSON.
- [ ] Add Catch2 integration coverage for mixed heap/index workloads across crash/restart drills.

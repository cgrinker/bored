# Index Management Milestones

## Milestone 0: Storage Formats & Catalog Plumbing
- [x] Finalise B+Tree page header/layout schema and version gates. (_Spec drafted in [index_btree_layout.md](index_btree_layout.md); structs live in `include/bored/storage/index_btree_page.hpp`_) 
- [x] Add index metadata descriptors (root page, fanout, comparator) to catalog bootstrap and DDL flows.
- [x] Introduce WAL record shapes for index page splits/merges and bulk build checkpoints.

## Milestone 1: Core CRUD & Recovery
- [x] Implement index insert/delete/update operations with latch discipline and TempResourceRegistry hooks for scratch space.
- [x] Extend executor scan operators with index probes and fallback to heap when predicates miss.
- [x] Teach `WalRecoveryDriver`/`WalReplayer` to reconstruct B+Tree structure, including split replay and orphan cleanup.

## Milestone 2: Maintenance & Telemetry
- [x] Build background compaction/prune routines plus checkpoint integration for index retention windows.
	- [x] Added `IndexRetentionManager` with checkpoint wiring and telemetry registration hooks.
	- [x] Introduced `IndexRetentionPruner` with default runtime wiring and Catch2 coverage for checkpoint-driven dispatch.
	- [x] Phase 1: Land default retention executor scaffolding (runtime wiring + stub prune callback) so checkpoints can drive background work without custom hooks.
	- [x] Phase 2: Implemented the prune executor so retention runs enumerate catalog descriptors, scan B+Tree leaves, and rewrite tuple pointers using compaction metadata while reporting executor telemetry.
	- [x] Phase 3: Added integration tests that compact table pages, schedule retention, and verify index pages are rewritten across crash/restart drills.
- [ ] Surface per-index telemetry (build durations, probe latency, split rate) through `StorageTelemetryRegistry` and diagnostics JSON.
- [ ] Add Catch2 integration coverage for mixed heap/index workloads across crash/restart drills.

 - [x] Verify that the copilot-instructions.md file in the .github directory is created. (created initial checklist)

 - [x] Clarify Project Requirements (user wants C++ project using vcpkg)

 - [x] Scaffold the Project (generated CMake/vcpkg layout manually after project setup query)

 - [x] Customize the Project (core greeter library plus storage page/WAL design scaffolding, CRC32C + tuple WAL payload helpers)

 - [x] Install Required Extensions (no extensions recommended)

 - [x] Compile the Project (configured with CMake + vcpkg, built Release, ran tests)

 - [x] Create and Run Task (added cmake-configure task via tasks.json)

 - [x] Launch the Project (validated Release binary output; ready to add debug config on request)

- [x] Ensure Documentation is Complete (README updated; removed inline checklist comments)
- Work through each checklist item systematically.
- Keep communication concise and focused.
- Follow development best practices.
- For any file exceeding 500 lines or 25 KB, apply edits exclusively via the `apply_patch` workflow (no direct-buffer writes).

## Current State (Oct 23, 2025)
- `WalWriter` provides aligned WAL buffering, segment rotation, and size/time/commit-driven flush hooks; endian-stable headers verified by tests.
- `WalTelemetryRegistry` aggregates per-writer telemetry snapshots for diagnostics surfaces.
- `WalReader` enumerates segments, validates checksums, and streams records across segment boundaries with Catch2 coverage.
- `WalRecoveryDriver` clusters WAL records by provisional transaction id, produces REDO/UNDO plans, and flags truncated tails with tests.
- `WalUndoWalker` groups undo spans by owning page, collects overflow dependencies, and prepares crash recovery orchestration hooks.
- `WalReplayer` rehydrates pages from recovery plans, applies tuple inserts/updates/deletes, and now consumes undo walker spans to pre-touch table and overflow pages before undo.
- `WalRetentionManager` enforces retention/archival knobs to keep WAL directories bounded without touching the active segment.
- `FreeSpaceMapPersistence` snapshots FSM hints to disk; `WalReplayContext` reloads and refreshes them during crash recovery.
- `PageManager` plans tuple inserts/deletes/updates, emits WAL records ahead of page mutations, and keeps page headers/free-space map LSNs consistent.
- `StorageTelemetryRegistry` now aggregates latch waits from page managers plus checkpoint cadence and retention pruning telemetry emitted by `CheckpointScheduler` samplers.
- `storage_diagnostics.hpp` collects aggregated storage telemetry snapshots and serialises them to JSON for diagnostics surfaces.
- Catch2 suites (`wal_writer_tests.cpp`, `wal_reader_tests.cpp`, `wal_recovery_tests.cpp`, `page_manager_tests.cpp`) parse emitted segments to confirm header chaining, payload encoding, truncated tail detection, and delete/update linkages.
- Docs updated (`docs/storage.md`, `docs/page_wal_design.md`) to reflect completed WAL sequencing milestones and new TODOs.
- Progress snapshot: WAL pipeline 100% complete; storage page roadmap ~65% complete.

## Next Tasks
1. Build crash/restart drills that exercise the undo walker across overflow chains and validate before-image consistency on restart.
2. Benchmark FSM refresh, retention pruning, and overflow replay using representative workloads to establish performance baselines and regression thresholds.
3. Finalise operator-facing tooling and documentation for retention, checkpoint scheduling, and recovery workflows.

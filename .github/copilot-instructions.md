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

## Current State (Oct 22, 2025)
- `WalWriter` provides aligned WAL buffering, segment rotation, and size/time/commit-driven flush hooks; endian-stable headers verified by tests.
- `WalReader` enumerates segments, validates checksums, and streams records across segment boundaries with Catch2 coverage.
- `WalRecoveryDriver` clusters WAL records by provisional transaction id, produces REDO/UNDO plans, and flags truncated tails with tests.
- `WalRecoveryDriver` sketches REDO/UNDO planning, groups records by provisional transaction id, and flags truncated tails; truncated segment handling now tested.
- `PageManager` plans tuple inserts/deletes/updates, emits WAL records ahead of page mutations, and keeps page headers/free-space map LSNs consistent.
- Catch2 suites (`wal_writer_tests.cpp`, `wal_reader_tests.cpp`, `wal_recovery_tests.cpp`, `page_manager_tests.cpp`) parse emitted segments to confirm header chaining, payload encoding, truncated tail detection, and delete/update linkages.
- Docs updated (`docs/storage.md`, `docs/page_wal_design.md`) to reflect completed WAL sequencing milestones and new TODOs.

## Next Tasks
1. Integrate `WalRecoveryDriver` plans with page replay primitives and add crash/restart integration tests.
2. Extend docs with redo/undo state diagrams plus WAL retention/archival policies.
3. Design WAL/FSM telemetry (append latency, hint journaling) to guide future instrumentation.

# Checkpoint & Recovery Coordination Milestones

## Context
The checkpoint and recovery coordination layer integrates relational components with the existing WAL, retention, and recovery infrastructure. These milestones extend the current storage engine by defining coordinated barriers, capturing consistent snapshots, and validating end-to-end restart behaviour across catalog, data, and index structures.

## Milestone 0: Coordination Skeleton
- **Goals:** Establish the coordination APIs between the checkpoint scheduler, transaction manager, and WAL retention manager.
- **Key Tasks:**
  - [x] Define a checkpoint contract that exposes begin/prepare/commit phases to participating subsystems.
  - [x] Surface transaction manager snapshot guards that block new writers during checkpoint prepare.
  - [x] Teach WAL retention to pin active segments until checkpoint commit acknowledgement.
  - [x] Emit dry-run telemetry that captures begin/prepare/commit readiness without mutating pages.
- **Exit Criteria:**
  - [x] Dry-run checkpoint cycles through all phases without mutating pages.
  - [x] Structured telemetry exported for coordinator readiness and phase durations.
- **Status:** Complete — coordinator contract, transaction checkpoint fence, retention pins, and telemetry-covered dry-run path available.

## Milestone 1: Catalog & Data Page Snapshots
- **Goals:** Capture consistent catalog and heap page images during checkpoints and restore them on restart.
- **Key Tasks:**
  - [x] Wire catalog bootstrapper and page manager to the coordination contract to flush dirty catalog and heap pages.
  - [x] Persist transaction-visible catalog snapshots alongside checkpoint metadata.
  - [x] Update recovery driver to hydrate catalog and heap pages from the most recent checkpoint before replaying WAL.
- **Exit Criteria:**
  - [x] End-to-end test demonstrates catalog mutations committed before the checkpoint survive restart without replaying WAL past the checkpoint LSN.
- **Status:** Complete — catalog bootstrapper and page manager feed checkpoint snapshots into `CheckpointImageStore`, recovery plans hydrate them prior to redo, and restart drills confirm catalog mutations survive without replaying beyond the checkpoint LSN.

## Milestone 2: Index Integration
- **Goals:** Extend checkpoint capture and recovery to cover B+Tree index segments and associated WAL ownership metadata.
- **Key Tasks:**
  - [x] Teach index maintenance operators to register dirty index pages with the checkpoint coordinator.
  - [x] Capture per-index high-water marks for WAL ownership so retention pruning honours index replay requirements.
  - [x] Rehydrate index structures from checkpoint images, falling back to WAL replay for tail segments as needed.
- **Exit Criteria:**
  - [x] Crash-restart drill with mixed heap/index workload recovers to checkpoint state, including secondary index integrity.
- **Status:** Complete — `IndexBtreeManager` publishes dirty page and ownership LSNs into `IndexCheckpointRegistry`, recovery hydrates checkpointed index pages and metadata, and `Wal crash drill rehydrates checkpointed heap and index pages` confirms mixed workloads restart cleanly with index high-water constraints enforced.

## Milestone 3: Incremental & Concurrent Checkpoints
- **Goals:** Support incremental checkpoints and overlap checkpoint preparation with ongoing workload under isolation guarantees.
- **Key Tasks:**
  - [x] Implement dirty-page tracking to minimise checkpoint write volume.
  - [x] Introduce throttling and scheduling policies so checkpoint and foreground workload share IO budget.
  - [x] Validate concurrent execution by running synthetic workloads during checkpoint prepare/commit phases.
- **Exit Criteria:**
  - [x] Telemetry reports reduced checkpoint duration and bounded foreground latency impact under load-driven tests.

**Current Status:** A shared `CheckpointPageRegistry` records highest-LSN heap page mutations alongside index metadata, `CheckpointScheduler` enforces a token-bucket IO budget with telemetry, simulated concurrency drills block/resume transactions around checkpoint prepare/commit, and load-driven tests capture checkpoint/fence duration telemetry to validate bounded impact.

## Milestone 4: Recovery Telemetry & Diagnostics
- **Goals:** Provide visibility into checkpoint lag, recovery progress, and failure scenarios for operators.
- **Key Tasks:**
  - [ ] Emit telemetry for checkpoint queue depth, blocked transactions, and recovery phase timing through `StorageTelemetryRegistry`.
  - [ ] Expand diagnostics surfaces to record last successful checkpoint LSN and outstanding replay backlog.
  - [ ] Document playbook for troubleshooting failed checkpoints and slow recovery in `docs/storage.md`.
- **Exit Criteria:**
  - [ ] Integration tests verify telemetry emission, and documentation covers operator workflows for monitoring and remediation.

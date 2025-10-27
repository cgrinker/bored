# Transaction & Concurrency Control Milestones (MVCC)

## Objectives
- Establish a transaction manager that enforces MVCC semantics across catalog and data pages.
- Provide transaction lifecycle APIs (begin/commit/abort) with WAL-backed durability and crash recovery hooks.
- Coordinate locking, latching, and retention policies so concurrent readers and writers observe consistent snapshots.
- Surface telemetry and diagnostics for transaction throughput, conflicts, and long-running blockers.

## Milestone 0: Concurrency Blueprint & API Contracts (0.5 sprint)
- [x] Produce an MVCC design brief covering isolation level guarantees (snapshot isolation target) and conflict resolution rules.
- [x] Define transaction state machine and lifecycle events (`begin`, `prepare`, `commit`, `abort`, `cleanup`).
- [x] Sketch public APIs for `TransactionManager`, `TransactionContext`, and latch/lock acquisition helpers consumed by the planner/executor.
- [x] Document catalog visibility rules (active, committed, aborted) tied to transaction/version timestamps.
- [x] Align retention and checkpoint requirements with the chosen MVCC window (oldest active transaction tracking).

## Milestone 1: Transaction Identity & Visibility Plumbing (1 sprint)
- [x] Introduce `TxnId` generator backed by WAL/LSN monotonicity and persist allocator state across checkpoints.
- [x] Wire catalog lookups to accept a snapshot descriptor (`TxnSnapshot`) and filter entries according to MVCC visibility rules.
- [x] Extend WAL commit records with transaction metadata (txn id, commit timestamp/LSN) for recovery replay.
- [x] Capture oldest-active/oldest-commit LSNs for retention management and checkpoint coordination.
- [x] Add telemetry counters for active transactions, commits, aborts, and snapshot age.

## Milestone 2: MVCC Tuple Versioning & Buffer Integration (1.5 sprints)
- [x] Extend page and tuple headers with creation/deletion transaction identifiers plus `undo` linkage for version chains (tuple headers now persisted alongside payloads and mirrored into WAL records).
- [x] Update `PageManager` mutation paths to emit MVCC-aware WAL records and maintain in-page version chains (tuple header wiring landed; undo chain population remains TODO).
- [x] Teach `WalReplayer` and `WalUndoWalker` to respect MVCC metadata when rehydrating and undoing tuples.
- [x] Introduce vacuum-style background task scaffolding to prune committed obsolete versions once safe (scheduler, worker, and background loop now integrated with telemetry and retry semantics).
	- [x] Add a `VacuumScheduler` with safe-horizon dispatch, deduplicated queues, and telemetry hooks.
	- [x] Run `VacuumWorker` inside a background loop with force-run pokes and telemetry surfacing for operators.
- [x] Expand unit tests to cover concurrent insert/update/delete visibility across transaction snapshots (catalog accessor suite now exercises pending/committed version chains).

**Next Task:** Back the commit pipeline with `WalWriter` so commit tickets flush to durability and retention/checkpoint hooks advance.

## Milestone 3: Locking, Latching, and Conflict Detection (1 sprint)
- [x] Implement row/page-intent lock hierarchy with deadlock detection or timeout policy (initial timeout policy shipped; deadlock detector hooks retained for follow-up tuning).
- [x] Integrate lock acquisition with `TransactionContext` so planner/executor operators request latches consistently (planner uses contextual guards to sequence row/table intent).
- [x] Ensure `WalRetentionManager` and checkpoint flows consult lock/txn state before pruning WAL segments (lock snapshots consulted prior to retention decisions).
- [x] Emit diagnostics for lock waits, deadlocks, and blocked writers (feeding `StorageTelemetryRegistry`).
- [x] Provide stress tests simulating conflicting writers/readers to validate lock ordering and fairness.

## Milestone 4: Commit, Abort, and Recovery Validation (1 sprint)
- [ ] Implement commit protocol that flushes WAL, advances durability ACK, and publishes snapshot visibility atomically (commit pipeline scaffolding landed; WAL writer integration pending).
- [ ] Add abort path that walks undo chains, restores tuples, and releases locks deterministically.
- [ ] Extend `WalRecoveryDriver` to rebuild in-flight transactions, reapply commits, and rollback incomplete operations.
- [ ] Capture integration tests that crash/restart during commit/abort windows and verify MVCC invariants.
- [ ] Document operational guidance (diagnostics, tuning knobs) in `docs/storage.md` and new transaction operator guide.

## Deliverables & Exit Criteria
- Transaction manager APIs landed with MVCC semantics validated by unit and crash-recovery integration tests.
- Planner/executor scaffolding can acquire snapshots and transaction contexts for statement execution.
- WAL, retention, and checkpoint subsystems observe transaction boundaries and MVCC windows.
- Telemetry surfaces expose transaction throughput, conflict rates, and oldest-active lag for operators.

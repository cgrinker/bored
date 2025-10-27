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
- [ ] Introduce `TxnId` generator backed by WAL/LSN monotonicity and persist allocator state across checkpoints.
- [ ] Wire catalog lookups to accept a snapshot descriptor (`TxnSnapshot`) and filter entries according to MVCC visibility rules.
- [x] Extend WAL commit records with transaction metadata (txn id, commit timestamp/LSN) for recovery replay.
- [ ] Capture oldest-active/oldest-commit LSNs for retention management and checkpoint coordination.
- [ ] Add telemetry counters for active transactions, commits, aborts, and snapshot age.

## Milestone 2: MVCC Tuple Versioning & Buffer Integration (1.5 sprints)
- [ ] Extend page and tuple headers with creation/deletion transaction identifiers plus `undo` linkage for version chains.
- [ ] Update `PageManager` mutation paths to emit MVCC-aware WAL records and maintain in-page version chains.
- [ ] Teach `WalReplayer` and `WalUndoWalker` to respect MVCC metadata when rehydrating and undoing tuples.
- [ ] Introduce vacuum-style background task scaffolding to prune committed obsolete versions once safe.
- [ ] Expand unit tests to cover concurrent insert/update/delete visibility across transaction snapshots.

## Milestone 3: Locking, Latching, and Conflict Detection (1 sprint)
- [ ] Implement row/page-intent lock hierarchy with deadlock detection or timeout policy.
- [ ] Integrate lock acquisition with `TransactionContext` so planner/executor operators request latches consistently.
- [ ] Ensure `WalRetentionManager` and checkpoint flows consult lock/txn state before pruning WAL segments.
- [ ] Emit diagnostics for lock waits, deadlocks, and blocked writers (feeding `StorageTelemetryRegistry`).
- [ ] Provide stress tests simulating conflicting writers/readers to validate lock ordering and fairness.

## Milestone 4: Commit, Abort, and Recovery Validation (1 sprint)
- [ ] Implement commit protocol that flushes WAL, advances durability ACK, and publishes snapshot visibility atomically.
- [ ] Add abort path that walks undo chains, restores tuples, and releases locks deterministically.
- [ ] Extend `WalRecoveryDriver` to rebuild in-flight transactions, reapply commits, and rollback incomplete operations.
- [ ] Capture integration tests that crash/restart during commit/abort windows and verify MVCC invariants.
- [ ] Document operational guidance (diagnostics, tuning knobs) in `docs/storage.md` and new transaction operator guide.

## Deliverables & Exit Criteria
- Transaction manager APIs landed with MVCC semantics validated by unit and crash-recovery integration tests.
- Planner/executor scaffolding can acquire snapshots and transaction contexts for statement execution.
- WAL, retention, and checkpoint subsystems observe transaction boundaries and MVCC windows.
- Telemetry surfaces expose transaction throughput, conflict rates, and oldest-active lag for operators.

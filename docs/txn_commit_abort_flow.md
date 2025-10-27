# Transaction Commit & Abort Flow

This note captures the current control flow for milestone 4 now that the WAL-backed commit pipeline is live. It focuses on the state transitions managed by `TransactionManager`, how `WalCommitPipeline` stages and flushes commit records through `WalWriter`, and where durability acknowledgements surface back to the caller.

## State Machine

```
Idle -> Active -> Preparing -> FlushingWal -> Durable -> Publishing -> Committed
                     |             |             |                |
                     |             |             |                -> AbortCleanup -> Aborted
                     |             |             -> AbortCleanup -> Aborted
                     |             -> AbortCleanup -> Aborted
                     -> AbortCleanup -> Aborted
```

- **Active**: Transaction holds a snapshot and may emit WAL records. Locks/latches are tracked on the context.
- **Preparing**: Statement execution is complete. The manager freezes mutation streams, ensures the WAL writer has seen all pending records, and collects commit callbacks.
- **FlushingWal**: A flush ticket is issued to `WalWriter`. The transaction waits for durable acknowledgement at `CommitSequence` granularity.
- **Durable**: The WAL writer signals that every record up to the transaction's commit LSN is durable. This is the point where crash recovery must replay the commit.
- **Publishing**: The context publishes the new visibility window (updates global xmin/xmax, snapshots, telemetry) and releases write locks.
- **Committed**: Final state; commit callbacks fire after locks are released and telemetry updated.
- **AbortCleanup**: Used for both explicit aborts and failure paths during prepare/flush. Undo chains are walked, locks released, and abort callbacks invoked.

`TransactionState` will gain `FlushingWal`, `Durable`, `Publishing`, and `AbortCleanup`. Existing code will continue to accept `Active`/`Preparing` while the new states are introduced; future milestones will update logic to walk the full sequence.

## Commit Path Ordering

1. **Prepare**
   - Transition `Active -> Preparing` under the manager mutex.
   - Stop new WAL append registration for the context, refresh the snapshot with the latest durable commit LSN, and freeze the mutation queue.
2. **Stage Commit Record**
   - Acquire the next WAL LSN from the writer and stamp the transaction's commit metadata (`txn id`, `next txn`, `oldest active`).
   - Use `WalWriter::stage_commit_record` to encode the commit payload while deferring the flush; the staged append tracks buffer offsets so a later rollback can rewind safely.
3. **Flush**
   - Move to `FlushingWal` and invoke `WalCommitPipeline::flush_commit`, which issues `WalWriter::flush()` to persist everything buffered up to (and including) the staged commit record.
4. **Durable Ack**
   - On flush completion transition to `Durable`.
   - `WalCommitPipeline::confirm_commit` fires durability hooks which now feed a shared `WalDurabilityHorizon`; retention and checkpoint schedulers consult the horizon to advance pruning windows safely.
   - `TransactionManager::update_durable_commit_lsn` persists the confirmed `CommitSequence` so snapshot builders reuse the newest durable horizon.
5. **Publish**
   - Transition to `Publishing`, update global xmin/xmax, clear from snapshot machinery, release locks.
6. **Complete**
   - Enter `Committed`, increment telemetry, fire commit callbacks, discard undo registrations.

## Abort Path Ordering

1. Ensure the context is in `Active`, `Preparing`, or `FlushingWal`. If already `Durable` or `Publishing`, escalate to crash recovery as the commit became durable.
2. Transition to `AbortCleanup` and invoke undo walkers for each registered mutation.
3. Release locks, unregister WAL intents (future hook: emit abort record if commit record queued).
4. Advance telemetry counters, fire abort callbacks, and move to `Aborted`.

## Durability Interfaces

- **Commit Staging**: `WalCommitPipeline::prepare_commit` captures `CommitRequest` metadata, stamps a `WalCommitHeader`, and calls `WalWriter::stage_commit_record`. The returned `CommitTicket` exposes the commit LSN (`CommitSequence`) while the pipeline retains rollback state.
- **Flush Path**: `WalCommitPipeline::flush_commit` synchronously invokes `WalWriter::flush()`. Failures trigger `rollback_commit`, which rewinds the staged append before surfacing the error back to `TransactionManager`.
- **Durability Hooks**: `WalCommitPipeline::confirm_commit` releases the pending entry and hands the `CommitTicket`, `WalCommitHeader`, and `WalAppendResult` to any registered hook. The default hook advances a shared `WalDurabilityHorizon` so higher layers (retention, checkpoint scheduler) can observe the new commit and oldest-active horizons once the transaction reaches `Publishing`.
- **Txn Context Integration**: `TransactionContext` caches the `CommitTicket` during `FlushingWal` and clears it once the manager enters `Publishing`. `TransactionManager` stores the durable `CommitSequence` for later snapshots via `update_durable_commit_lsn`, and callbacks registered through `TransactionContext::on_commit` now run only after durability confirmation.

## Recovery Implications

- Commit records remain the truth for replay ordering. `WalRecoveryDriver` will treat any durable commit record with absent abort markers as committed.
- In-flight transactions without durable commit records are candidates for rollback; on restart they will be undone using the undo chain metadata captured in milestone 2.

## Next Steps

1. Add crash/abort integration tests that inject failures between `FlushingWal` and `Publishing` to validate staged rollback and undo sequencing.
2. Refresh operator documentation (`docs/storage.md`, transaction operator guide) to highlight the durability horizon telemetry and its diagnostics surfaces.
3. Expose the durability horizon snapshot through CLI/HTTP tooling so operators can alert on stalled commit windows.

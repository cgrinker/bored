# Transaction Commit & Abort Flow

This note captures the intended control flow for milestone 4 before wiring the code paths. It focuses on the state transitions managed by `TransactionManager`, how the commit pipeline coordinates with the WAL writer, and where durability acknowledgements surface back to the caller.

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
   - Stop new WAL append registration for the context and freeze the mutation queue.
2. **Assign LSN**
   - Reserve a `CommitSequence` from the WAL writer (monotonic per commit).
   - Stamp the transaction's in-flight commit record and queue it with the writer.
3. **Flush**
   - Move to `FlushingWal` and invoke `WalWriter::flush_to(commit_lsn)`.
   - Wait for the writer's durability future/ticket to resolve.
4. **Durable Ack**
   - On flush completion transition to `Durable`.
   - Notify `WalRetentionManager` and checkpoint scheduler that commit horizon advanced.
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

- **Commit Sequence Tickets**: `WalWriter` will expose `acquire_commit_slot()` returning `(CommitSequence, WalFlushTicket)`.
- **Flush Ticket**: Awaitable/`std::future` signalling when the WAL has persisted every record up to the commit sequence.
- **Txn Context Integration**: `TransactionContext` holds the ticket during `FlushingWal` and swaps it into commit callbacks for extensions that need durability confirmation.

## Recovery Implications

- Commit records remain the truth for replay ordering. `WalRecoveryDriver` will treat any durable commit record with absent abort markers as committed.
- In-flight transactions without durable commit records are candidates for rollback; on restart they will be undone using the undo chain metadata captured in milestone 2.

## Next Steps

1. Extend `TransactionState` enum and transaction context storage to carry the new states and commit ticket.
2. Teach `TransactionManager::commit/abort` to walk the state machine and coordinate with `WalWriter` + undo walkers.
3. Add integration tests that inject failures between state transitions to validate durability and abort semantics.
4. Refresh operator documentation (`docs/storage.md`) with commit/abort telemetry and operational guidance once the code path is live.

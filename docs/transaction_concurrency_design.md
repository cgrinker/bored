# Transaction & Concurrency Control Design (MVCC Blueprint)

## Scope & Goals
- Deliver snapshot isolation with read-only and read-write transactions sharing the same infrastructure.
- Support catalog and heap access under MVCC rules so metadata and data obey identical visibility guarantees.
- Integrate with existing WAL, retention, and recovery components without introducing global blocking points.
- Expose observability hooks so operators can monitor throughput, conflicts, and oldest-active lag.

## Isolation Model
- **Target:** Snapshot isolation for read-write transactions, with degraded read committed mode available for internal maintenance tasks.
- **Serialization Source:** WAL LSNs provide a monotonically increasing timeline used for commit ordering and snapshot version stamps.
- **Write Skew:** Deferred to future enhancements (serializable upgrade). Snapshot isolation allows write skew; detection belongs to later milestones.

## Timeline & Identifier Primitives
- `TxnId`: 64-bit monotonically increasing identifier allocated by the transaction manager. Persist allocator high-water mark in WAL so restart resumes safely.
- `CommitSeq`: Alias for WAL LSN at commit flush time. Serves as the visibility boundary for newly committed tuples.
- `TxnSnapshot`: Struct bundling `read_lsn`, `min_active_txn`, and optional safe horizon for garbage collection. Used by catalog and heap readers to filter versions.
- `TxnTimestamp`: Logical wall clock derived from `CommitSeq` for diagnostics (exposed but not used for ordering).

## Lifecycle & State Machine
States tracked per transaction context:
- `Idle`: Context constructed, not yet begun.
- `Active`: `begin()` assigned `TxnId`, snapshot captured, locks eligible.
- `Preparing`: Commit requested; WAL flush in progress; new locks disallowed.
- `Committed`: Commit WAL durable, locks released, visibility published.
- `Aborted`: Rollback performed, undo chains restored, locks released.
- `Cleaning`: Deferred cleanup (vacuum) handling obsolete versions; system background state.

Transitions:
- `Idle -> Active`: `begin()` or `begin_read_only()`.
- `Active -> Preparing`: `commit()` invoked after validation.
- `Preparing -> Committed`: WAL flush acknowledgment + catalog visibility update.
- `Preparing -> Aborted`: WAL flush failure or validation error.
- `Active -> Aborted`: Explicit abort, conflict, or statement-level failure trigger.

## API Sketches
```cpp
struct TxnOptions {
    bool read_only = false;
    bool deferrable = false; // allow waiting for safe snapshot
};

class TransactionContext {
public:
    TxnId id() const noexcept;
    const TxnSnapshot& snapshot() const noexcept;
    void register_lock(LockHandle handle);
    void on_commit(std::function<void()> callback);
    void on_abort(std::function<void()> callback);
};

class TransactionManager {
public:
    TransactionContext begin(const TxnOptions& = {});
    void commit(TransactionContext& ctx);
    void abort(TransactionContext& ctx);
    TxnSnapshot current_snapshot() const;
    void advance_low_water_mark(TxnId oldest_active);
};
```
Supporting components:
- `LockManager`: Row/page intent hierarchy with deadlock detection.
- `SnapshotRegistry`: Tracks active snapshots and produces safe horizons for vacuum and retention.
- `TxnTelemetry`: Emits counters/timers for begins, commits, aborts, lock waits.

## Catalog & Heap Visibility Rules
- `txn_visible(txn_id, snapshot)`: Returns true if `txn_id` committed before `snapshot.read_lsn` and not aborted.
- Catalog entries and heap tuples store `create_txn`, optional `delete_txn`, plus pointer into undo chain for rollback.
- Read-only snapshots never wait on writers; they inspect visibility metadata only. Read-write transactions validate they do not overwrite a tuple committed after their snapshot (first-writer-wins).
- System transactions (DDL, vacuum) can request `read committed` mode to observe freshly committed catalog entries without snapshot pinning.

## Concurrency Primitives
- Lock modes: `NL`, `IS`, `IX`, `S`, `SIX`, `X`. Compatibility matrix mirrors classical intent locking.
- Latch tiers:
  - Page latch: short-lived physical access control.
  - Tuple lock: logical conflict management to protect writers.
- Deadlock detection: Wait-for graph with cycle detection; fallback to timeout for long chains.
- Lock escalation: Threshold-based (tuple to page to table) with telemetry counters for operator insight.

## WAL, Recovery & Retention Integration
- Commit pipeline: acquire `CommitSeq` (current WAL LSN), append commit record containing `TxnId`, finalize flush, then publish visibility.
- Abort pipeline: append abort record (optional optimization), walk undo chain, release locks, and clear snapshot from registry.
- Recovery uses commit records to rebuild transaction table, reapply committed operations, and undo incomplete transactions using existing `WalUndoWalker`.
- Retention manager queries `SnapshotRegistry` for `oldest_active_commit_lsn` to decide prune horizon; ensures no WAL segment needed by active snapshot is deleted.

## Telemetry & Diagnostics
- Counters: active transactions, commits/sec, aborts/sec, conflicts, deadlocks.
- Gauges: oldest active age, snapshot lag, lock wait histogram.
- Event traces: commit latency breakdown (lock wait, WAL flush, publish) with hooks into existing `StorageTelemetryRegistry`.
- Operator surfaces: integrate with `storage_diagnostics.hpp` JSON to expose transaction state and waiters for CLI dashboards.

## Open Questions & Follow-Ups
- Should read-only transactions reuse pooled contexts to reduce allocation churn? Prototype in Milestone 1.
- How aggressive should lock escalation be under mixed OLTP/OLAP loads? Gather data during Milestone 3 stress tests.
- Do we need explicit support for two-phase commit or external coordinators? Deferred until user stories demand it.

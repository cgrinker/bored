# Catalog Operator Guide

This guide summarises the operational steps and checkpoints for managing catalog DDL within the storage engine. It complements `docs/catalog_design.md` by focusing on runbook-style actions and diagnostics.

## DDL Workflow
1. **Acquire Locks** – Ensure the coordinator obtains database/schema/table exclusive locks before invoking catalog DDL helpers.
2. **Start Catalog Transaction** – Create a `CatalogTransaction` bound to the user transaction context so commit/abort hooks fire correctly.
3. **Stage Mutations** – Call the appropriate helper (`stage_create_schema`, `stage_create_table`, `stage_create_index`) or use `CatalogMutator::stage_*` directly. Validate returned identifiers and staged mutation counts prior to commit.
4. **Commit & Persist** – After `CatalogTransaction::commit()` succeeds, consume the published batch and:
   - Append all WAL fragments to the storage writer.
   - Apply tuple payloads to catalog pages using the page manager.
   - Flush WAL to durable media before acknowledging success upstream.
5. **Post-Commit Refresh** – Transactions reusing cached metadata should refresh snapshots or reconstruct `CatalogAccessor` instances to observe updated object definitions.

## Locking Expectations
- Treat DDL as mutually exclusive operations. Acquire locks at the scope of the object being modified (database for schema changes, schema for table/index changes).
- Readers can continue with shared locks; MVCC snapshots ensure they never observe half-applied DDL changes.
- Operators should monitor lock wait metrics (exposed through `StorageTelemetryRegistry`) if DDL workloads coexist with heavy OLTP traffic.

## Monitoring & Telemetry
- **WAL Writer Telemetry** – Use the telemetry registry to monitor `catalog_mutation_records`, WAL flush durations, and retention statistics. Sudden spikes can indicate runaway DDL or retention misconfiguration.
- **Catalog Mutation Counters** – Expose per-transaction counters to dashboards so repeated retries or aborts are visible.
- **Retention Manager** – Ensure WAL retention keeps catalog segments available until checkpoints confirm they are no longer needed. Adjust `retention_segments` and archive paths carefully.

## Troubleshooting Checklist
- **Commit Errors** – Inspect error codes. `invalid_argument` typically points to duplicate names or missing identifiers; `value_too_large` indicates oversized tuple payloads.
- **Staged But Unpublished** – If batches do not appear, confirm `CatalogTransaction::commit()` was invoked and that the consumer called `consume_published_batch()`.
- **Stale Metadata** – Trigger an epoch bump by committing through `CatalogMutator`; downstream services should recreate accessors or refresh snapshots.
- **Missing WAL Fragments** – All published batches include WAL fragments; ensure the WAL writer is accepting `WalRecordFragment::Catalog*` types. Misconfigured retention can prune segments before checkpoints are taken.
- **Recovery Validation** – After crash recovery, run the integration tests (or a subset) to validate catalog state. Catalog replay must precede user table replay.

## Operational Tips
- Schedule heavy DDL during low-traffic windows to minimise lock contention.
- Take periodic catalog snapshots (logical backups) by scanning catalog relations with a consistent snapshot; store alongside WAL archives.
- When recycling identifier ranges, verify retention has archived all segments containing references to dropped objects.
- Maintain observability hooks that alert if catalog commit latency exceeds predefined thresholds.

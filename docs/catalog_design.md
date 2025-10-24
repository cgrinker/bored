# Catalog Subsystem Design

## Objectives
- Provide durable metadata for databases, schemas, tables, columns, and indexes.
- Support transactional visibility with isolation-aware lookups and updates.
- Bootstrap the catalog on empty storage and restore consistently during recovery.
- Expose APIs for DDL execution and semantic analysis layers.

## Functional Scope
- Namespace management for databases and schemas.
- Table and index descriptors with column definitions, data types, and constraints.
- Allocation of storage identifiers (relation ids, segment ids) and free list management.
- Versioned metadata rows to track creation, alteration, and drop states.
- Catalog caching with invalidation on transactional commit.

## Page Architecture
- **Root Catalog Segment:** Fixed relation id reserved for catalog metadata, containing directory of catalog relations.
- **Catalog Relations:** Each metadata class (databases, schemas, tables, columns, indexes) stored as heap-like pages with WAL logging.
- **Tuple Layout:** Compact row format with tuple header (transaction id, commit LSN, visibility flags) followed by attribute payloads.
- **Indexes:** Lightweight hash index on object names for fast lookup; deferred full B+Tree pending index infrastructure (Roadmap Item 8).

## Bootstrapping & Recovery
- Bootstrap sequence writes initial catalog tuples for system namespace using single-writer WAL transaction.
- Recovery driver replays catalog WAL segments prior to user relations to ensure metadata availability.
- Catalog integrity checks run during startup to validate root segment and tuple visibility.

## Transaction Semantics
- Adopt MVCC visibility for catalog tuples: each tuple carries creating and deleting transaction ids (xmin/xmax).
- Readers select visible tuples based on their snapshot; writers append new versions instead of in-place overwrite.
- DDL operations emit WAL records describing catalog tuple insertions/deletions and associated physical changes.

## API Surface
- `CatalogTransaction`: mediates catalog reads/writes with active transaction id and snapshot.
- `CatalogAccessor`: provides typed access to metadata entities (e.g., `get_table(table_oid)`), caches lookups per snapshot.
- `CatalogMutator`: staged updates for DDL statements, producing WAL entries and new tuple versions.
- `CatalogBootstrapper`: initializes catalog segments and system relations during cluster start.

## Storage Identifiers
- `DatabaseId`, `RelationId`, `SchemaId`, `ColumnId`, `IndexId` defined as strongly-typed wrappers around 64-bit integers.
- Central allocator issues ids through WAL-protected counter pages; retention manager tracks id reuse after drops.
- System relations assigned reserved id ranges (< 1024) to simplify bootstrapping.

## Integration Points
- **WAL Writer:** Catalog tuples logged with dedicated record type; headers include relation id and tuple payload checksum.
- **Page Manager:** Provides catalog-specific page format helpers and ensures LSN updates propagate to free-space map.
- **Telemetry:** Emit catalog mutation counters and cache hit ratios into `StorageTelemetryRegistry`.

## Testing Strategy
- Unit tests for tuple visibility rules and MVCC filtering.
- WAL replay tests covering catalog bootstrap, rename, and drop scenarios.
- Crash-recovery drills verifying catalog integrity after simulated failures during DDL.

## Open Questions
- Define initial constraint set (primary key, not-null) scope for first milestone.
- Decide whether to inline small name strings or store in shared string table for deduplication.
- Determine catalog cache eviction policy and interaction with retention manager.

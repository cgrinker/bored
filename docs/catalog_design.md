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

## System Relations & Tuple Layouts
- **catalog_databases (`catalog_relations.hpp`):** `database_id (int64)`, `default_schema_id (int64)`, `name (utf8)` with tuple prefix carrying `xmin/xmax`.
- **catalog_schemas:** `schema_id`, `database_id`, `name` mirroring database layout for schema namespace resolution.
- **catalog_tables:** `table_id`, `schema_id`, `table_type (uint16)`, `root_page_id (uint32)`, `name` used for table-to-segment mapping; bootstrap seeds catalog relations themselves.
- **catalog_columns:** `column_id`, `table_id`, `column_type (uint32)`, `ordinal_position (uint16)`, `name` capturing basic type metadata for DDL validation.
- **catalog_indexes:** `index_id`, `table_id`, `index_type (uint16)`, `name` placeholder for future index descriptors.
- Shared tuple header (`CatalogTupleDescriptor`) stores `xmin/xmax`; encoding helpers in `catalog_encoding.hpp` normalise padding to 8-byte alignment.
- Reserved identifiers (`catalog_bootstrap_ids.hpp`) occupy disjoint ranges (databases=1, schemas=2, relations≥256, columns≥4096, indexes≥8192) to simplify sanity checks and prevent collisions.

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

- **Milestones**
- **Milestone 0: Bootstrapping Infrastructure (1 sprint)** _(Status: Complete)_
	- [x] **Task 0.1:** Author catalog relation schemas (`catalog_databases`, `catalog_schemas`, `catalog_tables`, `catalog_columns`, `catalog_indexes`) and tuple layouts in documentation and header stubs.
	- [x] **Task 0.2:** Implement tuple serialization helpers and size calculators for catalog rows.
	- [x] **Task 0.3:** Reserve system identifier ranges and encode them in `catalog_bootstrap_ids.hpp`.
	- [x] **Task 0.4:** Implement `CatalogBootstrapper` to create root catalog segment, instantiate catalog relations, and write seed tuples.
	- [x] **Task 0.5:** Define WAL record types for catalog tuple insert/update/delete; extend WAL writer to emit them.
	- [x] **Task 0.6:** Extend WAL replay path to recognize catalog records and apply them to catalog pages before user relations.
	- [x] **Task 0.7:** Build bootstrap smoke test covering fresh cluster initialization.
	- [x] **Task 0.8:** Build WAL replay test ensuring bootstrap WAL stream rehydrates catalog correctly.
	- [x] **Task 0.9:** Add sanity check test verifying reserved identifier ranges stay collision-free.
- **Milestone 1: MVCC Visibility & Snapshot Reads (1 sprint)** _(Status: Complete)_
	- [x] **Task 1.1:** Implement MVCC tuple headers (`xmin`, `xmax`, visibility flags) and snapshot evaluation helpers integrated with catalog tuple layouts.
	- [x] **Task 1.2:** Introduce `CatalogTransaction` to capture active transaction id, acquire snapshots, and expose visibility checks.
	- [x] **Task 1.3:** Deliver `CatalogAccessor` read APIs with per-transaction caching and typed metadata retrieval helpers.
	- [x] **Task 1.4:** Integrate catalog snapshot flow with the transaction id allocator and snapshot manager stubs.
	- [x] **Task 1.5:** Implement concurrency-focused tests covering parallel readers, snapshot edge cases, and catalog tuple visibility transitions.
	- [x] **Task 1.6:** Extend recovery tests to validate redo/undo handling of in-flight catalog tuples under MVCC.
- **Milestone 2: DDL Mutation Path (1-2 sprints)**
	- [x] **Task 2.1:** Finalise `CatalogMutator` API surface for insert/update/delete staging, including tuple version builders and undo scaffolding.
	- [x] **Task 2.2:** Implement mutation staging buffers that record new tuple payloads alongside WAL descriptors pending commit.
	- [x] **Task 2.3:** Integrate mutation path with transaction commit/abort hooks to publish or discard staged versions atomically.
	- [x] **Task 2.4:** Extend WAL emission to capture catalog mutation records with before-images and bind them to commit LSNs.
	- [ ] **Task 2.5:** Wire simple DDL handlers (create schema/table/index) to use `CatalogMutator`, including identifier allocation flows.
	- [ ] **Task 2.6:** Update catalog accessor caches and invalidation mechanisms triggered by committed catalog mutations.
	- [ ] **Task 2.7:** Build integration tests covering create/drop/alter cycles, rollback scenarios, and retention manager interaction.
	- [ ] **Task 2.8:** Document DDL mutation lifecycle, catalog locking expectations, and troubleshooting guidance in `docs/catalog_design.md` and operator docs.
- **Milestone 3: Caching, Telemetry, and Hardening (1 sprint)**
	- Add shared catalog cache with invalidation on commit and retention-aware eviction policy.
	- Emit catalog mutation and cache-hit telemetry into existing registries.
	- Conduct crash drills covering mid-flight DDL, id allocator rollbacks, and catalog corruption detection.
	- Tests: cache eviction correctness, telemetry smoke tests, recovery validation.

## Open Questions
- Define initial constraint set (primary key, not-null) scope for first milestone.
- Decide whether to inline small name strings or store in shared string table for deduplication.
- Determine catalog cache eviction policy and interaction with retention manager.

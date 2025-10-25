# DDL Verb Reference & Diagnostics

This reference explains how the storage engine executes Data Definition Language (DDL) verbs, the diagnostics returned to callers, and the recovery expectations for each operation. It complements `docs/ddl_schema_design.md` by describing the operator-facing contract exposed through `DdlCommandResponse` and the associated telemetry signals.

## Structured DDL Responses

Every DDL dispatch returns a `DdlCommandResponse` with the following fields:

- `success` and `result` indicate whether the operation committed and, if so, provide verb-specific payloads (for example, newly allocated identifiers).
- `error` and `message` describe the failure. The error always belongs to the `ddl_error_category` enumeration when raised by engine code.
- `severity` expresses the remediation class:
  - `Info` – request executed successfully; hints are empty.
  - `Warning` – caller input is invalid or the target object is missing; remediation is usually retrying with corrected parameters.
  - `Error` – infrastructure or executor failure (transaction commit failure, missing handler, storage fault); manual intervention is required before retrying.
- `remediation_hints` supplies actionable guidance aligned with the error category (for example, “Use IF NOT EXISTS…” for duplicate object errors or “Inspect server logs…” for commit failures).

Responses map to telemetry automatically: each dispatch records attempts, successes, failures, and duration metrics in the `DdlCommandTelemetry` sampler registered with `StorageTelemetryRegistry`.

## Recovery Guarantees

- All verbs stage catalog mutations inside a single `CatalogTransaction`. Mutations become durable only after the transaction commits and WAL fragments flush.
- On failure, the dispatcher aborts the transaction, discarding staged changes. Crash recovery replays committed WAL only, so partially applied DDL is rolled back automatically.
- Crash drills cover identifier allocator rollbacks, catalog before-image restoration, and index build interruptions. Operators can rely on WAL replay (plus undo walkers) to restore catalog state to the last successful commit.

## Verb Semantics

The table below lists supported verbs with success behavior, common failure codes, recommended remediation, and recovery notes.

### CREATE DATABASE
- **Success path:** Allocates a database identifier, stages metadata in `catalog_databases`, and publishes to retention/checkpoint schedulers.
- **Common failures:**
  - `DatabaseAlreadyExists` (Warning) – choose a different name or use `IF NOT EXISTS`.
  - `InvalidIdentifier` (Warning) – adjust the database name to match identifier rules.
  - `ExecutionFailed` (Error) – inspect logs for storage or commit faults.
- **Recovery:** WAL replay rehydrates catalog tuples; no additional action required after crash.

### DROP DATABASE
- **Success path:** Validates cascade mode, stages deletes for dependent schemas/tables, and updates dependency graph structures.
- **Common failures:**
  - `DatabaseNotFound` (Warning) – verify the database name or use `IF EXISTS`.
  - `ValidationFailed` (Warning) – dependent objects exist without `CASCADE`.
  - `ExecutionFailed` (Error) – indicates shutdown during cascading cleanup.
- **Recovery:** Recovery replays staged deletes; partially completed cascades are rolled back automatically.

### CREATE SCHEMA
- **Success path:** Reserves a schema identifier under the target database and stages schema metadata.
- **Common failures:**
  - `SchemaAlreadyExists` (Warning) – pick a new name or apply `IF NOT EXISTS`.
  - `DatabaseNotFound` (Warning) – confirm database existence.
  - `InvalidIdentifier` (Warning) – correct schema name formatting.
- **Recovery:** Transaction aborts on failure; replay ensures schema appears only after durable commit.

### DROP SCHEMA
- **Success path:** Validates dependencies, optionally cascades to child tables/indexes, and stages deletes for catalog tuples.
- **Common failures:**
  - `SchemaNotFound` (Warning) – verify schema name or use `IF EXISTS`.
  - `ValidationFailed` (Warning) – schema contains tables/indexes without `CASCADE`.
- **Recovery:** Cascaded deletions are idempotent under replay; aborted transactions leave catalog untouched.

### CREATE TABLE
- **Success path:** Allocates table and column identifiers, stages catalog tuples, and invokes storage hooks for page allocation and WAL instrumentation.
- **Common failures:**
  - `TableAlreadyExists` (Warning) – apply `IF NOT EXISTS` or choose a unique name.
  - `SchemaNotFound` (Warning) – ensure schema availability.
  - `ValidationFailed` (Warning) – column definitions violate constraints (duplicate names, unsupported types).
  - `ExecutionFailed` (Error) – downstream storage hooks failed; review logs.
- **Recovery:** WAL includes before-image data, enabling replay to recreate tables or roll back interrupted creations.

### DROP TABLE
- **Success path:** Runs cleanup hooks (retention, storage spill), stages deletion of table and column metadata, and marks related indexes for removal.
- **Common failures:**
  - `TableNotFound` (Warning) – use `IF EXISTS` or adjust table name.
  - `ValidationFailed` (Warning) – cleanup hooks reported dependencies.
- **Recovery:** Undo walkers handle partial drop sequences; crash recovery ensures table metadata reappears unless commit succeeded.

### ALTER TABLE
- **Success path:** Applies rename/add/drop column actions, updates column ordinals, and stages updated catalog tuples atomically.
- **Common failures:**
  - `TableNotFound` (Warning) – validate schema/table.
  - `ValidationFailed` (Warning) – action not supported or violates constraints.
- **Recovery:** Changes become visible only after commit; partial alters are rolled back on crash.

### CREATE INDEX
- **Success path:** Validates column compatibility, reserves index identifier and root page, stages catalog metadata, and records storage plan finalize hooks.
- **Common failures:**
  - `IndexAlreadyExists` (Warning) – rename or drop existing index.
  - `TableNotFound` / `SchemaNotFound` (Warning) – confirm target objects.
  - `ValidationFailed` (Warning) – unsupported column set.
  - `ExecutionFailed` (Error) – storage hook or finalize step failed; inspect logs.
- **Recovery:** Crash drills cover mid-build failure; WAL replay restores catalog entries only for committed builds.

### DROP INDEX
- **Success path:** Validates existence, invokes storage cleanup hook, and stages metadata deletion.
- **Common failures:**
  - `IndexNotFound` (Warning) – use `IF EXISTS` or fix identifier.
  - `ValidationFailed` (Warning) – cleanup hook rejected drop.
- **Recovery:** Staged deletes roll back if commit fails; WAL replay removes the index only after successful commit.

## Operational Checklist

- Monitor `storage_diagnostics` DDL sections for per-verb success/failure trends and failure taxonomy.
- Alert on sustained `ExecutionFailed` severities; they suggest infrastructure issues rather than user input errors.
- Retain WAL segments until checkpoints confirm catalog replay. This ensures recovery can undo or redo long-running DDL safely.
- Pair documentation updates with release notes so operators understand new severity and remediation semantics.

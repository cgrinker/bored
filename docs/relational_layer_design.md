# Relational Layer Design Roadmap

## Context
The storage engine now provides durable write-ahead logging, crash-safe page replay, free-space map persistence, and retention management. The next phase is to layer relational capabilities on top of this foundation. This document outlines the major features required for a relational database surface, ordered by their dependency chain.

## Dependency-Ordered Feature Plan

1. **Catalog & Metadata Subsystem** _(Status: Complete)_ ([catalog design](catalog_design.md))
   - **Responsibilities:** Persist database, schema, table, index, and column metadata; map logical descriptors to storage identifiers; expose catalog lookups with caching and transactional visibility.
   - **Prerequisites:** Existing WAL/page manager stack for persistent storage of catalog pages.
   - **Key Tasks:**
     - Define catalog page layouts and versioning rules.
     - Implement catalog bootstrap process and recovery hooks.
     - Provide transactional catalog updates with WAL integration.

2. **DDL & Schema Management Layer**
   - **Responsibilities:** Execute create/alter/drop operations for schemas, tables, and indexes; validate dependencies and constraints; materialize catalog changes into WAL-protected catalog pages.
   - **Prerequisites:** Catalog subsystem.
   - **Key Tasks:**
     - Define DDL command handlers and state machines.
     - Integrate catalog updates with checkpointing and retention policies.
     - Establish error handling and rollback semantics for partial DDL failures.

3. **Parser Front-End (PEGTL-Based)**
   - **Responsibilities:** Translate SQL text into an abstract syntax tree (AST) using the `taocpp/PEGTL` CMake package.
   - **Prerequisites:** Baseline DDL verb definitions to seed grammar; coordination with planned AST schema.
   - **Key Tasks:**
     - Vendor or reference the CMake `PEGTL` package in `vcpkg.json` and `CMakeLists.txt`.
     - Implement modular grammars (lexical rules, expressions, statements) with PEGTL actions building AST nodes.
     - Create parser error reporting aligned with diagnostic surfaces.

4. **AST & Logical IR Layer**
   - **Responsibilities:** Represent parsed SQL in a structured, type-safe form for planning and optimization.
   - **Prerequisites:** Parser front-end to construct AST nodes; catalog metadata types for semantic annotations.
   - **Key Tasks:**
     - Define AST node hierarchy and ownership semantics.
     - Implement semantic analysis to resolve identifiers against the catalog and annotate types.
     - Produce a logical plan tree (relational algebra IR) as input to the optimizer.

5. **Transaction & Concurrency Control Manager**
   - **Responsibilities:** Coordinate transactional lifecycle, isolation levels, and conflict detection (e.g., locking or MVCC).
   - **Prerequisites:** Catalog visibility rules; WAL/page manager hooks for transaction IDs and LSN tracking.
   - **Key Tasks:**
     - Choose concurrency model (initially strict 2PL or basic MVCC).
     - Instrument page and tuple access with latching/locking APIs.
     - Integrate with WAL commit protocol and retention manager.

6. **Query Planner & Optimizer**
   - **Responsibilities:** Transform logical plans into executable physical plans, applying rule-based and cost-based optimizations.
   - **Prerequisites:** AST/IR layer; catalog statistics; transaction manager for isolation-aware decisions.
   - **Key Tasks:**
     - Implement transformation framework (rule dispatcher, memoization optional).
     - Collect and maintain catalog statistics for cardinality estimates.
     - Produce operator pipelines compatible with the executor framework.

7. **Query Executor Framework**
   - **Responsibilities:** Execute physical plans using iterator or pipeline model; coordinate buffer and WAL interaction during data access and mutation.
   - **Prerequisites:** Planned physical operators; transaction manager for locking; storage engine APIs for page/tuple access.
   - **Key Tasks:**
     - Define executor interface (open/next/close or vectorized batches).
     - Implement base operators (seq scan, index scan stub, nested-loop join, projection, aggregation).
     - Ensure DML operators emit WAL records via page manager hooks.

8. **Index Management Infrastructure**
   - **Responsibilities:** Provide secondary index structures (initially B+Tree) with maintenance, lookup, and recovery support.
   - **Prerequisites:** Catalog entries for index metadata; executor framework for scan operators; transaction manager for concurrency.
   - **Key Tasks:**
     - Design index page formats and WAL semantics.
     - Implement index build, insert, delete, and search routines.
     - Extend WAL recovery and retention to cover index segments.

9. **Checkpoint & Recovery Coordination Layer**
   - **Responsibilities:** Tie relational components into checkpoint scheduling, ensuring consistent snapshots across catalog, data, and index pages.
   - **Prerequisites:** Transaction manager; index infrastructure; existing WAL retention and checkpoint scheduler.
   - **Key Tasks:**
     - Define checkpoint barriers for in-flight transactions and catalog mutations.
     - Extend recovery plans to rehydrate catalog and indexes alongside data pages.
     - Surface diagnostics for checkpoint lag and recovery progress.

10. **User-Facing Tooling & Diagnostics**
    - **Responsibilities:** Provide CLI tooling, SQL shell, configuration surfaces, and observability dashboards for relational workloads.
    - **Prerequisites:** Core relational functionality (parser through executor) and telemetry registry integration.
    - **Key Tasks:**
      - Implement interactive SQL shell powered by the parser and executor stack.
      - Expose catalog inspection commands and system views.
      - Integrate telemetry (query latencies, lock waits) into existing diagnostics pipelines.

## Next Steps
- Finalise detailed milestones for the DDL and schema management layer ([DDL & schema design](ddl_schema_design.md)).
- Introduce PEGTL as a third-party dependency and prototype minimal SQL grammar support for DDL statements (Items 2-3).
- Establish transaction manager design review to select the initial concurrency model (Item 5).

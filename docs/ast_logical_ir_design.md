# AST & Logical IR Design

## Context
The parser front-end now produces preliminary syntax trees for DDL verbs. To progress toward general query execution, we need a structured Abstract Syntax Tree (AST) representation and a logical relational Algebra Intermediate Representation (IR). These layers will sit between the PEGTL parser and the query planner, enriching parsed statements with semantic information from the catalog and producing optimizer-ready plan trees.

### Objectives
- Provide a type-safe, memory-efficient AST that mirrors the grammar while abstracting parser implementation details.
- Perform semantic analysis that resolves catalog objects, applies type inference rules, and surfaces actionable diagnostics.
- Emit a logical relational algebra IR that captures query intent for downstream optimization and execution phases.

## Milestone Breakdown

### Milestone 1: AST Schema Foundation
**Goal:** Establish the canonical AST node definitions, ownership, and traversal utilities independent of parser details.

**Exit Criteria:**
- Parser actions construct the new AST nodes via a stable factory surface.
- Visitors can traverse statements, expressions, and query specifications without referencing PEGTL internals.
- Unit tests cover construction and round-tripping of representative statements.

**Tasks:**
1. ✅ Define node structs/classes for statements, expressions, and query blocks with explicit child ownership semantics (arena or polymorphic wrappers).
2. ✅ Implement lightweight arena allocation and node interning helpers to avoid deep copy overhead.
3. ✅ Provide visitor interfaces and pattern-matching helpers for statements and expressions.
4. ✅ Build pretty-printer / debug serialization utilities to visualize ASTs during diagnostics and tests.
5. ✅ Update PEGTL actions to emit the new AST nodes and add Catch2 coverage for basic DDL/SELECT statements (SELECT grammar now drives relational AST allocations with new Catch2 coverage in `parser_select_tests.cpp`).

### Milestone 2: Semantic Analysis & Binding
**Goal:** Annotate AST nodes with catalog-validated bindings, resolved types, and coercion rules.

**Exit Criteria:**
- Identifier resolution succeeds against the catalog for tables, columns, and schemas, producing bound references on AST nodes.
- Expression nodes carry inferred result types with coercions recorded for later phases.
- Semantic diagnostics align with existing parser and DDL error reporting surfaces.

**Tasks:**
1. ✅ Design symbol table structures for statement scopes (queries, subqueries, CTEs) backed by catalog lookups.  
	Introduced `relational::BinderCatalog` plus scoped table/column symbol tables to drive SELECT binding.
2. ✅ Implement binder passes that resolve table, column, and function references, emitting structured diagnostics on failure.  
	Added a SELECT binder that resolves tables, columns, and qualified stars with Catch2 coverage; function binding remains out of scope for this sprint.
3. ✅ Add type inference rules for scalar expressions and ensure literals carry resolved types and collations.  \
	Binder now annotates identifiers, literals, and predicates with `ScalarType` metadata and surfaces incompatibility diagnostics (see new Catch2 coverage in `parser_binding_tests.cpp`).
4. ✅ Record coercion or cast requirements on AST nodes for mismatched operand types.  \
	Binder now analyses comparison operands, emits `required_coercion` targets on expressions, and tests cover numeric promotions plus string vs integer mismatches.
5. ✅ Extend Catch2 suites with binder-focused tests covering ambiguity, shadowing, and unresolved identifiers.  \
	Added multi-table FROM parsing plus new binder tests that assert ambiguous column diagnostics, duplicate alias handling, and unresolved qualifier errors.
6. ✅ Bind GROUP BY clauses, including alias support and diagnostics.  \
	Grammar now parses GROUP BY lists, AST captures them, binder resolves aliases and reports missing columns, and new Catch2 coverage validates both positive and negative scenarios.

### Milestone 3: Logical Plan Construction & Normalization
**Goal:** Lower bound ASTs into a logical relational algebra IR and perform initial normalization passes.

**Exit Criteria:**
- Logical operator nodes (scan, project, filter, join, aggregate, sort, limit, insert/update/delete) defined with catalog-aware metadata.
- AST-to-IR lowering produces canonical operator trees for SELECT, INSERT, UPDATE, DELETE, and simple DDL-backed queries.
- Normalization passes (e.g., predicate classification, projection pruning stubs) prepared for optimizer consumption.

**Tasks:**
1. ✅ Define logical operator structures and enums capturing required metadata (output schema, predicates, grouping keys).  \
	Introduced `logical_plan.hpp` with operator hierarchy (scan/project/filter/aggregate/sort/limit), output schema columns, and visitor support plus Catch2 coverage.
2. ✅ Implement an AST lowering walker that consumes bound nodes and emits logical operators with schema propagation.  \
	Added `logical_lowering.hpp`/`.cpp` to translate bound SELECT statements into scan/filter/project/sort/limit pipelines, preserving inferred types in output schemas and surfacing diagnostics for unsupported shapes, validated by dedicated Catch2 coverage.
3. ✅ Introduce normalization hooks for predicate separation, projection ordering, and join criteria classification.  \
	Added `logical_normalization.hpp`/`.cpp` to walk logical plans, collect filter predicates, preserve projection ordering, and provide join classification stubs (ready for future join support), validated via Catch2 normalization tests.
4. ✅ Surface logical plan dumps for diagnostics and integrate unit tests comparing expected operator trees for representative queries.  \
	Added `logical_plan_printer.hpp`/`.cpp` plus the new `dump_select_plan` helper so tooling can capture plan text after binding/lowering, and expanded Catch2 coverage in `parser_logical_plan_printer_tests.cpp` to assert filter/sort/limit pipelines render as expected and that lowering emits trace callbacks when configured.
5. ✅ Document extension points for the query planner and executor teams, ensuring IR stability guarantees.  \
	 codified `CatalogBinderAdapter` so runtime components can expose catalog metadata to the binder, added plan sink hooks plus `dump_select_plan` normalization output, and updated operator guide documentation to call out how tracers consume these surfaces.

### Milestone 4: Testing & Tooling Integration
**Goal:** Ensure the AST and logical IR layers integrate cleanly with existing tooling and diagnostics.

**Exit Criteria:**
- Continuous integration runs binder and logical IR tests alongside parser suites.
- Diagnostics surfaces expose AST and logical plan dumps behind trace flags or debug options.
- Developer workflow docs updated to describe compilation flags and debugging techniques for the new layers.

**Tasks:**
1. Wire new test binaries into CTest and ensure they run under existing sanitizer builds once available.
2. Add CLI or logging hooks to dump bound ASTs and logical plans for troubleshooting.
3. Update documentation (`docs/parser_operator_guide.md`, `docs/relational_layer_design.md`) with references to the AST/IR pipeline and debug workflows.
4. Capture performance baselines for binding and lowering stages using synthetic workloads in `benchmarks/` for regression tracking.
5. Establish coding conventions for future AST/IR extensions (naming, ownership, testing patterns).

### Milestone 5: Join-Aware Logical Planning
**Goal:** Introduce multi-relation query support with join-aware binding, lowering, and diagnostics so optimizer work can proceed on realistic SELECT workloads.

**Exit Criteria:**
- Binder recognises explicit and implicit join constructs, resolves participating relations, and categorises join predicates for normalization.
- Logical plan lowering emits join operators with propagated output schemas and predicate metadata ready for optimizer consumption.
- Plan dumps, normalization traces, and diagnostics surface join structure and predicate classification for debugging complex queries.

**Tasks:**
1. [ ] Extend bound AST structures and binder scopes to represent join relations, qualifying column references across joined tables and detecting ambiguous bindings.
2. [ ] Define logical join operator variants (inner, left/right outer, cross) with schema propagation helpers and update visitors accordingly.
3. [ ] Implement AST-to-IR lowering for joined FROM clauses, including join predicate extraction and residual filter separation.
4. [ ] Enhance normalization passes and `dump_select_plan` output to classify join predicates, residual filters, and provide readable join ordering traces.
5. [ ] Add Catch2 coverage for multi-join queries spanning binding, lowering, normalization, and textual dumps to lock in regression protection.

_Current Status:_ Parser now emits explicit join metadata and the binder binds join predicates; lowering still gates on single-table plans until join operators land.

## Upcoming Tasks
Tracked in `docs/project_backlog.md`.

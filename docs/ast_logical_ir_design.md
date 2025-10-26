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
1. Design symbol table structures for statement scopes (queries, subqueries, CTEs) backed by catalog lookups.
2. Implement binder passes that resolve table, column, and function references, emitting structured diagnostics on failure.
3. Add type inference rules for scalar expressions and ensure literals carry resolved types and collations.
4. Record coercion or cast requirements on AST nodes for mismatched operand types.
5. Extend Catch2 suites with binder-focused tests covering ambiguity, shadowing, and unresolved identifiers.

### Milestone 3: Logical Plan Construction & Normalization
**Goal:** Lower bound ASTs into a logical relational algebra IR and perform initial normalization passes.

**Exit Criteria:**
- Logical operator nodes (scan, project, filter, join, aggregate, sort, limit, insert/update/delete) defined with catalog-aware metadata.
- AST-to-IR lowering produces canonical operator trees for SELECT, INSERT, UPDATE, DELETE, and simple DDL-backed queries.
- Normalization passes (e.g., predicate classification, projection pruning stubs) prepared for optimizer consumption.

**Tasks:**
1. Define logical operator structures and enums capturing required metadata (output schema, predicates, grouping keys).
2. Implement an AST lowering walker that consumes bound nodes and emits logical operators with schema propagation.
3. Introduce normalization hooks for predicate separation, projection ordering, and join criteria classification.
4. Surface logical plan dumps for diagnostics and integrate unit tests comparing expected operator trees for representative queries.
5. Document extension points for the query planner and executor teams, ensuring IR stability guarantees.

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

# Query Planner & Optimizer Milestones

## Objectives
- Transform logical relational plans into efficient physical plans leveraging rule-based rewrites and cost-based guidance.
- Provide deterministic planning results for basic OLTP workloads (single table scans, simple joins, inserts/updates) with hooks for future analytic extensions.
- Integrate catalog statistics, transaction snapshots, and storage layout metadata to inform plan choices.
- Surface planner diagnostics and telemetry so operators can observe optimization decisions and regressions.

## Milestone 0: Planning Scaffolding & Logical IR Intake (0.5 sprint)
- [x] Finalise logical plan data structures (operators, properties) produced by the AST/IR pipeline.
	- Added immutable `LogicalOperator`/`LogicalPlan` scaffolding with cardinality, ordering, and projected column metadata.
- [x] Implement `PlannerContext` carrying catalog accessors, transaction snapshot, and configuration knobs.
	- Introduced `PlannerContext`/`PlannerOptions` wrappers so future rule execution can consult catalog snapshots and planner flags.
- [x] Introduce a planning pipeline skeleton (`plan_query`) that accepts logical plans and returns placeholder physical plans.
	- `plan_query` now lowers logical nodes into `PhysicalOperator` placeholders, preserving structural metadata and capturing diagnostics for empty inputs.
- [x] Establish Catch2 harness with golden logical-to-physical translation fixtures.
	- Added `planner_scaffolding_tests.cpp` to validate scan lowering and diagnostic reporting.
- [x] Document planner configuration options and extension points in `docs/query_planner_milestones.md` appendix.
	- Appendix now references naming conventions, immutability guidance, and statistics handles for future extensions.

## Milestone 1: Rule Framework & Canonical Transformations (1 sprint)
- [x] Build rule registry / dispatcher supporting pattern matching and rule priorities.
	- Registry now maintains per-rule priorities, pattern matching, and category toggles surfaced through planner options.
- [x] Implement mandatory logical simplifications (predicate pushdown, projection pruning, constant folding stubs).
	- Projection pruning eliminates identity projections; filter pushdown swaps identity projections ahead of filters; constant folding stub wired for future expression folding.
- [x] Add join reordering primitives for associative/commutative inner joins (left-deep search baseline).
	- Join commutativity and associativity rules now enumerate left-deep alternatives and register them with the memo for later costing.
- [x] Provide memo-style plan exploration stubs (grouping equivalent expressions) with hooks for future costing.
	- Added `Memo`/`MemoGroup` scaffolding so rules can insert grouped logical expressions ahead of cost selection logic.
- [x] Extend unit tests with rule application traces and failure diagnostics when no rules fire.
	- Added planner-rule suites that assert trace capture, disabled-rule behavior, and planner-level diagnostics when tracing is enabled.

## Milestone 2: Statistics & Costing Integration (1 sprint)
- [x] Define `StatisticsCatalog` API for accessing row counts, distinct counts, and histogram stubs.
	- Introduced planner-level `StatisticsCatalog` with table/column stat records, planner context wiring, and Catch2 coverage for registration and lookup semantics.
- [x] Implement a baseline cost model (IO + CPU) for scans, joins, and aggregations.
	- Added `CostModel` with deterministic IO/CPU costing factors across scans, filters, projections, and joins plus unit coverage; aggregates defer to logical cardinality until operators exist.
- [x] Connect cost model to rule framework so cheapest alternative per group is selected.
	- `plan_query` now scores memo groups with the `CostModel`, records chosen plan cost, and memo deduplication avoids join-rule loops during evaluation.
- [x] Add telemetry counters for rule applications, costing invocations, and chosen plan costs.
	- `PlannerResult` surfaces attempted/applied rules plus cost evaluation counts, with scaffolding tests covering tracing and non-tracing configurations.
- [x] Create regression tests that assert stable plan choices given frozen statistics fixtures.
	- Added a planner scaffolding test that seeds statistics, compares join order costs, and asserts the cheaper alternative is consistently selected.

## Milestone 3: Physical Operator Selection & Plan Lowering (1 sprint)
- [x] Map optimized logical operators to executor-ready physical operators (seq scan, nested-loop join, hash join stub, aggregation).
	- Planner lowerer now produces concrete physical operators with relation metadata, scan visibility flags, and respects memo-selected join ordering; hash join lowering currently selects hash vs. nested-loop joins via cardinality heuristics while aggregation stubs remain TODO pending logical operators.
- [x] Emit physical plan properties (output schema, ordering, partitioning) for executor validation.
	- Physical plans now annotate ordering and partitioning columns, propagating metadata from scans through projections and joins for downstream executor validation.
- [x] Wire transaction snapshot awareness into scan primitives (visibility filters, lock modes).
	- Planner lowering now attaches MVCC snapshots to table scans when required, ensuring executors can enforce visibility and lock semantics without re-querying context.
- [x] Ensure DML statements (INSERT/UPDATE/DELETE) request the correct physical write operators with WAL prerequisites.
	- Planner lowering now maps DML logical nodes to physical Insert/Update/Delete operators, attaching MVCC snapshots for update/delete and preserving relation metadata for WAL staging.
- [x] Expand integration tests to run planner + executor smoke tests using Catch2.
	- Added planner integration smoke tests that traverse physical plans with an executor stub, validating hash join pipelines and update snapshots end-to-end.

## Milestone 4: Diagnostics, Telemetry & Hardening (0.5 sprint)
## Milestone 4: Diagnostics, Telemetry & Hardening (0.5 sprint)
## Milestone 4: Diagnostics, Telemetry & Hardening (0.5 sprint)
- [x] Expose planner decisions through `planner::PlanDiagnostics` (chosen plan tree, alternative costs, rule traces).
- [x] Integrate planner telemetry with `StorageTelemetryRegistry` / diagnostics JSON surfaces.
- [ ] Add explain-style pretty printer for physical plans.
- [ ] Benchmark planner latency across representative workloads; set baseline thresholds for regression detection.
- [ ] Finish documentation (operator guide addendum, troubleshooting checklist).

## Appendix: Planner Extension Guidelines
- Keep new logical or physical operators in dedicated translation units under `src/planner/` with mirrored headers in `include/bored/planner/`.
- Name rules using `<Operation><Transformation>Rule` (e.g., `PushDownFilterRule`, `JoinAssociativityRule`) to match registry diagnostics.
- Prefer immutable logical plan nodes; use builder helpers to create transformed copies rather than mutating in place.
- Store statistics in the catalog using opaque handles so planner clients can swap implementations (in-memory vs. persisted) without code changes.
- When adding cost model components, include unit tests that pin baseline costs and integration tests that confirm plan selection remains stable.

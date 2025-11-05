#include "bored/planner/plan_printer.hpp"

#include "bored/planner/physical_plan.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using Catch::Matchers::ContainsSubstring;

namespace bored::planner {

TEST_CASE("explain_plan renders empty plan marker")
{
    PhysicalPlan plan{};
    CHECK(explain_plan(plan) == "(empty plan)");
}

TEST_CASE("explain_plan renders hierarchy with properties and snapshot")
{
    PhysicalProperties scan_props{};
    scan_props.expected_cardinality = 512U;
    scan_props.relation_name = "public.orders";
    scan_props.output_columns = {"order_id", "customer_id"};
    scan_props.partitioning_columns = scan_props.output_columns;
    scan_props.requires_visibility_check = true;
    scan_props.expected_batch_size = 128U;

    txn::Snapshot snapshot{};
    snapshot.read_lsn = 128U;
    snapshot.xmin = 7U;
    snapshot.xmax = 21U;
    snapshot.in_progress = {9U, 10U};
    scan_props.snapshot = snapshot;

    auto scan = PhysicalOperator::make(PhysicalOperatorType::SeqScan, {}, scan_props);

    PhysicalProperties projection_props{};
    projection_props.output_columns = scan_props.output_columns;
    projection_props.ordering_columns = {"order_id"};
    projection_props.preserves_order = true;
    projection_props.expected_batch_size = 128U;

    auto projection = PhysicalOperator::make(PhysicalOperatorType::Projection, {scan}, projection_props);
    PhysicalPlan plan{projection};

    ExplainOptions options{};
    options.include_snapshot = true;

    const auto rendered = explain_plan(plan, options);

    CHECK_THAT(rendered, ContainsSubstring("Projection [output=[order_id, customer_id], ordering=[order_id], batch=128]"));
    CHECK_THAT(rendered, ContainsSubstring("\n  - SeqScan [cardinality=512, relation=public.orders, output=[order_id, customer_id], partitioning=[order_id, customer_id], visibility=required, snapshot={read_lsn=128, xmin=7, xmax=21, in_progress_count=2}, batch=128]"));
}

TEST_CASE("explain_plan can omit properties")
{
    auto scan = PhysicalOperator::make(PhysicalOperatorType::SeqScan);
    PhysicalPlan plan{scan};

    ExplainOptions options{};
    options.include_properties = false;

    const auto rendered = explain_plan(plan, options);
    CHECK(rendered == "SeqScan");
}

TEST_CASE("explain_plan renders join strategy hint")
{
    PhysicalProperties left_props{};
    left_props.expected_cardinality = 1024U;
    left_props.output_columns = {"left_key"};
    left_props.expected_batch_size = 256U;

    PhysicalProperties right_props{};
    right_props.expected_cardinality = 1024U;
    right_props.output_columns = {"right_key"};
    right_props.expected_batch_size = 256U;

    auto left = PhysicalOperator::make(PhysicalOperatorType::SeqScan, {}, left_props);
    auto right = PhysicalOperator::make(PhysicalOperatorType::SeqScan, {}, right_props);

    PhysicalProperties join_props{};
    join_props.expected_cardinality = 1024U;
    join_props.expected_batch_size = 128U;
    join_props.executor_strategy = "hash(build=left, probe=right)";

    auto join = PhysicalOperator::make(PhysicalOperatorType::HashJoin, {left, right}, join_props);
    PhysicalPlan plan{join};

    const auto rendered = explain_plan(plan);
    CHECK_THAT(rendered, ContainsSubstring("HashJoin [cardinality=1024, batch=128, strategy=hash(build=left, probe=right)]"));
}

TEST_CASE("explain_plan annotates materialize spool properties")
{
    PhysicalProperties scan_props{};
    scan_props.output_columns = {"id"};
    auto scan = PhysicalOperator::make(PhysicalOperatorType::SeqScan, {}, scan_props);

    PhysicalProperties materialize_props{};
    materialize_props.output_columns = scan_props.output_columns;
    MaterializeProperties materialize_detail{};
    materialize_detail.worktable_id = 42U;
    materialize_detail.enable_recursive_cursor = true;
    materialize_props.materialize = materialize_detail;

    auto materialize = PhysicalOperator::make(PhysicalOperatorType::Materialize, {scan}, materialize_props);
    PhysicalPlan plan{materialize};

    ExplainOptions options{};
    options.include_properties = true;

    const auto rendered = explain_plan(plan, options);
    CHECK_THAT(rendered, ContainsSubstring("Materialize [output=[id], worktable=42, recursive_cursor=enabled]"));
    CHECK_THAT(rendered, ContainsSubstring("\n  - SeqScan"));
}

TEST_CASE("explain_plan attaches runtime stats when provided")
{
    PhysicalProperties scan_props{};
    scan_props.executor_operator_id = 2U;
    auto scan = PhysicalOperator::make(PhysicalOperatorType::SeqScan, {}, scan_props);

    PhysicalProperties filter_props{};
    filter_props.executor_operator_id = 1U;
    auto filter = PhysicalOperator::make(PhysicalOperatorType::Filter, {scan}, filter_props);
    PhysicalPlan plan{filter};

    ExplainRuntimeMap runtime{};
    runtime.emplace(1U, ExplainRuntimeStats{.loops = 2U, .rows = 5U, .total_duration_ns = 100U, .last_duration_ns = 40U});
    runtime.emplace(2U, ExplainRuntimeStats{.loops = 3U, .rows = 9U, .total_duration_ns = 160U, .last_duration_ns = 60U});

    ExplainOptions options{};
    options.runtime_stats = &runtime;

    const auto rendered = explain_plan(plan, options);
    CHECK_THAT(rendered, ContainsSubstring("Filter [runtime={loops=2, rows=5, total_ns=100, last_ns=40}]"));
    CHECK_THAT(rendered, ContainsSubstring("\n  - SeqScan [runtime={loops=3, rows=9, total_ns=160, last_ns=60}]"));

    ExplainOptions runtime_only{};
    runtime_only.include_properties = false;
    runtime_only.runtime_stats = &runtime;
    const auto runtime_only_rendered = explain_plan(plan, runtime_only);
    CHECK_THAT(runtime_only_rendered, ContainsSubstring("Filter [runtime={loops=2, rows=5, total_ns=100, last_ns=40}]"));
}

}  // namespace bored::planner

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
    CHECK_THAT(rendered, ContainsSubstring("HashJoin [batch=128, strategy=hash(build=left, probe=right)]"));
}

}  // namespace bored::planner

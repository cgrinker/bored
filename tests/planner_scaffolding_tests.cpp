#include "bored/planner/cost_model.hpp"
#include "bored/planner/planner.hpp"
#include "bored/planner/statistics_catalog.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalPlan;
using bored::planner::LogicalProperties;
using bored::planner::PhysicalOperatorType;
using bored::planner::PlannerContext;
using bored::planner::PlannerContextConfig;
using bored::planner::PlannerOptions;
using bored::planner::PlannerResult;
using bored::planner::CostModel;
using bored::planner::StatisticsCatalog;
using bored::planner::TableStatistics;
using bored::txn::Snapshot;
using Catch::Approx;

namespace {

LogicalOperatorPtr make_scan_plan()
{
    LogicalProperties properties{};
    properties.estimated_cardinality = 42U;
    properties.output_columns = {"id", "name"};
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
}

LogicalOperatorPtr make_identity_projection_plan()
{
    auto scan = make_scan_plan();
    LogicalProperties projection_props = scan->properties();
    std::vector<LogicalOperatorPtr> children{scan};
    return LogicalOperator::make(LogicalOperatorType::Projection, std::move(children), projection_props);
}

LogicalOperatorPtr make_table_scan(const std::string& relation,
                                   std::size_t rows,
                                   std::vector<std::string> columns)
{
    LogicalProperties properties{};
    properties.relation_name = relation;
    properties.estimated_cardinality = rows;
    properties.output_columns = std::move(columns);
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
}

LogicalOperatorPtr make_three_way_join_plan()
{
    auto scan_a = make_table_scan("public.a", 1000U, {"a_id"});
    auto scan_b = make_table_scan("public.b", 100U, {"b_id"});
    auto scan_c = make_table_scan("public.c", 10U, {"c_id"});

    LogicalProperties join_left_props{};
    join_left_props.output_columns = {"a_id", "b_id"};
    auto left = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{scan_a, scan_b},
        join_left_props);

    LogicalProperties join_root_props{};
    join_root_props.output_columns = {"a_id", "b_id", "c_id"};
    return LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{left, scan_c},
        join_root_props);
}

LogicalOperatorPtr make_rotated_three_way_join_plan()
{
    auto scan_a = make_table_scan("public.a", 1000U, {"a_id"});
    auto scan_b = make_table_scan("public.b", 100U, {"b_id"});
    auto scan_c = make_table_scan("public.c", 10U, {"c_id"});

    LogicalProperties join_right_props{};
    join_right_props.output_columns = {"b_id", "c_id"};
    auto right = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{scan_b, scan_c},
        join_right_props);

    LogicalProperties join_root_props{};
    join_root_props.output_columns = {"a_id", "b_id", "c_id"};
    return LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{scan_a, right},
        join_root_props);
}

}  // namespace

TEST_CASE("plan_query lowers logical scan to placeholder physical plan")
{
    LogicalPlan plan{make_scan_plan()};
    PlannerContext context{};

    PlannerResult result = plan_query(context, plan);
    REQUIRE(result.diagnostics.empty());

    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::SeqScan);
    CHECK(root->properties().expected_cardinality == 42U);
    CHECK(root->properties().output_columns.size() == 2U);
    CHECK(root->properties().relation_name.empty());
    CHECK_FALSE(root->properties().requires_visibility_check);
    CHECK(root->properties().ordering_columns.empty());
    CHECK(root->properties().partitioning_columns == root->properties().output_columns);
    CHECK(result.rules_attempted > 0U);
    CHECK(result.rules_applied == 0U);
    CHECK(result.cost_evaluations == 0U);
    CHECK(result.chosen_plan_cost == Approx(0.0));
}

TEST_CASE("plan_query captures diagnostics for empty logical plan")
{
    LogicalPlan empty_plan{};
    PlannerContext context{};

    PlannerResult result = plan_query(context, empty_plan);
    REQUIRE(result.plan.root() == nullptr);
    REQUIRE(result.diagnostics.size() == 1U);
    CHECK(result.diagnostics.front() == "logical plan is empty");
}

TEST_CASE("plan_query applies projection pruning alternative when tracing enabled")
{
    LogicalPlan plan{make_identity_projection_plan()};

    PlannerOptions options{};
    options.enable_rule_tracing = true;

    PlannerContextConfig config{};
    config.options = options;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    REQUIRE(result.plan.root());
    CHECK(result.plan.root()->type() == PhysicalOperatorType::SeqScan);
    REQUIRE(result.diagnostics.size() == 10U);
    CHECK(result.diagnostics[0] == "ProjectionPruning:applied");
    CHECK(result.diagnostics[1] == "FilterPushdown:skipped");
    CHECK(result.diagnostics[2] == "JoinCommutativity:skipped");
    CHECK(result.diagnostics[3] == "JoinAssociativity:skipped");
    CHECK(result.diagnostics[4] == "ConstantFolding:skipped");
    CHECK(result.diagnostics[5] == "ProjectionPruning:skipped");
    CHECK(result.diagnostics[6] == "FilterPushdown:skipped");
    CHECK(result.diagnostics[7] == "JoinCommutativity:skipped");
    CHECK(result.diagnostics[8] == "JoinAssociativity:skipped");
    CHECK(result.diagnostics[9] == "ConstantFolding:skipped");
    CHECK(result.rules_attempted == 10U);
    CHECK(result.rules_applied == 1U);
    CHECK(result.cost_evaluations == 0U);
    CHECK(result.chosen_plan_cost == Approx(0.0));
}

TEST_CASE("plan_query preserves ordering metadata through projection")
{
    LogicalProperties scan_props{};
    scan_props.estimated_cardinality = 5U;
    scan_props.output_columns = {"id"};
    scan_props.preserves_order = true;
    auto scan = LogicalOperator::make(LogicalOperatorType::TableScan, {}, scan_props);

    LogicalProperties projection_props = scan_props;
    projection_props.preserves_order = true;
    auto projection = LogicalOperator::make(LogicalOperatorType::Projection, {scan}, projection_props);

    LogicalPlan plan{projection};
    PlannerContext context{};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    const auto& physical_props = root->properties();
    CHECK(physical_props.preserves_order);
    CHECK(physical_props.ordering_columns == std::vector<std::string>{"id"});

    if (root->type() == PhysicalOperatorType::Projection) {
        REQUIRE(root->children().size() == 1U);
        const auto& child_props = root->children().front()->properties();
        CHECK(child_props.ordering_columns == std::vector<std::string>{"id"});
    } else {
        CHECK(root->type() == PhysicalOperatorType::SeqScan);
    }
}

TEST_CASE("plan_query uses cost model to choose cheaper join alternative")
{
    StatisticsCatalog statistics;

    TableStatistics stats_a;
    stats_a.set_row_count(1000.0);
    statistics.register_table("public.a", stats_a);

    TableStatistics stats_b;
    stats_b.set_row_count(100.0);
    statistics.register_table("public.b", stats_b);

    TableStatistics stats_c;
    stats_c.set_row_count(10.0);
    statistics.register_table("public.c", stats_c);

    CostModel cost_model{&statistics};

    PlannerContextConfig config{};
    config.statistics = &statistics;
    config.cost_model = &cost_model;
    PlannerContext context{config};

    LogicalPlan plan{make_three_way_join_plan()};

    PlannerResult result = plan_query(context, plan);

    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::NestedLoopJoin);
    REQUIRE(root->children().size() == 2U);

    const auto& left_child = root->children()[0];
    const auto& right_child = root->children()[1];
    REQUIRE(left_child);
    REQUIRE(right_child);

    CHECK(left_child->type() == PhysicalOperatorType::SeqScan);
    CHECK(right_child->type() == PhysicalOperatorType::NestedLoopJoin);

    const auto rotated_cost = cost_model.estimate_plan(make_rotated_three_way_join_plan()).cost.total();
    const auto original_cost = cost_model.estimate_plan(make_three_way_join_plan()).cost.total();

    CHECK(rotated_cost < original_cost);
    CHECK(result.rules_attempted > 0U);
    CHECK(result.rules_applied > 0U);
    CHECK(result.cost_evaluations >= 2U);
    CHECK(result.chosen_plan_cost == Approx(rotated_cost));
}

TEST_CASE("plan_query propagates relation metadata and snapshot requirements to physical scans")
{
    LogicalProperties properties{};
    properties.relation_name = "public.orders";
    properties.estimated_cardinality = 512U;
    properties.output_columns = {"order_id", "customer_id"};

    auto scan = LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
    LogicalPlan plan{scan};

    Snapshot snapshot{};
    snapshot.read_lsn = 42U;
    snapshot.xmin = 1U;
    snapshot.xmax = 10U;

    PlannerContextConfig config{};
    config.snapshot = snapshot;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::SeqScan);

    const auto& physical_props = root->properties();
    CHECK(physical_props.expected_cardinality == 512U);
    CHECK(physical_props.relation_name == "public.orders");
    CHECK(physical_props.output_columns == std::vector<std::string>{"order_id", "customer_id"});
    CHECK(physical_props.requires_visibility_check);
    CHECK(physical_props.partitioning_columns == std::vector<std::string>{"order_id", "customer_id"});
    CHECK(physical_props.ordering_columns.empty());
}

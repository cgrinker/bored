#include "bored/planner/cost_model.hpp"
#include "bored/planner/planner.hpp"
#include "bored/planner/statistics_catalog.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>

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
    CHECK_FALSE(root->properties().snapshot.has_value());
    CHECK(root->properties().ordering_columns.empty());
    CHECK(root->properties().partitioning_columns == root->properties().output_columns);
    CHECK(result.plan_diagnostics.rules_attempted > 0U);
    CHECK(result.plan_diagnostics.rules_applied == 0U);
    CHECK(result.plan_diagnostics.cost_evaluations == 0U);
    CHECK(result.plan_diagnostics.chosen_plan_cost == Approx(0.0));
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
    REQUIRE(result.diagnostics.size() == 12U);
    CHECK(result.diagnostics[0] == "ProjectionPruning:applied");
    CHECK(result.diagnostics[1] == "FilterPushdown:skipped");
    CHECK(result.diagnostics[2] == "JoinCommutativity:skipped");
    CHECK(result.diagnostics[3] == "JoinAssociativity:skipped");
    CHECK(result.diagnostics[4] == "JoinGreedyReorder:skipped");
    CHECK(result.diagnostics[5] == "ConstantFolding:skipped");
    CHECK(result.diagnostics[6] == "ProjectionPruning:skipped");
    CHECK(result.diagnostics[7] == "FilterPushdown:skipped");
    CHECK(result.diagnostics[8] == "JoinCommutativity:skipped");
    CHECK(result.diagnostics[9] == "JoinAssociativity:skipped");
    CHECK(result.diagnostics[10] == "JoinGreedyReorder:skipped");
    CHECK(result.diagnostics[11] == "ConstantFolding:skipped");
    CHECK(result.plan_diagnostics.rules_attempted == 12U);
    CHECK(result.plan_diagnostics.rules_applied == 1U);
    CHECK(result.plan_diagnostics.cost_evaluations == 0U);
    CHECK(result.plan_diagnostics.chosen_plan_cost == Approx(0.0));
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
    CHECK(result.plan_diagnostics.rules_attempted > 0U);
    CHECK(result.plan_diagnostics.rules_applied > 0U);
    CHECK(result.plan_diagnostics.cost_evaluations >= 2U);
    CHECK(result.plan_diagnostics.chosen_plan_cost == Approx(rotated_cost));
    REQUIRE(result.plan_diagnostics.chosen_logical_plan);
    REQUIRE_FALSE(result.plan_diagnostics.alternatives.empty());
    const auto has_rotated_cost = std::any_of(
        result.plan_diagnostics.alternatives.begin(),
        result.plan_diagnostics.alternatives.end(),
        [&](const auto& alternative) {
            return alternative.total_cost == Approx(rotated_cost);
        });
    CHECK(has_rotated_cost);
    CHECK(result.plan_diagnostics.rule_trace.size() == result.plan_diagnostics.rules_attempted);
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
    REQUIRE(physical_props.snapshot.has_value());
    const auto& snapshot_value = *physical_props.snapshot;
    CHECK(snapshot_value.read_lsn == 42U);
    CHECK(snapshot_value.xmin == 1U);
    CHECK(snapshot_value.xmax == 10U);
    CHECK(snapshot_value.in_progress.empty());
}

TEST_CASE("plan_query selects hash join when both inputs exceed threshold")
{
    auto left = make_table_scan("public.big_orders", 5000U, {"order_id"});
    auto right = make_table_scan("public.big_customers", 6000U, {"customer_id"});

    LogicalProperties join_props{};
    join_props.output_columns = {"order_id", "customer_id"};

    auto join = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{left, right},
        join_props);

    LogicalPlan plan{join};
    PlannerContext context{};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::HashJoin);
    auto partitions = root->properties().partitioning_columns;
    std::sort(partitions.begin(), partitions.end());
    CHECK(partitions == std::vector<std::string>{"customer_id", "order_id"});
}

TEST_CASE("plan_query lowers insert into physical insert operator")
{
    auto values = LogicalOperator::make(LogicalOperatorType::Values, {});

    LogicalProperties insert_props{};
    insert_props.relation_name = "public.inbox";
    insert_props.output_columns = {"message", "created_at"};

    auto insert_logical = LogicalOperator::make(
        LogicalOperatorType::Insert,
        std::vector<LogicalOperatorPtr>{values},
        insert_props);

    LogicalPlan plan{insert_logical};
    PlannerContext context{};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::Insert);
    CHECK(root->properties().relation_name == "public.inbox");
    CHECK(root->properties().output_columns == std::vector<std::string>{"message", "created_at"});
    CHECK_FALSE(root->properties().requires_visibility_check);
    CHECK_FALSE(root->properties().snapshot.has_value());
    REQUIRE(root->children().size() == 1U);
    CHECK(root->children().front()->type() == PhysicalOperatorType::Values);
}

TEST_CASE("plan_query lowers update into physical update operator with snapshot")
{
    auto scan = make_table_scan("public.accounts", 100U, {"account_id", "balance"});

    LogicalProperties update_props{};
    update_props.relation_name = "public.accounts";
    update_props.output_columns = {"account_id", "balance"};

    auto update_logical = LogicalOperator::make(
        LogicalOperatorType::Update,
        std::vector<LogicalOperatorPtr>{scan},
        update_props);

    LogicalPlan plan{update_logical};

    Snapshot snapshot{};
    snapshot.read_lsn = 128U;
    snapshot.xmin = 10U;
    snapshot.xmax = 20U;

    PlannerContextConfig config{};
    config.snapshot = snapshot;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::Update);
    CHECK(root->properties().relation_name == "public.accounts");
    CHECK(root->properties().requires_visibility_check);
    REQUIRE(root->properties().snapshot.has_value());
    const auto& physical_snapshot = *root->properties().snapshot;
    CHECK(physical_snapshot.read_lsn == 128U);
    CHECK(physical_snapshot.xmin == 10U);
    CHECK(physical_snapshot.xmax == 20U);
    REQUIRE(root->children().size() == 1U);
    CHECK(root->children().front()->type() == PhysicalOperatorType::SeqScan);
}

TEST_CASE("plan_query lowers delete into physical delete operator with snapshot")
{
    auto scan = make_table_scan("public.sessions", 200U, {"session_id"});

    LogicalProperties delete_props{};
    delete_props.relation_name = "public.sessions";
    delete_props.output_columns = {"session_id"};

    auto delete_logical = LogicalOperator::make(
        LogicalOperatorType::Delete,
        std::vector<LogicalOperatorPtr>{scan},
        delete_props);

    LogicalPlan plan{delete_logical};

    Snapshot snapshot{};
    snapshot.read_lsn = 256U;
    snapshot.xmin = 11U;
    snapshot.xmax = 21U;

    PlannerContextConfig config{};
    config.snapshot = snapshot;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    auto root = result.plan.root();
    REQUIRE(root);
    CHECK(root->type() == PhysicalOperatorType::Delete);
    CHECK(root->properties().relation_name == "public.sessions");
    CHECK(root->properties().requires_visibility_check);
    REQUIRE(root->properties().snapshot.has_value());
    const auto& physical_snapshot = *root->properties().snapshot;
    CHECK(physical_snapshot.read_lsn == 256U);
    CHECK(physical_snapshot.xmin == 11U);
    CHECK(physical_snapshot.xmax == 21U);
    REQUIRE(root->children().size() == 1U);
    CHECK(root->children().front()->type() == PhysicalOperatorType::SeqScan);
}

#include "bored/planner/cost_model.hpp"
#include "bored/planner/planner.hpp"
#include "bored/planner/statistics_catalog.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <vector>

using bored::planner::CostModel;
using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalPlan;
using bored::planner::PhysicalOperatorPtr;
using bored::planner::PhysicalOperatorType;
using bored::planner::PlannerContext;
using bored::planner::PlannerContextConfig;
using bored::planner::PlannerResult;
using bored::planner::StatisticsCatalog;
using bored::planner::TableStatistics;
using bored::txn::Snapshot;

namespace {

LogicalOperatorPtr make_table_scan(const std::string& relation,
                                   std::size_t rows,
                                   std::vector<std::string> columns)
{
    bored::planner::LogicalProperties properties{};
    properties.relation_name = relation;
    properties.estimated_cardinality = rows;
    properties.output_columns = std::move(columns);
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
}

std::vector<PhysicalOperatorType> collect_preorder(const PhysicalOperatorPtr& node)
{
    if (!node) {
        return {};
    }

    std::vector<PhysicalOperatorType> sequence;
    sequence.push_back(node->type());
    for (const auto& child : node->children()) {
        auto child_sequence = collect_preorder(child);
        sequence.insert(sequence.end(), child_sequence.begin(), child_sequence.end());
    }
    return sequence;
}

struct SmokeExecutor final {
    std::vector<PhysicalOperatorType> execute(const bored::planner::PhysicalPlan& plan) const
    {
        return collect_preorder(plan.root());
    }
};

}  // namespace

TEST_CASE("planner integration selects hash join and preserves metadata")
{
    StatisticsCatalog statistics;

    TableStatistics orders_stats;
    orders_stats.set_row_count(5000.0);
    statistics.register_table("public.orders", orders_stats);

    TableStatistics customers_stats;
    customers_stats.set_row_count(6000.0);
    statistics.register_table("public.customers", customers_stats);

    auto orders_scan = make_table_scan("public.orders", 5000U, {"order_id", "customer_id"});
    auto customers_scan = make_table_scan("public.customers", 6000U, {"customer_id"});

    bored::planner::LogicalProperties join_props{};
    join_props.output_columns = {"order_id", "customer_id"};

    auto join = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{orders_scan, customers_scan},
        join_props);

    LogicalPlan plan{join};

    CostModel cost_model{&statistics};

    PlannerContextConfig config{};
    config.statistics = &statistics;
    config.cost_model = &cost_model;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    REQUIRE(result.plan.root());

    SmokeExecutor executor;
    auto sequence = executor.execute(result.plan);
    REQUIRE(sequence.size() == 3U);
    CHECK(sequence[0] == PhysicalOperatorType::HashJoin);
    CHECK(sequence[1] == PhysicalOperatorType::SeqScan);
    CHECK(sequence[2] == PhysicalOperatorType::SeqScan);

    const auto& root_props = result.plan.root()->properties();
    CHECK(root_props.partitioning_columns.size() == 2U);
    std::vector<std::string> sorted_partitioning = root_props.partitioning_columns;
    std::sort(sorted_partitioning.begin(), sorted_partitioning.end());
    CHECK(sorted_partitioning == std::vector<std::string>{"customer_id", "order_id"});
    CHECK(result.plan_diagnostics.rules_attempted > 0U);
    CHECK(result.plan_diagnostics.cost_evaluations >= 2U);
    CHECK(result.plan_diagnostics.rule_trace.size() == result.plan_diagnostics.rules_attempted);
    REQUIRE(result.plan_diagnostics.chosen_logical_plan);
}

TEST_CASE("planner integration executes update pipeline with snapshot")
{
    auto scan = make_table_scan("public.accounts", 1000U, {"account_id", "balance"});

    bored::planner::LogicalProperties update_props{};
    update_props.relation_name = "public.accounts";
    update_props.output_columns = {"account_id", "balance"};

    auto update_logical = LogicalOperator::make(
        LogicalOperatorType::Update,
        std::vector<LogicalOperatorPtr>{scan},
        update_props);

    LogicalPlan plan{update_logical};

    bored::txn::Snapshot snapshot{};
    snapshot.read_lsn = 900U;
    snapshot.xmin = 40U;
    snapshot.xmax = 60U;

    PlannerContextConfig config{};
    config.snapshot = snapshot;
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    REQUIRE(result.plan.root());

    SmokeExecutor executor;
    auto sequence = executor.execute(result.plan);
    REQUIRE(sequence.size() == 2U);
    CHECK(sequence[0] == PhysicalOperatorType::Update);
    CHECK(sequence[1] == PhysicalOperatorType::SeqScan);

    const auto& root_props = result.plan.root()->properties();
    CHECK(root_props.relation_name == "public.accounts");
    CHECK(root_props.requires_visibility_check);
    REQUIRE(root_props.snapshot.has_value());
    const auto& physical_snapshot = *root_props.snapshot;
    CHECK(physical_snapshot.read_lsn == 900U);
    CHECK(physical_snapshot.xmin == 40U);
    CHECK(physical_snapshot.xmax == 60U);
}

TEST_CASE("planner integration handles nested projections from CTE inlining")
{
    auto scan = make_table_scan("public.inventory", 500U, {"id", "quantity"});

    bored::planner::LogicalProperties base_projection_props{};
    base_projection_props.output_columns = {"id"};
    base_projection_props.estimated_cardinality = 500U;

    auto cte_projection = LogicalOperator::make(
        LogicalOperatorType::Projection,
        std::vector<LogicalOperatorPtr>{scan},
        base_projection_props);

    bored::planner::LogicalProperties outer_projection_props{};
    outer_projection_props.output_columns = {"id"};
    outer_projection_props.estimated_cardinality = 500U;

    auto outer_projection = LogicalOperator::make(
        LogicalOperatorType::Projection,
        std::vector<LogicalOperatorPtr>{cte_projection},
        outer_projection_props);

    LogicalPlan plan{outer_projection};

    PlannerContextConfig config{};
    PlannerContext context{config};

    PlannerResult result = plan_query(context, plan);
    REQUIRE(result.plan.root());

    SmokeExecutor executor;
    auto sequence = executor.execute(result.plan);
    REQUIRE(sequence.size() == 2U);
    CHECK(sequence[0] == PhysicalOperatorType::Projection);
    CHECK(sequence[1] == PhysicalOperatorType::SeqScan);

    const auto& root_props = result.plan.root()->properties();
    REQUIRE(root_props.output_columns.size() == 1U);
    CHECK(root_props.output_columns.front() == "id");
}

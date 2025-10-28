#include "bored/planner/cost_model.hpp"
#include "bored/planner/logical_plan.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>

using bored::planner::CostEstimate;
using bored::planner::CostModel;
using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalProperties;
using bored::planner::PlanCost;
using bored::planner::StatisticsCatalog;
using bored::planner::TableStatistics;
using Catch::Approx;

namespace {

LogicalOperatorPtr make_scan(const std::string& relation, double rows)
{
    LogicalProperties properties{};
    properties.relation_name = relation;
    properties.estimated_cardinality = static_cast<std::size_t>(rows);
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
}

LogicalOperatorPtr make_filter(LogicalOperatorPtr child)
{
    LogicalProperties properties{};
    properties.estimated_cardinality = child ? child->properties().estimated_cardinality / 2U : 0U;
    return LogicalOperator::make(LogicalOperatorType::Filter, {std::move(child)}, properties);
}

LogicalOperatorPtr make_projection(LogicalOperatorPtr child)
{
    LogicalProperties properties{};
    properties.estimated_cardinality = child ? child->properties().estimated_cardinality : 0U;
    return LogicalOperator::make(LogicalOperatorType::Projection, {std::move(child)}, properties);
}

LogicalOperatorPtr make_join(LogicalOperatorPtr left, LogicalOperatorPtr right)
{
    LogicalProperties properties{};
    properties.estimated_cardinality = std::min(left ? left->properties().estimated_cardinality : 0U,
                                                right ? right->properties().estimated_cardinality : 0U);
    return LogicalOperator::make(LogicalOperatorType::Join, {std::move(left), std::move(right)}, properties);
}

}  // namespace

TEST_CASE("Cost model estimates scan cost using statistics catalog")
{
    StatisticsCatalog statistics;
    TableStatistics accounts_stats;
    accounts_stats.set_row_count(1000.0);
    statistics.register_table("public.accounts", accounts_stats);

    CostModel model{&statistics};

    auto scan = make_scan("public.accounts", 0.0);
    CostEstimate estimate = model.estimate_plan(scan);

    CHECK(estimate.output_rows == Approx(1000.0));
    CHECK(estimate.cost.io == Approx(1000.0 * 0.01));
    CHECK(estimate.cost.cpu == Approx(1000.0 * 0.001));
    CHECK(estimate.cost.total() == Approx(estimate.cost.io + estimate.cost.cpu));
}

TEST_CASE("Cost model estimates filter and projection overhead")
{
    StatisticsCatalog statistics;
    TableStatistics table_stats;
    table_stats.set_row_count(200.0);
    statistics.register_table("public.metrics", table_stats);

    CostModel model{&statistics};

    auto scan = make_scan("public.metrics", 0.0);
    auto filter = make_filter(scan);
    auto projection = make_projection(filter);

    CostEstimate estimate = model.estimate_plan(projection);
    CHECK(estimate.output_rows == Approx(100.0));
    CHECK(estimate.cost.io == Approx(200.0 * 0.01));
    CHECK(estimate.cost.cpu == Approx((200.0 * 0.001) + (200.0 * 0.0005) + (100.0 * 0.00025)));
}

TEST_CASE("Cost model estimates join cost baselined on inputs")
{
    StatisticsCatalog statistics;

    TableStatistics left_stats;
    left_stats.set_row_count(500.0);
    statistics.register_table("public.left", left_stats);

    TableStatistics right_stats;
    right_stats.set_row_count(50.0);
    statistics.register_table("public.right", right_stats);

    CostModel model{&statistics};

    auto left_scan = make_scan("public.left", 0.0);
    auto right_scan = make_scan("public.right", 0.0);
    auto join = make_join(left_scan, right_scan);

    CostEstimate estimate = model.estimate_plan(join);

    const double expected_scan_io = (500.0 + 50.0) * 0.01;
    const double expected_scan_cpu = (500.0 + 50.0) * 0.001;
    const double expected_join_io = (500.0 + 50.0) * 0.0025;
    const double expected_join_cpu = (500.0 * 50.0) * 0.00001;

    CHECK(estimate.output_rows == Approx(50.0));
    CHECK(estimate.cost.io == Approx(expected_scan_io + expected_join_io));
    CHECK(estimate.cost.cpu == Approx(expected_scan_cpu + expected_join_cpu));
}

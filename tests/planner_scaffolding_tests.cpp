#include "bored/planner/planner.hpp"

#include <catch2/catch_test_macros.hpp>

using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalPlan;
using bored::planner::LogicalProperties;
using bored::planner::PhysicalOperatorType;
using bored::planner::PlannerContext;
using bored::planner::PlannerResult;

namespace {

LogicalOperatorPtr make_scan_plan()
{
    LogicalProperties properties{};
    properties.estimated_cardinality = 42U;
    properties.output_columns = {"id", "name"};
    return LogicalOperator::make(LogicalOperatorType::TableScan, {}, properties);
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

#include "bored/planner/planner.hpp"

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
    REQUIRE(result.diagnostics.size() == 3U);
    CHECK(result.diagnostics[0] == "ProjectionPruning:applied");
    CHECK(result.diagnostics[1] == "FilterPushdown:skipped");
    CHECK(result.diagnostics[2] == "ConstantFolding:skipped");
}

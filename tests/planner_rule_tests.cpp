#include "bored/planner/planner.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"

#include <catch2/catch_test_macros.hpp>

using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalPlan;
using bored::planner::LogicalProperties;
using bored::planner::PlannerContext;
using bored::planner::PlannerContextConfig;
using bored::planner::PlannerOptions;
using bored::planner::RuleContext;
using bored::planner::RuleEngine;
using bored::planner::RuleRegistry;
using bored::planner::RuleTrace;

namespace {

LogicalOperatorPtr projection_over_scan()
{
    LogicalProperties scan_props{};
    scan_props.output_columns = {"id", "name"};
    auto scan = LogicalOperator::make(LogicalOperatorType::TableScan, {}, scan_props);

    LogicalProperties projection_props = scan_props;
    std::vector<LogicalOperatorPtr> children{scan};
    return LogicalOperator::make(LogicalOperatorType::Projection, std::move(children), projection_props);
}

LogicalOperatorPtr filter_over_projection_over_scan()
{
    auto projection = projection_over_scan();

    LogicalProperties filter_props = projection->properties();
    std::vector<LogicalOperatorPtr> children{projection};
    return LogicalOperator::make(LogicalOperatorType::Filter, std::move(children), filter_props);
}

}  // namespace

TEST_CASE("Projection pruning removes identity projection")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_projection_pruning_rule());

    PlannerContext planner_context{};
    RuleContext rule_context{&planner_context};
    RuleEngine engine{&registry};

    auto logical = projection_over_scan();
    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    REQUIRE(applied);
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "ProjectionPruning");
    CHECK(trace.applications.front().success);
    CHECK(alternatives.size() == 1U);
    CHECK(alternatives.front() == logical->children().front());
}

TEST_CASE("Rule engine respects disabled rule options")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_projection_pruning_rule());

    PlannerOptions options{};
    options.rule_options.enable_projection_pushdown = false;

    PlannerContextConfig config{};
    config.options = options;
    PlannerContext planner_context{config};
    RuleContext rule_context{&planner_context};
    RuleEngine engine{&registry, options.rule_options};

    auto logical = projection_over_scan();
    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    CHECK_FALSE(applied);
    CHECK(alternatives.empty());
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "ProjectionPruning");
    CHECK_FALSE(trace.applications.front().success);
}

TEST_CASE("Filter pushdown swaps filter and projection when projection is identity")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_filter_pushdown_rule());

    PlannerContext planner_context{};
    RuleContext rule_context{&planner_context};
    RuleEngine engine{&registry};

    auto logical = filter_over_projection_over_scan();
    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    REQUIRE(applied);
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "FilterPushdown");
    CHECK(trace.applications.front().success);
    REQUIRE(alternatives.size() == 1U);

    auto alternative = alternatives.front();
    REQUIRE(alternative);
    CHECK(alternative->type() == LogicalOperatorType::Projection);
    REQUIRE_FALSE(alternative->children().empty());
    auto pushed_filter = alternative->children().front();
    REQUIRE(pushed_filter);
    CHECK(pushed_filter->type() == LogicalOperatorType::Filter);
    REQUIRE_FALSE(pushed_filter->children().empty());
    CHECK(pushed_filter->children().front()->type() == LogicalOperatorType::TableScan);
}

TEST_CASE("Constant folding stub records attempt without alternatives")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_constant_folding_rule());

    PlannerContext planner_context{};
    RuleContext rule_context{&planner_context};
    RuleEngine engine{&registry};

    auto logical = projection_over_scan();
    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    CHECK_FALSE(applied);
    CHECK(alternatives.empty());
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "ConstantFolding");
    CHECK_FALSE(trace.applications.front().success);
}

TEST_CASE("Rule engine handles empty registry")
{
    RuleEngine engine{nullptr};
    PlannerContext planner_context{};
    RuleContext rule_context{&planner_context};
    std::vector<LogicalOperatorPtr> alternatives;

    auto applied = engine.apply_rules(rule_context, nullptr, alternatives);
    CHECK_FALSE(applied);
    CHECK(alternatives.empty());
}

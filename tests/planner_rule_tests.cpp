#include "bored/planner/planner.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/memo.hpp"
#include "bored/planner/rules/join_rules.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"

#include <catch2/catch_test_macros.hpp>

using bored::planner::LogicalOperator;
using bored::planner::LogicalOperatorPtr;
using bored::planner::LogicalOperatorType;
using bored::planner::LogicalPlan;
using bored::planner::LogicalProperties;
using bored::planner::Memo;
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

LogicalOperatorPtr join_of_scans()
{
    LogicalProperties left_props{};
    left_props.output_columns = {"a"};
    auto left = LogicalOperator::make(LogicalOperatorType::TableScan, {}, left_props);

    LogicalProperties right_props{};
    right_props.output_columns = {"b"};
    auto right = LogicalOperator::make(LogicalOperatorType::TableScan, {}, right_props);

    LogicalProperties join_props{};
    join_props.output_columns = {"a", "b"};

    return LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{left, right},
        join_props);
}

LogicalOperatorPtr left_deep_join_of_three()
{
    LogicalProperties props_a{};
    props_a.output_columns = {"a"};
    auto a = LogicalOperator::make(LogicalOperatorType::TableScan, {}, props_a);

    LogicalProperties props_b{};
    props_b.output_columns = {"b"};
    auto b = LogicalOperator::make(LogicalOperatorType::TableScan, {}, props_b);

    LogicalProperties props_c{};
    props_c.output_columns = {"c"};
    auto c = LogicalOperator::make(LogicalOperatorType::TableScan, {}, props_c);

    LogicalProperties props_ab{};
    props_ab.output_columns = {"a", "b"};
    auto ab = LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{a, b},
        props_ab);

    LogicalProperties props_abc{};
    props_abc.output_columns = {"a", "b", "c"};

    return LogicalOperator::make(
        LogicalOperatorType::Join,
        std::vector<LogicalOperatorPtr>{ab, c},
        props_abc);
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

TEST_CASE("Join commutativity produces swapped alternative and registers with memo")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_join_commutativity_rule());

    PlannerContext planner_context{};
    Memo memo;
    auto logical = join_of_scans();
    auto group = memo.add_group(logical);
    RuleContext rule_context{&planner_context, &memo, group};
    RuleEngine engine{&registry};

    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    REQUIRE(applied);
    REQUIRE(alternatives.size() == 1U);
    auto alternative = alternatives.front();
    REQUIRE(alternative);
    CHECK(alternative->type() == LogicalOperatorType::Join);
    REQUIRE(alternative->children().size() == 2U);
    CHECK(alternative->children()[0] == logical->children()[1]);
    CHECK(alternative->children()[1] == logical->children()[0]);

    const auto* group_ptr = memo.find_group(group);
    REQUIRE(group_ptr);
    CHECK(group_ptr->expressions().size() == 2U);
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "JoinCommutativity");
    CHECK(trace.applications.front().success);
}

TEST_CASE("Join associativity rotates left-deep join")
{
    RuleRegistry registry;
    registry.register_rule(bored::planner::make_join_associativity_rule());

    PlannerContext planner_context{};
    Memo memo;
    auto logical = left_deep_join_of_three();
    auto group = memo.add_group(logical);
    RuleContext rule_context{&planner_context, &memo, group};
    RuleEngine engine{&registry};

    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    auto applied = engine.apply_rules(rule_context, logical, alternatives, &trace);

    REQUIRE(applied);
    REQUIRE(alternatives.size() == 1U);
    auto alternative = alternatives.front();
    REQUIRE(alternative);
    CHECK(alternative->type() == LogicalOperatorType::Join);
    REQUIRE(alternative->children().size() == 2U);

    auto new_left = alternative->children()[0];
    auto new_right = alternative->children()[1];
    REQUIRE(new_left);
    REQUIRE(new_right);
    CHECK(new_right->type() == LogicalOperatorType::Join);
    REQUIRE(new_right->children().size() == 2U);
    CHECK(new_left == logical->children()[0]->children()[0]);
    CHECK(new_right->children()[0] == logical->children()[0]->children()[1]);
    CHECK(new_right->children()[1] == logical->children()[1]);

    const auto* group_ptr = memo.find_group(group);
    REQUIRE(group_ptr);
    CHECK(group_ptr->expressions().size() == 2U);
    REQUIRE(trace.applications.size() == 1U);
    CHECK(trace.applications.front().rule_name == "JoinAssociativity");
    CHECK(trace.applications.front().success);
}

TEST_CASE("Memo reuses groups for equivalent expressions")
{
    Memo memo;

    LogicalProperties scan_props{};
    scan_props.output_columns = {"id"};

    auto first_scan = LogicalOperator::make(LogicalOperatorType::TableScan, {}, scan_props);
    auto second_scan = LogicalOperator::make(LogicalOperatorType::TableScan, {}, scan_props);

    auto first_group = memo.add_group(first_scan);
    auto second_group = memo.add_group(second_scan);

    REQUIRE(first_group == second_group);

    const auto* group = memo.find_group(first_group);
    REQUIRE(group != nullptr);
    CHECK(group->expressions().size() == 2U);

    std::size_t materialize_count = 0U;
    for (const auto& expression : group->expressions()) {
        if (expression->type() == LogicalOperatorType::Materialize) {
            ++materialize_count;
        }
    }
    CHECK(materialize_count == 1U);

    LogicalProperties projection_props = scan_props;
    auto projection_one = LogicalOperator::make(
        LogicalOperatorType::Projection,
        std::vector<LogicalOperatorPtr>{first_scan},
        projection_props);
    memo.add_expression(first_group, projection_one);

    auto projection_two = LogicalOperator::make(
        LogicalOperatorType::Projection,
        std::vector<LogicalOperatorPtr>{second_scan},
        projection_props);
    memo.add_expression(first_group, projection_two);

    group = memo.find_group(first_group);
    REQUIRE(group != nullptr);
    CHECK(group->expressions().size() == 3U);

    materialize_count = 0U;
    for (const auto& expression : group->expressions()) {
        if (expression->type() == LogicalOperatorType::Materialize) {
            ++materialize_count;
        }
    }
    CHECK(materialize_count == 1U);
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

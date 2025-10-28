#include "bored/planner/planner.hpp"
#include "bored/planner/rule.hpp"
#include "bored/planner/rules/predicate_pushdown_rule.hpp"

#include <utility>

namespace bored::planner {
namespace {

PhysicalOperatorType to_physical(LogicalOperatorType type) noexcept
{
    switch (type) {
    case LogicalOperatorType::Projection:
        return PhysicalOperatorType::Projection;
    case LogicalOperatorType::Filter:
        return PhysicalOperatorType::Filter;
    case LogicalOperatorType::Join:
        return PhysicalOperatorType::NestedLoopJoin;
    case LogicalOperatorType::TableScan:
        return PhysicalOperatorType::SeqScan;
    case LogicalOperatorType::Values:
        return PhysicalOperatorType::Values;
    case LogicalOperatorType::Invalid:
    default:
        return PhysicalOperatorType::NoOp;
    }
}

PhysicalOperatorPtr lower_placeholder(const LogicalOperatorPtr& logical)
{
    if (!logical) {
        return PhysicalOperator::make(PhysicalOperatorType::NoOp);
    }

    std::vector<PhysicalOperatorPtr> lowered_children;
    lowered_children.reserve(logical->children().size());
    for (const auto& child : logical->children()) {
        lowered_children.push_back(lower_placeholder(child));
    }

    PhysicalProperties properties{};
    properties.expected_cardinality = logical->properties().estimated_cardinality;
    properties.preserves_order = logical->properties().preserves_order;
    properties.output_columns = logical->properties().output_columns;

    return PhysicalOperator::make(to_physical(logical->type()), std::move(lowered_children), std::move(properties));
}

const RuleRegistry& default_rule_registry()
{
    static RuleRegistry registry = [] {
        RuleRegistry reg;
        reg.register_rule(make_projection_pruning_rule());
        reg.register_rule(make_filter_pushdown_rule());
        reg.register_rule(make_constant_folding_rule());
        return reg;
    }();
    return registry;
}

}  // namespace

PlannerResult plan_query(const PlannerContext& context, const LogicalPlan& plan)
{
    PlannerResult result{};
    auto root = plan.root();
    if (!root) {
        result.diagnostics.emplace_back("logical plan is empty");
        return result;
    }

    RuleEngine engine{&default_rule_registry(), context.options().rule_options};
    RuleContext rule_context{&context};
    std::vector<LogicalOperatorPtr> alternatives;
    RuleTrace trace{};
    engine.apply_rules(rule_context, root, alternatives, context.options().enable_rule_tracing ? &trace : nullptr);

    if (!alternatives.empty()) {
        root = alternatives.front();
    }

    result.plan = PhysicalPlan{lower_placeholder(root)};
    if (context.options().enable_rule_tracing) {
        for (const auto& application : trace.applications) {
            result.diagnostics.push_back(application.rule_name + (application.success ? ":applied" : ":skipped"));
        }
    }
    return result;
}

}  // namespace bored::planner

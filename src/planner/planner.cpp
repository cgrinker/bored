#include "bored/planner/planner.hpp"

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

}  // namespace

PlannerResult plan_query(const PlannerContext&, const LogicalPlan& plan)
{
    PlannerResult result{};
    auto root = plan.root();
    if (!root) {
        result.diagnostics.emplace_back("logical plan is empty");
        return result;
    }

    result.plan = PhysicalPlan{lower_placeholder(root)};
    return result;
}

}  // namespace bored::planner

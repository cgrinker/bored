#include "bored/planner/physical_plan.hpp"

#include <utility>

namespace bored::planner {

PhysicalOperator::PhysicalOperator(PhysicalOperatorType type,
                                   std::vector<PhysicalOperatorPtr> children,
                                   PhysicalProperties properties)
    : type_{type}
    , children_{std::move(children)}
    , properties_{std::move(properties)}
{
}

PhysicalOperatorType PhysicalOperator::type() const noexcept
{
    return type_;
}

const std::vector<PhysicalOperatorPtr>& PhysicalOperator::children() const noexcept
{
    return children_;
}

const PhysicalProperties& PhysicalOperator::properties() const noexcept
{
    return properties_;
}

PhysicalOperatorPtr PhysicalOperator::make(PhysicalOperatorType type,
                                           std::vector<PhysicalOperatorPtr> children,
                                           PhysicalProperties properties)
{
    return std::make_shared<PhysicalOperator>(type, std::move(children), std::move(properties));
}

PhysicalPlan::PhysicalPlan(PhysicalOperatorPtr root)
    : root_{std::move(root)}
{
}

PhysicalOperatorPtr PhysicalPlan::root() const noexcept
{
    return root_;
}

}  // namespace bored::planner

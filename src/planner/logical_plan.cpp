#include "bored/planner/logical_plan.hpp"

#include <utility>

namespace bored::planner {

LogicalOperator::LogicalOperator(LogicalOperatorType type,
                                 std::vector<LogicalOperatorPtr> children,
                                 LogicalProperties properties)
    : type_{type}
    , children_{std::move(children)}
    , properties_{std::move(properties)}
{
}

LogicalOperatorType LogicalOperator::type() const noexcept
{
    return type_;
}

const std::vector<LogicalOperatorPtr>& LogicalOperator::children() const noexcept
{
    return children_;
}

const LogicalProperties& LogicalOperator::properties() const noexcept
{
    return properties_;
}

LogicalOperatorPtr LogicalOperator::make(LogicalOperatorType type,
                                         std::vector<LogicalOperatorPtr> children,
                                         LogicalProperties properties)
{
    return std::make_shared<LogicalOperator>(type, std::move(children), std::move(properties));
}

LogicalPlan::LogicalPlan(LogicalOperatorPtr root)
    : root_{std::move(root)}
{
}

LogicalOperatorPtr LogicalPlan::root() const noexcept
{
    return root_;
}

}  // namespace bored::planner

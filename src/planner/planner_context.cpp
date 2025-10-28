#include "bored/planner/planner_context.hpp"

#include <utility>

namespace bored::planner {

PlannerContext::PlannerContext(PlannerContextConfig config)
    : config_{std::move(config)}
{
}

const bored::catalog::CatalogAccessor* PlannerContext::catalog() const noexcept
{
    return config_.catalog;
}

const StatisticsCatalog* PlannerContext::statistics() const noexcept
{
    return config_.statistics;
}

const txn::Snapshot& PlannerContext::snapshot() const noexcept
{
    return config_.snapshot;
}

const PlannerOptions& PlannerContext::options() const noexcept
{
    return config_.options;
}

}  // namespace bored::planner

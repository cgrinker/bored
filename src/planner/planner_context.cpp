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

const CostModel* PlannerContext::cost_model() const noexcept
{
    return config_.cost_model;
}

const txn::Snapshot& PlannerContext::snapshot() const noexcept
{
    return config_.snapshot;
}

const PlannerOptions& PlannerContext::options() const noexcept
{
    return config_.options;
}

PlannerTelemetry* PlannerContext::telemetry() const noexcept
{
    return config_.telemetry;
}

std::uint64_t PlannerContext::allocate_executor_operator_id() const noexcept
{
    return next_executor_operator_id_++;
}

}  // namespace bored::planner

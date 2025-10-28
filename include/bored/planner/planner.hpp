#pragma once

#include "bored/planner/logical_plan.hpp"
#include "bored/planner/physical_plan.hpp"
#include "bored/planner/planner_context.hpp"

#include <string>
#include <vector>

namespace bored::planner {

struct PlannerResult final {
    PhysicalPlan plan{};
    std::vector<std::string> diagnostics{};
    std::size_t rules_attempted = 0U;
    std::size_t rules_applied = 0U;
    std::size_t cost_evaluations = 0U;
    double chosen_plan_cost = 0.0;
};

[[nodiscard]] PlannerResult plan_query(const PlannerContext& context, const LogicalPlan& plan);

}  // namespace bored::planner

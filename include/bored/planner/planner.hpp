#pragma once

#include "bored/planner/logical_plan.hpp"
#include "bored/planner/physical_plan.hpp"
#include "bored/planner/plan_diagnostics.hpp"
#include "bored/planner/planner_context.hpp"

#include <string>
#include <vector>

namespace bored::planner {

struct PlannerResult final {
    PhysicalPlan plan{};
    std::vector<std::string> diagnostics{};
    PlanDiagnostics plan_diagnostics{};
};

[[nodiscard]] PlannerResult plan_query(const PlannerContext& context, const LogicalPlan& plan);

}  // namespace bored::planner

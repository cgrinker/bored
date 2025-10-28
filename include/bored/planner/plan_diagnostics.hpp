#pragma once

#include "bored/planner/logical_plan.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace bored::planner {

struct RuleTraceEntry final {
    std::string rule_name;
    bool applied = false;
};

struct PlanAlternative final {
    LogicalOperatorPtr logical_plan;
    double total_cost = 0.0;
};

struct PlanDiagnostics final {
    std::size_t rules_attempted = 0U;
    std::size_t rules_applied = 0U;
    std::size_t cost_evaluations = 0U;
    double chosen_plan_cost = 0.0;
    LogicalOperatorPtr chosen_logical_plan{};
    std::vector<RuleTraceEntry> rule_trace{};
    std::vector<PlanAlternative> alternatives{};
};

}  // namespace bored::planner

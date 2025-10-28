#pragma once

#include "bored/planner/physical_plan.hpp"

#include <string>

namespace bored::planner {

struct ExplainOptions final {
    bool include_properties = true;
    bool include_snapshot = false;
};

[[nodiscard]] std::string explain_plan(const PhysicalPlan& plan, ExplainOptions options = {});

}  // namespace bored::planner

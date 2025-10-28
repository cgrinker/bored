#pragma once

#include "bored/planner/physical_plan.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>

namespace bored::planner {

struct ExplainRuntimeStats final {
    std::uint64_t loops = 0U;
    std::uint64_t rows = 0U;
    std::uint64_t total_duration_ns = 0U;
    std::uint64_t last_duration_ns = 0U;
};

using ExplainRuntimeMap = std::unordered_map<std::uint64_t, ExplainRuntimeStats>;

struct ExplainOptions final {
    bool include_properties = true;
    bool include_snapshot = false;
    const ExplainRuntimeMap* runtime_stats = nullptr;
};

[[nodiscard]] std::string explain_plan(const PhysicalPlan& plan, ExplainOptions options = {});

}  // namespace bored::planner

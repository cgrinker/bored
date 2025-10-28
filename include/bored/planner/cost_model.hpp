#pragma once

#include "bored/planner/logical_plan.hpp"
#include "bored/planner/statistics_catalog.hpp"

#include <optional>

namespace bored::planner {

struct PlanCost final {
    double io = 0.0;
    double cpu = 0.0;

    [[nodiscard]] double total() const noexcept
    {
        return io + cpu;
    }
};

struct CostEstimate final {
    PlanCost cost{};
    double output_rows = 0.0;
};

class CostModel final {
public:
    explicit CostModel(const StatisticsCatalog* statistics) noexcept;

    [[nodiscard]] CostEstimate estimate_plan(const LogicalOperatorPtr& root) const;

private:
    const StatisticsCatalog* statistics_ = nullptr;

    [[nodiscard]] CostEstimate estimate_node(const LogicalOperatorPtr& node) const;
    [[nodiscard]] double lookup_table_rows(const LogicalProperties& properties) const noexcept;
};

}  // namespace bored::planner

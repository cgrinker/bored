#pragma once

#include "bored/planner/rule.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstdint>
#include <optional>

namespace bored::catalog {
class CatalogAccessor;
}

namespace bored::planner {

class StatisticsCatalog;
class CostModel;
class PlannerTelemetry;

struct PlannerOptions final {
    bool enable_rule_tracing = false;
    bool enable_costing = true;
    PlannerRuleOptions rule_options{};
};

struct PlannerContextConfig final {
    const bored::catalog::CatalogAccessor* catalog = nullptr;
    const StatisticsCatalog* statistics = nullptr;
    const CostModel* cost_model = nullptr;
    txn::Snapshot snapshot{};
    PlannerOptions options{};
    PlannerTelemetry* telemetry = nullptr;
};

class PlannerContext final {
public:
    PlannerContext() = default;
    explicit PlannerContext(PlannerContextConfig config);

    [[nodiscard]] const bored::catalog::CatalogAccessor* catalog() const noexcept;
    [[nodiscard]] const StatisticsCatalog* statistics() const noexcept;
    [[nodiscard]] const CostModel* cost_model() const noexcept;
    [[nodiscard]] const txn::Snapshot& snapshot() const noexcept;
    [[nodiscard]] const PlannerOptions& options() const noexcept;
    [[nodiscard]] PlannerTelemetry* telemetry() const noexcept;
    [[nodiscard]] std::uint64_t allocate_executor_operator_id() const noexcept;
    [[nodiscard]] std::uint64_t allocate_worktable_id() const noexcept;

private:
    PlannerContextConfig config_{};
    mutable std::uint64_t next_executor_operator_id_ = 1U;
    mutable std::uint64_t next_worktable_id_ = 1U;
};

}  // namespace bored::planner

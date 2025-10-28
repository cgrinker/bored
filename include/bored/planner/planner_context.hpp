#pragma once

#include "bored/planner/rule.hpp"
#include "bored/txn/transaction_types.hpp"

#include <optional>

namespace bored::catalog {
class CatalogAccessor;
}

namespace bored::planner {

struct PlannerOptions final {
    bool enable_rule_tracing = false;
    bool enable_costing = true;
    PlannerRuleOptions rule_options{};
};

struct PlannerContextConfig final {
    const bored::catalog::CatalogAccessor* catalog = nullptr;
    txn::Snapshot snapshot{};
    PlannerOptions options{};
};

class PlannerContext final {
public:
    PlannerContext() = default;
    explicit PlannerContext(PlannerContextConfig config);

    [[nodiscard]] const bored::catalog::CatalogAccessor* catalog() const noexcept;
    [[nodiscard]] const txn::Snapshot& snapshot() const noexcept;
    [[nodiscard]] const PlannerOptions& options() const noexcept;

private:
    PlannerContextConfig config_{};
};

}  // namespace bored::planner

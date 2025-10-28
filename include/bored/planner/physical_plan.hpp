#pragma once

#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace bored::planner {

enum class PhysicalOperatorType {
    NoOp,
    Projection,
    Filter,
    SeqScan,
    NestedLoopJoin,
    HashJoin,
    Values,
    Insert,
    Update,
    Delete
};

struct PhysicalProperties final {
    std::size_t expected_cardinality = 0U;
    bool preserves_order = false;
    bool requires_visibility_check = false;
    std::optional<txn::Snapshot> snapshot{};
    std::string relation_name{};
    std::vector<std::string> ordering_columns{};
    std::vector<std::string> partitioning_columns{};
    std::vector<std::string> output_columns{};
};

class PhysicalOperator;
using PhysicalOperatorPtr = std::shared_ptr<const PhysicalOperator>;

class PhysicalOperator final {
public:
    PhysicalOperator(PhysicalOperatorType type,
                     std::vector<PhysicalOperatorPtr> children = {},
                     PhysicalProperties properties = {});

    [[nodiscard]] PhysicalOperatorType type() const noexcept;
    [[nodiscard]] const std::vector<PhysicalOperatorPtr>& children() const noexcept;
    [[nodiscard]] const PhysicalProperties& properties() const noexcept;

    static PhysicalOperatorPtr make(PhysicalOperatorType type,
                                    std::vector<PhysicalOperatorPtr> children = {},
                                    PhysicalProperties properties = {});

private:
    PhysicalOperatorType type_ = PhysicalOperatorType::NoOp;
    std::vector<PhysicalOperatorPtr> children_{};
    PhysicalProperties properties_{};
};

class PhysicalPlan final {
public:
    PhysicalPlan() = default;
    explicit PhysicalPlan(PhysicalOperatorPtr root);

    [[nodiscard]] PhysicalOperatorPtr root() const noexcept;

private:
    PhysicalOperatorPtr root_{};
};

}  // namespace bored::planner

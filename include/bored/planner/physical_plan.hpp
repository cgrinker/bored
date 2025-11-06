#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/planner/scalar_literal.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace bored::catalog {
struct RelationId;
struct ConstraintId;
struct IndexId;
}

namespace bored::planner {

enum class PhysicalOperatorType {
    NoOp,
    Projection,
    Filter,
    SeqScan,
    IndexScan,
    NestedLoopJoin,
    HashJoin,
    Values,
    Materialize,
    Insert,
    Update,
    Delete,
    UniqueEnforce,
    ForeignKeyCheck
};

struct UniqueEnforcementProperties final {
    catalog::ConstraintId constraint_id{};
    catalog::RelationId relation_id{};
    catalog::IndexId backing_index_id{};
    std::string constraint_name{};
    std::vector<std::string> key_columns{};
    bool is_primary_key = false;
};

struct ForeignKeyEnforcementProperties final {
    catalog::ConstraintId constraint_id{};
    catalog::RelationId relation_id{};
    catalog::RelationId referenced_relation_id{};
    catalog::IndexId backing_index_id{};
    std::string constraint_name{};
    std::vector<std::string> referencing_columns{};
    std::vector<std::string> referenced_columns{};
};

struct MaterializeProperties final {
    std::uint64_t worktable_id = 0U;
    bool enable_recursive_cursor = false;
};

struct IndexScanProperties final {
    catalog::IndexId index_id{};
    std::string index_name{};
    std::vector<std::string> key_columns{};
    std::vector<ScalarLiteralValue> key_values{};
    bool unique = false;
    double estimated_selectivity = 1.0;
    bool enable_heap_fallback = true;
};

struct PhysicalProperties final {
    std::size_t expected_cardinality = 0U;
    bool preserves_order = false;
    bool requires_visibility_check = false;
    std::optional<txn::Snapshot> snapshot{};
    std::string relation_name{};
    catalog::RelationId relation_id{};
    std::vector<std::string> ordering_columns{};
    std::vector<std::string> partitioning_columns{};
    std::vector<std::string> output_columns{};
    std::uint64_t executor_operator_id = 0U;
    std::size_t expected_batch_size = 0U;
    std::string executor_strategy{};
    std::optional<UniqueEnforcementProperties> unique_enforcement{};
    std::optional<ForeignKeyEnforcementProperties> foreign_key_enforcement{};
    std::optional<MaterializeProperties> materialize{};
    std::optional<IndexScanProperties> index_scan{};
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

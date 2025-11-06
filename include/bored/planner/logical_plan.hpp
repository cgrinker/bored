#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/planner/scalar_literal.hpp"
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace bored::catalog {
class CatalogAccessor;
struct RelationId;
}

namespace bored::planner {

enum class LogicalOperatorType {
    Invalid,
    Projection,
    Filter,
    Join,
    TableScan,
    Values,
    Materialize,
    Insert,
    Update,
    Delete
};

struct EqualityPredicate final {
    std::string column{};
    ScalarLiteralValue literal{};
};

struct IndexBinding final {
    catalog::IndexId index_id{};
    std::string name{};
    std::vector<std::string> key_columns{};
    bool unique = false;
};

struct LogicalProperties final {
    std::size_t estimated_cardinality = 0U;
    bool preserves_order = false;
    bool requires_recursive_cursor = false;
    std::string relation_name{};
    catalog::RelationId relation_id{};
    std::vector<std::string> output_columns{};
    std::vector<EqualityPredicate> equality_predicates{};
    std::vector<IndexBinding> available_indexes{};
};

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<const LogicalOperator>;

class LogicalOperator final {
public:
    LogicalOperator(LogicalOperatorType type,
                    std::vector<LogicalOperatorPtr> children = {},
                    LogicalProperties properties = {});

    [[nodiscard]] LogicalOperatorType type() const noexcept;
    [[nodiscard]] const std::vector<LogicalOperatorPtr>& children() const noexcept;
    [[nodiscard]] const LogicalProperties& properties() const noexcept;

    static LogicalOperatorPtr make(LogicalOperatorType type,
                                   std::vector<LogicalOperatorPtr> children = {},
                                   LogicalProperties properties = {});

private:
    LogicalOperatorType type_ = LogicalOperatorType::Invalid;
    std::vector<LogicalOperatorPtr> children_{};
    LogicalProperties properties_{};
};

class LogicalPlan final {
public:
    LogicalPlan() = default;
    explicit LogicalPlan(LogicalOperatorPtr root);

    [[nodiscard]] LogicalOperatorPtr root() const noexcept;

private:
    LogicalOperatorPtr root_{};
};

}  // namespace bored::planner

#pragma once

#include "bored/parser/relational/ast.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace bored::parser::relational {

struct LogicalColumn final {
    std::string name{};
    ScalarType type = ScalarType::Unknown;
    bool nullable = true;
};

enum class LogicalOperatorKind : std::uint8_t {
    Scan = 0,
    Project,
    Filter,
    Join,
    Aggregate,
    Sort,
    Limit
};

struct LogicalOperator;
struct LogicalScan;
struct LogicalProject;
struct LogicalFilter;
struct LogicalJoin;
struct LogicalAggregate;
struct LogicalSort;
struct LogicalLimit;

class LogicalOperatorVisitor {
public:
    virtual ~LogicalOperatorVisitor() = default;
    virtual void visit(const LogicalScan& op) = 0;
    virtual void visit(const LogicalProject& op) = 0;
    virtual void visit(const LogicalFilter& op) = 0;
    virtual void visit(const LogicalJoin& op) = 0;
    virtual void visit(const LogicalAggregate& op) = 0;
    virtual void visit(const LogicalSort& op) = 0;
    virtual void visit(const LogicalLimit& op) = 0;
};

struct LogicalOperator {
    explicit LogicalOperator(LogicalOperatorKind kind) noexcept : kind(kind) {}
    LogicalOperator(const LogicalOperator&) = delete;
    LogicalOperator& operator=(const LogicalOperator&) = delete;
    LogicalOperator(LogicalOperator&&) noexcept = default;
    LogicalOperator& operator=(LogicalOperator&&) noexcept = default;
    virtual ~LogicalOperator() = default;

    void accept(LogicalOperatorVisitor& visitor) const;

    LogicalOperatorKind kind;
    std::vector<LogicalColumn> output_schema{};
};

using LogicalOperatorPtr = std::unique_ptr<LogicalOperator>;

struct LogicalScan final : LogicalOperator {
    LogicalScan() noexcept : LogicalOperator(LogicalOperatorKind::Scan) {}

    TableBinding table{};
    std::vector<ColumnBinding> columns{};
};

struct LogicalProject final : LogicalOperator {
    struct Projection final {
        Expression* expression = nullptr;
        std::optional<std::string> alias{};
    };

    LogicalProject() noexcept : LogicalOperator(LogicalOperatorKind::Project) {}

    std::vector<Projection> projections{};
    LogicalOperatorPtr input{};
};

struct LogicalFilter final : LogicalOperator {
    LogicalFilter() noexcept : LogicalOperator(LogicalOperatorKind::Filter) {}

    Expression* predicate = nullptr;
    LogicalOperatorPtr input{};
};

struct LogicalJoin final : LogicalOperator {
    LogicalJoin() noexcept : LogicalOperator(LogicalOperatorKind::Join) {}

    JoinType join_type = JoinType::Inner;
    Expression* predicate = nullptr;
    LogicalOperatorPtr left{};
    LogicalOperatorPtr right{};
};

struct LogicalAggregate final : LogicalOperator {
    struct Aggregate final {
        Expression* expression = nullptr;
        std::optional<std::string> alias{};
    };

    LogicalAggregate() noexcept : LogicalOperator(LogicalOperatorKind::Aggregate) {}

    std::vector<Expression*> group_keys{};
    std::vector<Aggregate> aggregates{};
    LogicalOperatorPtr input{};
};

struct LogicalSort final : LogicalOperator {
    struct SortKey final {
        Expression* expression = nullptr;
        OrderByItem::Direction direction = OrderByItem::Direction::Ascending;
    };

    LogicalSort() noexcept : LogicalOperator(LogicalOperatorKind::Sort) {}

    std::vector<SortKey> keys{};
    LogicalOperatorPtr input{};
};

struct LogicalLimit final : LogicalOperator {
    LogicalLimit() noexcept : LogicalOperator(LogicalOperatorKind::Limit) {}

    Expression* row_count = nullptr;
    Expression* offset = nullptr;
    LogicalOperatorPtr input{};
};

inline void LogicalOperator::accept(LogicalOperatorVisitor& visitor) const
{
    switch (kind) {
    case LogicalOperatorKind::Scan:
        visitor.visit(static_cast<const LogicalScan&>(*this));
        break;
    case LogicalOperatorKind::Project:
        visitor.visit(static_cast<const LogicalProject&>(*this));
        break;
    case LogicalOperatorKind::Filter:
        visitor.visit(static_cast<const LogicalFilter&>(*this));
        break;
    case LogicalOperatorKind::Join:
        visitor.visit(static_cast<const LogicalJoin&>(*this));
        break;
    case LogicalOperatorKind::Aggregate:
        visitor.visit(static_cast<const LogicalAggregate&>(*this));
        break;
    case LogicalOperatorKind::Sort:
        visitor.visit(static_cast<const LogicalSort&>(*this));
        break;
    case LogicalOperatorKind::Limit:
        visitor.visit(static_cast<const LogicalLimit&>(*this));
        break;
    default:
        break;
    }
}

}  // namespace bored::parser::relational

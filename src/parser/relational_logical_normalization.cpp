#include "bored/parser/relational/logical_normalization.hpp"

#include <utility>

namespace bored::parser::relational {
namespace {

void collect_predicates(const Expression* expression, std::vector<const Expression*>& output)
{
    if (expression == nullptr) {
        return;
    }

    // AND support is not yet implemented in the grammar; keep the expression intact.
    output.push_back(expression);
}

void normalize_node(const LogicalOperator& node, NormalizationResult& result)
{
    switch (node.kind) {
    case LogicalOperatorKind::Filter: {
        const auto& filter = static_cast<const LogicalFilter&>(node);
        FilterInfo info{};
        info.node = &filter;
        collect_predicates(filter.predicate, info.predicates);
        result.filters.push_back(std::move(info));
        if (filter.input != nullptr) {
            normalize_node(*filter.input, result);
        }
        break;
    }
    case LogicalOperatorKind::Project: {
        const auto& project = static_cast<const LogicalProject&>(node);
        ProjectionInfo info{};
        info.node = &project;
        for (const auto& projection : project.projections) {
            info.projections.push_back(&projection);
        }
        result.projections.push_back(std::move(info));
        if (project.input != nullptr) {
            normalize_node(*project.input, result);
        }
        break;
    }
    case LogicalOperatorKind::Sort: {
        const auto& sort = static_cast<const LogicalSort&>(node);
        if (sort.input != nullptr) {
            normalize_node(*sort.input, result);
        }
        break;
    }
    case LogicalOperatorKind::Limit: {
        const auto& limit = static_cast<const LogicalLimit&>(node);
        if (limit.input != nullptr) {
            normalize_node(*limit.input, result);
        }
        break;
    }
    case LogicalOperatorKind::Aggregate: {
        const auto& aggregate = static_cast<const LogicalAggregate&>(node);
        if (aggregate.input != nullptr) {
            normalize_node(*aggregate.input, result);
        }
        break;
    }
    case LogicalOperatorKind::Scan: {
        break;
    }
    default:
        break;
    }
}

}  // namespace

NormalizationResult normalize_plan(const LogicalOperator& root)
{
    NormalizationResult result{};
    normalize_node(root, result);
    return result;
}

}  // namespace bored::parser::relational

#pragma once

#include "bored/parser/relational/logical_plan.hpp"

#include <vector>

namespace bored::parser::relational {

struct FilterInfo final {
    const LogicalFilter* node = nullptr;
    std::vector<const Expression*> predicates{};
};

struct ProjectionInfo final {
    const LogicalProject* node = nullptr;
    std::vector<const LogicalProject::Projection*> projections{};
};

struct JoinCriteria final {
    const LogicalJoin* node = nullptr;
    JoinType join_type = JoinType::Inner;
    const Expression* predicate = nullptr;
    std::vector<const Expression*> equi_conditions{};
};

struct NormalizationResult final {
    std::vector<FilterInfo> filters{};
    std::vector<ProjectionInfo> projections{};
    std::vector<JoinCriteria> joins{};
};

NormalizationResult normalize_plan(const LogicalOperator& root);

}  // namespace bored::parser::relational

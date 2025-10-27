#pragma once

#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/ast.hpp"
#include "bored/parser/relational/logical_plan.hpp"

#include <functional>
#include <memory>
#include <vector>

namespace bored::parser::relational {

struct LoweringConfig final {
    std::function<void(const LogicalOperator&)> plan_sink{};
};

struct LoweringResult final {
    LogicalOperatorPtr plan{};
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return plan != nullptr && diagnostics.empty(); }
};

LoweringResult lower_select(const SelectStatement& statement, const LoweringConfig& config);
LoweringResult lower_select(const SelectStatement& statement);

}  // namespace bored::parser::relational

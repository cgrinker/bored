#pragma once

#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bored::parser::relational {

struct LogicalPlanDumpResult final {
    std::string plan_text{};
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return !plan_text.empty() && diagnostics.empty(); }
};

struct LogicalPlanDumpOptions final {
    BinderConfig binder_config{};
};

LogicalPlanDumpResult dump_select_plan(std::string_view sql, const LogicalPlanDumpOptions& options);

}  // namespace bored::parser::relational

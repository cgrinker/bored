#pragma once

#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/logical_normalization.hpp"

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bored::parser::relational {

struct LogicalPlanDumpResult final {
    std::string plan_text{};
    std::vector<ParserDiagnostic> diagnostics{};
    std::optional<NormalizationResult> normalization{};

    [[nodiscard]] bool success() const noexcept { return !plan_text.empty() && diagnostics.empty(); }
};

struct LogicalPlanDumpOptions final {
    BinderConfig binder_config{};
    bool include_normalization = false;
    std::function<void(std::string_view)> plan_text_sink{};
};

LogicalPlanDumpResult dump_select_plan(std::string_view sql, const LogicalPlanDumpOptions& options);
LogicalPlanDumpResult dump_select_plan(std::string_view sql,
                                       LogicalPlanDumpOptions options,
                                       const std::function<void(std::string_view)>& sink);

}  // namespace bored::parser::relational

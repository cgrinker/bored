#include "bored/parser/relational/logical_plan_dump.hpp"

#include "bored/parser/relational/logical_lowering.hpp"
#include "bored/parser/relational/logical_plan_printer.hpp"

#include <utility>

namespace bored::parser::relational {

namespace {

void append_diagnostics(std::vector<ParserDiagnostic>& target, std::vector<ParserDiagnostic> source)
{
    for (auto& diagnostic : source) {
        target.push_back(std::move(diagnostic));
    }
}

}  // namespace

LogicalPlanDumpResult dump_select_plan(std::string_view sql, const LogicalPlanDumpOptions& options)
{
    LogicalPlanDumpResult result{};

    auto parse_result = bored::parser::parse_select(sql);
    append_diagnostics(result.diagnostics, std::move(parse_result.diagnostics));
    if (!parse_result.success()) {
        return result;
    }

    if (options.binder_config.catalog == nullptr) {
        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Logical plan dump requires a binder catalog.";
        diagnostic.remediation_hints = {"Provide a BinderCatalog implementation via LogicalPlanDumpOptions."};
        result.diagnostics.push_back(std::move(diagnostic));
        return result;
    }

    auto binding = bind_select(options.binder_config, *parse_result.statement);
    append_diagnostics(result.diagnostics, std::move(binding.diagnostics));
    if (!binding.success()) {
        return result;
    }

    auto lowering = lower_select(*parse_result.statement);
    append_diagnostics(result.diagnostics, std::move(lowering.diagnostics));
    if (!lowering.success()) {
        return result;
    }

    result.plan_text = describe_plan(*lowering.plan);
    return result;
}

}  // namespace bored::parser::relational

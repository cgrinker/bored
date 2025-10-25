#include "bored/ddl/parser_diagnostics.hpp"

#if BORED_ENABLE_PARSER

#include <algorithm>
#include <string>
#include <vector>

namespace bored::ddl {

namespace {

int severity_rank(parser::ParserSeverity severity) noexcept
{
    switch (severity) {
    case parser::ParserSeverity::Info:
        return 0;
    case parser::ParserSeverity::Warning:
        return 1;
    default:
        return 2;
    }
}

std::string format_location(const parser::ParserDiagnostic& diagnostic)
{
    if (diagnostic.line == 0U && diagnostic.column == 0U) {
        return {};
    }

    std::string location = " (";
    bool written = false;
    if (diagnostic.line > 0U) {
        location.append("line ");
        location.append(std::to_string(diagnostic.line));
        written = true;
    }
    if (diagnostic.column > 0U) {
        if (written) {
            location.append(", ");
        }
        location.append("column ");
        location.append(std::to_string(diagnostic.column));
    }
    location.push_back(')');
    return location;
}

void append_statement_hint(const parser::ParserDiagnostic& diagnostic, std::vector<std::string>& hints)
{
    if (diagnostic.statement.empty()) {
        return;
    }
    hints.push_back("Statement context: " + diagnostic.statement);
}

}  // namespace

DdlDiagnosticSeverity parser_severity_to_ddl(parser::ParserSeverity severity) noexcept
{
    switch (severity) {
    case parser::ParserSeverity::Info:
        return DdlDiagnosticSeverity::Info;
    case parser::ParserSeverity::Warning:
        return DdlDiagnosticSeverity::Warning;
    default:
        return DdlDiagnosticSeverity::Error;
    }
}

DdlCommandResponse make_parser_failure_response(const parser::ParserDiagnostic& diagnostic)
{
    return make_parser_failure_response(std::vector<parser::ParserDiagnostic>{diagnostic});
}

DdlCommandResponse make_parser_failure_response(const std::vector<parser::ParserDiagnostic>& diagnostics)
{
    DdlCommandResponse response{};
    response.success = false;

    if (diagnostics.empty()) {
        response.error = make_error_code(DdlErrc::ExecutionFailed);
        response.message = "Parser reported failure without diagnostics.";
        response.severity = DdlDiagnosticSeverity::Error;
        response.remediation_hints = {"Retry with parser diagnostics enabled and report the issue."};
        return response;
    }

    parser::ParserSeverity highest_parser_severity = parser::ParserSeverity::Info;
    for (const auto& diagnostic : diagnostics) {
        if (severity_rank(diagnostic.severity) > severity_rank(highest_parser_severity)) {
            highest_parser_severity = diagnostic.severity;
        }
    }

    const bool parser_bug = highest_parser_severity == parser::ParserSeverity::Error;
    response.error = make_error_code(parser_bug ? DdlErrc::ExecutionFailed : DdlErrc::ValidationFailed);
    response.severity = parser_severity_to_ddl(highest_parser_severity);

    std::string message;
    message.reserve(diagnostics.size() * 32U);
    for (std::size_t index = 0; index < diagnostics.size(); ++index) {
        const auto& diagnostic = diagnostics[index];
        if (index > 0U) {
            message.append("; ");
        }
        message.append(diagnostic.message);
        const auto location = format_location(diagnostic);
        if (!location.empty()) {
            message.append(location);
        }
    }
    response.message = std::move(message);

    std::vector<std::string> hints;
    hints.reserve(diagnostics.size() * 3U);
    bool has_custom_hint = false;
    for (const auto& diagnostic : diagnostics) {
        for (const auto& hint : diagnostic.remediation_hints) {
            if (!hint.empty()) {
                hints.push_back(hint);
                has_custom_hint = true;
            }
        }
        append_statement_hint(diagnostic, hints);
    }

    if (parser_bug && !has_custom_hint) {
        hints.push_back("Capture the failing statement and report a parser bug.");
    }

    if (hints.empty()) {
        hints.push_back("Review the SQL statement for syntax issues and retry.");
    }

    response.remediation_hints = std::move(hints);
    return response;
}

}  // namespace bored::ddl

#endif  // BORED_ENABLE_PARSER

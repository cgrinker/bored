#include "bored/shell/shell_engine.hpp"

#include "bored/ddl/ddl_command.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/parser/grammar.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <optional>
#include <sstream>
#include <string_view>

using bored::parser::ParserDiagnostic;
using bored::parser::ScriptStatement;
using bored::storage::StorageTelemetryRegistry;
using bored::ddl::DdlCommandResponse;
using bored::ddl::DdlDiagnosticSeverity;

namespace bored::shell {

namespace {

struct TelemetrySnapshot final {
    std::uint64_t rows_emitted = 0U;
    std::uint64_t wal_bytes = 0U;
};

[[nodiscard]] TelemetrySnapshot capture(StorageTelemetryRegistry* registry)
{
    TelemetrySnapshot snapshot{};
    if (!registry) {
        return snapshot;
    }

    const auto executors = registry->aggregate_executors();
    snapshot.rows_emitted = executors.projection_rows_emitted;
    snapshot.wal_bytes = executors.insert_wal_bytes + executors.update_wal_bytes + executors.delete_wal_bytes;
    return snapshot;
}

[[nodiscard]] std::string summarise_success(const std::vector<ScriptStatement>& statements)
{
    if (statements.empty()) {
        return "No statements parsed.";
    }

    std::size_t successes = 0U;
    for (const auto& statement : statements) {
        if (statement.success) {
            ++successes;
        }
    }

    std::ostringstream stream;
    stream << "Parsed " << statements.size() << " statement" << (statements.size() == 1U ? "" : "s")
           << " (" << successes << " succeeded)";
    return stream.str();
}

[[nodiscard]] std::vector<ParserDiagnostic> collect_diagnostics(const std::vector<ScriptStatement>& statements)
{
    std::vector<ParserDiagnostic> diagnostics;
    diagnostics.reserve(statements.size());
    for (const auto& statement : statements) {
        diagnostics.insert(diagnostics.end(), statement.diagnostics.begin(), statement.diagnostics.end());
    }
    return diagnostics;
}

[[nodiscard]] bool has_error(const std::vector<ParserDiagnostic>& diagnostics) noexcept
{
    return std::any_of(diagnostics.begin(), diagnostics.end(), [](const ParserDiagnostic& diagnostic) {
        return diagnostic.severity == parser::ParserSeverity::Error;
    });
}

[[nodiscard]] parser::ParserSeverity to_parser_severity(DdlDiagnosticSeverity severity) noexcept
{
    switch (severity) {
    case DdlDiagnosticSeverity::Info:
        return parser::ParserSeverity::Info;
    case DdlDiagnosticSeverity::Warning:
        return parser::ParserSeverity::Warning;
    case DdlDiagnosticSeverity::Error:
    default:
        return parser::ParserSeverity::Error;
    }
}

void append_response_diagnostics(const std::vector<DdlCommandResponse>& responses,
                                 std::vector<ParserDiagnostic>& diagnostics)
{
    for (const auto& response : responses) {
        if (response.message.empty() && response.success) {
            continue;
        }

        ParserDiagnostic diagnostic{};
        diagnostic.severity = to_parser_severity(response.severity);
        if (response.message.empty()) {
            diagnostic.message = response.success ? "DDL command succeeded" : "DDL command failed";
        } else {
            diagnostic.message = response.message;
        }
        diagnostic.remediation_hints = response.remediation_hints;
        diagnostics.push_back(std::move(diagnostic));
    }
}

[[nodiscard]] std::size_t count_successes(const std::vector<ScriptStatement>& statements)
{
    return static_cast<std::size_t>(std::count_if(statements.begin(), statements.end(), [](const ScriptStatement& statement) {
        return statement.success;
    }));
}

[[nodiscard]] std::size_t count_successes(const std::vector<DdlCommandResponse>& responses)
{
    return static_cast<std::size_t>(std::count_if(responses.begin(), responses.end(), [](const DdlCommandResponse& response) {
        return response.success;
    }));
}

[[nodiscard]] std::string uppercase(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (const unsigned char ch : text) {
        result.push_back(static_cast<char>(std::toupper(ch)));
    }
    return result;
}

[[nodiscard]] std::string_view skip_leading_whitespace(std::string_view text)
{
    std::size_t offset = 0U;
    while (offset < text.size() && std::isspace(static_cast<unsigned char>(text[offset])) != 0) {
        ++offset;
    }
    return text.substr(offset);
}

[[nodiscard]] std::string_view skip_line_comment(std::string_view text)
{
    const auto newline = text.find('\n');
    if (newline == std::string_view::npos) {
        return std::string_view{};
    }
    return text.substr(newline + 1U);
}

[[nodiscard]] std::string_view skip_block_comment(std::string_view text)
{
    const auto terminator = text.find("*/");
    if (terminator == std::string_view::npos) {
        return std::string_view{};
    }
    return text.substr(terminator + 2U);
}

[[nodiscard]] std::string_view consume_leading_comments(std::string_view text)
{
    std::string_view view = text;
    while (!view.empty()) {
        view = skip_leading_whitespace(view);
        if (view.size() >= 2U && view[0] == '-' && view[1] == '-') {
            view = skip_line_comment(view.substr(2U));
            continue;
        }
        if (view.size() >= 2U && view[0] == '/' && view[1] == '*') {
            view = skip_block_comment(view.substr(2U));
            continue;
        }
        break;
    }
    return skip_leading_whitespace(view);
}

[[nodiscard]] std::string_view first_token(std::string_view text)
{
    if (text.empty()) {
        return {};
    }

    const char first = text.front();
    if (!std::isalnum(static_cast<unsigned char>(first)) && first != '_' && first != '\\') {
        return std::string_view{text.data(), 1U};
    }

    std::size_t index = 0U;
    while (index < text.size()) {
        const auto ch = static_cast<unsigned char>(text[index]);
        if (std::isalnum(ch) == 0 && ch != '_' && ch != '\\') {
            break;
        }
        ++index;
    }
    return text.substr(0U, index);
}

[[nodiscard]] std::string_view extract_token_for_summary(std::string_view text)
{
    auto view = consume_leading_comments(text);
    return first_token(view);
}

}  // namespace

ShellEngine::ShellEngine() = default;

ShellEngine::ShellEngine(Config config)
    : config_{std::move(config)}
{}

CommandMetrics ShellEngine::execute_sql(const std::string& sql)
{
    CommandMetrics metrics{};
    const auto trimmed = trim(sql);
    if (trimmed.empty()) {
        metrics.success = true;
        metrics.summary = "Empty command.";
        return metrics;
    }

    switch (classify(trimmed)) {
    case CommandKind::Empty:
        metrics.success = true;
        metrics.summary = "Empty command.";
        return metrics;
    case CommandKind::Ddl:
        return execute_ddl(trimmed);
    case CommandKind::Dml:
        return execute_dml(trimmed);
    case CommandKind::Unknown:
    default:
        return unsupported_command(trimmed);
    }
}

std::string ShellEngine::trim(std::string_view text)
{
    std::size_t start = 0U;
    while (start < text.size() && std::isspace(static_cast<unsigned char>(text[start])) != 0) {
        ++start;
    }

    std::size_t end = text.size();
    while (end > start && std::isspace(static_cast<unsigned char>(text[end - 1U])) != 0) {
        --end;
    }

    return std::string{text.substr(start, end - start)};
}

std::string_view ShellEngine::skip_leading_comments(std::string_view text)
{
    return consume_leading_comments(text);
}

std::string_view ShellEngine::extract_first_token(std::string_view text)
{
    return first_token(text);
}

ShellEngine::CommandKind ShellEngine::classify(std::string_view text)
{
    auto view = consume_leading_comments(text);
    if (view.empty()) {
        return CommandKind::Empty;
    }

    const auto token = first_token(view);
    if (token.empty()) {
        return CommandKind::Unknown;
    }

    const auto upper = uppercase(token);
    if (upper == "CREATE" || upper == "DROP" || upper == "ALTER" || upper == "TRUNCATE") {
        return CommandKind::Ddl;
    }

    if (upper == "SELECT" || upper == "INSERT" || upper == "UPDATE" || upper == "DELETE" || upper == "WITH" ||
        upper == "EXPLAIN" || upper == "VALUES" || upper == "BEGIN" || upper == "COMMIT" || upper == "ROLLBACK") {
        return CommandKind::Dml;
    }

    if (!token.empty() && token.front() == '\\') {
        return CommandKind::Unknown;
    }

    return CommandKind::Unknown;
}

CommandMetrics ShellEngine::execute_ddl(const std::string& sql)
{
    if (config_.ddl_executor == nullptr) {
        return parse_only_ddl(sql);
    }

    auto* registry = bored::storage::get_global_storage_telemetry_registry();
    const auto before = capture(registry);
    const auto start = std::chrono::steady_clock::now();

    const auto result = config_.ddl_executor->execute(sql);

    CommandMetrics metrics{};
    metrics.diagnostics = result.diagnostics;
    append_response_diagnostics(result.responses, metrics.diagnostics);

    const auto statement_successes = count_successes(result.script.statements);
    const auto response_successes = count_successes(result.responses);

    std::ostringstream summary;
    summary << "Parsed " << result.script.statements.size() << " statement"
            << (result.script.statements.size() == 1U ? "" : "s") << " (" << statement_successes << " succeeded)";
    if (!result.responses.empty()) {
        summary << "; dispatched " << result.responses.size() << " command"
                << (result.responses.size() == 1U ? "" : "s") << " (" << response_successes << " succeeded)";
    }
    metrics.summary = summary.str();

    const bool statement_errors = has_error(result.diagnostics);
    const bool response_errors = std::any_of(result.responses.begin(), result.responses.end(), [](const DdlCommandResponse& response) {
        return !response.success;
    });

    metrics.success = !statement_errors && !response_errors;

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    metrics.duration_ms = static_cast<double>(duration_ns.count()) / 1'000'000.0;

    const auto after = capture(registry);
    metrics.rows_touched = after.rows_emitted - before.rows_emitted;
    metrics.wal_bytes = after.wal_bytes - before.wal_bytes;

    return metrics;
}

CommandMetrics ShellEngine::parse_only_ddl(const std::string& sql)
{
    auto* registry = bored::storage::get_global_storage_telemetry_registry();
    const auto before = capture(registry);
    const auto start = std::chrono::steady_clock::now();

    const auto parsed = bored::parser::parse_ddl_script(sql);

    CommandMetrics metrics{};
    metrics.success = std::all_of(parsed.statements.begin(), parsed.statements.end(), [](const ScriptStatement& statement) {
        return statement.success;
    });
    metrics.summary = summarise_success(parsed.statements);
    metrics.diagnostics = collect_diagnostics(parsed.statements);

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    metrics.duration_ms = static_cast<double>(duration_ns.count()) / 1'000'000.0;

    const auto after = capture(registry);
    metrics.rows_touched = after.rows_emitted - before.rows_emitted;
    metrics.wal_bytes = after.wal_bytes - before.wal_bytes;

    return metrics;
}

CommandMetrics ShellEngine::execute_dml(const std::string& sql)
{
    if (!config_.dml_executor) {
        return unsupported_command(sql);
    }

    auto* registry = bored::storage::get_global_storage_telemetry_registry();
    const auto before = capture(registry);
    const auto start = std::chrono::steady_clock::now();

    CommandMetrics metrics = config_.dml_executor(sql);

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    const auto computed_ms = static_cast<double>(duration_ns.count()) / 1'000'000.0;
    if (metrics.duration_ms <= 0.0) {
        metrics.duration_ms = computed_ms;
    }

    if (metrics.summary.empty()) {
        metrics.summary = "Executed DML command.";
    }

    const auto after = capture(registry);
    metrics.rows_touched = after.rows_emitted - before.rows_emitted;
    metrics.wal_bytes = after.wal_bytes - before.wal_bytes;

    return metrics;
}

CommandMetrics ShellEngine::unsupported_command(const std::string& sql)
{
    CommandMetrics metrics{};
    metrics.success = false;

    const auto token = extract_token_for_summary(sql);
    std::ostringstream summary;
    summary << "Unsupported command";
    if (!token.empty()) {
        summary << ": '" << token << "'";
    }
    metrics.summary = summary.str();

    ParserDiagnostic diagnostic{};
    diagnostic.severity = parser::ParserSeverity::Error;
    diagnostic.statement = sql;
    diagnostic.message = "Command type is not supported by the shell.";
    diagnostic.remediation_hints = {"Ensure DDL or DML statements use supported verbs (CREATE, DROP, SELECT, INSERT, etc.)."};
    metrics.diagnostics.push_back(std::move(diagnostic));

    return metrics;
}

}  // namespace bored::shell

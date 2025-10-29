#include "bored/shell/shell_engine.hpp"

#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <sstream>

using bored::parser::ParserDiagnostic;
using bored::parser::ScriptStatement;
using bored::storage::StorageTelemetryRegistry;

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

}  // namespace

ShellEngine::ShellEngine() = default;

CommandMetrics ShellEngine::execute_sql(const std::string& sql)
{
    CommandMetrics metrics{};
    const auto trimmed = trim(sql);
    if (trimmed.empty()) {
        metrics.success = true;
        metrics.summary = "Empty command.";
        return metrics;
    }

    auto* registry = bored::storage::get_global_storage_telemetry_registry();
    const auto before = capture(registry);
    const auto start = std::chrono::steady_clock::now();

    const auto parsed = bored::parser::parse_ddl_script(trimmed);

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    metrics.duration_ms = static_cast<double>(duration_ns.count()) / 1'000'000.0;

    const bool all_success = std::all_of(parsed.statements.begin(), parsed.statements.end(), [](const ScriptStatement& statement) {
        return statement.success;
    });

    metrics.success = all_success;
    metrics.summary = summarise_success(parsed.statements);
    metrics.diagnostics = collect_diagnostics(parsed.statements);

    const auto after = capture(registry);
    metrics.rows_touched = after.rows_emitted - before.rows_emitted;
    metrics.wal_bytes = after.wal_bytes - before.wal_bytes;

    return metrics;
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

}  // namespace bored::shell

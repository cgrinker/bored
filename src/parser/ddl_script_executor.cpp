#include "bored/parser/ddl_script_executor.hpp"

#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/parser/grammar.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace bored::parser {
namespace {

struct SeverityCounters final {
    std::size_t info = 0U;
    std::size_t warning = 0U;
    std::size_t error = 0U;
};

SeverityCounters tally_diagnostics(const std::vector<ParserDiagnostic>& diagnostics)
{
    SeverityCounters counters{};
    for (const auto& diagnostic : diagnostics) {
        switch (diagnostic.severity) {
            case ParserSeverity::Info:
                ++counters.info;
                break;
            case ParserSeverity::Warning:
                ++counters.warning;
                break;
            case ParserSeverity::Error:
            default:
                ++counters.error;
                break;
        }
    }
    return counters;
}

[[nodiscard]] std::uint64_t to_uint64(std::chrono::nanoseconds duration) noexcept
{
    const auto count = duration.count();
    return static_cast<std::uint64_t>(count < 0 ? 0 : count);
}

}  // namespace

DdlScriptExecutor::DdlScriptExecutor(Config config)
    : config_{std::move(config)}
    , registry_{config_.telemetry_registry}
    , storage_registry_{config_.storage_registry}
{
    if (registry_ != nullptr && !config_.telemetry_identifier.empty()) {
        registry_->register_sampler(config_.telemetry_identifier, [this] { return telemetry_.snapshot(); });
        registry_identifier_ = config_.telemetry_identifier;
        registered_parser_registry_ = true;
    }

    if (storage_registry_ != nullptr && !config_.telemetry_identifier.empty()) {
        storage_registry_->register_parser(config_.telemetry_identifier, [this] { return telemetry_.snapshot(); });
        registry_identifier_ = config_.telemetry_identifier;
        registered_storage_registry_ = true;
    }
}

DdlScriptExecutor::~DdlScriptExecutor()
{
    if (registered_parser_registry_ && registry_ != nullptr && !registry_identifier_.empty()) {
        registry_->unregister_sampler(registry_identifier_);
    }

    if (registered_storage_registry_ && storage_registry_ != nullptr && !registry_identifier_.empty()) {
        storage_registry_->unregister_parser(registry_identifier_);
    }
}

DdlScriptExecutionResult DdlScriptExecutor::execute(std::string_view script)
{
    DdlScriptExecutionResult result{};

    telemetry_.record_script_attempt();

    const auto start = std::chrono::steady_clock::now();

    result.script = parse_ddl_script(script);
    const auto statements_attempted = result.script.statements.size();

    for (const auto& statement : result.script.statements) {
        result.diagnostics.insert(result.diagnostics.end(), statement.diagnostics.begin(), statement.diagnostics.end());
    }

    result.builder = build_ddl_commands(result.script, config_.builder_config);
    result.diagnostics.insert(result.diagnostics.end(),
                              result.builder.diagnostics.begin(),
                              result.builder.diagnostics.end());

    const auto end = std::chrono::steady_clock::now();
    const auto parse_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    const auto counters = tally_diagnostics(result.diagnostics);

    std::size_t statements_succeeded = 0U;
    for (const auto& statement : result.script.statements) {
        if (statement.success) {
            ++statements_succeeded;
        }
    }

    const bool script_success = counters.error == 0U;

    telemetry_.record_script_result(script_success,
                                    to_uint64(parse_duration),
                                    statements_attempted,
                                    statements_succeeded,
                                    counters.info,
                                    counters.warning,
                                    counters.error);

    if (config_.dispatcher != nullptr && script_success) {
        result.responses.reserve(result.builder.commands.size());
        for (const auto& command : result.builder.commands) {
            result.responses.push_back(config_.dispatcher->dispatch(command));
        }
    }

    return result;
}

}  // namespace bored::parser

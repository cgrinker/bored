#pragma once

#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/parser_telemetry.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace bored::ddl {
class DdlCommandDispatcher;
}

namespace bored::storage {
class StorageTelemetryRegistry;
}

namespace bored::parser {

struct DdlScriptExecutionResult final {
    ScriptParseResult script;
    DdlCommandBuilderResult builder;
    std::vector<ddl::DdlCommandResponse> responses;
    std::vector<ParserDiagnostic> diagnostics;
};

class DdlScriptExecutor final {
public:
    struct Config final {
        DdlCommandBuilderConfig builder_config{};
        ddl::DdlCommandDispatcher* dispatcher = nullptr;
        ParserTelemetryRegistry* telemetry_registry = nullptr;
        storage::StorageTelemetryRegistry* storage_registry = nullptr;
        std::string telemetry_identifier{};
    };

    explicit DdlScriptExecutor(Config config);
    ~DdlScriptExecutor();

    DdlScriptExecutor(const DdlScriptExecutor&) = delete;
    DdlScriptExecutor& operator=(const DdlScriptExecutor&) = delete;
    DdlScriptExecutor(DdlScriptExecutor&&) = delete;
    DdlScriptExecutor& operator=(DdlScriptExecutor&&) = delete;

    [[nodiscard]] DdlScriptExecutionResult execute(std::string_view script);
    [[nodiscard]] const ParserTelemetry& telemetry() const noexcept { return telemetry_; }

private:
    Config config_{};
    ParserTelemetry telemetry_{};
    ParserTelemetryRegistry* registry_ = nullptr;
    storage::StorageTelemetryRegistry* storage_registry_ = nullptr;
    std::string registry_identifier_{};
    bool registered_parser_registry_ = false;
    bool registered_storage_registry_ = false;
};

}  // namespace bored::parser

#pragma once

#include "bored/catalog/catalog_introspection.hpp"
#include "bored/parser/grammar.hpp"
#include "bored/storage/lock_manager.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace bored::parser {
class DdlScriptExecutor;
}

namespace bored::shell {

struct CommandMetrics final {
    bool success = false;
    std::string summary{};
    double duration_ms = 0.0;
    std::uint64_t rows_touched = 0U;
    std::uint64_t wal_bytes = 0U;
    std::vector<bored::parser::ParserDiagnostic> diagnostics{};
    std::vector<std::string> detail_lines{};
    std::string command_text{};
    std::string correlation_id{};
    std::string command_category{};
    std::chrono::system_clock::time_point started_at{};
    std::chrono::system_clock::time_point finished_at{};
};

class ShellEngine final {
public:
    struct Config final {
        parser::DdlScriptExecutor* ddl_executor = nullptr;
        std::function<CommandMetrics(const std::string&)> dml_executor{};
        std::function<catalog::CatalogIntrospectionSnapshot()> catalog_snapshot{};
        std::function<std::vector<storage::LockManager::LockSnapshot>()> lock_snapshot{};
        std::function<void(const CommandMetrics&)> command_logger{};
    };

    ShellEngine();
    explicit ShellEngine(Config config);

    CommandMetrics execute_sql(const std::string& sql);

private:
    enum class CommandKind : std::uint8_t {
        Empty = 0,
        Ddl,
        Dml,
        Meta,
        Unknown
    };

    static std::string trim(std::string_view text);
    static std::string_view skip_leading_comments(std::string_view text);
    static std::string_view extract_first_token(std::string_view text);
    static CommandKind classify(std::string_view text);
    static std::string_view command_kind_to_string(CommandKind kind) noexcept;

    CommandMetrics execute_ddl(const std::string& sql);
    CommandMetrics parse_only_ddl(const std::string& sql);
    CommandMetrics execute_dml(const std::string& sql);
    CommandMetrics execute_meta(const std::string& command);
    CommandMetrics unsupported_command(const std::string& sql);

    Config config_{};
    std::atomic<std::uint64_t> correlation_counter_{1U};
};

}  // namespace bored::shell

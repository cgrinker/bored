#pragma once

#include "bored/parser/grammar.hpp"

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
};

class ShellEngine final {
public:
    struct Config final {
        parser::DdlScriptExecutor* ddl_executor = nullptr;
        std::function<CommandMetrics(const std::string&)> dml_executor{};
    };

    ShellEngine();
    explicit ShellEngine(Config config);

    CommandMetrics execute_sql(const std::string& sql);

private:
    enum class CommandKind : std::uint8_t {
        Empty = 0,
        Ddl,
        Dml,
        Unknown
    };

    static std::string trim(std::string_view text);
    static std::string_view skip_leading_comments(std::string_view text);
    static std::string_view extract_first_token(std::string_view text);
    static CommandKind classify(std::string_view text);

    CommandMetrics execute_ddl(const std::string& sql);
    CommandMetrics parse_only_ddl(const std::string& sql);
    CommandMetrics execute_dml(const std::string& sql);
    CommandMetrics unsupported_command(const std::string& sql);

    Config config_{};
};

}  // namespace bored::shell

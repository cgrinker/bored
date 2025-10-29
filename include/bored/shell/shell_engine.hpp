#pragma once

#include "bored/parser/grammar.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

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
    ShellEngine();

    CommandMetrics execute_sql(const std::string& sql);

private:
    static std::string trim(std::string_view text);
};

}  // namespace bored::shell

#pragma once

#include "bored/parser/ast.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bored::parser {

enum class ParserSeverity : std::uint8_t {
    Info = 0,
    Warning,
    Error
};

struct ParserDiagnostic final {
    ParserSeverity severity = ParserSeverity::Error;
    std::string message{};
    std::size_t line = 0U;
    std::size_t column = 0U;
};

template <typename T>
struct ParseResult final {
    std::optional<T> ast{};
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return ast.has_value(); }
};

ParseResult<Identifier> parse_identifier(std::string_view input);

}  // namespace bored::parser

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
    std::string statement{};
    std::vector<std::string> remediation_hints{};
};

template <typename T>
struct ParseResult final {
    std::optional<T> ast{};
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return ast.has_value(); }
};

ParseResult<Identifier> parse_identifier(std::string_view input);
ParseResult<CreateDatabaseStatement> parse_create_database(std::string_view input);
ParseResult<DropDatabaseStatement> parse_drop_database(std::string_view input);
ParseResult<CreateSchemaStatement> parse_create_schema(std::string_view input);
ParseResult<DropSchemaStatement> parse_drop_schema(std::string_view input);
ParseResult<CreateTableStatement> parse_create_table(std::string_view input);
ParseResult<DropTableStatement> parse_drop_table(std::string_view input);
ParseResult<CreateViewStatement> parse_create_view(std::string_view input);

}  // namespace bored::parser

#pragma once

#include "bored/parser/ast.hpp"
#include "bored/parser/relational/ast.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <variant>

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

enum class StatementType : std::uint8_t {
    Unknown = 0,
    CreateDatabase,
    DropDatabase,
    CreateSchema,
    DropSchema,
    CreateTable,
    DropTable,
    CreateView
};

using StatementAst = std::variant<std::monostate,
                                   CreateDatabaseStatement,
                                   DropDatabaseStatement,
                                   CreateSchemaStatement,
                                   DropSchemaStatement,
                                   CreateTableStatement,
                                   DropTableStatement,
                                   CreateViewStatement>;

struct ScriptStatement final {
    StatementType type = StatementType::Unknown;
    std::string text{};
    StatementAst ast{};
    std::vector<ParserDiagnostic> diagnostics{};
    bool success = false;
};

struct ScriptParseResult final {
    std::vector<ScriptStatement> statements{};
};
ParseResult<Identifier> parse_identifier(std::string_view input);
ParseResult<CreateDatabaseStatement> parse_create_database(std::string_view input);
ParseResult<DropDatabaseStatement> parse_drop_database(std::string_view input);
ParseResult<CreateSchemaStatement> parse_create_schema(std::string_view input);
ParseResult<DropSchemaStatement> parse_drop_schema(std::string_view input);
ParseResult<CreateTableStatement> parse_create_table(std::string_view input);
ParseResult<DropTableStatement> parse_drop_table(std::string_view input);
ParseResult<CreateViewStatement> parse_create_view(std::string_view input);

ScriptParseResult parse_ddl_script(std::string_view input);
struct SelectParseResult final {
    relational::AstArena arena{};
    relational::SelectStatement* statement = nullptr;
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return statement != nullptr; }
};

SelectParseResult parse_select(std::string_view input);

struct InsertParseResult final {
    relational::AstArena arena{};
    relational::InsertStatement* statement = nullptr;
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return statement != nullptr; }
};

struct UpdateParseResult final {
    relational::AstArena arena{};
    relational::UpdateStatement* statement = nullptr;
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return statement != nullptr; }
};

struct DeleteParseResult final {
    relational::AstArena arena{};
    relational::DeleteStatement* statement = nullptr;
    std::vector<ParserDiagnostic> diagnostics{};

    [[nodiscard]] bool success() const noexcept { return statement != nullptr; }
};

InsertParseResult parse_insert(std::string_view input);
UpdateParseResult parse_update(std::string_view input);
DeleteParseResult parse_delete(std::string_view input);
}  // namespace bored::parser

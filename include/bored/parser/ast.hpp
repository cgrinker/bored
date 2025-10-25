#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace bored::parser {

struct Identifier final {
    std::string value{};
};

struct SchemaName final {
    Identifier database{};
    Identifier name{};
    bool if_exists = false;
};

struct ColumnDefinition final {
    Identifier name{};
    Identifier type_name{};
    bool not_null = false;
    bool primary_key = false;
    bool unique = false;
    std::optional<std::string> default_expression{};
};

struct CreateTableStatement final {
    Identifier schema{};
    Identifier name{};
    std::vector<ColumnDefinition> columns{};
    bool if_not_exists = false;
};

struct DropTableStatement final {
    Identifier schema{};
    Identifier name{};
    bool if_exists = false;
    bool cascade = false;
};

struct CreateDatabaseStatement final {
    Identifier name{};
    bool if_not_exists = false;
};

struct DropDatabaseStatement final {
    Identifier name{};
    bool if_exists = false;
    bool cascade = false;
};

struct CreateViewStatement final {
    Identifier schema{};
    Identifier name{};
    std::string definition{};
    bool if_not_exists = false;
};

using SchemaEmbeddedStatement = std::variant<CreateTableStatement, CreateViewStatement>;

struct CreateSchemaStatement final {
    Identifier database{};
    Identifier name{};
    bool if_not_exists = false;
    std::optional<Identifier> authorization{};
    std::vector<SchemaEmbeddedStatement> embedded_statements{};
};

struct DropSchemaStatement final {
    enum class Behavior : std::uint8_t {
        Default = 0,
        Cascade,
        Restrict
    };

    std::vector<SchemaName> schemas{};
    bool if_exists = false;
    Behavior behavior = Behavior::Default;
};

}  // namespace bored::parser

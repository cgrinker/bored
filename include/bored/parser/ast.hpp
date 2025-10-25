#pragma once

#include <optional>
#include <string>
#include <vector>

namespace bored::parser {

struct Identifier final {
    std::string value{};
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

struct CreateSchemaStatement final {
    Identifier database{};
    Identifier name{};
    bool if_not_exists = false;
    std::optional<Identifier> authorization{};
    std::vector<std::string> embedded_statements{};
};

struct DropSchemaStatement final {
    Identifier database{};
    Identifier name{};
    bool if_exists = false;
    bool cascade = false;
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

}  // namespace bored::parser

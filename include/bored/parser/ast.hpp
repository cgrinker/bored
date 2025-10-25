#pragma once

#include <string>

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
};

struct DropSchemaStatement final {
    Identifier database{};
    Identifier name{};
    bool if_exists = false;
    bool cascade = false;
};

}  // namespace bored::parser

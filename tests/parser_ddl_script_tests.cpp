#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::parser;

TEST_CASE("parse_ddl_script parses multiple statements with recovery")
{
    const char* script = R"(CREATE TABLE accounts (id INT);
CREATE TABLE broken (id INT;
DROP TABLE legacy.accounts;
)";

    const auto result = parse_ddl_script(script);
    REQUIRE(result.statements.size() == 3);

    const auto& first = result.statements[0];
    REQUIRE(first.success);
    CHECK(first.type == StatementType::CreateTable);
    CHECK(first.diagnostics.empty());

    const auto& second = result.statements[1];
    CHECK_FALSE(second.success);
    REQUIRE_FALSE(second.diagnostics.empty());
    CHECK(second.diagnostics.front().severity == ParserSeverity::Warning);
    CHECK(second.type == StatementType::CreateTable);

    const auto& third = result.statements[2];
    REQUIRE(third.success);
    CHECK(third.type == StatementType::DropTable);
    CHECK(third.diagnostics.empty());
}

TEST_CASE("parse_ddl_script ignores leading comments between statements")
{
    const char* script = R"(-- prepare catalog
CREATE DATABASE analytics;

/* ensure schema exists */
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION admin;
)";

    const auto result = parse_ddl_script(script);
    REQUIRE(result.statements.size() == 2);

    const auto& create_database = result.statements[0];
    REQUIRE(create_database.success);
    CHECK(create_database.type == StatementType::CreateDatabase);

    const auto& create_schema = result.statements[1];
    REQUIRE(create_schema.success);
    CHECK(create_schema.type == StatementType::CreateSchema);
}

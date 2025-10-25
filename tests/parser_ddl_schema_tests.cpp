#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::parser;

TEST_CASE("parse_create_schema handles unqualified name")
{
    const auto result = parse_create_schema("CREATE SCHEMA reporting;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->name.value == "reporting");
    CHECK(result.ast->database.value.empty());
    CHECK_FALSE(result.ast->if_not_exists);
    CHECK(result.diagnostics.empty());
}

TEST_CASE("parse_create_schema supports IF NOT EXISTS and database qualifier")
{
    const auto result = parse_create_schema("create schema if not exists system.analytics");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->database.value == "system");
    CHECK(result.ast->name.value == "analytics");
    CHECK(result.ast->if_not_exists);
}

TEST_CASE("parse_create_schema rejects missing identifier")
{
    const auto result = parse_create_schema("CREATE SCHEMA IF NOT EXISTS;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

TEST_CASE("parse_drop_schema handles cascade clause")
{
    const auto result = parse_drop_schema("DROP SCHEMA IF EXISTS system.analytics CASCADE;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->database.value == "system");
    CHECK(result.ast->name.value == "analytics");
    CHECK(result.ast->if_exists);
    CHECK(result.ast->cascade);
}

TEST_CASE("parse_drop_schema rejects missing name")
{
    const auto result = parse_drop_schema("DROP SCHEMA IF EXISTS;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

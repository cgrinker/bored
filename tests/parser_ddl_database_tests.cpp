#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::parser;

TEST_CASE("parse_create_database handles minimal statement")
{
    const auto result = parse_create_database("CREATE DATABASE analytics;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->name.value == "analytics");
    CHECK_FALSE(result.ast->if_not_exists);
    CHECK(result.diagnostics.empty());
}

TEST_CASE("parse_create_database supports IF NOT EXISTS")
{
    const auto result = parse_create_database("create database IF NOT EXISTS analytics");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->name.value == "analytics");
    CHECK(result.ast->if_not_exists);
}

TEST_CASE("parse_create_database rejects missing identifier")
{
    const auto result = parse_create_database("CREATE DATABASE IF NOT EXISTS ;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

TEST_CASE("parse_drop_database handles cascade clause")
{
    const auto result = parse_drop_database("DROP DATABASE IF EXISTS analytics CASCADE;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->name.value == "analytics");
    CHECK(result.ast->if_exists);
    CHECK(result.ast->cascade);
}

TEST_CASE("parse_drop_database rejects missing name")
{
    const auto result = parse_drop_database("DROP DATABASE;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

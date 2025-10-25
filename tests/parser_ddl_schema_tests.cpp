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
    CHECK_FALSE(result.ast->authorization.has_value());
    CHECK(result.ast->embedded_statements.empty());
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
    CHECK_FALSE(result.ast->authorization.has_value());
    CHECK(result.ast->embedded_statements.empty());
}

TEST_CASE("parse_create_schema parses authorization clause")
{
    const auto result = parse_create_schema("CREATE SCHEMA analytics AUTHORIZATION owner;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->name.value == "analytics");
    REQUIRE(result.ast->authorization.has_value());
    CHECK(result.ast->authorization->value == "owner");
    CHECK(result.ast->embedded_statements.empty());
}

TEST_CASE("parse_create_schema captures embedded statements")
{
    const auto sql = R"(CREATE SCHEMA analytics AUTHORIZATION owner
        CREATE TABLE analytics.events (event_id BIGINT);
        CREATE VIEW analytics.events_view AS SELECT event_id FROM analytics.events;
    )";
    const auto result = parse_create_schema(sql);
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    REQUIRE(result.ast->authorization.has_value());
    CHECK(result.ast->authorization->value == "owner");
    REQUIRE(result.ast->embedded_statements.size() == 2);
    CHECK(result.ast->embedded_statements[0] == "CREATE TABLE analytics.events (event_id BIGINT)");
    CHECK(result.ast->embedded_statements[1]
          == "CREATE VIEW analytics.events_view AS SELECT event_id FROM analytics.events");
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
    CHECK(result.ast->if_exists);
    REQUIRE(result.ast->schemas.size() == 1);
    const auto& schema = result.ast->schemas.front();
    CHECK(schema.database.value == "system");
    CHECK(schema.name.value == "analytics");
    CHECK(result.ast->behavior == DropSchemaStatement::Behavior::Cascade);
}

TEST_CASE("parse_drop_schema rejects missing name")
{
    const auto result = parse_drop_schema("DROP SCHEMA IF EXISTS;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

TEST_CASE("parse_drop_schema supports restrict and multiple targets")
{
    const auto result = parse_drop_schema("DROP SCHEMA sales.stage, public.temp RESTRICT");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK_FALSE(result.ast->if_exists);
    REQUIRE(result.ast->schemas.size() == 2);
    CHECK(result.ast->schemas[0].database.value == "sales");
    CHECK(result.ast->schemas[0].name.value == "stage");
    CHECK(result.ast->schemas[1].database.value == "public");
    CHECK(result.ast->schemas[1].name.value == "temp");
    CHECK(result.ast->behavior == DropSchemaStatement::Behavior::Restrict);
}

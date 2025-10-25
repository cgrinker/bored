#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::parser;

TEST_CASE("parse_create_table handles basic definition")
{
    const auto result = parse_create_table("CREATE TABLE customer (id INT);");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->schema.value.empty());
    CHECK(result.ast->name.value == "customer");
    REQUIRE(result.ast->columns.size() == 1);
    CHECK(result.ast->columns[0].name.value == "id");
    CHECK(result.ast->columns[0].type_name.value == "INT");
    CHECK_FALSE(result.ast->columns[0].not_null);
    CHECK(result.diagnostics.empty());
}

TEST_CASE("parse_create_table supports qualifiers defaults and constraints")
{
    const auto result = parse_create_table(R"(CREATE TABLE IF NOT EXISTS analytics.events (
        event_id BIGINT PRIMARY KEY,
        payload JSON DEFAULT '{}' NOT NULL,
        user_id INT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
)");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->if_not_exists);
    CHECK(result.ast->schema.value == "analytics");
    CHECK(result.ast->name.value == "events");
    REQUIRE(result.ast->columns.size() == 4);
    CHECK(result.diagnostics.empty());

    const auto& event_id = result.ast->columns[0];
    CHECK(event_id.name.value == "event_id");
    CHECK(event_id.type_name.value == "BIGINT");
    CHECK(event_id.not_null);
    CHECK(event_id.primary_key);
    CHECK_FALSE(event_id.default_expression.has_value());
    CHECK_FALSE(event_id.unique);

    const auto& payload = result.ast->columns[1];
    CHECK(payload.name.value == "payload");
    CHECK(payload.type_name.value == "JSON");
    CHECK(payload.not_null);
    REQUIRE(payload.default_expression.has_value());
    CHECK(*payload.default_expression == "'{}'");
    CHECK_FALSE(payload.primary_key);
    CHECK_FALSE(payload.unique);

    const auto& user_id = result.ast->columns[2];
    CHECK(user_id.name.value == "user_id");
    CHECK(user_id.type_name.value == "INT");
    CHECK_FALSE(user_id.not_null);
    CHECK(user_id.unique);
    CHECK_FALSE(user_id.primary_key);
    CHECK_FALSE(user_id.default_expression.has_value());

    const auto& created_at = result.ast->columns[3];
    CHECK(created_at.name.value == "created_at");
    CHECK(created_at.type_name.value == "TIMESTAMP");
    CHECK_FALSE(created_at.not_null);
    REQUIRE(created_at.default_expression.has_value());
    CHECK(*created_at.default_expression == "CURRENT_TIMESTAMP");
    CHECK_FALSE(created_at.primary_key);
    CHECK_FALSE(created_at.unique);
}

TEST_CASE("parse_create_table rejects missing column list")
{
    const auto result = parse_create_table("CREATE TABLE accounts;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

TEST_CASE("parse_drop_table handles cascade clause")
{
    const auto result = parse_drop_table("DROP TABLE IF EXISTS analytics.events CASCADE;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->schema.value == "analytics");
    CHECK(result.ast->name.value == "events");
    CHECK(result.ast->if_exists);
    CHECK(result.ast->cascade);
}

TEST_CASE("parse_drop_table rejects missing name")
{
    const auto result = parse_drop_table("DROP TABLE IF EXISTS;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

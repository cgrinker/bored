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
    CHECK_FALSE(result.ast->columns[0].default_constraint_name.has_value());
    CHECK_FALSE(result.ast->columns[0].not_null_constraint_name.has_value());
    CHECK_FALSE(result.ast->columns[0].primary_key_constraint_name.has_value());
    CHECK_FALSE(result.ast->columns[0].unique_constraint_name.has_value());
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
    CHECK_FALSE(event_id.default_constraint_name.has_value());
    CHECK_FALSE(event_id.not_null_constraint_name.has_value());
    CHECK_FALSE(event_id.primary_key_constraint_name.has_value());
    CHECK_FALSE(event_id.unique_constraint_name.has_value());

    const auto& payload = result.ast->columns[1];
    CHECK(payload.name.value == "payload");
    CHECK(payload.type_name.value == "JSON");
    CHECK(payload.not_null);
    REQUIRE(payload.default_expression.has_value());
    CHECK(*payload.default_expression == "'{}'");
    CHECK_FALSE(payload.primary_key);
    CHECK_FALSE(payload.unique);
    CHECK_FALSE(payload.default_constraint_name.has_value());
    CHECK_FALSE(payload.not_null_constraint_name.has_value());
    CHECK_FALSE(payload.primary_key_constraint_name.has_value());
    CHECK_FALSE(payload.unique_constraint_name.has_value());

    const auto& user_id = result.ast->columns[2];
    CHECK(user_id.name.value == "user_id");
    CHECK(user_id.type_name.value == "INT");
    CHECK_FALSE(user_id.not_null);
    CHECK(user_id.unique);
    CHECK_FALSE(user_id.primary_key);
    CHECK_FALSE(user_id.default_expression.has_value());
    CHECK_FALSE(user_id.default_constraint_name.has_value());
    CHECK_FALSE(user_id.not_null_constraint_name.has_value());
    CHECK_FALSE(user_id.primary_key_constraint_name.has_value());
    CHECK_FALSE(user_id.unique_constraint_name.has_value());

    const auto& created_at = result.ast->columns[3];
    CHECK(created_at.name.value == "created_at");
    CHECK(created_at.type_name.value == "TIMESTAMP");
    CHECK_FALSE(created_at.not_null);
    REQUIRE(created_at.default_expression.has_value());
    CHECK(*created_at.default_expression == "CURRENT_TIMESTAMP");
    CHECK_FALSE(created_at.primary_key);
    CHECK_FALSE(created_at.unique);
    CHECK_FALSE(created_at.default_constraint_name.has_value());
    CHECK_FALSE(created_at.not_null_constraint_name.has_value());
    CHECK_FALSE(created_at.primary_key_constraint_name.has_value());
    CHECK_FALSE(created_at.unique_constraint_name.has_value());
}

TEST_CASE("parse_create_table captures function and arithmetic default expressions")
{
    const auto result = parse_create_table(R"(CREATE TABLE pricing (
        id INT,
        created_at TIMESTAMP DEFAULT now(),
        discount NUMERIC DEFAULT (base_price * 0.2) + tax_amount,
        payload JSON DEFAULT coalesce(payload, '{}')
    ))");

    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    REQUIRE(result.ast->columns.size() == 4);

    const auto& created_at = result.ast->columns[1];
    REQUIRE(created_at.default_expression.has_value());
    CHECK(*created_at.default_expression == "now()");
    CHECK_FALSE(created_at.default_constraint_name.has_value());

    const auto& discount = result.ast->columns[2];
    REQUIRE(discount.default_expression.has_value());
    CHECK(*discount.default_expression == "(base_price * 0.2) + tax_amount");
    CHECK_FALSE(discount.default_constraint_name.has_value());

    const auto& payload = result.ast->columns[3];
    REQUIRE(payload.default_expression.has_value());
    CHECK(*payload.default_expression == "coalesce(payload, '{}')");
    CHECK_FALSE(payload.default_constraint_name.has_value());
}

TEST_CASE("parse_create_table captures inline constraint names")
{
    const auto result = parse_create_table(R"(CREATE TABLE accounts (
        id INT CONSTRAINT pk_accounts PRIMARY KEY,
        email TEXT CONSTRAINT uq_accounts_email UNIQUE,
        created_at TIMESTAMP CONSTRAINT nn_created_at NOT NULL,
        updated_at TIMESTAMP CONSTRAINT df_updated_at DEFAULT now()
    ))");

    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    REQUIRE(result.ast->columns.size() == 4);

    const auto& id = result.ast->columns[0];
    CHECK(id.primary_key);
    REQUIRE(id.primary_key_constraint_name.has_value());
    CHECK(id.primary_key_constraint_name->value == "pk_accounts");
    CHECK(id.not_null);
    CHECK_FALSE(id.default_constraint_name.has_value());

    const auto& email = result.ast->columns[1];
    CHECK(email.unique);
    REQUIRE(email.unique_constraint_name.has_value());
    CHECK(email.unique_constraint_name->value == "uq_accounts_email");

    const auto& created_at = result.ast->columns[2];
    CHECK(created_at.not_null);
    REQUIRE(created_at.not_null_constraint_name.has_value());
    CHECK(created_at.not_null_constraint_name->value == "nn_created_at");

    const auto& updated_at = result.ast->columns[3];
    REQUIRE(updated_at.default_expression.has_value());
    REQUIRE(updated_at.default_constraint_name.has_value());
    CHECK(updated_at.default_constraint_name->value == "df_updated_at");
}

TEST_CASE("parse_create_table rejects missing column list")
{
    const auto result = parse_create_table("CREATE TABLE accounts;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
}

TEST_CASE("parse_create_table reports duplicate column names")
{
    const auto result = parse_create_table("CREATE TABLE accounts (id INT, id INT);");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    REQUIRE_FALSE(result.diagnostics.empty());
    CHECK(result.diagnostics[0].severity == ParserSeverity::Error);
    CHECK(result.diagnostics[0].message == "Duplicate column name 'id'");
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

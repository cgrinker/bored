#include "bored/parser/ddl_command_builder.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <optional>
#include <string_view>
#include <variant>

using namespace bored::parser;

namespace {

auto make_config()
{
    DdlCommandBuilderConfig config{};
    config.default_database_id = bored::catalog::DatabaseId{42U};
    config.default_schema_id = bored::catalog::SchemaId{84U};
    return config;
}

}  // namespace

TEST_CASE("build_ddl_commands translates CREATE DATABASE")
{
    const auto script = parse_ddl_script("CREATE DATABASE analytics;");
    const auto result = build_ddl_commands(script, DdlCommandBuilderConfig{});

    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::CreateDatabaseRequest>(command));
    const auto& request = std::get<bored::ddl::CreateDatabaseRequest>(command);
    CHECK(request.name == "analytics");
    CHECK_FALSE(request.if_not_exists);
}

TEST_CASE("build_ddl_commands fills default schema for CREATE TABLE")
{
    auto config = make_config();
    const auto script = parse_ddl_script("CREATE TABLE metrics (id INT);");
    const auto result = build_ddl_commands(script, config);

    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::CreateTableRequest>(command));
    const auto& request = std::get<bored::ddl::CreateTableRequest>(command);
    CHECK(request.schema_id == *config.default_schema_id);
    CHECK(request.name == "metrics");
    REQUIRE(request.columns.size() == 1U);
    CHECK(request.columns[0].name == "id");
    CHECK(request.columns[0].column_type == bored::catalog::CatalogColumnType::Int64);
}

TEST_CASE("build_ddl_commands warns about default expressions")
{
    auto config = make_config();
    const auto script = parse_ddl_script("CREATE TABLE metrics (id INT DEFAULT 42);");
    const auto result = build_ddl_commands(script, config);

    REQUIRE(result.commands.size() == 1U);
    const auto severity = std::count_if(result.diagnostics.begin(),
                                        result.diagnostics.end(),
                                        [](const ParserDiagnostic& diagnostic) {
                                            return diagnostic.severity == ParserSeverity::Warning;
                                        });
    REQUIRE(severity >= 1);
}

TEST_CASE("build_ddl_commands rejects unknown column types")
{
    auto config = make_config();
    const auto script = parse_ddl_script("CREATE TABLE metrics (id GEOMETRY);");
    const auto result = build_ddl_commands(script, config);

    CHECK(result.commands.empty());
    REQUIRE_FALSE(result.diagnostics.empty());
    const auto& diagnostic = result.diagnostics.back();
    CHECK(diagnostic.severity == ParserSeverity::Error);
    CHECK(diagnostic.message.find("Unsupported column type") != std::string::npos);
}

TEST_CASE("build_ddl_commands resolves named schema")
{
    auto config = make_config();
    config.schema_lookup = [](bored::catalog::DatabaseId database_id, std::string_view schema_name) -> std::optional<bored::catalog::SchemaId> {
        if (database_id.value == 42U && schema_name == "analytics") {
            return bored::catalog::SchemaId{101U};
        }
        return std::nullopt;
    };

    const auto script = parse_ddl_script("CREATE TABLE analytics.events (id INT);");
    const auto result = build_ddl_commands(script, config);

    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::CreateTableRequest>(command));
    const auto& request = std::get<bored::ddl::CreateTableRequest>(command);
    CHECK(request.schema_id.value == 101U);
}

TEST_CASE("build_ddl_commands reports missing default schema")
{
    DdlCommandBuilderConfig config{};
    config.default_database_id = bored::catalog::DatabaseId{42U};
    const auto script = parse_ddl_script("CREATE TABLE metrics (id INT);");
    const auto result = build_ddl_commands(script, config);

    CHECK(result.commands.empty());
    REQUIRE_FALSE(result.diagnostics.empty());
    const auto& diagnostic = result.diagnostics.back();
    CHECK(diagnostic.severity == ParserSeverity::Error);
    CHECK(diagnostic.message.find("default schema") != std::string::npos);
}

TEST_CASE("build_ddl_commands maps DROP SCHEMA cascade flag")
{
    auto config = make_config();
    const auto script = parse_ddl_script("DROP SCHEMA IF EXISTS analytics CASCADE;");
    const auto result = build_ddl_commands(script, config);

    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::DropSchemaRequest>(command));
    const auto& request = std::get<bored::ddl::DropSchemaRequest>(command);
    CHECK(request.database_id == *config.default_database_id);
    CHECK(request.name == "analytics");
    CHECK(request.if_exists);
    CHECK(request.cascade);
}

#include "bored/parser/ddl_command_builder.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
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

TEST_CASE("build_ddl_commands translates CREATE INDEX")
{
    auto config = make_config();

    CreateIndexStatement index_ast{};
    index_ast.name.value = "idx_metrics_tenant";
    index_ast.table.value = "metrics";
    index_ast.columns.push_back(Identifier{"tenant_id"});
    index_ast.unique = true;
    index_ast.if_not_exists = true;
    index_ast.max_fanout = 128U;
    index_ast.comparator = "tenant_cmp";
    index_ast.covering_columns.push_back(Identifier{"payload"});
    index_ast.predicate = "tenant_id >= 0";

    ScriptStatement statement{};
    statement.type = StatementType::CreateIndex;
    statement.success = true;
    statement.ast.emplace<CreateIndexStatement>(index_ast);

    const auto result = build_ddl_commands(statement, config);

    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::CreateIndexRequest>(command));
    const auto& request = std::get<bored::ddl::CreateIndexRequest>(command);
    CHECK(request.schema_id == *config.default_schema_id);
    CHECK(request.table_name == "metrics");
    CHECK(request.index_name == "idx_metrics_tenant");
    CHECK(request.if_not_exists);
    CHECK(request.unique);
    REQUIRE(request.column_names.size() == 1U);
    CHECK(request.column_names.front() == "tenant_id");
    REQUIRE(request.covering_column_names.size() == 1U);
    CHECK(request.covering_column_names.front() == "payload");
    CHECK(request.max_fanout == 128);
    CHECK(request.comparator == "tenant_cmp");
    CHECK(request.predicate == "tenant_id >= 0");
}

TEST_CASE("build_ddl_commands rejects CREATE INDEX without columns")
{
    auto config = make_config();

    CreateIndexStatement index_ast{};
    index_ast.name.value = "idx_metrics_empty";
    index_ast.table.value = "metrics";

    ScriptStatement statement{};
    statement.type = StatementType::CreateIndex;
    statement.success = true;
    statement.text = "CREATE INDEX idx_metrics_empty ON metrics ();";
    statement.ast.emplace<CreateIndexStatement>(index_ast);

    const auto result = build_ddl_commands(statement, config);

    CHECK(result.commands.empty());
    REQUIRE_FALSE(result.diagnostics.empty());
    const auto has_error_diag = std::any_of(result.diagnostics.begin(),
                                            result.diagnostics.end(),
                                            [](const ParserDiagnostic& diagnostic) {
                                                return diagnostic.severity == ParserSeverity::Error &&
                                                       diagnostic.message.find("CREATE INDEX") != std::string::npos;
                                            });
    CHECK(has_error_diag);
}

TEST_CASE("parse_create_index captures optional clauses")
{
    const auto result = parse_create_index(
        "CREATE UNIQUE INDEX IF NOT EXISTS analytics.idx_metrics_tenant ON metrics USING tenant_cmp (tenant_id, created_at) INCLUDE (payload, notes) WITH ( FANOUT = 256, COMPARATOR = tenant_custom_cmp ) WHERE tenant_id >= 0;");

    REQUIRE(result.ast);
    const auto& statement = *result.ast;
    CHECK(statement.unique);
    CHECK(statement.if_not_exists);
    CHECK(statement.schema.value == "analytics");
    CHECK(statement.name.value == "idx_metrics_tenant");
    CHECK(statement.table.value == "metrics");
    CHECK(statement.comparator == "tenant_custom_cmp");
    CHECK(statement.max_fanout == 256U);
    REQUIRE(statement.columns.size() == 2U);
    CHECK(statement.columns[0].value == "tenant_id");
    CHECK(statement.columns[1].value == "created_at");
    REQUIRE(statement.covering_columns.size() == 2U);
    CHECK(statement.covering_columns[0].value == "payload");
    CHECK(statement.covering_columns[1].value == "notes");
    REQUIRE(statement.predicate);
    CHECK(*statement.predicate == "tenant_id >= 0");
    CHECK(result.diagnostics.empty());
}

TEST_CASE("build_ddl_commands translates CREATE INDEX from script")
{
    auto config = make_config();
    const auto script = parse_ddl_script(
        "CREATE INDEX IF NOT EXISTS idx_metrics_tenant ON metrics (tenant_id) WITH ( FANOUT = 512 );");

    const auto result = build_ddl_commands(script, config);

    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.commands.size() == 1U);
    const auto& command = result.commands.front();
    REQUIRE(std::holds_alternative<bored::ddl::CreateIndexRequest>(command));
    const auto& request = std::get<bored::ddl::CreateIndexRequest>(command);
    CHECK(request.schema_id == *config.default_schema_id);
    CHECK(request.table_name == "metrics");
    CHECK(request.index_name == "idx_metrics_tenant");
    REQUIRE(request.column_names.size() == 1U);
    CHECK(request.column_names.front() == "tenant_id");
    CHECK(request.max_fanout == 512);
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

#include "bored/catalog/catalog_ids.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using bored::shell::ShellEngine;
using bored::shell::CommandMetrics;
using Catch::Matchers::ContainsSubstring;

TEST_CASE("ShellEngine parses simple DDL statement")
{
    ShellEngine engine;
    const auto result = engine.execute_sql("CREATE TABLE metrics (id INT);");

    REQUIRE(result.success);
    CHECK(result.rows_touched == 0U);
    CHECK(result.wal_bytes == 0U);
    CHECK(result.diagnostics.empty());
}

TEST_CASE("ShellEngine reports parser errors")
{
    ShellEngine engine;
    const auto result = engine.execute_sql("CREATE TABLE metrics id INT);" /* missing parentheses */);

    REQUIRE_FALSE(result.success);
    CHECK_FALSE(result.diagnostics.empty());
}

TEST_CASE("ShellEngine executes DDL via configured executor")
{
    bored::parser::DdlScriptExecutor executor({
        .builder_config = {
            .default_database_id = bored::catalog::DatabaseId{1U},
            .default_schema_id = bored::catalog::SchemaId{10U}
        }
    });

    ShellEngine::Config config{};
    config.ddl_executor = &executor;

    ShellEngine engine{config};
    const auto result = engine.execute_sql("CREATE TABLE metrics (id INT);");

    REQUIRE(result.success);
    CHECK_THAT(result.summary, ContainsSubstring("Parsed 1 statement"));

    const auto snapshot = executor.telemetry().snapshot();
    CHECK(snapshot.scripts_attempted == 1U);
}

TEST_CASE("ShellEngine routes DML through configured executor")
{
    bool invoked = false;

    ShellEngine::Config config{};
    config.dml_executor = [&invoked](const std::string& sql) {
        invoked = true;
        CHECK(sql == "SELECT * FROM metrics;");
        CommandMetrics metrics{};
        metrics.success = true;
        metrics.summary = "Mock DML success";
        metrics.duration_ms = 1.5;
        return metrics;
    };

    ShellEngine engine{config};
    const auto result = engine.execute_sql("SELECT * FROM metrics;");

    REQUIRE(invoked);
    REQUIRE(result.success);
    CHECK(result.summary == "Mock DML success");
    CHECK(result.duration_ms > 0.0);
}

TEST_CASE("ShellEngine rejects unsupported commands")
{
    ShellEngine engine;
    const auto result = engine.execute_sql("VACUUM;");

    REQUIRE_FALSE(result.success);
    CHECK_THAT(result.summary, ContainsSubstring("Unsupported command"));
    CHECK_FALSE(result.diagnostics.empty());
}

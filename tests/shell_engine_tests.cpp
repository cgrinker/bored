#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>

using bored::shell::ShellEngine;

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

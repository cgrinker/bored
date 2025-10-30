#include "bored/catalog/catalog_ids.hpp"
#include "bored/catalog/catalog_introspection.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/shell/shell_engine.hpp"
#include "bored/storage/lock_manager.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>

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

TEST_CASE("ShellEngine renders catalog meta commands")
{
    ShellEngine::Config config{};
    config.catalog_snapshot = [] {
        bored::catalog::CatalogIntrospectionSnapshot snapshot{};
        bored::catalog::CatalogRelationSummary relation{};
        relation.database_name = "system";
        relation.schema_name = "analytics";
        relation.relation_name = "metrics";
        relation.relation_kind = bored::catalog::CatalogRelationKind::Table;
        relation.column_count = 3U;
        relation.root_page_id = 128U;
        snapshot.relations.push_back(relation);

        bored::catalog::CatalogRelationSummary system_relation{};
        system_relation.database_name = "system";
        system_relation.schema_name = "system";
        system_relation.relation_name = "catalog_tables";
        system_relation.relation_kind = bored::catalog::CatalogRelationKind::SystemTable;
        system_relation.column_count = 5U;
        system_relation.root_page_id = 5U;
        snapshot.relations.push_back(system_relation);

        bored::catalog::CatalogIndexSummary index{};
        index.schema_name = "analytics";
        index.relation_name = "metrics";
        index.index_name = "metrics_pk";
        index.index_type = bored::catalog::CatalogIndexType::BTree;
        index.max_fanout = 64U;
        index.root_page_id = 140U;
        snapshot.indexes.push_back(index);

        return snapshot;
    };

    ShellEngine engine{config};

    const auto tables = engine.execute_sql("\\dt");
    REQUIRE(tables.success);
    REQUIRE_FALSE(tables.detail_lines.empty());
    CHECK_THAT(tables.detail_lines.front(), ContainsSubstring("database"));
    const bool metrics_present = std::any_of(tables.detail_lines.begin(), tables.detail_lines.end(), [](const std::string& line) {
        return line.find("metrics") != std::string::npos;
    });
    CHECK(metrics_present);
    CHECK_THAT(tables.summary, ContainsSubstring("Listed 2 relations"));

    const auto filtered = engine.execute_sql("\\dt metrics");
    REQUIRE(filtered.success);
    REQUIRE(filtered.detail_lines.size() >= 3U);  // header + separator + row
    CHECK_THAT(filtered.summary, ContainsSubstring("Listed 1 relation"));

    const auto indexes = engine.execute_sql("\\di");
    REQUIRE(indexes.success);
    REQUIRE_THAT(indexes.summary, ContainsSubstring("Listed 1 index"));
    REQUIRE_FALSE(indexes.detail_lines.empty());
}

TEST_CASE("ShellEngine reports lock meta command output")
{
    ShellEngine::Config config{};
    config.lock_snapshot = [] {
        std::vector<bored::storage::LockManager::LockSnapshot> locks;
        bored::storage::LockManager::LockSnapshot snapshot{};
        snapshot.page_id = 777U;
        snapshot.total_shared = 2U;
        snapshot.exclusive_depth = 1U;
        snapshot.exclusive_owner = "thread-main";

        bored::storage::LockManager::LockHolderSnapshot holder{};
        holder.thread_id = "thread-main";
        holder.shared = 1U;
        holder.exclusive = 1U;
        snapshot.holders.push_back(holder);

        locks.push_back(snapshot);
        return locks;
    };

    ShellEngine engine{config};
    const auto locks = engine.execute_sql("\\dl");

    REQUIRE(locks.success);
    REQUIRE_THAT(locks.summary, ContainsSubstring("Listed 1 lock"));
    REQUIRE_FALSE(locks.detail_lines.empty());
    CHECK_THAT(locks.detail_lines.back(), ContainsSubstring("777"));
}

TEST_CASE("ShellEngine surfaces missing meta providers")
{
    ShellEngine engine;
    const auto result = engine.execute_sql("\\dt");

    REQUIRE_FALSE(result.success);
    CHECK_THAT(result.summary, ContainsSubstring("Catalog introspection"));
    REQUIRE_FALSE(result.diagnostics.empty());
}

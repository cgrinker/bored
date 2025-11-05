#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <stdexcept>
#include <variant>
#include <vector>

using bored::shell::ShellBackend;
using bored::shell::ShellEngine;
using bored::shell::ShellBackendTestAccess;
using Catch::Matchers::ContainsSubstring;

namespace bored::shell {

struct ShellBackendTestAccess final {
    static bored::executor::WorkTableRegistry& worktable_registry(ShellBackend& backend)
    {
        return backend.worktable_registry_;
    }

    static ShellBackend::TableData* find_table_or_default_schema(ShellBackend& backend, std::string_view name)
    {
        return backend.find_table_or_default_schema(name);
    }

    static std::optional<ShellBackend::ConstraintIndexPredicate> parse_index_predicate(
        const ShellBackend::TableData& table,
        std::string_view predicate,
        std::string& error)
    {
        return ShellBackend::parse_constraint_predicate(table, predicate, error);
    }

    static bool evaluate_index_predicate(const ShellBackend::ConstraintIndexPredicate& predicate,
                                         const std::vector<ShellBackend::ScalarValue>& values)
    {
        return ShellBackend::evaluate_constraint_predicate(predicate, values);
    }
};

}  // namespace bored::shell

namespace {

struct ShellBackendHarness final {
    ShellBackendHarness()
        : storage_root{make_storage_root()}
        , cleanup{storage_root}
        , backend{make_backend_config(storage_root)}
        , engine{backend.make_config()}
    {
        auto create = engine.execute_sql("CREATE TABLE metrics (id BIGINT, counter BIGINT, name TEXT);");
        INFO(create.summary);
        if (!create.success) {
            for (const auto& diagnostic : create.diagnostics) {
                INFO(diagnostic.message);
                for (const auto& hint : diagnostic.remediation_hints) {
                    INFO(hint);
                }
            }
        }
        REQUIRE(create.success);

        auto insert = engine.execute_sql("INSERT INTO metrics (id, counter, name) VALUES (1, 7, 'alpha');");
        INFO(insert.summary);
        if (!insert.success) {
            for (const auto& diagnostic : insert.diagnostics) {
                INFO(diagnostic.message);
                for (const auto& hint : diagnostic.remediation_hints) {
                    INFO(hint);
                }
            }
        }
        REQUIRE(insert.success);
        CHECK(insert.rows_touched == 1U);
        CHECK(insert.wal_bytes > 0U);
    }

    static ShellBackend::Config make_backend_config(const std::filesystem::path& root)
    {
        ShellBackend::Config config{};
        config.storage_directory = root;
        config.wal_directory = root / "wal";
        config.io_use_full_fsync = false;
        return config;
    }

    static std::filesystem::path make_storage_root()
    {
        const auto base = std::filesystem::temp_directory_path();
        std::random_device rd;
        for (int attempt = 0; attempt < 32; ++attempt) {
            const auto candidate = base / ("bored_shell_backend_" + std::to_string(static_cast<unsigned long long>(rd())));
            std::error_code ec;
            std::filesystem::remove_all(candidate, ec);
            ec = {};
            if (std::filesystem::create_directories(candidate, ec) && !ec) {
                return candidate;
            }
        }
        throw std::runtime_error("Failed to create shell backend storage directory");
    }

    struct StorageCleanup final {
        explicit StorageCleanup(std::filesystem::path root) noexcept
            : path{std::move(root)}
        {
        }

        ~StorageCleanup()
        {
            if (path.empty()) {
                return;
            }
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }

        std::filesystem::path path{};
    };

    std::filesystem::path storage_root{};
    StorageCleanup cleanup;
    ShellBackend backend;
    ShellEngine engine;
};

[[nodiscard]] bool starts_with(std::string_view text, std::string_view prefix)
{
    return text.substr(0U, prefix.size()) == prefix;
}

}  // namespace

TEST_CASE("ShellBackend emits executor pipeline diagnostics for SELECT")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("SELECT id, counter FROM metrics;");
    REQUIRE(metrics.success);
    CHECK(metrics.rows_touched == 1U);
    CHECK(metrics.wal_bytes == 0U);

    auto logical_root_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.root=");
    });
    REQUIRE(logical_root_it != metrics.detail_lines.end());

    auto logical_plan_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.plan:");
    });
    REQUIRE(logical_plan_it != metrics.detail_lines.end());

    auto root_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(root_it != metrics.detail_lines.end());

    auto pipeline_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.pipeline=SELECT");
    });
    REQUIRE(pipeline_it != metrics.detail_lines.end());
    CHECK_THAT(*pipeline_it, ContainsSubstring("root=SeqScan"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("chain=<none>"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("materialized=false"));

    auto plan_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    REQUIRE(plan_it != metrics.detail_lines.end());

    CHECK(std::distance(metrics.detail_lines.begin(), root_it) <
          std::distance(metrics.detail_lines.begin(), pipeline_it));
    CHECK(std::distance(metrics.detail_lines.begin(), logical_root_it) <
          std::distance(metrics.detail_lines.begin(), root_it));
}

TEST_CASE("ShellBackend parses and evaluates numeric constraint predicates")
{
    ShellBackendHarness harness;
    auto* table = ShellBackendTestAccess::find_table_or_default_schema(harness.backend, "metrics");
    REQUIRE(table != nullptr);

    std::string error;
    auto predicate = ShellBackendTestAccess::parse_index_predicate(
        *table,
        "counter >= 5",
        error);
    REQUIRE(predicate.has_value());
    CHECK(error.empty());
    CHECK(predicate->column_index == 1U);
    CHECK(predicate->comparison == ShellBackend::ConstraintIndexPredicate::Comparison::GreaterEqual);

    std::vector<ShellBackend::ScalarValue> row_values{std::int64_t{1}, std::int64_t{6}, std::string{"alpha"}};
    CHECK(ShellBackendTestAccess::evaluate_index_predicate(*predicate, row_values));

    row_values[1] = std::int64_t{3};
    CHECK_FALSE(ShellBackendTestAccess::evaluate_index_predicate(*predicate, row_values));
}

TEST_CASE("ShellBackend parses and evaluates string constraint predicates")
{
    ShellBackendHarness harness;
    auto* table = ShellBackendTestAccess::find_table_or_default_schema(harness.backend, "metrics");
    REQUIRE(table != nullptr);

    std::string error;
    auto predicate = ShellBackendTestAccess::parse_index_predicate(
        *table,
        "name = 'alpha'",
        error);
    REQUIRE(predicate.has_value());
    CHECK(error.empty());
    CHECK(predicate->column_index == 2U);
    CHECK(predicate->comparison == ShellBackend::ConstraintIndexPredicate::Comparison::Equal);

    std::vector<ShellBackend::ScalarValue> row_values{std::int64_t{1}, std::int64_t{6}, std::string{"alpha"}};
    CHECK(ShellBackendTestAccess::evaluate_index_predicate(*predicate, row_values));

    row_values[2] = std::string{"beta"};
    CHECK_FALSE(ShellBackendTestAccess::evaluate_index_predicate(*predicate, row_values));
}

TEST_CASE("ShellBackend predicate parsing surfaces errors for unknown columns")
{
    ShellBackendHarness harness;
    auto* table = ShellBackendTestAccess::find_table_or_default_schema(harness.backend, "metrics");
    REQUIRE(table != nullptr);

    std::string error;
    auto predicate = ShellBackendTestAccess::parse_index_predicate(
        *table,
        "missing = 42",
        error);
    CHECK_FALSE(predicate.has_value());
    CHECK_FALSE(error.empty());
}

TEST_CASE("ShellBackend emits executor pipeline diagnostics for INSERT")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("INSERT INTO metrics (id, counter, name) VALUES (2, 11, 'beta');");
    REQUIRE(metrics.success);
    CHECK_THAT(metrics.summary, ContainsSubstring("Inserted 1 row"));
    CHECK(metrics.rows_touched == 1U);
    CHECK(metrics.wal_bytes > 0U);

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.insert");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto root_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(root_it != metrics.detail_lines.end());
    CHECK_THAT(*root_it, ContainsSubstring("Insert"));

    auto pipeline_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.pipeline=INSERT");
    });
    REQUIRE(pipeline_it != metrics.detail_lines.end());
    CHECK_THAT(*pipeline_it, ContainsSubstring("root=Insert"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("chain=Values"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("materialized=false"));

    auto plan_line_count = std::count_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    CHECK(plan_line_count > 0);
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), root_it));
}

TEST_CASE("ShellBackend emits executor pipeline diagnostics for UPDATE")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("UPDATE metrics SET counter = counter + 1 WHERE id = 1;");
    REQUIRE(metrics.success);
    CHECK(metrics.rows_touched == 1U);
    CHECK(metrics.wal_bytes > 0U);

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.update");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto planner_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(planner_it != metrics.detail_lines.end());
    CHECK_THAT(*planner_it, ContainsSubstring("Update"));

    auto pipeline_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.pipeline=UPDATE");
    });
    REQUIRE(pipeline_it != metrics.detail_lines.end());
    CHECK_THAT(*pipeline_it, ContainsSubstring("root=Update"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("chain=Projection->Filter->SeqScan"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("materialized=false"));

    auto plan_line_count = std::count_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    CHECK(plan_line_count > 0);
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), planner_it));
    CHECK(std::distance(metrics.detail_lines.begin(), planner_it) <
          std::distance(metrics.detail_lines.begin(), pipeline_it));
}

TEST_CASE("ShellBackend emits executor pipeline diagnostics for DELETE")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("DELETE FROM metrics WHERE id = 1;");
    REQUIRE(metrics.success);
    CHECK_THAT(metrics.summary, ContainsSubstring("Deleted 1 row"));
    CHECK(metrics.rows_touched == 1U);
    CHECK(metrics.wal_bytes > 0U);

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.delete");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto planner_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(planner_it != metrics.detail_lines.end());
    CHECK_THAT(*planner_it, ContainsSubstring("Delete"));

    auto pipeline_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.pipeline=DELETE");
    });
    REQUIRE(pipeline_it != metrics.detail_lines.end());
    CHECK_THAT(*pipeline_it, ContainsSubstring("root=Delete"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("chain=Projection->Filter->SeqScan"));
    CHECK_THAT(*pipeline_it, ContainsSubstring("materialized=false"));

    auto plan_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    REQUIRE(plan_it != metrics.detail_lines.end());
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), planner_it));
    CHECK(std::distance(metrics.detail_lines.begin(), planner_it) <
          std::distance(metrics.detail_lines.begin(), pipeline_it));
}

TEST_CASE("ShellBackend executes DML statements when preceded by comments")
{
    ShellBackendHarness harness;

    const auto insert = harness.engine.execute_sql("-- leading comment for insert\nINSERT INTO metrics (id, counter, name) VALUES (2, 3, 'beta');");
    REQUIRE(insert.success);
    CHECK_THAT(insert.summary, ContainsSubstring("Inserted 1 row"));
    CHECK(insert.rows_touched == 1U);
    CHECK(insert.wal_bytes > 0U);

    const auto update = harness.engine.execute_sql("/* block comment before update */\nUPDATE metrics SET counter = counter + 4 WHERE id = 2;");
    REQUIRE(update.success);
    CHECK_THAT(update.summary, ContainsSubstring("Updated 1 row"));
    CHECK(update.rows_touched == 1U);
    CHECK(update.wal_bytes > 0U);

    const auto select = harness.engine.execute_sql("-- leading comment for select\nSELECT id, counter FROM metrics;");
    REQUIRE(select.success);
    CHECK_THAT(select.summary, ContainsSubstring("Selected 2 rows"));
    CHECK(select.rows_touched == 2U);
    CHECK(select.wal_bytes == 0U);

    const auto remove = harness.engine.execute_sql("/* removal comment */\nDELETE FROM metrics WHERE id = 2;");
    REQUIRE(remove.success);
    CHECK_THAT(remove.summary, ContainsSubstring("Deleted 1 row"));
    CHECK(remove.rows_touched == 1U);
    CHECK(remove.wal_bytes > 0U);

    const auto final_select = harness.engine.execute_sql("SELECT id FROM metrics;");
    REQUIRE(final_select.success);
    CHECK_THAT(final_select.summary, ContainsSubstring("Selected 1 row"));
    CHECK(final_select.rows_touched == 1U);
    CHECK(final_select.wal_bytes == 0U);
}

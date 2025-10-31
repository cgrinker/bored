#include "bored/shell/shell_backend.hpp"
#include "bored/shell/shell_engine.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <string>
#include <string_view>

using bored::shell::ShellBackend;
using bored::shell::ShellEngine;
using Catch::Matchers::ContainsSubstring;

namespace {

struct ShellBackendHarness final {
    ShellBackendHarness()
        : backend{}
        , engine{backend.make_config()}
    {
        auto create = engine.execute_sql("CREATE TABLE metrics (id BIGINT, counter BIGINT, name TEXT);");
        REQUIRE(create.success);

        auto insert = engine.execute_sql("INSERT INTO metrics (id, counter, name) VALUES (1, 7, 'alpha');");
        REQUIRE(insert.success);
    }

    ShellBackend backend{};
    ShellEngine engine;
};

[[nodiscard]] bool starts_with(std::string_view text, std::string_view prefix)
{
    return text.substr(0U, prefix.size()) == prefix;
}

}  // namespace

TEST_CASE("ShellBackend emits executor stub diagnostics for SELECT")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("SELECT id, counter FROM metrics;");
    REQUIRE(metrics.success);

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

    auto stub_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.stub=SELECT pipeline ready");
    });
    REQUIRE(stub_it != metrics.detail_lines.end());
    CHECK_THAT(*stub_it, ContainsSubstring("Projection"));

    auto plan_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    REQUIRE(plan_it != metrics.detail_lines.end());

    CHECK(std::distance(metrics.detail_lines.begin(), root_it) <
          std::distance(metrics.detail_lines.begin(), stub_it));
    CHECK(std::distance(metrics.detail_lines.begin(), logical_root_it) <
          std::distance(metrics.detail_lines.begin(), root_it));
}

TEST_CASE("ShellBackend emits executor stub diagnostics for INSERT")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("INSERT INTO metrics (id, counter, name) VALUES (2, 11, 'beta');");
    REQUIRE(metrics.success);
    CHECK_THAT(metrics.summary, ContainsSubstring("Inserted 1 row"));

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.insert");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto root_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(root_it != metrics.detail_lines.end());
    CHECK_THAT(*root_it, ContainsSubstring("Insert"));

    auto stub_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.stub=INSERT pipeline ready");
    });
    REQUIRE(stub_it != metrics.detail_lines.end());
    CHECK_THAT(*stub_it, ContainsSubstring("root=Insert"));

    auto plan_line_count = std::count_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    CHECK(plan_line_count > 0);
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), root_it));
}

TEST_CASE("ShellBackend emits executor stub diagnostics for UPDATE")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("UPDATE metrics SET counter = counter + 1 WHERE id = 1;");
    REQUIRE(metrics.success);

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.update");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto planner_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(planner_it != metrics.detail_lines.end());
    CHECK_THAT(*planner_it, ContainsSubstring("Update"));

    auto stub_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.stub=UPDATE pipeline ready");
    });
    REQUIRE(stub_it != metrics.detail_lines.end());
    CHECK_THAT(*stub_it, ContainsSubstring("root=Update"));
    CHECK_THAT(*stub_it, ContainsSubstring("SeqScan"));

    auto plan_line_count = std::count_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    CHECK(plan_line_count > 0);
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), planner_it));
    CHECK(std::distance(metrics.detail_lines.begin(), planner_it) <
          std::distance(metrics.detail_lines.begin(), stub_it));
}

TEST_CASE("ShellBackend emits executor stub diagnostics for DELETE")
{
    ShellBackendHarness harness;

    const auto metrics = harness.engine.execute_sql("DELETE FROM metrics WHERE id = 1;");
    REQUIRE(metrics.success);
    CHECK_THAT(metrics.summary, ContainsSubstring("Deleted 1 row"));

    auto logical_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "logical.delete");
    });
    REQUIRE(logical_it != metrics.detail_lines.end());

    auto planner_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "planner.root=");
    });
    REQUIRE(planner_it != metrics.detail_lines.end());
    CHECK_THAT(*planner_it, ContainsSubstring("Delete"));

    auto stub_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.stub=DELETE pipeline ready");
    });
    REQUIRE(stub_it != metrics.detail_lines.end());
    CHECK_THAT(*stub_it, ContainsSubstring("root=Delete"));

    auto plan_it = std::find_if(metrics.detail_lines.begin(), metrics.detail_lines.end(), [](const std::string& line) {
        return starts_with(line, "executor.plan:");
    });
    REQUIRE(plan_it != metrics.detail_lines.end());
    CHECK(std::distance(metrics.detail_lines.begin(), logical_it) <
          std::distance(metrics.detail_lines.begin(), planner_it));
    CHECK(std::distance(metrics.detail_lines.begin(), planner_it) <
          std::distance(metrics.detail_lines.begin(), stub_it));
}

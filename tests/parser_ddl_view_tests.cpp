#include "bored/parser/grammar.hpp"

#include <catch2/catch_test_macros.hpp>

using namespace bored::parser;

TEST_CASE("parse_create_view handles minimal definition")
{
    const auto result = parse_create_view("CREATE VIEW analytics.daily_summary AS SELECT 1;");
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->schema.value == "analytics");
    CHECK(result.ast->name.value == "daily_summary");
    CHECK_FALSE(result.ast->if_not_exists);
    CHECK(result.ast->definition == "SELECT 1");
    CHECK(result.diagnostics.empty());
}

TEST_CASE("parse_create_view supports IF NOT EXISTS and trims definition")
{
    const auto sql = R"(CREATE VIEW IF NOT EXISTS staging.weekly_report AS
        SELECT date_trunc('week', created_at) AS week_start,
               COUNT(*) AS visit_count
        FROM staging.events
    )";

    const auto result = parse_create_view(sql);
    REQUIRE(result.success());
    REQUIRE(result.ast.has_value());
    CHECK(result.ast->if_not_exists);
    CHECK(result.ast->schema.value == "staging");
    CHECK(result.ast->name.value == "weekly_report");
    CHECK(result.ast->definition.starts_with("SELECT"));
    CHECK(result.ast->definition.find("FROM staging.events") != std::string::npos);
}

TEST_CASE("parse_create_view rejects missing AS keyword")
{
    const auto result = parse_create_view("CREATE VIEW analytics.daily_summary SELECT 1;");
    CHECK_FALSE(result.success());
    REQUIRE_FALSE(result.diagnostics.empty());
    CHECK(result.diagnostics.front().severity == ParserSeverity::Warning);
}

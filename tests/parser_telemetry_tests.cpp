#include "bored/parser/parser_telemetry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <vector>

using namespace bored::parser;

TEST_CASE("ParserTelemetry tracks attempts and duration")
{
    ParserTelemetry telemetry;
    telemetry.record_script_attempt();
    telemetry.record_script_result(true, 1'000U, 3U, 2U, 1U, 2U, 0U);

    const auto snapshot = telemetry.snapshot();
    CHECK(snapshot.scripts_attempted == 1U);
    CHECK(snapshot.scripts_succeeded == 1U);
    CHECK(snapshot.statements_attempted == 3U);
    CHECK(snapshot.statements_succeeded == 2U);
    CHECK(snapshot.diagnostics_info == 1U);
    CHECK(snapshot.diagnostics_warning == 2U);
    CHECK(snapshot.diagnostics_error == 0U);
    CHECK(snapshot.total_parse_duration_ns == 1'000U);
    CHECK(snapshot.last_parse_duration_ns == 1'000U);

    telemetry.reset();
    const auto reset = telemetry.snapshot();
    CHECK(reset.scripts_attempted == 0U);
    CHECK(reset.total_parse_duration_ns == 0U);
}

TEST_CASE("ParserTelemetryRegistry aggregates and visits samplers")
{
    ParserTelemetryRegistry registry;
    registry.register_sampler("parser/a", [] {
        ParserTelemetrySnapshot snapshot{};
        snapshot.scripts_attempted = 2U;
        snapshot.diagnostics_warning = 1U;
        snapshot.last_parse_duration_ns = 50U;
        return snapshot;
    });

    registry.register_sampler("parser/b", [] {
        ParserTelemetrySnapshot snapshot{};
        snapshot.scripts_attempted = 3U;
        snapshot.scripts_succeeded = 1U;
        snapshot.diagnostics_error = 2U;
        snapshot.total_parse_duration_ns = 150U;
        snapshot.last_parse_duration_ns = 75U;
        return snapshot;
    });

    const auto total = registry.aggregate();
    CHECK(total.scripts_attempted == 5U);
    CHECK(total.scripts_succeeded == 1U);
    CHECK(total.diagnostics_warning == 1U);
    CHECK(total.diagnostics_error == 2U);
    CHECK(total.total_parse_duration_ns == 150U);
    CHECK(total.last_parse_duration_ns == 75U);

    std::vector<std::string> ids;
    registry.visit([&](const std::string& id, const ParserTelemetrySnapshot&) { ids.push_back(id); });
    REQUIRE(ids.size() == 2U);
    CHECK(std::find(ids.begin(), ids.end(), "parser/a") != ids.end());
    CHECK(std::find(ids.begin(), ids.end(), "parser/b") != ids.end());

    registry.unregister_sampler("parser/a");
    registry.unregister_sampler("parser/b");
    const auto empty_total = registry.aggregate();
    CHECK(empty_total.scripts_attempted == 0U);
    CHECK(empty_total.diagnostics_error == 0U);
}

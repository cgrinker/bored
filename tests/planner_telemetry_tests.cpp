#include "bored/planner/planner_telemetry.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

using bored::planner::PlanDiagnostics;
using bored::planner::PlannerTelemetry;
using bored::planner::PlannerTelemetrySnapshot;
using Catch::Approx;

namespace {

PlanDiagnostics make_diagnostics(double chosen_cost)
{
    PlanDiagnostics diagnostics{};
    diagnostics.rules_attempted = 5U;
    diagnostics.rules_applied = 3U;
    diagnostics.cost_evaluations = 4U;
    diagnostics.chosen_plan_cost = chosen_cost;
    diagnostics.alternatives.push_back({});
    diagnostics.alternatives.push_back({});
    return diagnostics;
}

}  // namespace

TEST_CASE("PlannerTelemetry tracks attempts, success, and failure")
{
    PlannerTelemetry telemetry;

    telemetry.record_plan_attempt();
    telemetry.record_plan_success(make_diagnostics(42.5));
    telemetry.record_plan_attempt();
    telemetry.record_plan_failure();

    PlannerTelemetrySnapshot snapshot = telemetry.snapshot();
    REQUIRE(snapshot.plans_attempted == 2U);
    REQUIRE(snapshot.plans_succeeded == 1U);
    REQUIRE(snapshot.plans_failed == 1U);
    REQUIRE(snapshot.rules_attempted == 5U);
    REQUIRE(snapshot.rules_applied == 3U);
    REQUIRE(snapshot.cost_evaluations == 4U);
    REQUIRE(snapshot.alternatives_considered == 2U);
    REQUIRE(snapshot.total_chosen_cost == Approx(42.5));
    REQUIRE(snapshot.last_chosen_cost == Approx(42.5));
    REQUIRE(snapshot.min_chosen_cost == Approx(42.5));
    REQUIRE(snapshot.max_chosen_cost == Approx(42.5));
}

TEST_CASE("PlannerTelemetry reset clears counters")
{
    PlannerTelemetry telemetry;
    telemetry.record_plan_attempt();
    telemetry.record_plan_success(make_diagnostics(10.0));

    telemetry.reset();
    PlannerTelemetrySnapshot snapshot = telemetry.snapshot();

    REQUIRE(snapshot.plans_attempted == 0U);
    REQUIRE(snapshot.plans_succeeded == 0U);
    REQUIRE(snapshot.plans_failed == 0U);
    REQUIRE(snapshot.rules_attempted == 0U);
    REQUIRE(snapshot.rules_applied == 0U);
    REQUIRE(snapshot.cost_evaluations == 0U);
    REQUIRE(snapshot.alternatives_considered == 0U);
    REQUIRE(snapshot.total_chosen_cost == Approx(0.0));
    REQUIRE(snapshot.last_chosen_cost == Approx(0.0));
    REQUIRE(snapshot.min_chosen_cost == Approx(0.0));
    REQUIRE(snapshot.max_chosen_cost == Approx(0.0));
}

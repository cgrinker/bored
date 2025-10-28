#pragma once

#include "bored/planner/plan_diagnostics.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace bored::planner {

struct PlannerTelemetrySnapshot final {
    std::uint64_t plans_attempted = 0U;
    std::uint64_t plans_succeeded = 0U;
    std::uint64_t plans_failed = 0U;
    std::uint64_t rules_attempted = 0U;
    std::uint64_t rules_applied = 0U;
    std::uint64_t cost_evaluations = 0U;
    std::uint64_t alternatives_considered = 0U;
    double total_chosen_cost = 0.0;
    double last_chosen_cost = 0.0;
    double min_chosen_cost = std::numeric_limits<double>::infinity();
    double max_chosen_cost = -std::numeric_limits<double>::infinity();
};

class PlannerTelemetry final {
public:
    void record_plan_attempt() noexcept;
    void record_plan_success(const PlanDiagnostics& diagnostics) noexcept;
    void record_plan_failure() noexcept;

    [[nodiscard]] PlannerTelemetrySnapshot snapshot() const noexcept;
    void reset() noexcept;

private:
    static void update_min(std::atomic<double>& target, double value) noexcept;
    static void update_max(std::atomic<double>& target, double value) noexcept;
    static void add_relaxed(std::atomic<double>& target, double value) noexcept;

    std::atomic<std::uint64_t> plans_attempted_{0U};
    std::atomic<std::uint64_t> plans_succeeded_{0U};
    std::atomic<std::uint64_t> plans_failed_{0U};
    std::atomic<std::uint64_t> rules_attempted_{0U};
    std::atomic<std::uint64_t> rules_applied_{0U};
    std::atomic<std::uint64_t> cost_evaluations_{0U};
    std::atomic<std::uint64_t> alternatives_considered_{0U};
    std::atomic<double> total_chosen_cost_{0.0};
    std::atomic<double> last_chosen_cost_{0.0};
    std::atomic<double> min_chosen_cost_{std::numeric_limits<double>::infinity()};
    std::atomic<double> max_chosen_cost_{-std::numeric_limits<double>::infinity()};
};

}  // namespace bored::planner

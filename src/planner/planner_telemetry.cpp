#include "bored/planner/planner_telemetry.hpp"

#include <cmath>
#include <limits>

namespace bored::planner {

void PlannerTelemetry::record_plan_attempt() noexcept
{
    plans_attempted_.fetch_add(1U, std::memory_order_relaxed);
}

void PlannerTelemetry::record_plan_success(const PlanDiagnostics& diagnostics) noexcept
{
    plans_succeeded_.fetch_add(1U, std::memory_order_relaxed);
    rules_attempted_.fetch_add(static_cast<std::uint64_t>(diagnostics.rules_attempted), std::memory_order_relaxed);
    rules_applied_.fetch_add(static_cast<std::uint64_t>(diagnostics.rules_applied), std::memory_order_relaxed);
    cost_evaluations_.fetch_add(static_cast<std::uint64_t>(diagnostics.cost_evaluations), std::memory_order_relaxed);
    alternatives_considered_.fetch_add(static_cast<std::uint64_t>(diagnostics.alternatives.size()), std::memory_order_relaxed);

    const double chosen_cost = diagnostics.chosen_plan_cost;
    if (std::isfinite(chosen_cost)) {
        add_relaxed(total_chosen_cost_, chosen_cost);
        last_chosen_cost_.store(chosen_cost, std::memory_order_relaxed);
        update_min(min_chosen_cost_, chosen_cost);
        update_max(max_chosen_cost_, chosen_cost);
    }
}

void PlannerTelemetry::record_plan_failure() noexcept
{
    plans_failed_.fetch_add(1U, std::memory_order_relaxed);
}

PlannerTelemetrySnapshot PlannerTelemetry::snapshot() const noexcept
{
    PlannerTelemetrySnapshot snapshot{};
    snapshot.plans_attempted = plans_attempted_.load(std::memory_order_relaxed);
    snapshot.plans_succeeded = plans_succeeded_.load(std::memory_order_relaxed);
    snapshot.plans_failed = plans_failed_.load(std::memory_order_relaxed);
    snapshot.rules_attempted = rules_attempted_.load(std::memory_order_relaxed);
    snapshot.rules_applied = rules_applied_.load(std::memory_order_relaxed);
    snapshot.cost_evaluations = cost_evaluations_.load(std::memory_order_relaxed);
    snapshot.alternatives_considered = alternatives_considered_.load(std::memory_order_relaxed);
    snapshot.total_chosen_cost = total_chosen_cost_.load(std::memory_order_relaxed);
    snapshot.last_chosen_cost = last_chosen_cost_.load(std::memory_order_relaxed);

    auto min_cost = min_chosen_cost_.load(std::memory_order_relaxed);
    if (!std::isfinite(min_cost)) {
        min_cost = 0.0;
    }
    snapshot.min_chosen_cost = min_cost;

    auto max_cost = max_chosen_cost_.load(std::memory_order_relaxed);
    if (!std::isfinite(max_cost)) {
        max_cost = 0.0;
    }
    snapshot.max_chosen_cost = max_cost;

    if (!std::isfinite(snapshot.last_chosen_cost)) {
        snapshot.last_chosen_cost = 0.0;
    }

    return snapshot;
}

void PlannerTelemetry::reset() noexcept
{
    plans_attempted_.store(0U, std::memory_order_relaxed);
    plans_succeeded_.store(0U, std::memory_order_relaxed);
    plans_failed_.store(0U, std::memory_order_relaxed);
    rules_attempted_.store(0U, std::memory_order_relaxed);
    rules_applied_.store(0U, std::memory_order_relaxed);
    cost_evaluations_.store(0U, std::memory_order_relaxed);
    alternatives_considered_.store(0U, std::memory_order_relaxed);
    total_chosen_cost_.store(0.0, std::memory_order_relaxed);
    last_chosen_cost_.store(0.0, std::memory_order_relaxed);
    min_chosen_cost_.store(std::numeric_limits<double>::infinity(), std::memory_order_relaxed);
    max_chosen_cost_.store(-std::numeric_limits<double>::infinity(), std::memory_order_relaxed);
}

void PlannerTelemetry::update_min(std::atomic<double>& target, double value) noexcept
{
    auto current = target.load(std::memory_order_relaxed);
    while (value < current) {
        if (target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
            break;
        }
    }
}

void PlannerTelemetry::update_max(std::atomic<double>& target, double value) noexcept
{
    auto current = target.load(std::memory_order_relaxed);
    while (value > current) {
        if (target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
            break;
        }
    }
}

void PlannerTelemetry::add_relaxed(std::atomic<double>& target, double value) noexcept
{
    auto current = target.load(std::memory_order_relaxed);
    while (true) {
        const auto desired = current + value;
        if (target.compare_exchange_weak(current, desired, std::memory_order_relaxed)) {
            return;
        }
    }
}

}  // namespace bored::planner

#include "bored/ddl/ddl_telemetry.hpp"

#include <algorithm>
#include <vector>

namespace bored::ddl {

namespace {

inline std::size_t to_index(DdlVerb verb) noexcept
{
    return static_cast<std::size_t>(verb);
}

void accumulate(DdlVerbTelemetrySnapshot& target, const DdlVerbTelemetrySnapshot& source) noexcept
{
    target.attempts += source.attempts;
    target.successes += source.successes;
    target.failures += source.failures;
}

void accumulate(DdlFailureTelemetrySnapshot& target, const DdlFailureTelemetrySnapshot& source) noexcept
{
    target.handler_missing += source.handler_missing;
    target.validation_failures += source.validation_failures;
    target.execution_failures += source.execution_failures;
    target.other_failures += source.other_failures;
}

}  // namespace

void DdlCommandTelemetry::record_attempt(DdlVerb verb) noexcept
{
    attempts_[to_index(verb)].fetch_add(1U, std::memory_order_relaxed);
}

void DdlCommandTelemetry::record_success(DdlVerb verb) noexcept
{
    successes_[to_index(verb)].fetch_add(1U, std::memory_order_relaxed);
}

void DdlCommandTelemetry::record_failure(DdlVerb verb, std::error_code error) noexcept
{
    failures_[to_index(verb)].fetch_add(1U, std::memory_order_relaxed);

    if (!error) {
        other_failures_.fetch_add(1U, std::memory_order_relaxed);
        return;
    }

    switch (static_cast<DdlErrc>(error.value())) {
    case DdlErrc::HandlerMissing:
        handler_missing_.fetch_add(1U, std::memory_order_relaxed);
        break;
    case DdlErrc::ValidationFailed:
        validation_failures_.fetch_add(1U, std::memory_order_relaxed);
        break;
    case DdlErrc::ExecutionFailed:
        execution_failures_.fetch_add(1U, std::memory_order_relaxed);
        break;
    default:
        other_failures_.fetch_add(1U, std::memory_order_relaxed);
        break;
    }
}

DdlTelemetrySnapshot DdlCommandTelemetry::snapshot() const noexcept
{
    DdlTelemetrySnapshot snapshot{};
    for (std::size_t i = 0; i < verb_count; ++i) {
        snapshot.verbs[i].attempts = attempts_[i].load(std::memory_order_relaxed);
        snapshot.verbs[i].successes = successes_[i].load(std::memory_order_relaxed);
        snapshot.verbs[i].failures = failures_[i].load(std::memory_order_relaxed);
    }

    snapshot.failures.handler_missing = handler_missing_.load(std::memory_order_relaxed);
    snapshot.failures.validation_failures = validation_failures_.load(std::memory_order_relaxed);
    snapshot.failures.execution_failures = execution_failures_.load(std::memory_order_relaxed);
    snapshot.failures.other_failures = other_failures_.load(std::memory_order_relaxed);
    return snapshot;
}

void DdlCommandTelemetry::reset() noexcept
{
    for (std::size_t i = 0; i < verb_count; ++i) {
        attempts_[i].store(0U, std::memory_order_relaxed);
        successes_[i].store(0U, std::memory_order_relaxed);
        failures_[i].store(0U, std::memory_order_relaxed);
    }

    handler_missing_.store(0U, std::memory_order_relaxed);
    validation_failures_.store(0U, std::memory_order_relaxed);
    execution_failures_.store(0U, std::memory_order_relaxed);
    other_failures_.store(0U, std::memory_order_relaxed);
}

void DdlTelemetryRegistry::register_sampler(std::string identifier, Sampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void DdlTelemetryRegistry::unregister_sampler(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    samplers_.erase(identifier);
}

DdlTelemetrySnapshot DdlTelemetryRegistry::aggregate() const
{
    std::vector<Sampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(samplers_.size());
        for (const auto& [_, sampler] : samplers_) {
            samplers.push_back(sampler);
        }
    }

    DdlTelemetrySnapshot total{};
    for (const auto& sampler : samplers) {
        if (!sampler) {
            continue;
        }
        const auto snapshot = sampler();
        for (std::size_t i = 0; i < snapshot.verbs.size(); ++i) {
            accumulate(total.verbs[i], snapshot.verbs[i]);
        }
        accumulate(total.failures, snapshot.failures);
    }

    return total;
}

void DdlTelemetryRegistry::visit(const Visitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, Sampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(samplers_.size());
        for (const auto& [identifier, sampler] : samplers_) {
            entries.emplace_back(identifier, sampler);
        }
    }

    for (const auto& [identifier, sampler] : entries) {
        if (!sampler) {
            continue;
        }
        visitor(identifier, sampler());
    }
}

}  // namespace bored::ddl

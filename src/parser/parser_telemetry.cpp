#include "bored/parser/parser_telemetry.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace bored::parser {
namespace {

ParserTelemetrySnapshot& accumulate(ParserTelemetrySnapshot& target, const ParserTelemetrySnapshot& source)
{
    target.scripts_attempted += source.scripts_attempted;
    target.scripts_succeeded += source.scripts_succeeded;
    target.statements_attempted += source.statements_attempted;
    target.statements_succeeded += source.statements_succeeded;
    target.diagnostics_info += source.diagnostics_info;
    target.diagnostics_warning += source.diagnostics_warning;
    target.diagnostics_error += source.diagnostics_error;
    target.total_parse_duration_ns += source.total_parse_duration_ns;
    target.last_parse_duration_ns = std::max(target.last_parse_duration_ns, source.last_parse_duration_ns);
    return target;
}

}  // namespace

void ParserTelemetry::add_relaxed(std::atomic<std::uint64_t>& target, std::uint64_t value) noexcept
{
    target.fetch_add(value, std::memory_order_relaxed);
}

void ParserTelemetry::record_script_attempt() noexcept
{
    add_relaxed(scripts_attempted_, 1U);
}

void ParserTelemetry::record_script_result(bool success,
                                           std::uint64_t parse_duration_ns,
                                           std::size_t statements_attempted,
                                           std::size_t statements_succeeded,
                                           std::size_t info_diagnostics,
                                           std::size_t warning_diagnostics,
                                           std::size_t error_diagnostics) noexcept
{
    if (success) {
        add_relaxed(scripts_succeeded_, 1U);
    }

    add_relaxed(statements_attempted_, static_cast<std::uint64_t>(statements_attempted));
    add_relaxed(statements_succeeded_, static_cast<std::uint64_t>(statements_succeeded));
    add_relaxed(diagnostics_info_, static_cast<std::uint64_t>(info_diagnostics));
    add_relaxed(diagnostics_warning_, static_cast<std::uint64_t>(warning_diagnostics));
    add_relaxed(diagnostics_error_, static_cast<std::uint64_t>(error_diagnostics));
    add_relaxed(total_parse_duration_ns_, parse_duration_ns);
    last_parse_duration_ns_.store(parse_duration_ns, std::memory_order_relaxed);
}

ParserTelemetrySnapshot ParserTelemetry::snapshot() const noexcept
{
    ParserTelemetrySnapshot snapshot{};
    snapshot.scripts_attempted = scripts_attempted_.load(std::memory_order_relaxed);
    snapshot.scripts_succeeded = scripts_succeeded_.load(std::memory_order_relaxed);
    snapshot.statements_attempted = statements_attempted_.load(std::memory_order_relaxed);
    snapshot.statements_succeeded = statements_succeeded_.load(std::memory_order_relaxed);
    snapshot.diagnostics_info = diagnostics_info_.load(std::memory_order_relaxed);
    snapshot.diagnostics_warning = diagnostics_warning_.load(std::memory_order_relaxed);
    snapshot.diagnostics_error = diagnostics_error_.load(std::memory_order_relaxed);
    snapshot.total_parse_duration_ns = total_parse_duration_ns_.load(std::memory_order_relaxed);
    snapshot.last_parse_duration_ns = last_parse_duration_ns_.load(std::memory_order_relaxed);
    return snapshot;
}

void ParserTelemetry::reset() noexcept
{
    scripts_attempted_.store(0U, std::memory_order_relaxed);
    scripts_succeeded_.store(0U, std::memory_order_relaxed);
    statements_attempted_.store(0U, std::memory_order_relaxed);
    statements_succeeded_.store(0U, std::memory_order_relaxed);
    diagnostics_info_.store(0U, std::memory_order_relaxed);
    diagnostics_warning_.store(0U, std::memory_order_relaxed);
    diagnostics_error_.store(0U, std::memory_order_relaxed);
    total_parse_duration_ns_.store(0U, std::memory_order_relaxed);
    last_parse_duration_ns_.store(0U, std::memory_order_relaxed);
}

void ParserTelemetryRegistry::register_sampler(std::string identifier, Sampler sampler)
{
    if (!sampler) {
        return;
    }

    std::lock_guard guard(mutex_);
    samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void ParserTelemetryRegistry::unregister_sampler(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    samplers_.erase(identifier);
}

ParserTelemetrySnapshot ParserTelemetryRegistry::aggregate() const
{
    std::vector<Sampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(samplers_.size());
        for (const auto& [_, sampler] : samplers_) {
            samplers.push_back(sampler);
        }
    }

    ParserTelemetrySnapshot total{};
    for (const auto& sampler : samplers) {
        if (!sampler) {
            continue;
        }
        accumulate(total, sampler());
    }
    return total;
}

void ParserTelemetryRegistry::visit(const Visitor& visitor) const
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

}  // namespace bored::parser

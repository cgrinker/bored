#include "bored/storage/wal_telemetry_registry.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace bored::storage {

namespace {

WalWriterTelemetrySnapshot& accumulate_snapshot(WalWriterTelemetrySnapshot& target, const WalWriterTelemetrySnapshot& source)
{
    target.append_calls += source.append_calls;
    target.appended_bytes += source.appended_bytes;
    target.total_append_duration_ns += source.total_append_duration_ns;
    target.last_append_duration_ns = std::max(target.last_append_duration_ns, source.last_append_duration_ns);
    target.flush_calls += source.flush_calls;
    target.flushed_bytes += source.flushed_bytes;
    target.max_flush_bytes = std::max(target.max_flush_bytes, source.max_flush_bytes);
    target.total_flush_duration_ns += source.total_flush_duration_ns;
    target.last_flush_duration_ns = std::max(target.last_flush_duration_ns, source.last_flush_duration_ns);
    return target;
}

}  // namespace

void WalTelemetryRegistry::register_sampler(std::string identifier, Sampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void WalTelemetryRegistry::unregister_sampler(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    samplers_.erase(identifier);
}

WalWriterTelemetrySnapshot WalTelemetryRegistry::aggregate() const
{
    std::vector<Sampler> callbacks;
    {
        std::lock_guard guard(mutex_);
        callbacks.reserve(samplers_.size());
        for (const auto& [_, sampler] : samplers_) {
            callbacks.push_back(sampler);
        }
    }

    WalWriterTelemetrySnapshot total{};
    for (const auto& callback : callbacks) {
        if (!callback) {
            continue;
        }
        total = accumulate_snapshot(total, callback());
    }
    return total;
}

void WalTelemetryRegistry::visit(const Visitor& visitor) const
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

}  // namespace bored::storage

#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <functional>
#include <utility>
#include <vector>

namespace bored::storage {

namespace {

PageManagerTelemetrySnapshot& accumulate(PageManagerTelemetrySnapshot& target, const PageManagerTelemetrySnapshot& source)
{
    auto accumulate_operation = [](OperationTelemetrySnapshot& lhs, const OperationTelemetrySnapshot& rhs) {
        lhs.attempts += rhs.attempts;
        lhs.failures += rhs.failures;
        lhs.total_duration_ns += rhs.total_duration_ns;
        lhs.last_duration_ns = std::max(lhs.last_duration_ns, rhs.last_duration_ns);
    };

    auto accumulate_latch = [](LatchTelemetrySnapshot& lhs, const LatchTelemetrySnapshot& rhs) {
        lhs.attempts += rhs.attempts;
        lhs.failures += rhs.failures;
        lhs.total_wait_ns += rhs.total_wait_ns;
        lhs.last_wait_ns = std::max(lhs.last_wait_ns, rhs.last_wait_ns);
    };

    accumulate_operation(target.initialize, source.initialize);
    accumulate_operation(target.insert, source.insert);
    accumulate_operation(target.remove, source.remove);
    accumulate_operation(target.update, source.update);
    accumulate_operation(target.compact, source.compact);

    accumulate_latch(target.shared_latch, source.shared_latch);
    accumulate_latch(target.exclusive_latch, source.exclusive_latch);

    return target;
}

CheckpointTelemetrySnapshot& accumulate(CheckpointTelemetrySnapshot& target, const CheckpointTelemetrySnapshot& source)
{
    target.invocations += source.invocations;
    target.forced_requests += source.forced_requests;
    target.skipped_runs += source.skipped_runs;
    target.emitted_checkpoints += source.emitted_checkpoints;
    target.emit_failures += source.emit_failures;
    target.flush_failures += source.flush_failures;
    target.retention_invocations += source.retention_invocations;
    target.retention_failures += source.retention_failures;
    target.trigger_force += source.trigger_force;
    target.trigger_first += source.trigger_first;
    target.trigger_dirty += source.trigger_dirty;
    target.trigger_active += source.trigger_active;
    target.trigger_interval += source.trigger_interval;
    target.trigger_lsn_gap += source.trigger_lsn_gap;
    target.total_emit_duration_ns += source.total_emit_duration_ns;
    target.last_emit_duration_ns = std::max(target.last_emit_duration_ns, source.last_emit_duration_ns);
    target.total_flush_duration_ns += source.total_flush_duration_ns;
    target.last_flush_duration_ns = std::max(target.last_flush_duration_ns, source.last_flush_duration_ns);
    target.total_retention_duration_ns += source.total_retention_duration_ns;
    target.last_retention_duration_ns = std::max(target.last_retention_duration_ns, source.last_retention_duration_ns);
    target.last_checkpoint_id = std::max(target.last_checkpoint_id, source.last_checkpoint_id);
    target.last_checkpoint_lsn = std::max(target.last_checkpoint_lsn, source.last_checkpoint_lsn);
    target.last_checkpoint_timestamp_ns = std::max(target.last_checkpoint_timestamp_ns, source.last_checkpoint_timestamp_ns);
    return target;
}

WalRetentionTelemetrySnapshot& accumulate(WalRetentionTelemetrySnapshot& target, const WalRetentionTelemetrySnapshot& source)
{
    target.invocations += source.invocations;
    target.failures += source.failures;
    target.scanned_segments += source.scanned_segments;
    target.candidate_segments += source.candidate_segments;
    target.pruned_segments += source.pruned_segments;
    target.archived_segments += source.archived_segments;
    target.total_duration_ns += source.total_duration_ns;
    target.last_duration_ns = std::max(target.last_duration_ns, source.last_duration_ns);
    return target;
}

template <typename Snapshot>
Snapshot aggregate_snapshots(const std::vector<std::function<Snapshot()>>& samplers)
{
    Snapshot total{};
    for (const auto& sampler : samplers) {
        if (!sampler) {
            continue;
        }
        accumulate(total, sampler());
    }
    return total;
}

}  // namespace

void StorageTelemetryRegistry::register_page_manager(std::string identifier, PageManagerSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    page_manager_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_page_manager(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    page_manager_samplers_.erase(identifier);
}

PageManagerTelemetrySnapshot StorageTelemetryRegistry::aggregate_page_managers() const
{
    std::vector<PageManagerSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(page_manager_samplers_.size());
        for (const auto& [_, sampler] : page_manager_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<PageManagerTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_page_managers(const PageManagerVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, PageManagerSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(page_manager_samplers_.size());
        for (const auto& [identifier, sampler] : page_manager_samplers_) {
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

void StorageTelemetryRegistry::register_checkpoint_scheduler(std::string identifier, CheckpointSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    checkpoint_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_checkpoint_scheduler(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    checkpoint_samplers_.erase(identifier);
}

CheckpointTelemetrySnapshot StorageTelemetryRegistry::aggregate_checkpoint_schedulers() const
{
    std::vector<CheckpointSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(checkpoint_samplers_.size());
        for (const auto& [_, sampler] : checkpoint_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<CheckpointTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_checkpoint_schedulers(const CheckpointVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, CheckpointSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(checkpoint_samplers_.size());
        for (const auto& [identifier, sampler] : checkpoint_samplers_) {
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

void StorageTelemetryRegistry::register_wal_retention(std::string identifier, WalRetentionSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    wal_retention_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_wal_retention(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    wal_retention_samplers_.erase(identifier);
}

WalRetentionTelemetrySnapshot StorageTelemetryRegistry::aggregate_wal_retention() const
{
    std::vector<WalRetentionSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(wal_retention_samplers_.size());
        for (const auto& [_, sampler] : wal_retention_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<WalRetentionTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_wal_retention(const WalRetentionVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, WalRetentionSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(wal_retention_samplers_.size());
        for (const auto& [identifier, sampler] : wal_retention_samplers_) {
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

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

VacuumTelemetrySnapshot& accumulate(VacuumTelemetrySnapshot& target, const VacuumTelemetrySnapshot& source)
{
    target.scheduled_pages += source.scheduled_pages;
    target.dropped_pages += source.dropped_pages;
    target.runs += source.runs;
    target.forced_runs += source.forced_runs;
    target.skipped_runs += source.skipped_runs;
    target.batches_dispatched += source.batches_dispatched;
    target.dispatch_failures += source.dispatch_failures;
    target.pages_dispatched += source.pages_dispatched;
    target.pending_pages += source.pending_pages;
    target.total_dispatch_duration_ns += source.total_dispatch_duration_ns;
    target.last_dispatch_duration_ns = std::max(target.last_dispatch_duration_ns, source.last_dispatch_duration_ns);
    target.last_safe_horizon = std::max(target.last_safe_horizon, source.last_safe_horizon);
    return target;
}

CatalogTelemetrySnapshot& accumulate(CatalogTelemetrySnapshot& target, const CatalogTelemetrySnapshot& source)
{
    target.cache_hits += source.cache_hits;
    target.cache_misses += source.cache_misses;
    target.cache_relations += source.cache_relations;
    target.cache_total_bytes += source.cache_total_bytes;
    target.published_batches += source.published_batches;
    target.published_mutations += source.published_mutations;
    target.published_wal_records += source.published_wal_records;
    target.publish_failures += source.publish_failures;
    target.aborted_batches += source.aborted_batches;
    target.aborted_mutations += source.aborted_mutations;
    return target;
}

ddl::DdlTelemetrySnapshot& accumulate(ddl::DdlTelemetrySnapshot& target, const ddl::DdlTelemetrySnapshot& source)
{
    for (std::size_t i = 0; i < target.verbs.size(); ++i) {
        target.verbs[i].attempts += source.verbs[i].attempts;
        target.verbs[i].successes += source.verbs[i].successes;
        target.verbs[i].failures += source.verbs[i].failures;
        target.verbs[i].total_duration_ns += source.verbs[i].total_duration_ns;
        target.verbs[i].last_duration_ns = std::max(target.verbs[i].last_duration_ns, source.verbs[i].last_duration_ns);
    }

    target.failures.handler_missing += source.failures.handler_missing;
    target.failures.validation_failures += source.failures.validation_failures;
    target.failures.execution_failures += source.failures.execution_failures;
    target.failures.other_failures += source.failures.other_failures;
    return target;
}

bored::parser::ParserTelemetrySnapshot& accumulate(bored::parser::ParserTelemetrySnapshot& target,
                                                   const bored::parser::ParserTelemetrySnapshot& source)
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

bored::txn::TransactionTelemetrySnapshot& accumulate(bored::txn::TransactionTelemetrySnapshot& target,
                                                      const bored::txn::TransactionTelemetrySnapshot& source)
{
    target.active_transactions += source.active_transactions;
    target.committed_transactions += source.committed_transactions;
    target.aborted_transactions += source.aborted_transactions;
    if (target.last_snapshot_xmin == 0U || (source.last_snapshot_xmin != 0U && source.last_snapshot_xmin < target.last_snapshot_xmin)) {
        target.last_snapshot_xmin = source.last_snapshot_xmin;
    }
    target.last_snapshot_xmax = std::max(target.last_snapshot_xmax, source.last_snapshot_xmax);
    target.last_snapshot_age = std::max(target.last_snapshot_age, source.last_snapshot_age);
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

void StorageTelemetryRegistry::register_vacuum(std::string identifier, VacuumSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    vacuum_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_vacuum(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    vacuum_samplers_.erase(identifier);
}

VacuumTelemetrySnapshot StorageTelemetryRegistry::aggregate_vacuums() const
{
    std::vector<VacuumSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(vacuum_samplers_.size());
        for (const auto& [_, sampler] : vacuum_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<VacuumTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_vacuums(const VacuumVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, VacuumSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(vacuum_samplers_.size());
        for (const auto& [identifier, sampler] : vacuum_samplers_) {
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

void StorageTelemetryRegistry::register_catalog(std::string identifier, CatalogSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    catalog_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_catalog(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    catalog_samplers_.erase(identifier);
}

CatalogTelemetrySnapshot StorageTelemetryRegistry::aggregate_catalog() const
{
    std::vector<CatalogSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(catalog_samplers_.size());
        for (const auto& [_, sampler] : catalog_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<CatalogTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_catalog(const CatalogVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, CatalogSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(catalog_samplers_.size());
        for (const auto& [identifier, sampler] : catalog_samplers_) {
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

void StorageTelemetryRegistry::register_ddl(std::string identifier, DdlSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    ddl_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_ddl(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    ddl_samplers_.erase(identifier);
}

ddl::DdlTelemetrySnapshot StorageTelemetryRegistry::aggregate_ddl() const
{
    std::vector<DdlSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(ddl_samplers_.size());
        for (const auto& [_, sampler] : ddl_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<ddl::DdlTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_ddl(const DdlVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, DdlSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(ddl_samplers_.size());
        for (const auto& [identifier, sampler] : ddl_samplers_) {
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

void StorageTelemetryRegistry::register_parser(std::string identifier, ParserSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    parser_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::register_transaction(std::string identifier, TransactionSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    transaction_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_transaction(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    transaction_samplers_.erase(identifier);
}

bored::txn::TransactionTelemetrySnapshot StorageTelemetryRegistry::aggregate_transactions() const
{
    std::vector<TransactionSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(transaction_samplers_.size());
        for (const auto& [_, sampler] : transaction_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<bored::txn::TransactionTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_transactions(const TransactionVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, TransactionSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(transaction_samplers_.size());
        for (const auto& [identifier, sampler] : transaction_samplers_) {
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

void StorageTelemetryRegistry::unregister_parser(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    parser_samplers_.erase(identifier);
}

bored::parser::ParserTelemetrySnapshot StorageTelemetryRegistry::aggregate_parser() const
{
    std::vector<ParserSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(parser_samplers_.size());
        for (const auto& [_, sampler] : parser_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<bored::parser::ParserTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_parser(const ParserVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, ParserSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(parser_samplers_.size());
        for (const auto& [identifier, sampler] : parser_samplers_) {
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

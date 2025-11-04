#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <functional>
#include <utility>
#include <vector>

namespace bored::storage {

namespace {

void accumulate_operation(OperationTelemetrySnapshot& lhs, const OperationTelemetrySnapshot& rhs)
{
    lhs.attempts += rhs.attempts;
    lhs.failures += rhs.failures;
    lhs.total_duration_ns += rhs.total_duration_ns;
    lhs.last_duration_ns = std::max(lhs.last_duration_ns, rhs.last_duration_ns);
}

void accumulate_latch(LatchTelemetrySnapshot& lhs, const LatchTelemetrySnapshot& rhs)
{
    lhs.attempts += rhs.attempts;
    lhs.failures += rhs.failures;
    lhs.total_wait_ns += rhs.total_wait_ns;
    lhs.last_wait_ns = std::max(lhs.last_wait_ns, rhs.last_wait_ns);
}

PageManagerTelemetrySnapshot& accumulate(PageManagerTelemetrySnapshot& target, const PageManagerTelemetrySnapshot& source)
{
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
    target.total_checkpoint_duration_ns += source.total_checkpoint_duration_ns;
    target.last_checkpoint_duration_ns = std::max(target.last_checkpoint_duration_ns,
                                                  source.last_checkpoint_duration_ns);
    target.max_checkpoint_duration_ns = std::max(target.max_checkpoint_duration_ns,
                                                 source.max_checkpoint_duration_ns);
    target.total_flush_duration_ns += source.total_flush_duration_ns;
    target.last_flush_duration_ns = std::max(target.last_flush_duration_ns, source.last_flush_duration_ns);
    target.total_retention_duration_ns += source.total_retention_duration_ns;
    target.last_retention_duration_ns = std::max(target.last_retention_duration_ns, source.last_retention_duration_ns);
    target.total_fence_duration_ns += source.total_fence_duration_ns;
    target.last_fence_duration_ns = std::max(target.last_fence_duration_ns, source.last_fence_duration_ns);
    target.max_fence_duration_ns = std::max(target.max_fence_duration_ns, source.max_fence_duration_ns);
    target.coordinator_begin_calls += source.coordinator_begin_calls;
    target.coordinator_begin_failures += source.coordinator_begin_failures;
    target.coordinator_prepare_calls += source.coordinator_prepare_calls;
    target.coordinator_prepare_failures += source.coordinator_prepare_failures;
    target.coordinator_commit_calls += source.coordinator_commit_calls;
    target.coordinator_commit_failures += source.coordinator_commit_failures;
    target.coordinator_abort_calls += source.coordinator_abort_calls;
    target.coordinator_dry_runs += source.coordinator_dry_runs;
    target.coordinator_total_begin_duration_ns += source.coordinator_total_begin_duration_ns;
    target.coordinator_last_begin_duration_ns = std::max(target.coordinator_last_begin_duration_ns,
                                                         source.coordinator_last_begin_duration_ns);
    target.coordinator_total_prepare_duration_ns += source.coordinator_total_prepare_duration_ns;
    target.coordinator_last_prepare_duration_ns = std::max(target.coordinator_last_prepare_duration_ns,
                                                           source.coordinator_last_prepare_duration_ns);
    target.coordinator_total_commit_duration_ns += source.coordinator_total_commit_duration_ns;
    target.coordinator_last_commit_duration_ns = std::max(target.coordinator_last_commit_duration_ns,
                                                         source.coordinator_last_commit_duration_ns);
    target.last_checkpoint_id = std::max(target.last_checkpoint_id, source.last_checkpoint_id);
    target.last_checkpoint_lsn = std::max(target.last_checkpoint_lsn, source.last_checkpoint_lsn);
    target.last_checkpoint_timestamp_ns = std::max(target.last_checkpoint_timestamp_ns, source.last_checkpoint_timestamp_ns);
    target.io_throttle_deferrals += source.io_throttle_deferrals;
    target.io_throttle_bytes_consumed += source.io_throttle_bytes_consumed;
    target.io_throttle_budget = std::max(target.io_throttle_budget, source.io_throttle_budget);
    target.queue_waits += source.queue_waits;
    target.last_queue_depth = std::max(target.last_queue_depth, source.last_queue_depth);
    target.max_queue_depth = std::max(target.max_queue_depth, source.max_queue_depth);
    target.blocked_transactions += source.blocked_transactions;
    target.total_blocked_duration_ns += source.total_blocked_duration_ns;
    target.last_blocked_duration_ns = std::max(target.last_blocked_duration_ns, source.last_blocked_duration_ns);
    target.max_blocked_duration_ns = std::max(target.max_blocked_duration_ns, source.max_blocked_duration_ns);
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

RecoveryTelemetrySnapshot& accumulate(RecoveryTelemetrySnapshot& target, const RecoveryTelemetrySnapshot& source)
{
    target.plan_invocations += source.plan_invocations;
    target.plan_failures += source.plan_failures;
    target.total_enumerate_duration_ns += source.total_enumerate_duration_ns;
    target.last_enumerate_duration_ns = std::max(target.last_enumerate_duration_ns, source.last_enumerate_duration_ns);
    target.max_enumerate_duration_ns = std::max(target.max_enumerate_duration_ns, source.max_enumerate_duration_ns);
    target.total_plan_duration_ns += source.total_plan_duration_ns;
    target.last_plan_duration_ns = std::max(target.last_plan_duration_ns, source.last_plan_duration_ns);
    target.max_plan_duration_ns = std::max(target.max_plan_duration_ns, source.max_plan_duration_ns);
    target.redo_invocations += source.redo_invocations;
    target.redo_failures += source.redo_failures;
    target.total_redo_duration_ns += source.total_redo_duration_ns;
    target.last_redo_duration_ns = std::max(target.last_redo_duration_ns, source.last_redo_duration_ns);
    target.max_redo_duration_ns = std::max(target.max_redo_duration_ns, source.max_redo_duration_ns);
    target.undo_invocations += source.undo_invocations;
    target.undo_failures += source.undo_failures;
    target.total_undo_duration_ns += source.total_undo_duration_ns;
    target.last_undo_duration_ns = std::max(target.last_undo_duration_ns, source.last_undo_duration_ns);
    target.max_undo_duration_ns = std::max(target.max_undo_duration_ns, source.max_undo_duration_ns);
    target.cleanup_invocations += source.cleanup_invocations;
    target.cleanup_failures += source.cleanup_failures;
    target.total_cleanup_duration_ns += source.total_cleanup_duration_ns;
    target.last_cleanup_duration_ns = std::max(target.last_cleanup_duration_ns, source.last_cleanup_duration_ns);
    target.max_cleanup_duration_ns = std::max(target.max_cleanup_duration_ns, source.max_cleanup_duration_ns);
    target.last_replay_backlog_bytes = std::max(target.last_replay_backlog_bytes, source.last_replay_backlog_bytes);
    target.max_replay_backlog_bytes = std::max(target.max_replay_backlog_bytes, source.max_replay_backlog_bytes);
    return target;
}

IndexTelemetrySnapshot& accumulate(IndexTelemetrySnapshot& target, const IndexTelemetrySnapshot& source)
{
    accumulate_operation(target.build, source.build);
    accumulate_operation(target.probe, source.probe);
    target.mutation_attempts += source.mutation_attempts;
    target.split_events += source.split_events;
    return target;
}

IndexRetentionTelemetrySnapshot& accumulate(IndexRetentionTelemetrySnapshot& target,
                                            const IndexRetentionTelemetrySnapshot& source)
{
    target.scheduled_candidates += source.scheduled_candidates;
    target.dropped_candidates += source.dropped_candidates;
    target.checkpoint_runs += source.checkpoint_runs;
    target.checkpoint_failures += source.checkpoint_failures;
    target.dispatch_batches += source.dispatch_batches;
    target.dispatch_failures += source.dispatch_failures;
    target.dispatched_candidates += source.dispatched_candidates;
    target.skipped_candidates += source.skipped_candidates;
    target.pruned_candidates += source.pruned_candidates;
    target.total_checkpoint_duration_ns += source.total_checkpoint_duration_ns;
    target.last_checkpoint_duration_ns = std::max(target.last_checkpoint_duration_ns, source.last_checkpoint_duration_ns);
    target.total_dispatch_duration_ns += source.total_dispatch_duration_ns;
    target.last_dispatch_duration_ns = std::max(target.last_dispatch_duration_ns, source.last_dispatch_duration_ns);
    target.pending_candidates += source.pending_candidates;
    return target;
}

TempCleanupTelemetrySnapshot& accumulate(TempCleanupTelemetrySnapshot& target, const TempCleanupTelemetrySnapshot& source)
{
    target.invocations += source.invocations;
    target.failures += source.failures;
    target.removed_entries += source.removed_entries;
    target.total_duration_ns += source.total_duration_ns;
    target.last_duration_ns = std::max(target.last_duration_ns, source.last_duration_ns);
    return target;
}

StorageControlTelemetrySnapshot& accumulate(StorageControlTelemetrySnapshot& target,
                                            const StorageControlTelemetrySnapshot& source)
{
    accumulate_operation(target.checkpoint, source.checkpoint);
    accumulate_operation(target.retention, source.retention);
    accumulate_operation(target.recovery, source.recovery);
    return target;
}

DurabilityTelemetrySnapshot& accumulate(DurabilityTelemetrySnapshot& target, const DurabilityTelemetrySnapshot& source)
{
    target.last_commit_lsn = std::max(target.last_commit_lsn, source.last_commit_lsn);
    target.oldest_active_commit_lsn = std::max(target.oldest_active_commit_lsn, source.oldest_active_commit_lsn);
    target.last_commit_segment_id = std::max(target.last_commit_segment_id, source.last_commit_segment_id);
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

bored::planner::PlannerTelemetrySnapshot& accumulate(bored::planner::PlannerTelemetrySnapshot& target,
                                                      const bored::planner::PlannerTelemetrySnapshot& source)
{
    target.plans_attempted += source.plans_attempted;
    target.plans_succeeded += source.plans_succeeded;
    target.plans_failed += source.plans_failed;
    target.rules_attempted += source.rules_attempted;
    target.rules_applied += source.rules_applied;
    target.cost_evaluations += source.cost_evaluations;
    target.alternatives_considered += source.alternatives_considered;
    target.total_chosen_cost += source.total_chosen_cost;
    if (std::isfinite(source.last_chosen_cost)) {
        target.last_chosen_cost = source.last_chosen_cost;
    }
    if (std::isfinite(source.min_chosen_cost) &&
        (!std::isfinite(target.min_chosen_cost) || source.min_chosen_cost < target.min_chosen_cost)) {
        target.min_chosen_cost = source.min_chosen_cost;
    }
    if (std::isfinite(source.max_chosen_cost) &&
        (!std::isfinite(target.max_chosen_cost) || source.max_chosen_cost > target.max_chosen_cost)) {
        target.max_chosen_cost = source.max_chosen_cost;
    }
    return target;
}

bored::executor::ExecutorTelemetrySnapshot& accumulate(bored::executor::ExecutorTelemetrySnapshot& target,
                                                        const bored::executor::ExecutorTelemetrySnapshot& source)
{
    auto accumulate_latency = [](auto& lhs, const auto& rhs) {
        lhs.invocations += rhs.invocations;
        lhs.total_duration_ns += rhs.total_duration_ns;
        lhs.last_duration_ns = std::max(lhs.last_duration_ns, rhs.last_duration_ns);
    };

    target.seq_scan_rows_read += source.seq_scan_rows_read;
    target.seq_scan_rows_visible += source.seq_scan_rows_visible;
    target.filter_rows_evaluated += source.filter_rows_evaluated;
    target.filter_rows_passed += source.filter_rows_passed;
    target.projection_rows_emitted += source.projection_rows_emitted;
    target.nested_loop_rows_compared += source.nested_loop_rows_compared;
    target.nested_loop_rows_matched += source.nested_loop_rows_matched;
    target.nested_loop_rows_emitted += source.nested_loop_rows_emitted;
    target.hash_join_build_rows += source.hash_join_build_rows;
    target.hash_join_probe_rows += source.hash_join_probe_rows;
    target.hash_join_rows_matched += source.hash_join_rows_matched;
    target.aggregation_input_rows += source.aggregation_input_rows;
    target.aggregation_groups_emitted += source.aggregation_groups_emitted;
    target.insert_rows_attempted += source.insert_rows_attempted;
    target.insert_rows_succeeded += source.insert_rows_succeeded;
    target.insert_payload_bytes += source.insert_payload_bytes;
    target.insert_wal_bytes += source.insert_wal_bytes;
    target.update_rows_attempted += source.update_rows_attempted;
    target.update_rows_succeeded += source.update_rows_succeeded;
    target.update_new_payload_bytes += source.update_new_payload_bytes;
    target.update_old_payload_bytes += source.update_old_payload_bytes;
    target.update_wal_bytes += source.update_wal_bytes;
    target.delete_rows_attempted += source.delete_rows_attempted;
    target.delete_rows_succeeded += source.delete_rows_succeeded;
    target.delete_reclaimed_bytes += source.delete_reclaimed_bytes;
    target.delete_wal_bytes += source.delete_wal_bytes;

    accumulate_latency(target.seq_scan_latency, source.seq_scan_latency);
    accumulate_latency(target.filter_latency, source.filter_latency);
    accumulate_latency(target.projection_latency, source.projection_latency);
    accumulate_latency(target.spool_latency, source.spool_latency);
    accumulate_latency(target.nested_loop_latency, source.nested_loop_latency);
    accumulate_latency(target.hash_join_latency, source.hash_join_latency);
    accumulate_latency(target.aggregation_latency, source.aggregation_latency);
    accumulate_latency(target.insert_latency, source.insert_latency);
    accumulate_latency(target.update_latency, source.update_latency);
    accumulate_latency(target.delete_latency, source.delete_latency);
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

void StorageTelemetryRegistry::register_recovery(std::string identifier, RecoverySampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    recovery_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_recovery(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    recovery_samplers_.erase(identifier);
}

RecoveryTelemetrySnapshot StorageTelemetryRegistry::aggregate_recovery() const
{
    std::vector<RecoverySampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(recovery_samplers_.size());
        for (const auto& [_, sampler] : recovery_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<RecoveryTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_recovery(const RecoveryVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, RecoverySampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(recovery_samplers_.size());
        for (const auto& [identifier, sampler] : recovery_samplers_) {
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

void StorageTelemetryRegistry::register_index_retention(std::string identifier, IndexRetentionSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    index_retention_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_index_retention(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    index_retention_samplers_.erase(identifier);
}

IndexRetentionTelemetrySnapshot StorageTelemetryRegistry::aggregate_index_retention() const
{
    std::vector<IndexRetentionSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(index_retention_samplers_.size());
        for (const auto& [_, sampler] : index_retention_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<IndexRetentionTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_index_retention(const IndexRetentionVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, IndexRetentionSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(index_retention_samplers_.size());
        for (const auto& [identifier, sampler] : index_retention_samplers_) {
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

void StorageTelemetryRegistry::register_temp_cleanup(std::string identifier, TempCleanupSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    temp_cleanup_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_temp_cleanup(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    temp_cleanup_samplers_.erase(identifier);
}

TempCleanupTelemetrySnapshot StorageTelemetryRegistry::aggregate_temp_cleanup() const
{
    std::vector<TempCleanupSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(temp_cleanup_samplers_.size());
        for (const auto& [_, sampler] : temp_cleanup_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<TempCleanupTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_temp_cleanup(const TempCleanupVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, TempCleanupSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(temp_cleanup_samplers_.size());
        for (const auto& [identifier, sampler] : temp_cleanup_samplers_) {
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

void StorageTelemetryRegistry::register_durability_horizon(std::string identifier, DurabilitySampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    durability_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_durability_horizon(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    durability_samplers_.erase(identifier);
}

DurabilityTelemetrySnapshot StorageTelemetryRegistry::aggregate_durability_horizons() const
{
    std::vector<DurabilitySampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(durability_samplers_.size());
        for (const auto& [_, sampler] : durability_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<DurabilityTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_durability_horizons(const DurabilityVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, DurabilitySampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(durability_samplers_.size());
        for (const auto& [identifier, sampler] : durability_samplers_) {
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

void StorageTelemetryRegistry::register_index(std::string identifier, IndexSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard{mutex_};
    index_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_index(const std::string& identifier)
{
    std::lock_guard guard{mutex_};
    index_samplers_.erase(identifier);
}

IndexTelemetrySnapshot StorageTelemetryRegistry::aggregate_indexes() const
{
    std::vector<IndexSampler> samplers;
    {
        std::lock_guard guard{mutex_};
        samplers.reserve(index_samplers_.size());
        for (const auto& [_, sampler] : index_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<IndexTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_indexes(const IndexVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, IndexSampler>> entries;
    {
        std::lock_guard guard{mutex_};
        entries.reserve(index_samplers_.size());
        for (const auto& [identifier, sampler] : index_samplers_) {
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

void StorageTelemetryRegistry::register_planner(std::string identifier, PlannerSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    planner_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_planner(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    planner_samplers_.erase(identifier);
}

bored::planner::PlannerTelemetrySnapshot StorageTelemetryRegistry::aggregate_planner() const
{
    std::vector<PlannerSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(planner_samplers_.size());
        for (const auto& [_, sampler] : planner_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<bored::planner::PlannerTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_planner(const PlannerVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, PlannerSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(planner_samplers_.size());
        for (const auto& [identifier, sampler] : planner_samplers_) {
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

void StorageTelemetryRegistry::register_control(std::string identifier, ControlSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    control_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_control(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    control_samplers_.erase(identifier);
}

StorageControlTelemetrySnapshot StorageTelemetryRegistry::aggregate_control() const
{
    std::vector<ControlSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(control_samplers_.size());
        for (const auto& [_, sampler] : control_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<StorageControlTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_control(const ControlVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, ControlSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(control_samplers_.size());
        for (const auto& [identifier, sampler] : control_samplers_) {
            entries.emplace_back(identifier, sampler);
        }
    }

    for (const auto& [identifier, sampler] : entries) {
        visitor(identifier, sampler ? sampler() : StorageControlTelemetrySnapshot{});
    }
}

void StorageTelemetryRegistry::register_executor(std::string identifier, ExecutorSampler sampler)
{
    if (!sampler) {
        return;
    }
    std::lock_guard guard(mutex_);
    executor_samplers_.insert_or_assign(std::move(identifier), std::move(sampler));
}

void StorageTelemetryRegistry::unregister_executor(const std::string& identifier)
{
    std::lock_guard guard(mutex_);
    executor_samplers_.erase(identifier);
}

bored::executor::ExecutorTelemetrySnapshot StorageTelemetryRegistry::aggregate_executors() const
{
    std::vector<ExecutorSampler> samplers;
    {
        std::lock_guard guard(mutex_);
        samplers.reserve(executor_samplers_.size());
        for (const auto& [_, sampler] : executor_samplers_) {
            samplers.push_back(sampler);
        }
    }
    return aggregate_snapshots<bored::executor::ExecutorTelemetrySnapshot>(samplers);
}

void StorageTelemetryRegistry::visit_executors(const ExecutorVisitor& visitor) const
{
    if (!visitor) {
        return;
    }

    std::vector<std::pair<std::string, ExecutorSampler>> entries;
    {
        std::lock_guard guard(mutex_);
        entries.reserve(executor_samplers_.size());
        for (const auto& [identifier, sampler] : executor_samplers_) {
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

namespace {

std::atomic<StorageTelemetryRegistry*> g_global_storage_registry{nullptr};

}  // namespace

StorageTelemetryRegistry* get_global_storage_telemetry_registry() noexcept
{
    return g_global_storage_registry.load(std::memory_order_acquire);
}

void set_global_storage_telemetry_registry(StorageTelemetryRegistry* registry) noexcept
{
    g_global_storage_registry.store(registry, std::memory_order_release);
}

}  // namespace bored::storage

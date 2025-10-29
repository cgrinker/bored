#include "bored/storage/checkpoint_scheduler.hpp"

#include "bored/storage/checkpoint_coordinator.hpp"

#include "bored/storage/index_retention.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/page_format.hpp"

#include <algorithm>
#include <vector>
#include <utility>
#include <limits>

namespace bored::storage {

namespace {

[[nodiscard]] bool has_retention_policy(const WalRetentionConfig& config)
{
    return config.retention_segments > 0U
        || config.retention_hours.count() > 0
        || !config.archive_path.empty();
}

[[nodiscard]] std::size_t saturating_add(std::size_t lhs, std::size_t rhs) noexcept
{
    const auto max_value = std::numeric_limits<std::size_t>::max();
    if (max_value - lhs < rhs) {
        return max_value;
    }
    return lhs + rhs;
}

}  // namespace

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager)
    : CheckpointScheduler(std::move(checkpoint_manager), Config{}, RetentionHook{}, IndexRetentionHook{})
{
}

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager, Config config)
    : CheckpointScheduler(std::move(checkpoint_manager), std::move(config), RetentionHook{}, IndexRetentionHook{})
{
}

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager,
                                         Config config,
                                         RetentionHook retention_hook,
                                         IndexRetentionHook index_retention_hook)
    : checkpoint_manager_{std::move(checkpoint_manager)}
    , config_{config}
    , retention_hook_{std::move(retention_hook)}
    , index_retention_hook_{std::move(index_retention_hook)}
    , coordinator_{config_.coordinator}
    , telemetry_registry_{config_.telemetry_registry}
    , telemetry_identifier_{config_.telemetry_identifier}
    , retention_telemetry_identifier_{config_.retention_telemetry_identifier}
    , retention_config_{config.retention}
    , durability_horizon_{config.durability_horizon}
{
    if (config_.lsn_gap_trigger == 0U) {
        config_.lsn_gap_trigger = 4U * kWalBlockSize;
    }

    if (!retention_hook_) {
        retention_hook_ = [this](const WalRetentionConfig& config, std::uint64_t segment_id, WalRetentionStats* stats) -> std::error_code {
            auto writer = this->wal_writer();
            if (!writer) {
                return std::make_error_code(std::errc::operation_not_permitted);
            }
            return writer->apply_retention(config, segment_id, stats);
        };
    }

    if (!index_retention_hook_) {
        index_retention_hook_ = std::move(config_.index_retention_hook);
    }

    if (telemetry_registry_) {
        if (!telemetry_identifier_.empty()) {
            telemetry_registry_->register_checkpoint_scheduler(telemetry_identifier_, [this] {
                return this->telemetry_snapshot();
            });
        }
        if (!retention_telemetry_identifier_.empty()) {
            telemetry_registry_->register_wal_retention(retention_telemetry_identifier_, [this] {
                return this->retention_telemetry_snapshot();
            });
        }
    }

    if (config_.io_target_bytes_per_second > 0U) {
        io_throttle_enabled_ = true;
        io_target_bytes_per_second_ = config_.io_target_bytes_per_second;
        std::size_t burst = config_.io_burst_bytes != 0U ? config_.io_burst_bytes : config_.io_target_bytes_per_second;
        if (burst == 0U) {
            burst = config_.io_target_bytes_per_second;
        }
        const auto max_int64 = static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max());
        if (burst > max_int64) {
            burst = max_int64;
        }
        io_budget_limit_bytes_ = static_cast<std::int64_t>(burst);
        if (io_budget_limit_bytes_ <= 0) {
            io_budget_limit_bytes_ = static_cast<std::int64_t>(config_.io_target_bytes_per_second);
        }
    }
}

CheckpointScheduler::~CheckpointScheduler()
{
    if (telemetry_registry_) {
        if (!telemetry_identifier_.empty()) {
            telemetry_registry_->unregister_checkpoint_scheduler(telemetry_identifier_);
        }
        if (!retention_telemetry_identifier_.empty()) {
            telemetry_registry_->unregister_wal_retention(retention_telemetry_identifier_);
        }
    }
}

std::shared_ptr<WalWriter> CheckpointScheduler::wal_writer() const noexcept
{
    if (!checkpoint_manager_) {
        return {};
    }
    return checkpoint_manager_->wal_writer();
}

std::error_code CheckpointScheduler::maybe_run(std::chrono::steady_clock::time_point now,
                                               const SnapshotProvider& provider,
                                               bool force,
                                               std::optional<WalAppendResult>& out_result)
{
    out_result.reset();

    if (!checkpoint_manager_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    if (!provider) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.invocations += 1U;
        if (force) {
            telemetry_.forced_requests += 1U;
        }
    }

    const auto attempt_start = std::chrono::steady_clock::now();

    CheckpointSnapshot snapshot;
    if (auto ec = provider(snapshot); ec) {
        return ec;
    }

    const auto estimated_bytes = estimate_checkpoint_bytes(snapshot);

    const auto writer = wal_writer();
    std::uint64_t current_lsn = 0U;
    if (durability_horizon_) {
        current_lsn = durability_horizon_->last_commit_lsn();
    }
    if (current_lsn == 0U && writer) {
        current_lsn = writer->next_lsn();
    }

    TriggerReason trigger_reason = TriggerReason::None;
    if (!should_run(now, snapshot, current_lsn, force, trigger_reason)) {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.skipped_runs += 1U;
        return {};
    }

    const auto mark_trigger = [&](TriggerReason reason) {
        std::lock_guard guard{telemetry_mutex_};
        switch (reason) {
            case TriggerReason::Force:
                telemetry_.trigger_force += 1U;
                break;
            case TriggerReason::First:
                telemetry_.trigger_first += 1U;
                break;
            case TriggerReason::DirtyPages:
                telemetry_.trigger_dirty += 1U;
                break;
            case TriggerReason::ActiveTransactions:
                telemetry_.trigger_active += 1U;
                break;
            case TriggerReason::Interval:
                telemetry_.trigger_interval += 1U;
                break;
            case TriggerReason::LsnGap:
                telemetry_.trigger_lsn_gap += 1U;
                break;
            case TriggerReason::None:
            default:
                break;
        }
    };
    mark_trigger(trigger_reason);

    if (io_throttle_enabled_ && !force) {
        if (!can_schedule_checkpoint(now, estimated_bytes)) {
            std::lock_guard guard{telemetry_mutex_};
            telemetry_.skipped_runs += 1U;
            telemetry_.io_throttle_deferrals += 1U;
            update_io_telemetry_locked();
            return {};
        }
    }

    CheckpointCoordinator::ActiveCheckpoint coordinator_checkpoint{};
    const bool use_coordinator = coordinator_ != nullptr;
    const bool dry_run_only = config_.dry_run_only;

    std::chrono::steady_clock::time_point fence_start{};
    bool fence_active = false;

    if (use_coordinator) {
        const auto begin_start = std::chrono::steady_clock::now();
        auto begin_ec = coordinator_->begin_checkpoint(next_checkpoint_id_, coordinator_checkpoint);
        const auto begin_end = std::chrono::steady_clock::now();
        const auto begin_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(begin_end - begin_start);
        const auto begin_duration_ns = begin_ns.count() > 0 ? static_cast<std::uint64_t>(begin_ns.count()) : 0ULL;

        {
            std::lock_guard guard{telemetry_mutex_};
            telemetry_.coordinator_begin_calls += 1U;
            telemetry_.coordinator_total_begin_duration_ns += begin_duration_ns;
            telemetry_.coordinator_last_begin_duration_ns = begin_duration_ns;
            if (begin_ec) {
                telemetry_.coordinator_begin_failures += 1U;
            }
        }

        if (begin_ec) {
            coordinator_->abort_checkpoint(coordinator_checkpoint);
            {
                std::lock_guard guard{telemetry_mutex_};
                telemetry_.coordinator_abort_calls += 1U;
            }
            return begin_ec;
        }

        fence_start = begin_end;
        fence_active = true;

        const auto prepare_start = std::chrono::steady_clock::now();
        auto prepare_ec = coordinator_->prepare_checkpoint(provider, coordinator_checkpoint);
        const auto prepare_end = std::chrono::steady_clock::now();
        const auto prepare_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(prepare_end - prepare_start);
        const auto prepare_duration_ns = prepare_ns.count() > 0 ? static_cast<std::uint64_t>(prepare_ns.count()) : 0ULL;

        {
            std::lock_guard guard{telemetry_mutex_};
            telemetry_.coordinator_prepare_calls += 1U;
            telemetry_.coordinator_total_prepare_duration_ns += prepare_duration_ns;
            telemetry_.coordinator_last_prepare_duration_ns = prepare_duration_ns;
            if (prepare_ec) {
                telemetry_.coordinator_prepare_failures += 1U;
            }
        }

        if (prepare_ec) {
            coordinator_->abort_checkpoint(coordinator_checkpoint);
            {
                std::lock_guard guard{telemetry_mutex_};
                telemetry_.coordinator_abort_calls += 1U;
            }
            return prepare_ec;
        }

        if (dry_run_only) {
            coordinator_->abort_checkpoint(coordinator_checkpoint);
            {
                std::lock_guard guard{telemetry_mutex_};
                telemetry_.coordinator_abort_calls += 1U;
                telemetry_.coordinator_dry_runs += 1U;
            }
            return {};
        }
    }

    WalAppendResult append_result{};
    const auto emit_start = std::chrono::steady_clock::now();
    std::error_code emit_ec{};
    if (use_coordinator) {
        emit_ec = coordinator_->commit_checkpoint(coordinator_checkpoint, append_result);
    } else {
        std::vector<WalCheckpointIndexEntry> wal_index_metadata;
        wal_index_metadata.reserve(snapshot.index_metadata.size());
        for (const auto& metadata : snapshot.index_metadata) {
            WalCheckpointIndexEntry entry{};
            entry.index_id = metadata.index_id;
            entry.high_water_lsn = metadata.high_water_lsn;
            wal_index_metadata.push_back(entry);
        }
        emit_ec = checkpoint_manager_->emit_checkpoint(next_checkpoint_id_,
                                                       snapshot.redo_lsn,
                                                       snapshot.undo_lsn,
                                                       snapshot.dirty_pages,
                                                       snapshot.active_transactions,
                                                       wal_index_metadata,
                                                       append_result);
    }
    const auto emit_end = std::chrono::steady_clock::now();
    const auto emit_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(emit_end - emit_start);
    const auto emit_duration_ns = emit_ns.count() > 0 ? static_cast<std::uint64_t>(emit_ns.count()) : 0ULL;

    if (use_coordinator) {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.coordinator_commit_calls += 1U;
        telemetry_.coordinator_total_commit_duration_ns += emit_duration_ns;
        telemetry_.coordinator_last_commit_duration_ns = emit_duration_ns;
        if (emit_ec) {
            telemetry_.coordinator_commit_failures += 1U;
        }
    }

    {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.total_emit_duration_ns += emit_duration_ns;
        telemetry_.last_emit_duration_ns = emit_duration_ns;
        if (emit_ec) {
            telemetry_.emit_failures += 1U;
        }
    }

    if (emit_ec) {
        return emit_ec;
    }

    const auto emitted_bytes = use_coordinator
        ? compute_checkpoint_bytes(coordinator_checkpoint.snapshot, append_result)
        : compute_checkpoint_bytes(snapshot, append_result);
    consume_io_budget(emit_end, emitted_bytes, force);

    const auto checkpoint_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(emit_end - attempt_start);
    const auto checkpoint_duration_ns = checkpoint_ns.count() > 0 ? static_cast<std::uint64_t>(checkpoint_ns.count()) : 0ULL;

    std::uint64_t fence_duration_ns = 0ULL;
    if (fence_active) {
        const auto fence_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(emit_end - fence_start);
        fence_duration_ns = fence_ns.count() > 0 ? static_cast<std::uint64_t>(fence_ns.count()) : 0ULL;
    }

    if (config_.flush_after_emit && writer) {
        const auto flush_start = std::chrono::steady_clock::now();
        if (auto flush_ec = writer->flush(); flush_ec) {
            const auto flush_end = std::chrono::steady_clock::now();
            const auto flush_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(flush_end - flush_start);
            const auto flush_duration_ns = flush_ns.count() > 0 ? static_cast<std::uint64_t>(flush_ns.count()) : 0ULL;
            {
                std::lock_guard guard{telemetry_mutex_};
                telemetry_.total_flush_duration_ns += flush_duration_ns;
                telemetry_.last_flush_duration_ns = flush_duration_ns;
                telemetry_.flush_failures += 1U;
            }
            return flush_ec;
        }

        const auto flush_end = std::chrono::steady_clock::now();
        const auto flush_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(flush_end - flush_start);
        const auto flush_duration_ns = flush_ns.count() > 0 ? static_cast<std::uint64_t>(flush_ns.count()) : 0ULL;
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.total_flush_duration_ns += flush_duration_ns;
        telemetry_.last_flush_duration_ns = flush_duration_ns;
    }

    if (retention_hook_ && has_retention_policy(retention_config_)) {
        WalRetentionStats retention_stats{};
        const auto retention_start = std::chrono::steady_clock::now();
        if (auto retention_ec = retention_hook_(retention_config_, append_result.segment_id, &retention_stats); retention_ec) {
            const auto retention_end = std::chrono::steady_clock::now();
            const auto retention_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(retention_end - retention_start);
            const auto retention_duration_ns = retention_ns.count() > 0 ? static_cast<std::uint64_t>(retention_ns.count()) : 0ULL;
            {
                std::lock_guard guard{telemetry_mutex_};
                telemetry_.retention_failures += 1U;
                telemetry_.retention_invocations += 1U;
                telemetry_.total_retention_duration_ns += retention_duration_ns;
                telemetry_.last_retention_duration_ns = retention_duration_ns;

                retention_telemetry_.invocations += 1U;
                retention_telemetry_.failures += 1U;
                retention_telemetry_.scanned_segments += retention_stats.scanned_segments;
                retention_telemetry_.candidate_segments += retention_stats.candidate_segments;
                retention_telemetry_.pruned_segments += retention_stats.pruned_segments;
                retention_telemetry_.archived_segments += retention_stats.archived_segments;
                retention_telemetry_.total_duration_ns += retention_duration_ns;
                retention_telemetry_.last_duration_ns = retention_duration_ns;
            }
            return retention_ec;
        }

        const auto retention_end = std::chrono::steady_clock::now();
        const auto retention_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(retention_end - retention_start);
        const auto retention_duration_ns = retention_ns.count() > 0 ? static_cast<std::uint64_t>(retention_ns.count()) : 0ULL;
        {
            std::lock_guard guard{telemetry_mutex_};
            telemetry_.retention_invocations += 1U;
            telemetry_.total_retention_duration_ns += retention_duration_ns;
            telemetry_.last_retention_duration_ns = retention_duration_ns;

            retention_telemetry_.invocations += 1U;
            retention_telemetry_.scanned_segments += retention_stats.scanned_segments;
            retention_telemetry_.candidate_segments += retention_stats.candidate_segments;
            retention_telemetry_.pruned_segments += retention_stats.pruned_segments;
            retention_telemetry_.archived_segments += retention_stats.archived_segments;
            retention_telemetry_.total_duration_ns += retention_duration_ns;
            retention_telemetry_.last_duration_ns = retention_duration_ns;
        }
    }

    if (index_retention_hook_) {
        IndexRetentionStats index_stats{};
        if (auto index_retention_ec = index_retention_hook_(now, append_result.lsn, &index_stats); index_retention_ec) {
            return index_retention_ec;
        }
    }

    has_emitted_ = true;
    last_checkpoint_time_ = now;
    last_checkpoint_id_ = next_checkpoint_id_;
    ++next_checkpoint_id_;
    if (durability_horizon_) {
        const auto durable_lsn = durability_horizon_->last_commit_lsn();
        if (durable_lsn != 0U) {
            last_checkpoint_lsn_ = durable_lsn;
        } else {
            last_checkpoint_lsn_ = current_lsn;
        }
    } else if (writer) {
        last_checkpoint_lsn_ = writer->next_lsn();
    } else {
        last_checkpoint_lsn_ = current_lsn;
    }

    {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.emitted_checkpoints += 1U;
        telemetry_.last_checkpoint_id = last_checkpoint_id_;
        telemetry_.last_checkpoint_lsn = last_checkpoint_lsn_;
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
        telemetry_.last_checkpoint_timestamp_ns = now_ns.count() > 0 ? static_cast<std::uint64_t>(now_ns.count()) : 0ULL;
        telemetry_.total_checkpoint_duration_ns += checkpoint_duration_ns;
        telemetry_.last_checkpoint_duration_ns = checkpoint_duration_ns;
        telemetry_.max_checkpoint_duration_ns = std::max(telemetry_.max_checkpoint_duration_ns, checkpoint_duration_ns);
        telemetry_.total_fence_duration_ns += fence_duration_ns;
        telemetry_.last_fence_duration_ns = fence_duration_ns;
        telemetry_.max_fence_duration_ns = std::max(telemetry_.max_fence_duration_ns, fence_duration_ns);
    }

    out_result = append_result;
    return {};
}

bool CheckpointScheduler::should_run(std::chrono::steady_clock::time_point now,
                                     const CheckpointSnapshot& snapshot,
                                     std::uint64_t current_lsn,
                                     bool force,
                                     TriggerReason& reason) const
{
    if (force) {
        reason = TriggerReason::Force;
        return true;
    }

    if (!has_emitted_) {
        reason = TriggerReason::First;
        return true;
    }

    if (config_.dirty_page_trigger > 0U && snapshot.dirty_pages.size() >= config_.dirty_page_trigger) {
        reason = TriggerReason::DirtyPages;
        return true;
    }

    if (config_.active_transaction_trigger > 0U
        && snapshot.active_transactions.size() >= config_.active_transaction_trigger) {
        reason = TriggerReason::ActiveTransactions;
        return true;
    }

    if (config_.min_interval.count() > 0) {
        const auto elapsed = now - last_checkpoint_time_;
        if (elapsed >= config_.min_interval) {
            reason = TriggerReason::Interval;
            return true;
        }
    }

    if (config_.lsn_gap_trigger > 0U && current_lsn >= last_checkpoint_lsn_) {
        const auto gap = current_lsn - last_checkpoint_lsn_;
        if (gap >= config_.lsn_gap_trigger) {
            reason = TriggerReason::LsnGap;
            return true;
        }
    }

    reason = TriggerReason::None;
    return false;
}

void CheckpointScheduler::update_retention_config(const WalRetentionConfig& config)
{
    retention_config_ = config;
}

WalRetentionConfig CheckpointScheduler::retention_config() const noexcept
{
    return retention_config_;
}

void CheckpointScheduler::reset()
{
    has_emitted_ = false;
    last_checkpoint_time_ = {};
    next_checkpoint_id_ = 1U;
    last_checkpoint_id_ = 0U;
    last_checkpoint_lsn_ = 0U;
}

CheckpointScheduler::Config CheckpointScheduler::config() const noexcept
{
    return config_;
}

std::uint64_t CheckpointScheduler::next_checkpoint_id() const noexcept
{
    return next_checkpoint_id_;
}

std::uint64_t CheckpointScheduler::last_checkpoint_id() const noexcept
{
    return last_checkpoint_id_;
}

std::uint64_t CheckpointScheduler::last_checkpoint_lsn() const noexcept
{
    return last_checkpoint_lsn_;
}

CheckpointTelemetrySnapshot CheckpointScheduler::telemetry_snapshot() const
{
    std::lock_guard guard{telemetry_mutex_};
    return telemetry_;
}

WalRetentionTelemetrySnapshot CheckpointScheduler::retention_telemetry_snapshot() const
{
    std::lock_guard guard{telemetry_mutex_};
    return retention_telemetry_;
}

void CheckpointScheduler::refill_io_budget(std::chrono::steady_clock::time_point now) const
{
    if (!io_throttle_enabled_) {
        return;
    }

    if (!io_refill_initialized_) {
        io_last_refill_ = now;
        io_budget_balance_bytes_ = io_budget_limit_bytes_;
        io_refill_initialized_ = true;
        return;
    }

    if (io_target_bytes_per_second_ == 0U) {
        return;
    }

    const auto elapsed = now - io_last_refill_;
    if (elapsed <= std::chrono::steady_clock::duration::zero()) {
        return;
    }

    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    const auto produced = static_cast<long double>(io_target_bytes_per_second_) * static_cast<long double>(elapsed_ns)
        / static_cast<long double>(std::chrono::nanoseconds{std::chrono::seconds{1}}.count());
    if (produced <= 0.0L) {
        return;
    }

    const auto produced_int = static_cast<std::int64_t>(produced);
    if (produced_int <= 0) {
        return;
    }

    io_budget_balance_bytes_ = std::min(io_budget_balance_bytes_ + produced_int, io_budget_limit_bytes_);
    io_last_refill_ = now;
}

bool CheckpointScheduler::can_schedule_checkpoint(std::chrono::steady_clock::time_point now,
                                                   std::size_t estimated_bytes) const
{
    if (!io_throttle_enabled_) {
        return true;
    }

    refill_io_budget(now);
    if (io_budget_balance_bytes_ >= static_cast<std::int64_t>(estimated_bytes)) {
        return true;
    }
    return false;
}

void CheckpointScheduler::consume_io_budget(std::chrono::steady_clock::time_point now,
                                            std::size_t bytes,
                                            bool forced)
{
    if (!io_throttle_enabled_) {
        return;
    }

    refill_io_budget(now);
    const auto cost = static_cast<std::int64_t>(bytes);
    io_budget_balance_bytes_ = std::max<std::int64_t>(-io_budget_limit_bytes_, io_budget_balance_bytes_ - cost);

    {
        std::lock_guard guard{telemetry_mutex_};
        telemetry_.io_throttle_bytes_consumed += bytes;
        if (!forced) {
            telemetry_.io_throttle_budget = static_cast<std::uint64_t>(std::max<std::int64_t>(0, io_budget_balance_bytes_));
        }
    }
}

void CheckpointScheduler::update_io_telemetry_locked() const
{
    if (!io_throttle_enabled_) {
        return;
    }
    telemetry_.io_throttle_budget = static_cast<std::uint64_t>(std::max<std::int64_t>(0, io_budget_balance_bytes_));
}

std::size_t CheckpointScheduler::estimate_checkpoint_bytes(const CheckpointSnapshot& snapshot) const
{
    if (!io_throttle_enabled_) {
        return 0U;
    }

    const auto page_bytes = snapshot.dirty_pages.size() * bored::storage::kPageSize;
    const auto txn_bytes = snapshot.active_transactions.size() * sizeof(WalCheckpointTxnEntry);
    const auto index_bytes = snapshot.index_metadata.size() * sizeof(CheckpointIndexMetadata);
    return saturating_add(page_bytes, saturating_add(txn_bytes, index_bytes));
}

std::size_t CheckpointScheduler::compute_checkpoint_bytes(const CheckpointSnapshot& snapshot,
                                                          const WalAppendResult& append_result) const
{
    if (!io_throttle_enabled_) {
        return append_result.written_bytes;
    }

    const auto metadata_bytes = estimate_checkpoint_bytes(snapshot);
    return saturating_add(metadata_bytes, append_result.written_bytes);
}

}  // namespace bored::storage

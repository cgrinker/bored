#include "bored/storage/checkpoint_scheduler.hpp"

#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_format.hpp"

#include <algorithm>
#include <utility>

namespace bored::storage {

namespace {

[[nodiscard]] bool has_retention_policy(const WalRetentionConfig& config)
{
    return config.retention_segments > 0U
        || config.retention_hours.count() > 0
        || !config.archive_path.empty();
}

}  // namespace

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager)
    : CheckpointScheduler(std::move(checkpoint_manager), Config{}, RetentionHook{})
{
}

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager, Config config)
    : CheckpointScheduler(std::move(checkpoint_manager), std::move(config), RetentionHook{})
{
}

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager,
                                         Config config,
                                         RetentionHook retention_hook)
    : checkpoint_manager_{std::move(checkpoint_manager)}
    , config_{config}
    , retention_hook_{std::move(retention_hook)}
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

    CheckpointSnapshot snapshot;
    if (auto ec = provider(snapshot); ec) {
        return ec;
    }

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

    WalAppendResult append_result{};
    const auto emit_start = std::chrono::steady_clock::now();
    auto emit_ec = checkpoint_manager_->emit_checkpoint(next_checkpoint_id_,
                                                        snapshot.redo_lsn,
                                                        snapshot.undo_lsn,
                                                        snapshot.dirty_pages,
                                                        snapshot.active_transactions,
                                                        append_result);
    const auto emit_end = std::chrono::steady_clock::now();
    const auto emit_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(emit_end - emit_start);
    const auto emit_duration_ns = emit_ns.count() > 0 ? static_cast<std::uint64_t>(emit_ns.count()) : 0ULL;

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

}  // namespace bored::storage

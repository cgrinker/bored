#include "bored/storage/checkpoint_scheduler.hpp"

#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_format.hpp"

#include <algorithm>

namespace bored::storage {

namespace {

[[nodiscard]] bool has_retention_policy(const WalRetentionConfig& config)
{
    return config.retention_segments > 0U
        || config.retention_hours.count() > 0
        || !config.archive_path.empty();
}

}  // namespace

CheckpointScheduler::CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager,
                                         Config config,
                                         RetentionHook retention_hook)
    : checkpoint_manager_{std::move(checkpoint_manager)}
    , config_{config}
    , retention_hook_{std::move(retention_hook)}
    , retention_config_{config.retention}
{
    if (config_.lsn_gap_trigger == 0U) {
        config_.lsn_gap_trigger = 4U * kWalBlockSize;
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

    CheckpointSnapshot snapshot;
    if (auto ec = provider(snapshot); ec) {
        return ec;
    }

    const auto writer = wal_writer();
    const auto current_lsn = writer ? writer->next_lsn() : 0U;

    if (!should_run(now, snapshot, current_lsn, force)) {
        return {};
    }

    WalAppendResult append_result{};
    auto emit_ec = checkpoint_manager_->emit_checkpoint(next_checkpoint_id_,
                                                        snapshot.redo_lsn,
                                                        snapshot.undo_lsn,
                                                        snapshot.dirty_pages,
                                                        snapshot.active_transactions,
                                                        append_result);
    if (emit_ec) {
        return emit_ec;
    }

    if (config_.flush_after_emit && writer) {
        if (auto flush_ec = writer->flush(); flush_ec) {
            return flush_ec;
        }
    }

    if (retention_hook_ && has_retention_policy(retention_config_)) {
        if (auto retention_ec = retention_hook_(retention_config_, append_result.segment_id); retention_ec) {
            return retention_ec;
        }
    }

    has_emitted_ = true;
    last_checkpoint_time_ = now;
    last_checkpoint_id_ = next_checkpoint_id_;
    ++next_checkpoint_id_;
    if (writer) {
        last_checkpoint_lsn_ = writer->next_lsn();
    } else {
        last_checkpoint_lsn_ = current_lsn;
    }

    out_result = append_result;
    return {};
}

bool CheckpointScheduler::should_run(std::chrono::steady_clock::time_point now,
                                     const CheckpointSnapshot& snapshot,
                                     std::uint64_t current_lsn,
                                     bool force) const
{
    if (force) {
        return true;
    }

    if (!has_emitted_) {
        return true;
    }

    if (config_.dirty_page_trigger > 0U && snapshot.dirty_pages.size() >= config_.dirty_page_trigger) {
        return true;
    }

    if (config_.active_transaction_trigger > 0U
        && snapshot.active_transactions.size() >= config_.active_transaction_trigger) {
        return true;
    }

    if (config_.min_interval.count() > 0) {
        const auto elapsed = now - last_checkpoint_time_;
        if (elapsed >= config_.min_interval) {
            return true;
        }
    }

    if (config_.lsn_gap_trigger > 0U && current_lsn >= last_checkpoint_lsn_) {
        const auto gap = current_lsn - last_checkpoint_lsn_;
        if (gap >= config_.lsn_gap_trigger) {
            return true;
        }
    }

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

}  // namespace bored::storage

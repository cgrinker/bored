#include "bored/storage/checkpoint_coordinator.hpp"

#include "bored/storage/checkpoint_image_store.hpp"

#include <span>
#include <utility>

namespace bored::storage {

CheckpointCoordinator::CheckpointCoordinator(std::shared_ptr<CheckpointManager> checkpoint_manager,
                                             txn::TransactionManager& transaction_manager,
                                             WalRetentionManager& retention_manager,
                                             CheckpointImageStore* image_store) noexcept
    : checkpoint_manager_{std::move(checkpoint_manager)}
    , transaction_manager_{&transaction_manager}
    , retention_manager_{&retention_manager}
    , image_store_{image_store}
{
}

std::error_code CheckpointCoordinator::begin_checkpoint(std::uint64_t checkpoint_id,
                                                        ActiveCheckpoint& checkpoint)
{
    if (!checkpoint_manager_ || !transaction_manager_ || !retention_manager_) {
        return std::make_error_code(std::errc::not_connected);
    }

    checkpoint.checkpoint_id = checkpoint_id;
    checkpoint.transaction_fence = transaction_manager_->acquire_checkpoint_fence();
    checkpoint.retention_pin = retention_manager_->pin();
    checkpoint.snapshot = CheckpointSnapshot{};
    return {};
}

std::error_code CheckpointCoordinator::prepare_checkpoint(const SnapshotProvider& provider,
                                                          ActiveCheckpoint& checkpoint)
{
    if (!provider) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    CheckpointSnapshot snapshot{};
    if (auto ec = provider(snapshot); ec) {
        return ec;
    }

    checkpoint.snapshot = std::move(snapshot);
    return {};
}

std::error_code CheckpointCoordinator::commit_checkpoint(ActiveCheckpoint& checkpoint,
                                                         WalAppendResult& out_result)
{
    if (!checkpoint_manager_) {
        checkpoint.transaction_fence.release();
        checkpoint.retention_pin.release();
        return std::make_error_code(std::errc::not_connected);
    }

    auto ec = checkpoint_manager_->emit_checkpoint(checkpoint.checkpoint_id,
                                                   checkpoint.snapshot.redo_lsn,
                                                   checkpoint.snapshot.undo_lsn,
                                                   checkpoint.snapshot.dirty_pages,
                                                   checkpoint.snapshot.active_transactions,
                                                   out_result);

    if (!ec && image_store_ != nullptr) {
        auto persist_ec = image_store_->persist(checkpoint.checkpoint_id,
                                                std::span<const CheckpointPageSnapshot>(checkpoint.snapshot.page_snapshots.data(),
                                                                                         checkpoint.snapshot.page_snapshots.size()));
        if (!persist_ec) {
            persist_ec = image_store_->discard_older_than(checkpoint.checkpoint_id);
        }
        if (persist_ec) {
            ec = persist_ec;
        }
    }

    checkpoint.transaction_fence.release();
    checkpoint.retention_pin.release();
    return ec;
}

void CheckpointCoordinator::abort_checkpoint(ActiveCheckpoint& checkpoint) noexcept
{
    checkpoint.transaction_fence.release();
    checkpoint.retention_pin.release();
    checkpoint.snapshot = CheckpointSnapshot{};
    checkpoint.checkpoint_id = 0U;
}

}  // namespace bored::storage

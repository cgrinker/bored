#pragma once

#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/checkpoint_types.hpp"
#include "bored/storage/wal_retention.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <system_error>

namespace bored::storage {

class WalRetentionManager;
class CheckpointImageStore;

class CheckpointCoordinator final {
public:
    using SnapshotProvider = std::function<std::error_code(CheckpointSnapshot&)>;

    struct ActiveCheckpoint final {
        std::uint64_t checkpoint_id = 0U;
        txn::TransactionManager::CheckpointFence transaction_fence{};
        WalRetentionManager::ScopedPin retention_pin{};
        CheckpointSnapshot snapshot{};
    };

    CheckpointCoordinator(std::shared_ptr<CheckpointManager> checkpoint_manager,
                          txn::TransactionManager& transaction_manager,
                          WalRetentionManager& retention_manager,
                          CheckpointImageStore* image_store = nullptr) noexcept;

    [[nodiscard]] std::error_code begin_checkpoint(std::uint64_t checkpoint_id,
                                                   ActiveCheckpoint& checkpoint);

    [[nodiscard]] std::error_code prepare_checkpoint(const SnapshotProvider& provider,
                                                     ActiveCheckpoint& checkpoint);

    [[nodiscard]] std::error_code commit_checkpoint(ActiveCheckpoint& checkpoint,
                                                    WalAppendResult& out_result);

    void abort_checkpoint(ActiveCheckpoint& checkpoint) noexcept;

private:
    std::shared_ptr<CheckpointManager> checkpoint_manager_{};
    txn::TransactionManager* transaction_manager_ = nullptr;
    WalRetentionManager* retention_manager_ = nullptr;
    CheckpointImageStore* image_store_ = nullptr;
};

}  // namespace bored::storage

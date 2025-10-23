#pragma once

#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/wal_retention.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <system_error>
#include <vector>

namespace bored::storage {

struct CheckpointSnapshot final {
    std::uint64_t redo_lsn = 0U;
    std::uint64_t undo_lsn = 0U;
    std::vector<WalCheckpointDirtyPageEntry> dirty_pages{};
    std::vector<WalCheckpointTxnEntry> active_transactions{};
};

class CheckpointScheduler final {
public:
    struct Config final {
        std::chrono::milliseconds min_interval{std::chrono::seconds(30)};
        std::size_t dirty_page_trigger = 128U;
        std::size_t active_transaction_trigger = 32U;
        std::uint64_t lsn_gap_trigger = 0U;
        bool flush_after_emit = true;
        WalRetentionConfig retention{};
    };

    using SnapshotProvider = std::function<std::error_code(CheckpointSnapshot&)>;
    using RetentionHook = std::function<std::error_code(const WalRetentionConfig&, std::uint64_t)>;

    CheckpointScheduler(std::shared_ptr<CheckpointManager> checkpoint_manager,
                        Config config = {},
                        RetentionHook retention_hook = {});

    [[nodiscard]] std::error_code maybe_run(std::chrono::steady_clock::time_point now,
                                            const SnapshotProvider& provider,
                                            bool force,
                                            std::optional<WalAppendResult>& out_result);

    void update_retention_config(const WalRetentionConfig& config);
    [[nodiscard]] WalRetentionConfig retention_config() const noexcept;

    void reset();

    [[nodiscard]] Config config() const noexcept;
    [[nodiscard]] std::uint64_t next_checkpoint_id() const noexcept;
    [[nodiscard]] std::uint64_t last_checkpoint_id() const noexcept;
    [[nodiscard]] std::uint64_t last_checkpoint_lsn() const noexcept;

private:
    [[nodiscard]] bool should_run(std::chrono::steady_clock::time_point now,
                                  const CheckpointSnapshot& snapshot,
                                  std::uint64_t current_lsn,
                                  bool force) const;

    [[nodiscard]] std::shared_ptr<WalWriter> wal_writer() const noexcept;

    std::shared_ptr<CheckpointManager> checkpoint_manager_{};
    Config config_{};
    RetentionHook retention_hook_{};

    bool has_emitted_ = false;
    std::chrono::steady_clock::time_point last_checkpoint_time_{};
    std::uint64_t next_checkpoint_id_ = 1U;
    std::uint64_t last_checkpoint_id_ = 0U;
    std::uint64_t last_checkpoint_lsn_ = 0U;
    WalRetentionConfig retention_config_{};
};

}  // namespace bored::storage

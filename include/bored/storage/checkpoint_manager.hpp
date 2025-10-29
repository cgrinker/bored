#pragma once

#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_writer.hpp"

#include <cstdint>
#include <memory>
#include <span>
#include <system_error>

namespace bored::storage {

class CheckpointManager final {
public:
    explicit CheckpointManager(std::shared_ptr<WalWriter> wal_writer) noexcept;

    [[nodiscard]] std::error_code emit_checkpoint(std::uint64_t checkpoint_id,
                                                  std::uint64_t redo_lsn,
                                                  std::uint64_t undo_lsn,
                                                  std::span<const WalCheckpointDirtyPageEntry> dirty_pages,
                                                  std::span<const WalCheckpointTxnEntry> active_transactions,
                                                  std::span<const WalCheckpointIndexEntry> index_metadata,
                                                  WalAppendResult& out_result) const;

    [[nodiscard]] std::shared_ptr<WalWriter> wal_writer() const noexcept;

private:
    std::shared_ptr<WalWriter> wal_writer_{};
};

}  // namespace bored::storage

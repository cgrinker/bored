#pragma once

#include "bored/storage/wal_payloads.hpp"

#include <cstdint>
#include <vector>

namespace bored::storage {

struct CheckpointSnapshot final {
    std::uint64_t redo_lsn = 0U;
    std::uint64_t undo_lsn = 0U;
    std::vector<WalCheckpointDirtyPageEntry> dirty_pages{};
    std::vector<WalCheckpointTxnEntry> active_transactions{};
};

}  // namespace bored::storage

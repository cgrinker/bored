#pragma once

#include "bored/storage/page_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <cstdint>
#include <vector>

namespace bored::storage {

struct CheckpointPageSnapshot final {
    WalCheckpointDirtyPageEntry entry{};
    PageType page_type = PageType::Table;
    std::vector<std::byte> image{};
};

struct CheckpointSnapshot final {
    std::uint64_t redo_lsn = 0U;
    std::uint64_t undo_lsn = 0U;
    std::vector<WalCheckpointDirtyPageEntry> dirty_pages{};
    std::vector<WalCheckpointTxnEntry> active_transactions{};
    std::vector<CheckpointPageSnapshot> page_snapshots{};
};

}  // namespace bored::storage

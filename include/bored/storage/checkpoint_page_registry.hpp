#pragma once

#include "bored/storage/wal_payloads.hpp"

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace bored::storage {

class CheckpointPageRegistry final {
public:
    CheckpointPageRegistry() = default;

    void register_dirty_page(std::uint32_t page_id, std::uint64_t page_lsn);
    void snapshot_into(std::vector<WalCheckpointDirtyPageEntry>& out) const;
    void clear() noexcept;

private:
    mutable std::mutex mutex_{};
    std::unordered_map<std::uint32_t, std::uint64_t> dirty_pages_{};
};

}  // namespace bored::storage

#pragma once

#include "bored/storage/checkpoint_types.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace bored::storage {

class IndexCheckpointRegistry final {
public:
    IndexCheckpointRegistry() = default;

    void register_dirty_page(std::uint64_t index_id, std::uint32_t page_id, std::uint64_t page_lsn);
    void register_index_progress(std::uint64_t index_id, std::uint64_t lsn);

    void merge_into(CheckpointSnapshot& snapshot) const;
    void snapshot_dirty_pages(std::vector<WalCheckpointDirtyPageEntry>& out) const;
    void snapshot_index_metadata(std::vector<CheckpointIndexMetadata>& out) const;
    void clear() noexcept;

private:
    struct IndexEntry final {
        std::unordered_map<std::uint32_t, std::uint64_t> dirty_pages{};
        std::uint64_t high_water_lsn = 0U;
    };

    void update_index_entry(IndexEntry& entry, std::uint32_t page_id, std::uint64_t page_lsn) noexcept;

    mutable std::mutex mutex_{};
    std::unordered_map<std::uint64_t, IndexEntry> indexes_{};
};

}  // namespace bored::storage

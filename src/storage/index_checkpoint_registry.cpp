#include "bored/storage/index_checkpoint_registry.hpp"

#include <algorithm>

namespace bored::storage {

namespace {

constexpr bool should_track(std::uint64_t index_id, std::uint32_t page_id, std::uint64_t page_lsn) noexcept
{
    return index_id != 0U && page_id != 0U && page_lsn != 0U;
}

}  // namespace

void IndexCheckpointRegistry::register_dirty_page(std::uint64_t index_id,
                                                  std::uint32_t page_id,
                                                  std::uint64_t page_lsn)
{
    if (!should_track(index_id, page_id, page_lsn)) {
        return;
    }

    std::lock_guard guard{mutex_};
    auto& entry = indexes_[index_id];
    update_index_entry(entry, page_id, page_lsn);
}

void IndexCheckpointRegistry::register_index_progress(std::uint64_t index_id, std::uint64_t lsn)
{
    if (index_id == 0U || lsn == 0U) {
        return;
    }

    std::lock_guard guard{mutex_};
    auto& entry = indexes_[index_id];
    entry.high_water_lsn = std::max(entry.high_water_lsn, lsn);
}

void IndexCheckpointRegistry::merge_into(CheckpointSnapshot& snapshot) const
{
    std::lock_guard guard{mutex_};
    snapshot.dirty_pages.reserve(snapshot.dirty_pages.size() + indexes_.size() * 2U);
    snapshot.index_metadata.reserve(snapshot.index_metadata.size() + indexes_.size());

    for (const auto& [index_id, entry] : indexes_) {
        for (const auto& [page_id, page_lsn] : entry.dirty_pages) {
            WalCheckpointDirtyPageEntry page_entry{};
            page_entry.page_id = page_id;
            page_entry.page_lsn = page_lsn;
            snapshot.dirty_pages.push_back(page_entry);
        }

        if (entry.high_water_lsn != 0U) {
            CheckpointIndexMetadata metadata{};
            metadata.index_id = index_id;
            metadata.high_water_lsn = entry.high_water_lsn;
            snapshot.index_metadata.push_back(metadata);
        }
    }
}

void IndexCheckpointRegistry::snapshot_dirty_pages(std::vector<WalCheckpointDirtyPageEntry>& out) const
{
    std::lock_guard guard{mutex_};
    out.reserve(out.size() + indexes_.size() * 2U);
    for (const auto& [_, entry] : indexes_) {
        for (const auto& [page_id, page_lsn] : entry.dirty_pages) {
            WalCheckpointDirtyPageEntry page_entry{};
            page_entry.page_id = page_id;
            page_entry.page_lsn = page_lsn;
            out.push_back(page_entry);
        }
    }
}

void IndexCheckpointRegistry::snapshot_index_metadata(std::vector<CheckpointIndexMetadata>& out) const
{
    std::lock_guard guard{mutex_};
    out.reserve(out.size() + indexes_.size());
    for (const auto& [index_id, entry] : indexes_) {
        if (entry.high_water_lsn == 0U) {
            continue;
        }
        CheckpointIndexMetadata metadata{};
        metadata.index_id = index_id;
        metadata.high_water_lsn = entry.high_water_lsn;
        out.push_back(metadata);
    }
}

void IndexCheckpointRegistry::clear() noexcept
{
    std::lock_guard guard{mutex_};
    indexes_.clear();
}

void IndexCheckpointRegistry::update_index_entry(IndexEntry& entry,
                                                 std::uint32_t page_id,
                                                 std::uint64_t page_lsn) noexcept
{
    auto& stored_lsn = entry.dirty_pages[page_id];
    stored_lsn = std::max(stored_lsn, page_lsn);
    entry.high_water_lsn = std::max(entry.high_water_lsn, page_lsn);
}

}  // namespace bored::storage

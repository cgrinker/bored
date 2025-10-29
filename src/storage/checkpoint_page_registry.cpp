#include "bored/storage/checkpoint_page_registry.hpp"

#include <algorithm>

namespace bored::storage {

void CheckpointPageRegistry::register_dirty_page(std::uint32_t page_id, std::uint64_t page_lsn)
{
    if (page_id == 0U) {
        return;
    }

    std::lock_guard guard{mutex_};
    auto& stored_lsn = dirty_pages_[page_id];
    stored_lsn = std::max(stored_lsn, page_lsn);
}

void CheckpointPageRegistry::snapshot_into(std::vector<WalCheckpointDirtyPageEntry>& out) const
{
    std::lock_guard guard{mutex_};
    out.reserve(out.size() + dirty_pages_.size());
    for (const auto& [page_id, page_lsn] : dirty_pages_) {
        WalCheckpointDirtyPageEntry entry{};
        entry.page_id = page_id;
        entry.page_lsn = page_lsn;
        out.push_back(entry);
    }
}

void CheckpointPageRegistry::clear() noexcept
{
    std::lock_guard guard{mutex_};
    dirty_pages_.clear();
}

}  // namespace bored::storage

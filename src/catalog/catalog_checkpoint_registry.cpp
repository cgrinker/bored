#include "bored/catalog/catalog_checkpoint_registry.hpp"

#include "bored/catalog/catalog_pages.hpp"
#include "bored/storage/checkpoint_scheduler.hpp"

#include <algorithm>

namespace bored::catalog {

std::error_code CatalogCheckpointRegistry::record_relations(std::span<const RelationId> relations,
                                                            std::uint64_t commit_lsn)
{
    if (relations.empty()) {
        return {};
    }

    std::lock_guard lock{mutex_};
    for (const auto& relation : relations) {
        const auto page_id = catalog_relation_page(relation);
        if (!page_id) {
            continue;
        }
        auto& stored_lsn = dirty_pages_[*page_id];
        if (commit_lsn > stored_lsn) {
            stored_lsn = commit_lsn;
        }
    }
    return {};
}

void CatalogCheckpointRegistry::merge_into(storage::CheckpointSnapshot& snapshot) const
{
    std::lock_guard lock{mutex_};
    snapshot.dirty_pages.reserve(snapshot.dirty_pages.size() + dirty_pages_.size());
    for (const auto& [page_id, lsn] : dirty_pages_) {
        storage::WalCheckpointDirtyPageEntry entry{};
        entry.page_id = page_id;
        entry.page_lsn = lsn;
        snapshot.dirty_pages.push_back(entry);
    }
}

void CatalogCheckpointRegistry::snapshot_into(std::vector<storage::WalCheckpointDirtyPageEntry>& out) const
{
    std::lock_guard lock{mutex_};
    out.reserve(out.size() + dirty_pages_.size());
    for (const auto& [page_id, lsn] : dirty_pages_) {
        storage::WalCheckpointDirtyPageEntry entry{};
        entry.page_id = page_id;
        entry.page_lsn = lsn;
        out.push_back(entry);
    }
}

void CatalogCheckpointRegistry::clear() noexcept
{
    std::lock_guard lock{mutex_};
    dirty_pages_.clear();
}

}  // namespace bored::catalog

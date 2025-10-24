#pragma once

#include "bored/catalog/catalog_relations.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <cstdint>
#include <mutex>
#include <span>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::storage {
struct CheckpointSnapshot;
}

namespace bored::catalog {

class CatalogCheckpointRegistry final {
public:
    std::error_code record_relations(std::span<const RelationId> relations, std::uint64_t commit_lsn);
    void merge_into(storage::CheckpointSnapshot& snapshot) const;
    void snapshot_into(std::vector<storage::WalCheckpointDirtyPageEntry>& out) const;
    void clear() noexcept;

private:
    using PageMap = std::unordered_map<std::uint32_t, std::uint64_t>;

    mutable std::mutex mutex_{};
    PageMap dirty_pages_{};
};

}  // namespace bored::catalog

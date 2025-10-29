#include "bored/storage/index_checkpoint_registry.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

#include <catch2/catch_test_macros.hpp>

using bored::storage::CheckpointIndexMetadata;
using bored::storage::CheckpointSnapshot;
using bored::storage::IndexCheckpointRegistry;
using bored::storage::WalCheckpointDirtyPageEntry;

namespace {

std::unordered_map<std::uint32_t, std::uint64_t> to_page_map(const std::vector<WalCheckpointDirtyPageEntry>& pages)
{
    std::unordered_map<std::uint32_t, std::uint64_t> map;
    map.reserve(pages.size());
    for (const auto& entry : pages) {
        map.emplace(entry.page_id, entry.page_lsn);
    }
    return map;
}

CheckpointIndexMetadata find_metadata(const std::vector<CheckpointIndexMetadata>& metadata, std::uint64_t index_id)
{
    for (const auto& entry : metadata) {
        if (entry.index_id == index_id) {
            return entry;
        }
    }
    return {};
}

}  // namespace

TEST_CASE("Index checkpoint registry publishes dirty pages and metadata")
{
    IndexCheckpointRegistry registry;

    registry.register_dirty_page(42U, 5U, 100U);
    registry.register_dirty_page(42U, 5U, 80U);
    registry.register_dirty_page(42U, 9U, 140U);
    registry.register_index_progress(42U, 180U);

    CheckpointSnapshot snapshot{};
    registry.merge_into(snapshot);

    const auto page_map = to_page_map(snapshot.dirty_pages);
    REQUIRE(page_map.size() == 2U);
    CHECK(page_map.at(5U) == 100U);
    CHECK(page_map.at(9U) == 140U);

    REQUIRE(snapshot.index_metadata.size() == 1U);
    const auto metadata = find_metadata(snapshot.index_metadata, 42U);
    CHECK(metadata.index_id == 42U);
    CHECK(metadata.high_water_lsn == 180U);
}

TEST_CASE("Index checkpoint registry snapshots only valid entries")
{
    IndexCheckpointRegistry registry;
    registry.register_dirty_page(0U, 1U, 10U);
    registry.register_dirty_page(51U, 0U, 10U);
    registry.register_dirty_page(51U, 3U, 0U);
    registry.register_index_progress(0U, 1U);

    std::vector<WalCheckpointDirtyPageEntry> pages;
    registry.snapshot_dirty_pages(pages);
    CHECK(pages.empty());

    std::vector<CheckpointIndexMetadata> metadata;
    registry.snapshot_index_metadata(metadata);
    CHECK(metadata.empty());
}

TEST_CASE("Index checkpoint registry clears state")
{
    IndexCheckpointRegistry registry;
    registry.register_dirty_page(7U, 11U, 200U);
    registry.register_index_progress(7U, 220U);

    registry.clear();

    CheckpointSnapshot snapshot{};
    registry.merge_into(snapshot);
    CHECK(snapshot.dirty_pages.empty());
    CHECK(snapshot.index_metadata.empty());
}

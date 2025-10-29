#include "bored/storage/checkpoint_page_registry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unordered_map>
#include <vector>

using bored::storage::CheckpointPageRegistry;
using bored::storage::WalCheckpointDirtyPageEntry;

namespace {

std::unordered_map<std::uint32_t, std::uint64_t> to_map(const std::vector<WalCheckpointDirtyPageEntry>& entries)
{
    std::unordered_map<std::uint32_t, std::uint64_t> result;
    for (const auto& entry : entries) {
        result.emplace(entry.page_id, entry.page_lsn);
    }
    return result;
}

}  // namespace

TEST_CASE("CheckpointPageRegistry keeps highest LSN per page")
{
    CheckpointPageRegistry registry;
    registry.register_dirty_page(42U, 100U);
    registry.register_dirty_page(42U, 90U);
    registry.register_dirty_page(42U, 200U);
    registry.register_dirty_page(77U, 150U);
    registry.register_dirty_page(0U, 300U);
    registry.register_dirty_page(88U, 0U);
    registry.register_dirty_page(99U, 0U);
    registry.register_dirty_page(88U, 50U);

    std::vector<WalCheckpointDirtyPageEntry> entries;
    registry.snapshot_into(entries);

    auto page_map = to_map(entries);
    REQUIRE(page_map.size() == 4U);
    REQUIRE(page_map.at(42U) == 200U);
    REQUIRE(page_map.at(77U) == 150U);
    REQUIRE(page_map.at(88U) == 50U);
    REQUIRE(page_map.at(99U) == 0U);
}

TEST_CASE("CheckpointPageRegistry clears tracked pages")
{
    CheckpointPageRegistry registry;
    registry.register_dirty_page(11U, 99U);

    std::vector<WalCheckpointDirtyPageEntry> entries;
    registry.snapshot_into(entries);
    REQUIRE(entries.size() == 1U);

    registry.clear();
    entries.clear();
    registry.snapshot_into(entries);
    REQUIRE(entries.empty());
}

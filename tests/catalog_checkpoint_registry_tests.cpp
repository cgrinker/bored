#include "bored/catalog/catalog_checkpoint_registry.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/storage/checkpoint_scheduler.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <ranges>

using namespace bored;
using namespace bored::catalog;

namespace {

std::vector<std::uint32_t> extract_page_ids(const std::vector<storage::WalCheckpointDirtyPageEntry>& entries)
{
    std::vector<std::uint32_t> ids;
    ids.reserve(entries.size());
    for (const auto& entry : entries) {
        ids.push_back(entry.page_id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

}  // namespace

TEST_CASE("CatalogCheckpointRegistry records relations with latest LSN")
{
    CatalogCheckpointRegistry registry;

    const std::array relations{catalog::kCatalogSchemasRelationId, catalog::kCatalogTablesRelationId};
    REQUIRE_FALSE(registry.record_relations(relations, 128U));

    std::vector<storage::WalCheckpointDirtyPageEntry> entries;
    registry.snapshot_into(entries);
    REQUIRE(entries.size() == 2U);

    const auto page_ids = extract_page_ids(entries);
    CHECK(std::ranges::binary_search(page_ids, catalog::kCatalogSchemasPageId));
    CHECK(std::ranges::binary_search(page_ids, catalog::kCatalogTablesPageId));

    for (const auto& entry : entries) {
        CHECK(entry.page_lsn == 128U);
    }
}

TEST_CASE("CatalogCheckpointRegistry updates to newer commit LSN")
{
    CatalogCheckpointRegistry registry;

    const std::array first{catalog::kCatalogTablesRelationId};
    REQUIRE_FALSE(registry.record_relations(first, 64U));

    const std::array second{catalog::kCatalogTablesRelationId};
    REQUIRE_FALSE(registry.record_relations(second, 512U));

    std::vector<storage::WalCheckpointDirtyPageEntry> entries;
    registry.snapshot_into(entries);
    REQUIRE(entries.size() == 1U);
    CHECK(entries[0].page_id == catalog::kCatalogTablesPageId);
    CHECK(entries[0].page_lsn == 512U);
}

TEST_CASE("CatalogCheckpointRegistry merges into snapshot")
{
    CatalogCheckpointRegistry registry;
    const std::array relations{catalog::kCatalogColumnsRelationId};
    REQUIRE_FALSE(registry.record_relations(relations, 42U));

    storage::CheckpointSnapshot snapshot;
    registry.merge_into(snapshot);

    REQUIRE(snapshot.dirty_pages.size() == 1U);
    CHECK(snapshot.dirty_pages[0].page_id == catalog::kCatalogColumnsPageId);
    CHECK(snapshot.dirty_pages[0].page_lsn == 42U);

    registry.clear();
    std::vector<storage::WalCheckpointDirtyPageEntry> entries;
    registry.snapshot_into(entries);
    CHECK(entries.empty());
}

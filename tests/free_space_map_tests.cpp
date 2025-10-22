#include "bored/storage/free_space_map.hpp"
#include "bored/storage/free_space_map_persistence.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <vector>

using bored::storage::FreeSpaceMap;
using bored::storage::FreeSpaceMapPersistence;

namespace {

std::filesystem::path make_temp_snapshot(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto path = root / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))
        ;
    std::filesystem::remove(path);
    return path;
}

}  // namespace

TEST_CASE("FreeSpaceMapPersistence round-trips snapshots")
{
    FreeSpaceMap map;
    map.record_page(1U, 2048U, 0U);
    map.record_page(2U, 512U, 3U);

    auto snapshot_path = make_temp_snapshot("bored_fsm_snapshot_");
    REQUIRE_FALSE(FreeSpaceMapPersistence::write_snapshot(map, snapshot_path));

    FreeSpaceMap restored;
    REQUIRE_FALSE(FreeSpaceMapPersistence::load_snapshot(snapshot_path, restored));

    CHECK(restored.current_free_bytes(1U) == 2048U);
    CHECK(restored.current_fragment_count(1U) == 0U);
    CHECK(restored.current_free_bytes(2U) == 512U);
    CHECK(restored.current_fragment_count(2U) == 3U);

    std::filesystem::remove(snapshot_path);
}

TEST_CASE("FreeSpaceMapPersistence handles missing files")
{
    FreeSpaceMap map;
    auto path = make_temp_snapshot("bored_fsm_missing_");
    auto ec = FreeSpaceMapPersistence::load_snapshot(path, map);
    REQUIRE(ec == std::make_error_code(std::errc::no_such_file_or_directory));
}

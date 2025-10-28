#include "bored/executor/executor_temp_resource_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

using bored::executor::ExecutorTempResourceManager;
using bored::storage::TempResourcePurgeReason;
using bored::storage::TempResourcePurgeStats;

namespace {

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto dir = root / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    return dir;
}

}  // namespace

TEST_CASE("ExecutorTempResourceManager purges spill directories", "[executor][temp]")
{
    auto base = make_temp_dir("bored_executor_spill_test_");
    ExecutorTempResourceManager manager{ExecutorTempResourceManager::Config{.base_directory = base}};

    std::filesystem::path spill_dir;
    REQUIRE_FALSE(manager.create_spill_directory("hash_join", spill_dir));
    REQUIRE(std::filesystem::is_directory(spill_dir));

    auto spill_file = spill_dir / "chunk.tmp";
    {
        std::ofstream stream{spill_file, std::ios::binary};
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));

    TempResourcePurgeStats stats{};
    REQUIRE_FALSE(manager.purge(TempResourcePurgeReason::Checkpoint, &stats));
    CHECK(stats.checked_entries == 1U);
    CHECK(stats.errors == 0U);
    CHECK(std::filesystem::exists(spill_dir));
    CHECK_FALSE(std::filesystem::exists(spill_file));

    std::filesystem::remove_all(base);
}

TEST_CASE("ExecutorTempResourceManager tracks scratch segments", "[executor][temp]")
{
    auto base = make_temp_dir("bored_executor_scratch_test_");
    ExecutorTempResourceManager manager{ExecutorTempResourceManager::Config{.base_directory = base}};

    auto scratch_file = base / "segments" / "spill.segment";
    REQUIRE_FALSE(manager.track_scratch_segment(scratch_file));

    {
        std::ofstream stream{scratch_file, std::ios::binary};
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(scratch_file));

    TempResourcePurgeStats stats{};
    REQUIRE_FALSE(manager.purge(TempResourcePurgeReason::Recovery, &stats));
    CHECK(stats.checked_entries == 1U);
    CHECK(stats.errors == 0U);
    CHECK_FALSE(std::filesystem::exists(scratch_file));

    std::filesystem::remove_all(base);
}

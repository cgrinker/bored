#include "bored/catalog/catalog_ids.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/index_btree_leaf_ops.hpp"
#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/index_btree_manager.hpp"
#include "bored/storage/index_retention.hpp"
#include "bored/storage/index_retention_executor.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

using namespace std::chrono_literals;
using bored::storage::IndexRetentionCandidate;
using bored::storage::IndexRetentionManager;
using bored::storage::IndexRetentionStats;
using bored::storage::IndexRetentionTelemetrySnapshot;

namespace {

std::shared_ptr<bored::storage::AsyncIo> make_async_io()
{
    bored::storage::AsyncIoConfig config{};
    config.backend = bored::storage::AsyncIoBackend::ThreadPool;
    config.worker_threads = 1U;
    config.queue_depth = 4U;
    auto instance = bored::storage::create_async_io(config);
    return std::shared_ptr<bored::storage::AsyncIo>(std::move(instance));
}

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto dir = root / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

}  // namespace

TEST_CASE("IndexRetentionManager dispatches eligible candidates after checkpoint")
{
    std::vector<IndexRetentionCandidate> dispatched;
    auto dispatch = [&](std::span<const IndexRetentionCandidate> span, IndexRetentionStats& stats) {
        dispatched.insert(dispatched.end(), span.begin(), span.end());
        stats.pruned_candidates += span.size();
        return std::error_code{};
    };

    IndexRetentionManager::Config config{};
    config.retention_window = 0ms;
    config.max_dispatch_batch = 8U;

    IndexRetentionManager manager{config, dispatch};

    IndexRetentionCandidate candidate{};
    candidate.index_id = 101U;
    candidate.page_id = 42U;
    candidate.level = 1U;
    candidate.prune_lsn = 1234U;

    manager.schedule_candidate(candidate);

    IndexRetentionStats stats{};
    const auto now = std::chrono::steady_clock::now();
    REQUIRE_FALSE(manager.apply_checkpoint(now, 2000U, &stats));

    REQUIRE(dispatched.size() == 1U);
    CHECK(dispatched.front().index_id == candidate.index_id);
    CHECK(dispatched.front().page_id == candidate.page_id);
    CHECK(stats.dispatched_candidates == 1U);
    CHECK(stats.pruned_candidates == 1U);

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.dispatched_candidates == 1U);
    CHECK(telemetry.pruned_candidates == 1U);
}

TEST_CASE("IndexRetentionManager enforces retention window before dispatch")
{
    IndexRetentionManager::Config config{};
    config.retention_window = std::chrono::hours{1};
    config.min_checkpoint_interval = 0ms;

    IndexRetentionManager manager{config};

    IndexRetentionCandidate recent{};
    recent.index_id = 7U;
    recent.page_id = 11U;
    recent.level = 0U;
    recent.prune_lsn = 900U;
    recent.scheduled_at = std::chrono::steady_clock::now();
    manager.schedule_candidate(recent);

    IndexRetentionStats first_stats{};
    REQUIRE_FALSE(manager.apply_checkpoint(std::chrono::steady_clock::now(), 1200U, &first_stats));
    CHECK(first_stats.dispatched_candidates == 0U);
    CHECK(first_stats.pruned_candidates == 0U);

    IndexRetentionCandidate stale{};
    stale.index_id = 7U;
    stale.page_id = 12U;
    stale.level = 0U;
    stale.prune_lsn = 950U;
    stale.scheduled_at = std::chrono::steady_clock::now() - std::chrono::hours{2};
    manager.schedule_candidate(stale);

    IndexRetentionStats second_stats{};
    REQUIRE_FALSE(manager.apply_checkpoint(std::chrono::steady_clock::now(), 2000U, &second_stats));
    CHECK(second_stats.dispatched_candidates == 0U);
    CHECK(second_stats.pruned_candidates == 0U);  // no dispatch hook defined

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.scheduled_candidates >= 2U);
    CHECK(telemetry.skipped_candidates >= 1U);
}

TEST_CASE("IndexRetentionManager respects pending candidate limit")
{
    IndexRetentionManager::Config config{};
    config.max_pending_candidates = 1U;

    IndexRetentionManager manager{config};

    IndexRetentionCandidate first{};
    first.index_id = 1U;
    first.page_id = 1U;
    first.level = 0U;
    first.prune_lsn = 100U;
    manager.schedule_candidate(first);

    IndexRetentionCandidate second = first;
    second.page_id = 2U;
    manager.schedule_candidate(second);

    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.scheduled_candidates == 1U);
    CHECK(telemetry.dropped_candidates == 1U);
    CHECK(telemetry.pending_candidates == 1U);
}

TEST_CASE("IndexRetentionManager reinserts candidates when dispatch fails")
{
    bool should_fail = true;
    auto dispatch = [&](std::span<const IndexRetentionCandidate> span, IndexRetentionStats&) {
        if (should_fail) {
            should_fail = false;
            return std::make_error_code(std::errc::io_error);
        }
        return std::error_code{};
    };

    IndexRetentionManager::Config config{};
    config.retention_window = 0ms;
    config.min_checkpoint_interval = 0ms;

    IndexRetentionManager manager{config, dispatch};

    IndexRetentionCandidate candidate{};
    candidate.index_id = 55U;
    candidate.page_id = 99U;
    candidate.level = 2U;
    candidate.prune_lsn = 777U;
    manager.schedule_candidate(candidate);

    IndexRetentionStats stats{};
    const auto now = std::chrono::steady_clock::now();
    auto first = manager.apply_checkpoint(now, 900U, &stats);
    REQUIRE(first);

    REQUIRE_FALSE(manager.apply_checkpoint(now + 1ms, 900U, &stats));
    const auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.dispatch_failures == 1U);
    CHECK(telemetry.dispatched_candidates == 1U);
    CHECK(telemetry.pending_candidates == 0U);
}

TEST_CASE("IndexRetention rewrites index pointers across compaction restart drill", "[storage][index_retention]")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_index_retention_compaction_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    std::vector<WalCompactionEntry> compaction_entries;
    PageManager::Config page_config{};
    page_config.index_metadata_callback = [&](const WalCompactionEntry& entry) {
        compaction_entries.push_back(entry);
    };

    PageManager page_manager{&fsm, wal_writer, page_config};

    alignas(8) std::array<std::byte, kPageSize> heap_page{};
    auto heap_span = std::span<std::byte>(heap_page.data(), heap_page.size());
    constexpr std::uint32_t heap_page_id = 4'096U;
    REQUIRE_FALSE(page_manager.initialize_page(heap_span, PageType::Table, heap_page_id));

    std::array<std::byte, 32> first_tuple{};
    first_tuple.fill(std::byte{0x1});
    PageManager::TupleInsertResult first_insert{};
    REQUIRE_FALSE(page_manager.insert_tuple(heap_span, first_tuple, 1U, first_insert));

    std::array<std::byte, 48> second_tuple{};
    second_tuple.fill(std::byte{0x2});
    PageManager::TupleInsertResult second_insert{};
    REQUIRE_FALSE(page_manager.insert_tuple(heap_span, second_tuple, 2U, second_insert));

    const auto old_offset = second_insert.slot.offset;

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(page_manager.delete_tuple(heap_span, first_insert.slot.index, 1U, delete_result));

    PageManager::PageCompactionResult compaction_result{};
    REQUIRE_FALSE(page_manager.compact_page(heap_span, compaction_result));
    REQUIRE(compaction_result.performed);
    REQUIRE(compaction_result.relocations.size() == 1U);
    const auto relocation = compaction_result.relocations.front();
    CHECK(relocation.old_offset == old_offset);
    REQUIRE(relocation.new_offset != old_offset);
    REQUIRE(relocation.new_offset <= std::numeric_limits<std::uint16_t>::max());

    REQUIRE_FALSE(compaction_entries.empty());
    CHECK(static_cast<WalIndexMaintenanceAction>(compaction_entries.front().index_action) == WalIndexMaintenanceAction::RefreshPointers);

    REQUIRE_FALSE(page_manager.flush_wal());

    constexpr std::uint32_t index_page_id = 8'123U;
    constexpr std::uint64_t index_id_value = 9'001U;

    std::unordered_map<std::uint32_t, std::vector<std::byte>> index_pages;
    index_pages.emplace(index_page_id, std::vector<std::byte>(kPageSize));
    auto leaf_span = std::span<std::byte>(index_pages[index_page_id].data(), index_pages[index_page_id].size());
    REQUIRE_FALSE(initialize_index_page(leaf_span, index_page_id, 0U, true));

    IndexBtreeLeafEntry entry{};
    entry.pointer.heap_page_id = heap_page_id;
    entry.pointer.heap_slot_id = static_cast<std::uint16_t>(old_offset);
    std::uint64_t key_value = 42U;
    entry.key.resize(sizeof(key_value));
    std::memcpy(entry.key.data(), &key_value, sizeof(key_value));
    std::vector<IndexBtreeLeafEntry> leaf_entries;
    leaf_entries.push_back(entry);
    REQUIRE(rebuild_index_leaf_page(leaf_span, std::span<const IndexBtreeLeafEntry>(leaf_entries.data(), leaf_entries.size()), 0U));

    using bored::catalog::CatalogIndexDescriptor;
    using bored::catalog::CatalogIndexType;
    using bored::catalog::CatalogTupleDescriptor;
    using bored::catalog::IndexId;
    using bored::catalog::RelationId;

    CatalogTupleDescriptor tuple_desc{};
    CatalogIndexDescriptor descriptor{tuple_desc,
                                      IndexId{index_id_value},
                                      RelationId{111U},
                                      CatalogIndexType::BTree,
                                      index_page_id,
                                      0U,
                                      "int64_ascending",
                                      "compact_retention"};

    IndexRetentionExecutor::Config executor_config{};
    executor_config.index_lookup = [descriptor](std::uint64_t idx) -> std::optional<CatalogIndexDescriptor> {
        if (idx == index_id_value) {
            return descriptor;
        }
        return std::nullopt;
    };
    executor_config.leaf_page_enumerator = [index_page_id, index_id_value](std::uint64_t idx) {
        if (idx != index_id_value) {
            return std::vector<std::uint32_t>{};
        }
        return std::vector<std::uint32_t>{index_page_id};
    };
    executor_config.page_reader = [&index_pages](std::uint64_t, std::uint32_t page_id, std::span<std::byte> buffer) -> std::error_code {
        auto it = index_pages.find(page_id);
        if (it == index_pages.end()) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        std::memcpy(buffer.data(), it->second.data(), buffer.size());
        return {};
    };
    executor_config.page_writer = [&index_pages](std::uint64_t, std::uint32_t page_id, std::span<const std::byte> buffer, std::uint64_t) -> std::error_code {
        auto it = index_pages.find(page_id);
        if (it == index_pages.end()) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        it->second.assign(buffer.begin(), buffer.end());
        return {};
    };
    executor_config.compaction_lookup = [&compaction_entries, heap_page_id](std::uint32_t heap_page, std::uint64_t) {
        if (heap_page != heap_page_id) {
            return std::vector<WalCompactionEntry>{};
        }
        return compaction_entries;
    };

    IndexRetentionExecutor executor{executor_config};

    auto dispatch = [&executor](std::span<const IndexRetentionCandidate> span, IndexRetentionStats& stats) -> std::error_code {
        for (const auto& candidate : span) {
            if (auto ec = executor.prune(candidate); ec) {
                return ec;
            }
            stats.pruned_candidates += 1U;
        }
        return std::error_code{};
    };

    IndexRetentionManager::Config manager_config{};
    manager_config.retention_window = 0ms;
    manager_config.min_checkpoint_interval = 0ms;
    manager_config.max_dispatch_batch = 4U;

    IndexRetentionManager manager{manager_config, dispatch};

    IndexRetentionCandidate candidate{};
    candidate.index_id = index_id_value;
    candidate.page_id = heap_page_id;
    candidate.level = 0U;
    candidate.prune_lsn = compaction_result.compaction_wal.lsn != 0U ? compaction_result.compaction_wal.lsn : 1U;
    candidate.scheduled_at = std::chrono::steady_clock::now();

    manager.schedule_candidate(candidate);

    IndexRetentionStats stats{};
    const auto checkpoint_time = std::chrono::steady_clock::now();
    REQUIRE_FALSE(manager.apply_checkpoint(checkpoint_time, candidate.prune_lsn, &stats));
    CHECK(stats.dispatched_candidates == 1U);
    CHECK(stats.pruned_candidates == 1U);

    const auto& stored_page = index_pages.at(index_page_id);
    auto stored_span = std::span<const std::byte>(stored_page.data(), stored_page.size());
    std::error_code parse_error{};
    auto decoded_entries = read_index_leaf_entries(stored_span, parse_error);
    REQUIRE_FALSE(parse_error);
    REQUIRE(decoded_entries.size() == 1U);
    CHECK(decoded_entries.front().pointer.heap_page_id == heap_page_id);
    CHECK(decoded_entries.front().pointer.heap_slot_id == static_cast<std::uint16_t>(relocation.new_offset));
    const auto& header = page_header(stored_span);
    CHECK(header.lsn == candidate.prune_lsn);

    const auto persisted_page = stored_page;
    auto persisted_span = std::span<const std::byte>(persisted_page.data(), persisted_page.size());
    std::error_code restart_error{};
    auto restarted_entries = read_index_leaf_entries(persisted_span, restart_error);
    REQUIRE_FALSE(restart_error);
    REQUIRE(restarted_entries.size() == 1U);
    CHECK(restarted_entries.front().pointer.heap_slot_id == static_cast<std::uint16_t>(relocation.new_offset));

    REQUIRE_FALSE(page_manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

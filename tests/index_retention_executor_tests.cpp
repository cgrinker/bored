#include "bored/storage/index_retention_executor.hpp"

#include "bored/storage/index_btree_leaf_ops.hpp"
#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/index_btree_manager.hpp"
#include "bored/storage/index_comparator_registry.hpp"
#include "bored/storage/page_operations.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <system_error>
#include <string>
#include <unordered_map>
#include <vector>

using bored::catalog::CatalogIndexDescriptor;
using bored::catalog::CatalogIndexType;
using bored::catalog::CatalogTupleDescriptor;
using bored::catalog::IndexId;
using bored::catalog::RelationId;
using bored::storage::IndexBtreeLeafEntry;
using bored::storage::IndexBtreeTuplePointer;
using bored::storage::IndexRetentionCandidate;
using bored::storage::IndexRetentionExecutor;
using TelemetrySnapshot = bored::storage::IndexRetentionExecutor::TelemetrySnapshot;
using bored::storage::WalCompactionEntry;
using bored::storage::WalIndexMaintenanceAction;

namespace {

constexpr std::uint64_t kIndexId = 42U;
constexpr std::uint32_t kHeapPageId = 900U;
constexpr std::uint32_t kLeafPageId = 1200U;

CatalogIndexDescriptor make_descriptor()
{
    static const std::string comparator_name = "int64_ascending";
    static const std::string index_name = "executor_refresh";
    CatalogTupleDescriptor tuple{};
    return CatalogIndexDescriptor{tuple,
                                  IndexId{kIndexId},
                                  RelationId{0U},
                                  CatalogIndexType::BTree,
                                  0U,
                                  0U,
                                  comparator_name,
                                  index_name};
}

std::vector<std::byte> make_key(std::uint64_t value)
{
    std::vector<std::byte> key(sizeof(value));
    std::memcpy(key.data(), &value, sizeof(value));
    return key;
}

}  // namespace

TEST_CASE("IndexRetentionExecutor refreshes index leaf pointers", "[storage][index_retention]")
{
    using namespace bored::storage;

    auto comparator = find_index_comparator("int64_ascending");
    REQUIRE(comparator != nullptr);

    std::unordered_map<std::uint32_t, std::array<std::byte, kPageSize>> page_store;

    {
        std::array<std::byte, kPageSize> page{};
        auto span = std::span<std::byte>(page.data(), page.size());
        REQUIRE_FALSE(initialize_index_page(span, kLeafPageId, 0U, true));

        std::vector<IndexBtreeLeafEntry> entries;
        IndexBtreeLeafEntry refresh_entry{};
        refresh_entry.pointer.heap_page_id = kHeapPageId;
        refresh_entry.pointer.heap_slot_id = 128U;
        auto first_key = make_key(11U);
        refresh_entry.key.assign(first_key.begin(), first_key.end());
        entries.push_back(refresh_entry);

        IndexBtreeLeafEntry untouched_entry{};
        untouched_entry.pointer.heap_page_id = kHeapPageId;
        untouched_entry.pointer.heap_slot_id = 4096U;
        auto second_key = make_key(22U);
        untouched_entry.key.assign(second_key.begin(), second_key.end());
        entries.push_back(untouched_entry);

        REQUIRE(rebuild_index_leaf_page(span, std::span<const IndexBtreeLeafEntry>(entries.data(), entries.size()), 55U));
        page_store.emplace(kLeafPageId, page);
    }

    IndexRetentionExecutor::Config config{};
    config.index_lookup = [](std::uint64_t index_id) -> std::optional<CatalogIndexDescriptor> {
        if (index_id != kIndexId) {
            return std::nullopt;
        }
        return make_descriptor();
    };
    config.leaf_page_enumerator = [](std::uint64_t index_id) {
        if (index_id != kIndexId) {
            return std::vector<std::uint32_t>{};
        }
        return std::vector<std::uint32_t>{kLeafPageId};
    };
    config.page_reader = [&page_store](std::uint64_t, std::uint32_t page_id, std::span<std::byte> buffer) -> std::error_code {
        auto it = page_store.find(page_id);
        if (it == page_store.end()) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        std::memcpy(buffer.data(), it->second.data(), buffer.size());
        return {};
    };
    config.page_writer = [&page_store](std::uint64_t, std::uint32_t page_id, std::span<const std::byte> buffer, std::uint64_t) -> std::error_code {
        auto it = page_store.find(page_id);
        if (it == page_store.end()) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        std::memcpy(it->second.data(), buffer.data(), buffer.size());
        return {};
    };
    config.compaction_lookup = [](std::uint32_t heap_page_id, std::uint64_t) {
        std::vector<WalCompactionEntry> entries;
        if (heap_page_id != kHeapPageId) {
            return entries;
        }
        WalCompactionEntry refresh{};
        refresh.old_offset = 128U;
        refresh.new_offset = 512U;
        refresh.index_action = static_cast<std::uint32_t>(WalIndexMaintenanceAction::RefreshPointers);
        entries.push_back(refresh);

        WalCompactionEntry ignore{};
        ignore.old_offset = 999U;
        ignore.new_offset = 1000U;
        ignore.index_action = static_cast<std::uint32_t>(WalIndexMaintenanceAction::None);
        entries.push_back(ignore);
        return entries;
    };

    IndexRetentionExecutor executor{config};

    IndexRetentionCandidate candidate{};
    candidate.index_id = kIndexId;
    candidate.page_id = kHeapPageId;
    candidate.level = 0U;
    candidate.prune_lsn = 777U;

    REQUIRE_FALSE(executor.prune(candidate));

    auto stored_page = page_store.at(kLeafPageId);
    auto span = std::span<const std::byte>(stored_page.data(), stored_page.size());
    std::error_code parse_error;
    auto decoded = read_index_leaf_entries(span, parse_error);
    REQUIRE_FALSE(parse_error);
    REQUIRE(decoded.size() == 2U);
    CHECK(decoded.front().pointer.heap_slot_id == 512U);
    CHECK(decoded.front().pointer.heap_page_id == kHeapPageId);
    CHECK(decoded.back().pointer.heap_slot_id == 4096U);

    const auto& header = page_header(span);
    CHECK(header.lsn == candidate.prune_lsn);

    TelemetrySnapshot telemetry = executor.telemetry_snapshot();
    CHECK(telemetry.runs == 1U);
    CHECK(telemetry.successes == 1U);
    CHECK(telemetry.failures == 0U);
    CHECK(telemetry.skipped_candidates == 0U);
    CHECK(telemetry.scanned_pages == 1U);
    CHECK(telemetry.updated_pointers == 1U);
}

TEST_CASE("IndexRetentionExecutor supports custom prune callback", "[storage][index_retention]")
{
    using namespace bored::storage;

    std::vector<IndexRetentionCandidate> invoked;
    IndexRetentionExecutor::Config config{};
    config.prune_callback = [&invoked](const IndexRetentionCandidate& candidate) -> std::error_code {
        invoked.push_back(candidate);
        return {};
    };

    IndexRetentionExecutor executor{config};

    IndexRetentionCandidate candidate{};
    candidate.index_id = 9U;
    candidate.page_id = 99U;
    candidate.level = 0U;
    candidate.prune_lsn = 44U;

    REQUIRE_FALSE(executor.prune(candidate));
    REQUIRE(invoked.size() == 1U);
    CHECK(invoked.front().index_id == candidate.index_id);

    TelemetrySnapshot telemetry = executor.telemetry_snapshot();
    CHECK(telemetry.runs == 1U);
    CHECK(telemetry.successes == 1U);
    CHECK(telemetry.updated_pointers == 0U);
}

TEST_CASE("IndexRetentionExecutor skips when hooks absent", "[storage][index_retention]")
{
    using namespace bored::storage;

    IndexRetentionExecutor executor{};
    IndexRetentionCandidate candidate{};
    candidate.index_id = 1U;
    candidate.page_id = 2U;
    candidate.level = 0U;
    candidate.prune_lsn = 3U;

    REQUIRE_FALSE(executor.prune(candidate));

    TelemetrySnapshot telemetry = executor.telemetry_snapshot();
    CHECK(telemetry.runs == 1U);
    CHECK(telemetry.successes == 1U);
    CHECK(telemetry.skipped_candidates == 1U);
    CHECK(telemetry.updated_pointers == 0U);
}

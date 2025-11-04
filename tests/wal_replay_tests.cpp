#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/executor/delete_executor.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/update_executor.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_image_store.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/free_space_map_persistence.hpp"
#include "bored/storage/index_btree_leaf_ops.hpp"
#include "bored/storage/index_btree_manager.hpp"
#include "bored/storage/index_btree_page.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_undo_walker.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/snapshot_utils.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <system_error>
#include <sstream>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::FreeSpaceMapPersistence;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::CheckpointIndexMetadata;
using bored::storage::WalRecoveryDriver;
using bored::storage::WalRecoveryPlan;
using bored::storage::WalRecoveryRecord;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordType;
using bored::storage::WalReplayContext;
using bored::storage::WalReplayer;
using bored::storage::WalUndoWalker;
using bored::storage::WalUndoSpan;
using bored::storage::WalUndoWorkItem;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;
using bored::storage::WalCompactionEntry;
using bored::storage::WalIndexMaintenanceAction;
using bored::storage::any;

namespace {

constexpr std::size_t kIndexHeaderOffset = sizeof(bored::storage::PageHeader);
constexpr std::size_t kIndexPayloadBase = sizeof(bored::storage::PageHeader) + sizeof(bored::storage::IndexBtreePageHeader);

struct alignas(8) CatalogAllocatorState final {
    std::uint64_t next_schema_id = 0U;
    std::uint64_t next_table_id = 0U;
    std::uint64_t next_index_id = 0U;
    std::uint64_t next_column_id = 0U;
};

static_assert(sizeof(CatalogAllocatorState) == 32U, "CatalogAllocatorState expected to be 32 bytes");

std::shared_ptr<AsyncIo> make_async_io()
{
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 16U;
    auto instance = bored::storage::create_async_io(config);
    return std::shared_ptr<AsyncIo>(std::move(instance));
}

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto dir = root / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

std::span<const std::byte> tuple_payload_view(std::span<const std::byte> storage, std::size_t tuple_length)
{
    const auto header_size = bored::storage::tuple_header_size();
    if (storage.size() < header_size || tuple_length <= header_size) {
        return {};
    }
    const auto available = storage.size() - header_size;
    const auto logical = std::min<std::size_t>(available, tuple_length - header_size);
    return storage.subspan(header_size, logical);
}

std::span<const std::byte> tuple_payload_view(std::span<const std::byte> storage)
{
    return tuple_payload_view(storage, storage.size());
}

std::vector<std::byte> tuple_payload_vector(std::span<const std::byte> storage, std::size_t tuple_length)
{
    auto payload = tuple_payload_view(storage, tuple_length);
    return std::vector<std::byte>(payload.begin(), payload.end());
}

std::vector<std::byte> tuple_payload_vector(std::span<const std::byte> storage)
{
    auto payload = tuple_payload_view(storage);
    return std::vector<std::byte>(payload.begin(), payload.end());
}

bored::storage::TupleHeader decode_tuple_header(std::span<const std::byte> storage)
{
    bored::storage::TupleHeader header{};
    if (storage.size() >= bored::storage::tuple_header_size()) {
        std::memcpy(&header, storage.data(), bored::storage::tuple_header_size());
    }
    return header;
}

bored::storage::WalCommitHeader make_commit_header(const std::shared_ptr<WalWriter>& wal_writer,
                                                   std::uint64_t transaction_id,
                                                   std::uint64_t next_transaction_id = 0U,
                                                   std::uint64_t oldest_active_txn = 0U,
                                                   std::uint64_t oldest_active_commit_lsn = 0U)
{
    bored::storage::WalCommitHeader header{};
    header.transaction_id = transaction_id;
    header.commit_lsn = wal_writer ? wal_writer->next_lsn() : 0U;
    header.next_transaction_id = next_transaction_id != 0U ? next_transaction_id : (transaction_id + 1U);
    header.oldest_active_transaction_id = oldest_active_txn;
    header.oldest_active_commit_lsn = oldest_active_commit_lsn != 0U ? oldest_active_commit_lsn : header.commit_lsn;
    return header;
}

class VectorChild final : public bored::executor::ExecutorNode {
public:
    VectorChild(const std::vector<std::vector<std::byte>>* rows, std::size_t* iterations)
        : rows_{rows}
        , iterations_{iterations}
    {
    }

    void open(bored::executor::ExecutorContext&) override
    {
        position_ = 0U;
    }

    bool next(bored::executor::ExecutorContext&, bored::executor::TupleBuffer& buffer) override
    {
        if (rows_ == nullptr || position_ >= rows_->size()) {
            return false;
        }

        buffer.reset();
        const auto& row = (*rows_)[position_++];
        buffer.write(std::span<const std::byte>(row.data(), row.size()));
        if (iterations_ != nullptr) {
            ++(*iterations_);
        }
        return true;
    }

    void close(bored::executor::ExecutorContext&) override {}

private:
    const std::vector<std::vector<std::byte>>* rows_ = nullptr;
    std::size_t* iterations_ = nullptr;
    std::size_t position_ = 0U;
};

std::vector<std::vector<std::byte>> collect_page_rows(std::span<const std::byte> page,
                                                      const std::vector<std::uint16_t>& slots)
{
    std::vector<std::vector<std::byte>> rows;
    rows.reserve(slots.size());
    for (auto slot : slots) {
        auto tuple = bored::storage::read_tuple(page, slot);
        rows.emplace_back(tuple.begin(), tuple.end());
    }
    return rows;
}

std::vector<std::byte> make_update_tuple_row(std::uint64_t row_id, std::span<const std::byte> new_payload)
{
    bored::executor::TupleBuffer buffer{};
    bored::executor::TupleWriter writer{buffer};
    std::array<std::byte, sizeof(row_id)> row_bytes{};
    std::memcpy(row_bytes.data(), &row_id, row_bytes.size());
    writer.append_column(std::span<const std::byte>(row_bytes.data(), row_bytes.size()), false);
    writer.append_column(new_payload, false);
    writer.finalize();
    auto span = buffer.span();
    return std::vector<std::byte>(span.begin(), span.end());
}

std::vector<std::byte> make_delete_tuple_row(std::uint64_t row_id)
{
    bored::executor::TupleBuffer buffer{};
    bored::executor::TupleWriter writer{buffer};
    std::array<std::byte, sizeof(row_id)> row_bytes{};
    std::memcpy(row_bytes.data(), &row_id, row_bytes.size());
    writer.append_column(std::span<const std::byte>(row_bytes.data(), row_bytes.size()), false);
    writer.finalize();
    auto span = buffer.span();
    return std::vector<std::byte>(span.begin(), span.end());
}

struct SpoolTestRow final {
    std::uint64_t row_id = 0U;
    std::uint16_t slot_index = 0U;
    std::vector<std::byte> payload{};
};

class PageUpdateTarget final : public bored::executor::UpdateExecutor::Target {
public:
    PageUpdateTarget(bored::storage::PageManager* manager,
                     std::span<std::byte> page,
                     std::unordered_map<std::uint64_t, std::uint16_t>* slot_map,
                     std::unordered_map<std::uint64_t, std::vector<std::byte>>* expected_payloads)
        : manager_{manager}
        , page_{page}
        , slot_map_{slot_map}
        , expected_payloads_{expected_payloads}
    {
    }

    std::error_code update_tuple(const bored::executor::TupleView& tuple,
                                 bored::executor::ExecutorContext&,
                                 bored::executor::UpdateExecutor::UpdateStats& out_stats) override
    {
        if (!manager_ || !slot_map_ || tuple.column_count() < 2U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_view = tuple.column(0U);
        const auto new_payload_view = tuple.column(1U);
        if (row_view.is_null || new_payload_view.is_null || row_view.data.size() != sizeof(std::uint64_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::memcpy(&row_id, row_view.data.data(), sizeof(row_id));
        auto slot_it = slot_map_->find(row_id);
        if (slot_it == slot_map_->end()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        bored::storage::PageManager::TupleUpdateResult result{};
        auto ec = manager_->update_tuple(page_, slot_it->second, new_payload_view.data, row_id, result);
        if (ec) {
            return ec;
        }

        slot_it->second = result.slot.index;
        out_stats.new_payload_bytes = new_payload_view.data.size();
        out_stats.old_payload_bytes = result.old_length;
        out_stats.wal_bytes = result.wal.written_bytes;

        if (expected_payloads_ != nullptr) {
            (*expected_payloads_)[row_id] = std::vector<std::byte>(new_payload_view.data.begin(),
                                                                   new_payload_view.data.end());
        }

        ++updated_;
        return {};
    }

    std::error_code flush(bored::executor::ExecutorContext&) override
    {
        if (!manager_) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return manager_->flush_wal();
    }

    [[nodiscard]] std::size_t updated_count() const noexcept { return updated_; }

private:
    bored::storage::PageManager* manager_ = nullptr;
    std::span<std::byte> page_{};
    std::unordered_map<std::uint64_t, std::uint16_t>* slot_map_ = nullptr;
    std::unordered_map<std::uint64_t, std::vector<std::byte>>* expected_payloads_ = nullptr;
    std::size_t updated_ = 0U;
};

class PageDeleteTarget final : public bored::executor::DeleteExecutor::Target {
public:
    PageDeleteTarget(bored::storage::PageManager* manager,
                     std::span<std::byte> page,
                     std::unordered_map<std::uint64_t, std::uint16_t>* slot_map)
        : manager_{manager}
        , page_{page}
        , slot_map_{slot_map}
    {
    }

    std::error_code delete_tuple(const bored::executor::TupleView& tuple,
                                 bored::executor::ExecutorContext&,
                                 bored::executor::DeleteExecutor::DeleteStats& out_stats) override
    {
        if (!manager_ || !slot_map_ || tuple.column_count() < 1U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_view = tuple.column(0U);
        if (row_view.is_null || row_view.data.size() != sizeof(std::uint64_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::memcpy(&row_id, row_view.data.data(), sizeof(row_id));
        auto slot_it = slot_map_->find(row_id);
        if (slot_it == slot_map_->end()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        auto page_const = std::span<const std::byte>(page_.data(), page_.size());
        auto tuple_span = bored::storage::read_tuple(page_const, slot_it->second);
        const auto reclaimed = tuple_span.size();

        bored::storage::PageManager::TupleDeleteResult result{};
        auto ec = manager_->delete_tuple(page_, slot_it->second, row_id, result);
        if (ec) {
            return ec;
        }

        slot_map_->erase(slot_it);
        out_stats.reclaimed_bytes = reclaimed;
        out_stats.wal_bytes = result.wal.written_bytes;
        ++deleted_;
        return {};
    }

    std::error_code flush(bored::executor::ExecutorContext&) override
    {
        if (!manager_) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        return manager_->flush_wal();
    }

    [[nodiscard]] std::size_t deleted_count() const noexcept { return deleted_; }

private:
    bored::storage::PageManager* manager_ = nullptr;
    std::span<std::byte> page_{};
    std::unordered_map<std::uint64_t, std::uint16_t>* slot_map_ = nullptr;
    std::size_t deleted_ = 0U;
};

}  // namespace

TEST_CASE("WalReplayer rehydrates checkpoint page snapshots before redo", "[storage][wal][replay]")
{
    using namespace bored::storage;

    constexpr std::uint32_t page_id = 4242U;

    std::array<std::byte, kPageSize> snapshot_image{};
    auto snapshot_span = std::span<std::byte>(snapshot_image.data(), snapshot_image.size());
    REQUIRE(initialize_page(snapshot_span, PageType::Table, page_id, 0U, nullptr));
    snapshot_span[128] = std::byte{0xAA};
    snapshot_span[409] = std::byte{0xBC};

    SECTION("rehydrates valid snapshot")
    {
    WalRecoveryPlan plan{};
    CheckpointPageSnapshot snapshot{};
        snapshot.entry.page_id = page_id;
        snapshot.entry.page_lsn = 8192U;
        snapshot.page_type = PageType::Table;
        snapshot.image.assign(snapshot_image.begin(), snapshot_image.end());
    plan.checkpoint_page_snapshots.push_back(snapshot);

        WalReplayContext context{};
        WalReplayer replayer{context};
        REQUIRE_FALSE(replayer.apply_redo(plan));

        auto hydrated = context.get_page(page_id);
        REQUIRE(hydrated.size() == snapshot_image.size());
        CHECK(std::memcmp(hydrated.data(), snapshot_image.data(), snapshot_image.size()) == 0);
    }

    SECTION("rejects snapshot with invalid image length")
    {
    WalRecoveryPlan plan{};
    CheckpointPageSnapshot snapshot{};
        snapshot.entry.page_id = page_id;
        snapshot.image.resize(kPageSize - 1U, std::byte{0});
    plan.checkpoint_page_snapshots.push_back(snapshot);

        WalReplayContext context{};
        WalReplayer replayer{context};
        auto ec = replayer.apply_redo(plan);
        CHECK(ec == std::make_error_code(std::errc::invalid_argument));
    }

    SECTION("rehydrates index snapshot and checkpoint metadata")
    {
        WalRecoveryPlan plan{};

        std::array<std::byte, kPageSize> index_image{};
        auto index_span = std::span<std::byte>(index_image.data(), index_image.size());
        const std::uint64_t snapshot_lsn = 12288U;
        REQUIRE(bored::storage::initialize_page(index_span, PageType::Index, page_id, snapshot_lsn, nullptr));
        auto* index_header_ptr = reinterpret_cast<bored::storage::IndexBtreePageHeader*>(index_span.data() + kIndexHeaderOffset);
        index_header_ptr->magic = bored::storage::kIndexPageMagic;
        index_header_ptr->version = bored::storage::kIndexPageVersion;
        index_header_ptr->flags = bored::storage::kIndexPageFlagLeaf;
        index_span[256] = std::byte{0x44};
        index_span[512] = std::byte{0x99};

        CheckpointPageSnapshot snapshot{};
        snapshot.entry.page_id = page_id;
        snapshot.entry.page_lsn = snapshot_lsn;
        snapshot.page_type = PageType::Index;
        snapshot.image.assign(index_image.begin(), index_image.end());
        plan.checkpoint_page_snapshots.push_back(snapshot);

        CheckpointIndexMetadata metadata{};
        metadata.index_id = 55U;
        metadata.high_water_lsn = snapshot_lsn;
        plan.checkpoint_index_metadata.push_back(metadata);

        WalReplayContext context{};
        WalReplayer replayer{context};
        REQUIRE_FALSE(replayer.apply_redo(plan));

        auto hydrated = context.get_page(page_id);
        REQUIRE(hydrated.size() == index_image.size());
        CHECK(std::memcmp(hydrated.data(), index_image.data(), index_image.size()) == 0);

        const auto& checkpoint_metadata = context.checkpoint_index_metadata();
        REQUIRE(checkpoint_metadata.size() == 1U);
        CHECK(checkpoint_metadata.front().index_id == metadata.index_id);
        CHECK(checkpoint_metadata.front().high_water_lsn == metadata.high_water_lsn);
    }
}

TEST_CASE("WalReplayer rebuilds index leaf split pages")
{
    using bored::storage::IndexBtreeSlotEntry;
    using bored::storage::IndexBtreeTuplePointer;
    using bored::storage::IndexBtreePageHeader;
    using bored::storage::IndexBtreeChildPointer;
    using bored::storage::WalIndexSplitFlag;
    using bored::storage::WalIndexSplitHeader;
    using bored::storage::WalRecordHeader;
    using bored::storage::WalRecoveryPlan;
    using bored::storage::WalRecoveryRecord;
    using bored::storage::WalRecordType;

    constexpr std::size_t pointer_size = sizeof(IndexBtreeTuplePointer);
    STATIC_REQUIRE(pointer_size == 8U);

    auto make_key_bytes = [](std::uint64_t value) {
        std::array<std::byte, sizeof(std::uint64_t)> bytes{};
        std::memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    auto append_leaf_entry = [&](std::vector<IndexBtreeSlotEntry>& slots,
                                 std::vector<std::byte>& payload,
                                 std::uint32_t heap_page,
                                 std::uint16_t heap_slot,
                                 std::uint64_t key_value) {
        IndexBtreeTuplePointer pointer{};
        pointer.heap_page_id = heap_page;
        pointer.heap_slot_id = heap_slot;

        const auto* pointer_bytes = reinterpret_cast<const std::byte*>(&pointer);
        payload.insert(payload.end(), pointer_bytes, pointer_bytes + pointer_size);

        auto key_bytes = make_key_bytes(key_value);
        const auto key_offset = static_cast<std::uint16_t>(kIndexPayloadBase + payload.size());
        payload.insert(payload.end(), key_bytes.begin(), key_bytes.end());

        IndexBtreeSlotEntry slot{};
        slot.key_offset = key_offset;
        slot.key_length = static_cast<std::uint16_t>(key_bytes.size());
        slots.push_back(slot);
    };

    std::vector<IndexBtreeSlotEntry> left_slots;
    std::vector<std::byte> left_payload;
    append_leaf_entry(left_slots, left_payload, 11'001U, 4U, 10U);
    append_leaf_entry(left_slots, left_payload, 11'001U, 5U, 20U);

    std::vector<IndexBtreeSlotEntry> right_slots;
    std::vector<std::byte> right_payload;
    append_leaf_entry(right_slots, right_payload, 11'002U, 7U, 30U);
    append_leaf_entry(right_slots, right_payload, 11'002U, 8U, 40U);

    auto pivot_key_array = make_key_bytes(30U);
    auto pivot_key = std::span<const std::byte>(pivot_key_array.data(), pivot_key_array.size());

    WalIndexSplitHeader split_header{};
    split_header.index_id = 9'900U;
    split_header.left_page_id = 5'100U;
    split_header.right_page_id = 5'101U;
    split_header.parent_page_id = 4'000U;
    split_header.right_sibling_page_id = 5'010U;
    split_header.level = 0U;
    split_header.flags = static_cast<std::uint16_t>(WalIndexSplitFlag::Leaf);
    split_header.parent_insert_slot = 1U;

    const auto buffer_size = bored::storage::wal_index_split_payload_size(left_slots.size(),
                                                                          left_payload.size(),
                                                                          right_slots.size(),
                                                                          right_payload.size(),
                                                                          pivot_key.size());
    std::vector<std::byte> payload(buffer_size);
    REQUIRE(bored::storage::encode_wal_index_split(std::span<std::byte>(payload.data(), payload.size()),
                                                   split_header,
                                                   std::span<const IndexBtreeSlotEntry>(left_slots.data(), left_slots.size()),
                                                   std::span<const std::byte>(left_payload.data(), left_payload.size()),
                                                   std::span<const IndexBtreeSlotEntry>(right_slots.data(), right_slots.size()),
                                                   std::span<const std::byte>(right_payload.data(), right_payload.size()),
                                                   pivot_key));

    WalRecoveryRecord record{};
    record.header.type = static_cast<std::uint16_t>(WalRecordType::IndexSplit);
    record.header.lsn = 0xABCD'0001ULL;
    record.header.page_id = split_header.left_page_id;
    record.header.total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) + payload.size());
    record.payload = payload;

    WalRecoveryPlan plan{};
    plan.redo.push_back(record);

    bored::storage::WalReplayContext context{bored::storage::PageType::Table, nullptr};
    bored::storage::WalReplayer replayer{context};

    auto parent_page = context.get_page(split_header.parent_page_id);
    REQUIRE_FALSE(bored::storage::initialize_index_page(parent_page, split_header.parent_page_id, split_header.level + 1U, false));

    std::vector<std::byte> parent_payload;
    std::vector<IndexBtreeSlotEntry> parent_slots;

    auto append_internal_entry = [&](std::uint32_t child_page_id, std::span<const std::byte> key_bytes, bool infinite) {
        IndexBtreeChildPointer child_ptr{};
        child_ptr.page_id = child_page_id;
        const auto* child_bytes = reinterpret_cast<const std::byte*>(&child_ptr);
        parent_payload.insert(parent_payload.end(), child_bytes, child_bytes + sizeof(IndexBtreeChildPointer));

        IndexBtreeSlotEntry slot{};
        slot.key_offset = static_cast<std::uint16_t>(kIndexPayloadBase + parent_payload.size());
        if (!key_bytes.empty()) {
            parent_payload.insert(parent_payload.end(), key_bytes.begin(), key_bytes.end());
            slot.key_length = static_cast<std::uint16_t>(key_bytes.size());
        }
        if (infinite) {
            slot.key_length |= static_cast<std::uint16_t>(bored::storage::kIndexBtreeSlotInfiniteKeyMask);
        }
        parent_slots.push_back(slot);
    };

    auto key10 = make_key_bytes(10U);
    auto key45 = make_key_bytes(45U);

    append_internal_entry(4'900U, std::span<const std::byte>(key10.data(), key10.size()), false);
    append_internal_entry(split_header.left_page_id, std::span<const std::byte>(key45.data(), key45.size()), false);
    append_internal_entry(5'500U, std::span<const std::byte>(), true);

    if (!parent_payload.empty()) {
        std::memcpy(parent_page.data() + kIndexPayloadBase, parent_payload.data(), parent_payload.size());
    }

    const auto slot_bytes = parent_slots.size() * sizeof(IndexBtreeSlotEntry);
    const auto slot_start = bored::storage::kPageSize - slot_bytes;
    std::memcpy(parent_page.data() + slot_start, parent_slots.data(), slot_bytes);

    auto& parent_header = bored::storage::page_header(std::span<std::byte>(parent_page.data(), parent_page.size()));
    parent_header.free_start = static_cast<std::uint16_t>(kIndexPayloadBase + parent_payload.size());
    parent_header.free_end = static_cast<std::uint16_t>(slot_start);
    parent_header.tuple_count = static_cast<std::uint16_t>(parent_slots.size());

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto verify_page = [&](std::uint32_t page_id,
                           const std::vector<IndexBtreeSlotEntry>& expected_slots,
                           const std::vector<std::byte>& expected_payload,
                           std::uint32_t expected_right_sibling) {
        auto page = context.get_page(page_id);
        auto page_view = std::span<const std::byte>(page.data(), page.size());

        const auto& header = bored::storage::page_header(page_view);
        CHECK(header.page_id == page_id);
        CHECK(static_cast<bored::storage::PageType>(header.type) == bored::storage::PageType::Index);
        CHECK(header.tuple_count == expected_slots.size());
        CHECK(header.free_start == kIndexPayloadBase + expected_payload.size());
        const auto slot_bytes = expected_slots.size() * sizeof(IndexBtreeSlotEntry);
        CHECK(header.free_end == static_cast<std::uint16_t>(bored::storage::kPageSize - slot_bytes));

        const auto& index_header = *reinterpret_cast<const IndexBtreePageHeader*>(page_view.data() + kIndexHeaderOffset);
        CHECK(bored::storage::index_page_is_leaf(index_header));
        CHECK(index_header.level == split_header.level);
        CHECK(index_header.parent_page_id == split_header.parent_page_id);
        CHECK(index_header.right_sibling_page_id == expected_right_sibling);

        auto payload_view = page_view.subspan(kIndexPayloadBase, expected_payload.size());
        REQUIRE(payload_view.size() == expected_payload.size());
        CHECK(std::equal(payload_view.begin(), payload_view.end(), expected_payload.begin()));

        auto slot_view = std::span<const IndexBtreeSlotEntry>(reinterpret_cast<const IndexBtreeSlotEntry*>(page_view.data() + header.free_end), expected_slots.size());
        REQUIRE(slot_view.size() == expected_slots.size());
        for (std::size_t i = 0; i < expected_slots.size(); ++i) {
            CHECK(slot_view[i].key_offset == expected_slots[i].key_offset);
            CHECK(slot_view[i].key_length == expected_slots[i].key_length);
        }
    };

    verify_page(split_header.left_page_id, left_slots, left_payload, split_header.right_page_id);

    verify_page(split_header.right_page_id, right_slots, right_payload, split_header.right_sibling_page_id);

    {
        auto parent_view = context.get_page(split_header.parent_page_id);
        auto const_parent_view = std::span<const std::byte>(parent_view.data(), parent_view.size());

        const auto& post_header = bored::storage::page_header(const_parent_view);
        CHECK(post_header.page_id == split_header.parent_page_id);
        CHECK(post_header.tuple_count == 4U);
        CHECK(post_header.free_start > kIndexPayloadBase);

        const auto& post_index_header = *reinterpret_cast<const IndexBtreePageHeader*>(const_parent_view.data() + kIndexHeaderOffset);
        CHECK_FALSE(bored::storage::index_page_is_leaf(post_index_header));
        CHECK(post_index_header.level == split_header.level + 1U);

        const auto parent_slots_view = reinterpret_cast<const IndexBtreeSlotEntry*>(const_parent_view.data() + post_header.free_end);

        auto decode_pointer = [&](const IndexBtreeSlotEntry& slot) {
            IndexBtreeChildPointer child_ptr{};
            const auto pointer_offset = static_cast<std::size_t>(slot.key_offset) - sizeof(IndexBtreeChildPointer);
            std::memcpy(&child_ptr, const_parent_view.data() + pointer_offset, sizeof(IndexBtreeChildPointer));
            return child_ptr.page_id;
        };

        auto decode_key = [&](const IndexBtreeSlotEntry& slot) -> std::uint64_t {
            std::uint64_t value = 0U;
            if (slot.effective_length() != 0U) {
                std::memcpy(&value, const_parent_view.data() + slot.key_offset, sizeof(value));
            }
            return value;
        };

        std::ostringstream parent_dump;
        for (std::size_t i = 0; i < post_header.tuple_count; ++i) {
            parent_dump << "[" << i << ": ptr=" << decode_pointer(parent_slots_view[i])
                        << ", key=" << decode_key(parent_slots_view[i])
                        << ", inf=" << (parent_slots_view[i].infinite_key() ? "true" : "false") << "]";
        }
        INFO("parent entries: " << parent_dump.str());

        REQUIRE(post_header.tuple_count >= 4U);
        CHECK(decode_pointer(parent_slots_view[0]) == 4'900U);
        CHECK(decode_key(parent_slots_view[0]) == 10U);
        CHECK_FALSE(parent_slots_view[0].infinite_key());

        CHECK(decode_pointer(parent_slots_view[1]) == split_header.left_page_id);
        CHECK(decode_key(parent_slots_view[1]) == 30U);
        CHECK_FALSE(parent_slots_view[1].infinite_key());

        CHECK(decode_pointer(parent_slots_view[2]) == split_header.right_page_id);
        CHECK(decode_key(parent_slots_view[2]) == 45U);
        CHECK_FALSE(parent_slots_view[2].infinite_key());

        CHECK(decode_pointer(parent_slots_view[3]) == 5'500U);
        CHECK(parent_slots_view[3].infinite_key());
        CHECK(decode_key(parent_slots_view[3]) == 0U);
    }
}

TEST_CASE("WalReplayer rebuilds index root split parent page")
{
            using bored::storage::IndexBtreeSlotEntry;
            using bored::storage::IndexBtreeTuplePointer;
            using bored::storage::IndexBtreePageHeader;
            using bored::storage::IndexBtreeChildPointer;

            auto make_key_bytes = [](std::uint64_t value) {
                std::array<std::byte, sizeof(std::uint64_t)> bytes{};
                std::memcpy(bytes.data(), &value, sizeof(value));
                return bytes;
            };

            std::vector<IndexBtreeSlotEntry> left_slots;
            std::vector<std::byte> left_payload;

            auto append_leaf_entry = [&](std::vector<IndexBtreeSlotEntry>& slots,
                                         std::vector<std::byte>& payload,
                                         std::uint32_t heap_page,
                                         std::uint16_t heap_slot,
                                         std::uint64_t key_value) {
                IndexBtreeTuplePointer pointer{};
                pointer.heap_page_id = heap_page;
                pointer.heap_slot_id = heap_slot;

                const auto* pointer_bytes = reinterpret_cast<const std::byte*>(&pointer);
                payload.insert(payload.end(), pointer_bytes, pointer_bytes + sizeof(IndexBtreeTuplePointer));

                auto key_bytes = make_key_bytes(key_value);
                const auto key_offset = static_cast<std::uint16_t>(kIndexPayloadBase + payload.size());
                payload.insert(payload.end(), key_bytes.begin(), key_bytes.end());

                IndexBtreeSlotEntry slot{};
                slot.key_offset = key_offset;
                slot.key_length = static_cast<std::uint16_t>(key_bytes.size());
                slots.push_back(slot);
            };

            append_leaf_entry(left_slots, left_payload, 21'000U, 1U, 5U);
            append_leaf_entry(left_slots, left_payload, 21'000U, 2U, 15U);

            std::vector<IndexBtreeSlotEntry> right_slots;
            std::vector<std::byte> right_payload;

            append_leaf_entry(right_slots, right_payload, 21'001U, 1U, 25U);
            append_leaf_entry(right_slots, right_payload, 21'001U, 2U, 35U);

            auto pivot_bytes = make_key_bytes(25U);
            auto pivot_key = std::span<const std::byte>(pivot_bytes.data(), pivot_bytes.size());

            bored::storage::WalIndexSplitHeader split_header{};
            split_header.index_id = 9'901U;
            split_header.left_page_id = 6'500U;
            split_header.right_page_id = 6'501U;
            split_header.parent_page_id = 6'000U;
            split_header.level = 0U;
            split_header.flags = static_cast<std::uint16_t>(bored::storage::WalIndexSplitFlag::Leaf | bored::storage::WalIndexSplitFlag::Root);
            split_header.parent_insert_slot = 0U;

            const auto buffer_size = bored::storage::wal_index_split_payload_size(left_slots.size(),
                                                                                  left_payload.size(),
                                                                                  right_slots.size(),
                                                                                  right_payload.size(),
                                                                                  pivot_key.size());
            std::vector<std::byte> payload(buffer_size);
            REQUIRE(bored::storage::encode_wal_index_split(std::span<std::byte>(payload.data(), payload.size()),
                                                           split_header,
                                                           std::span<const IndexBtreeSlotEntry>(left_slots.data(), left_slots.size()),
                                                           std::span<const std::byte>(left_payload.data(), left_payload.size()),
                                                           std::span<const IndexBtreeSlotEntry>(right_slots.data(), right_slots.size()),
                                                           std::span<const std::byte>(right_payload.data(), right_payload.size()),
                                                           pivot_key));

            bored::storage::WalRecoveryRecord record{};
            record.header.type = static_cast<std::uint16_t>(bored::storage::WalRecordType::IndexSplit);
            record.header.lsn = 0xABCD'1001ULL;
            record.header.page_id = split_header.left_page_id;
            record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + payload.size());
            record.payload = payload;

            bored::storage::WalRecoveryPlan plan{};
            plan.redo.push_back(record);

            bored::storage::WalReplayContext context{bored::storage::PageType::Table, nullptr};
            bored::storage::WalReplayer replayer{context};

            REQUIRE_FALSE(replayer.apply_redo(plan));

            auto parent_page = context.get_page(split_header.parent_page_id);
            auto parent_view = std::span<const std::byte>(parent_page.data(), parent_page.size());

            const auto& header = bored::storage::page_header(parent_view);
            CHECK(header.tuple_count == 2U);

            const auto& index_header = *reinterpret_cast<const IndexBtreePageHeader*>(parent_view.data() + kIndexHeaderOffset);
            CHECK_FALSE(bored::storage::index_page_is_leaf(index_header));
            CHECK(index_header.level == split_header.level + 1U);

            const auto slot_ptr = reinterpret_cast<const IndexBtreeSlotEntry*>(parent_view.data() + header.free_end);

            auto decode_pointer = [&](const IndexBtreeSlotEntry& slot) {
                IndexBtreeChildPointer child_ptr{};
                const auto pointer_offset = static_cast<std::size_t>(slot.key_offset) - sizeof(IndexBtreeChildPointer);
                std::memcpy(&child_ptr, parent_view.data() + pointer_offset, sizeof(IndexBtreeChildPointer));
                return child_ptr.page_id;
            };

            CHECK(decode_pointer(slot_ptr[0]) == split_header.left_page_id);
            CHECK_FALSE(slot_ptr[0].infinite_key());

            std::uint64_t pivot_value = 0U;
            std::memcpy(&pivot_value, parent_view.data() + slot_ptr[0].key_offset, sizeof(pivot_value));
            CHECK(pivot_value == 25U);

    CHECK(decode_pointer(slot_ptr[1]) == split_header.right_page_id);
    CHECK(slot_ptr[1].infinite_key());
}

TEST_CASE("WalReplayer replays committed tuple changes")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_committed_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 4242U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    const auto disk_image_before = page_buffer;

    const std::array<std::byte, 5> tuple_insert{std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, tuple_insert, 1001U, insert_result));

    const std::array<std::byte, 7> tuple_update{std::byte{'u'}, std::byte{'p'}, std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'e'}, std::byte{'!'}};
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, tuple_update, 1001U, update_result));

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    const auto page_after = page_buffer;

    auto fsm_snapshot_path = wal_dir / "fsm.snapshot";
    REQUIRE_FALSE(FreeSpaceMapPersistence::write_snapshot(fsm, fsm_snapshot_path));

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.truncated_tail);
    REQUIRE(plan.redo.size() == 2U);

    {
        const auto& record = plan.redo.front();
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        auto meta = bored::storage::decode_wal_tuple_meta(payload);
        REQUIRE(meta);
    CAPTURE(meta->slot_index);
    CAPTURE(meta->tuple_length);
    }

    {
        const auto& record = plan.redo.back();
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        auto meta = bored::storage::decode_wal_tuple_update_meta(payload);
        REQUIRE(meta);
    CAPTURE(meta->base.slot_index);
    CAPTURE(meta->base.tuple_length);
    }

    REQUIRE(plan.redo.size() >= 2U);
    const auto update_record_lsn = plan.redo[1].header.lsn;
    const auto before_image_lsn = plan.redo[1].header.prev_lsn;
    REQUIRE(update_record_lsn != 0U);
    REQUIRE(before_image_lsn != 0U);

    FreeSpaceMap restored_fsm;
    REQUIRE_FALSE(FreeSpaceMapPersistence::load_snapshot(fsm_snapshot_path, restored_fsm));

    WalReplayContext context(PageType::Table, &restored_fsm);
    context.set_page(page_id, std::span<const std::byte>(disk_image_before.data(), disk_image_before.size()));

    WalReplayer replayer{context};

    WalRecoveryPlan insert_plan{};
    insert_plan.redo.push_back(plan.redo[0]);
    REQUIRE_FALSE(replayer.apply_redo(insert_plan));

    {
        auto page_after_insert = context.get_page(page_id);
        auto header = bored::storage::page_header(std::span<const std::byte>(page_after_insert.data(), page_after_insert.size()));
        CHECK(header.tuple_count == 1U);
        CHECK(header.fragment_count == 0U);
        CHECK(header.lsn == plan.redo[0].header.lsn);
    }

    WalRecoveryPlan update_plan{};
    update_plan.redo.push_back(plan.redo[1]);
    REQUIRE_FALSE(replayer.apply_redo(update_plan));

    auto repeat_ec = replayer.apply_redo(plan);
    if (repeat_ec) {
        FAIL("repeat_ec=" << repeat_ec.value() << " message=" << repeat_ec.message());
    }

    auto replayed_page = context.get_page(page_id);
    auto expected_page = std::span<const std::byte>(page_after.data(), page_after.size());

    REQUIRE(std::equal(replayed_page.begin(), replayed_page.end(), expected_page.begin(), expected_page.end()));

    CHECK(restored_fsm.current_free_bytes(page_id) == bored::storage::compute_free_bytes(bored::storage::page_header(expected_page)));
    CHECK(restored_fsm.current_fragment_count(page_id) == bored::storage::page_header(expected_page).fragment_count);

    auto expected_tuple_storage = bored::storage::read_tuple_storage(expected_page, 0U);
    REQUIRE_FALSE(expected_tuple_storage.empty());
    auto expected_tuple_header = decode_tuple_header(expected_tuple_storage);
    CHECK(expected_tuple_header.undo_next_lsn == before_image_lsn);

    auto replayed_tuple_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(replayed_page.data(), replayed_page.size()), 0U);
    REQUIRE_FALSE(replayed_tuple_storage.empty());
    auto replayed_tuple_header = decode_tuple_header(replayed_tuple_storage);
    CHECK(replayed_tuple_header.undo_next_lsn == before_image_lsn);

    std::filesystem::remove(fsm_snapshot_path);
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer replays overflow chunk records")
{
    FreeSpaceMap fsm;
    WalReplayContext context{PageType::Table, &fsm};
    WalReplayer replayer{context};

    bored::storage::WalOverflowChunkMeta chunk_meta{};
    chunk_meta.owner.page_id = 5123U;
    chunk_meta.owner.slot_index = 2U;
    chunk_meta.owner.tuple_length = 48U;
    chunk_meta.owner.row_id = 42U;
    chunk_meta.overflow_page_id = 9000U;
    chunk_meta.next_overflow_page_id = 0U;
    chunk_meta.chunk_offset = 0U;
    chunk_meta.chunk_length = 24U;
    chunk_meta.chunk_index = 0U;
    chunk_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainStart |
                                                  bored::storage::WalOverflowChunkFlag::ChainEnd);

    std::array<std::byte, 24> chunk_payload{};
    for (std::size_t index = 0; index < chunk_payload.size(); ++index) {
        chunk_payload[index] = static_cast<std::byte>(index);
    }

    const auto payload_size = bored::storage::wal_overflow_chunk_payload_size(chunk_payload.size());
    std::vector<std::byte> wal_payload(payload_size);
    auto payload_span = std::span<std::byte>(wal_payload.data(), wal_payload.size());
    REQUIRE(bored::storage::encode_wal_overflow_chunk(payload_span,
                                                      chunk_meta,
                                                      std::span<const std::byte>(chunk_payload.data(), chunk_payload.size())));

    WalRecoveryRecord chunk_record{};
    chunk_record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowChunk);
    chunk_record.header.lsn = 0xDEADBEEF;
    chunk_record.header.page_id = chunk_meta.overflow_page_id;
    chunk_record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + wal_payload.size());
    chunk_record.payload = wal_payload;

    WalRecoveryPlan redo_plan{};
    redo_plan.redo.push_back(chunk_record);
    REQUIRE_FALSE(replayer.apply_redo(redo_plan));

    auto page = context.get_page(chunk_meta.overflow_page_id);
    auto const_page = std::span<const std::byte>(page.data(), page.size());
    auto stored_meta = bored::storage::read_overflow_chunk_meta(const_page);
    REQUIRE(stored_meta);
    CHECK(stored_meta->owner.page_id == chunk_meta.owner.page_id);
    CHECK(stored_meta->chunk_length == chunk_meta.chunk_length);
    auto stored_data = bored::storage::overflow_chunk_payload(const_page, *stored_meta);
    REQUIRE(stored_data.size() == chunk_payload.size());
    REQUIRE(std::equal(stored_data.begin(), stored_data.end(), chunk_payload.begin(), chunk_payload.end()));
    CHECK(bored::storage::page_header(const_page).lsn == chunk_record.header.lsn);

    WalRecoveryPlan undo_plan{};
    undo_plan.undo.push_back(chunk_record);
    REQUIRE_FALSE(replayer.apply_undo(undo_plan));

    auto cleared_page = std::span<const std::byte>(page.data(), page.size());
    auto cleared_meta = bored::storage::read_overflow_chunk_meta(cleared_page);
    CHECK_FALSE(cleared_meta);
    CHECK(bored::storage::page_header(cleared_page).lsn == chunk_record.header.lsn);
}

TEST_CASE("WalReplayer truncates overflow chains")
{
    FreeSpaceMap fsm;
    WalReplayContext context{PageType::Table, &fsm};
    WalReplayer replayer{context};

    bored::storage::WalOverflowChunkMeta first_meta{};
    first_meta.owner.page_id = 7123U;
    first_meta.owner.slot_index = 5U;
    first_meta.owner.tuple_length = 64U;
    first_meta.owner.row_id = 84U;
    first_meta.overflow_page_id = 9100U;
    first_meta.next_overflow_page_id = 9101U;
    first_meta.chunk_offset = 0U;
    first_meta.chunk_length = 32U;
    first_meta.chunk_index = 0U;
    first_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainStart);

    bored::storage::WalOverflowChunkMeta second_meta{};
    second_meta.owner = first_meta.owner;
    second_meta.overflow_page_id = 9101U;
    second_meta.next_overflow_page_id = 0U;
    second_meta.chunk_offset = first_meta.chunk_length;
    second_meta.chunk_length = 16U;
    second_meta.chunk_index = 1U;
    second_meta.flags = static_cast<std::uint16_t>(bored::storage::WalOverflowChunkFlag::ChainEnd);

    std::array<std::byte, 32> first_payload{};
    for (std::size_t index = 0; index < first_payload.size(); ++index) {
        first_payload[index] = static_cast<std::byte>(0xA0 + index);
    }

    std::array<std::byte, 16> second_payload{};
    for (std::size_t index = 0; index < second_payload.size(); ++index) {
        second_payload[index] = static_cast<std::byte>(0xF0 + index);
    }

    auto make_chunk_record = [](const bored::storage::WalOverflowChunkMeta& meta,
                                std::span<const std::byte> data,
                                std::uint64_t lsn) {
        const auto payload_size = bored::storage::wal_overflow_chunk_payload_size(meta.chunk_length);
        std::vector<std::byte> wal_payload(payload_size);
        auto payload_span = std::span<std::byte>(wal_payload.data(), wal_payload.size());
        REQUIRE(bored::storage::encode_wal_overflow_chunk(payload_span, meta, data));

        WalRecoveryRecord record{};
        record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowChunk);
        record.header.lsn = lsn;
        record.header.page_id = meta.overflow_page_id;
        record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + wal_payload.size());
        record.payload = std::move(wal_payload);
        return record;
    };

    auto first_chunk_record = make_chunk_record(first_meta, std::span<const std::byte>(first_payload.data(), first_payload.size()), 0x1000);
    auto second_chunk_record = make_chunk_record(second_meta, std::span<const std::byte>(second_payload.data(), second_payload.size()), 0x1100);

    WalRecoveryPlan chunk_plan{};
    chunk_plan.redo.push_back(first_chunk_record);
    chunk_plan.redo.push_back(second_chunk_record);
    REQUIRE_FALSE(replayer.apply_redo(chunk_plan));

    auto first_page = context.get_page(first_meta.overflow_page_id);
    auto second_page = context.get_page(second_meta.overflow_page_id);
    REQUIRE(bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(first_page.data(), first_page.size())));
    REQUIRE(bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(second_page.data(), second_page.size())));

    bored::storage::WalOverflowTruncateMeta truncate_meta{};
    truncate_meta.owner = first_meta.owner;
    truncate_meta.first_overflow_page_id = first_meta.overflow_page_id;
    truncate_meta.released_page_count = 2U;

    std::vector<bored::storage::WalOverflowChunkMeta> truncate_chunk_metas{first_meta, second_meta};
    std::vector<std::span<const std::byte>> truncate_payload_views{
        std::span<const std::byte>(first_payload.data(), first_payload.size()),
        std::span<const std::byte>(second_payload.data(), second_payload.size())};

    const auto truncate_payload_size = bored::storage::wal_overflow_truncate_payload_size(
        std::span<const bored::storage::WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size()));
    std::vector<std::byte> truncate_payload(truncate_payload_size);
    REQUIRE(bored::storage::encode_wal_overflow_truncate(std::span<std::byte>(truncate_payload.data(), truncate_payload.size()),
                                                         truncate_meta,
                                                         std::span<const bored::storage::WalOverflowChunkMeta>(truncate_chunk_metas.data(), truncate_chunk_metas.size()),
                                                         std::span<const std::span<const std::byte>>(truncate_payload_views.data(), truncate_payload_views.size())));

    WalRecoveryRecord truncate_record{};
    truncate_record.header.type = static_cast<std::uint16_t>(WalRecordType::TupleOverflowTruncate);
    truncate_record.header.lsn = 0x1200;
    truncate_record.header.page_id = truncate_meta.first_overflow_page_id;
    truncate_record.header.total_length = static_cast<std::uint32_t>(sizeof(bored::storage::WalRecordHeader) + truncate_payload.size());
    truncate_record.payload = truncate_payload;

    WalRecoveryPlan truncate_plan{};
    truncate_plan.redo.push_back(truncate_record);
    REQUIRE_FALSE(replayer.apply_redo(truncate_plan));

    auto first_page_after = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_after = std::span<const std::byte>(second_page.data(), second_page.size());
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(first_page_after));
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(second_page_after));
    CHECK(bored::storage::page_header(first_page_after).lsn == truncate_record.header.lsn);
    CHECK(bored::storage::page_header(second_page_after).lsn == truncate_record.header.lsn);

    REQUIRE_FALSE(replayer.apply_redo(truncate_plan));
    auto first_page_reapplied = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_reapplied = std::span<const std::byte>(second_page.data(), second_page.size());
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(first_page_reapplied));
    CHECK_FALSE(bored::storage::read_overflow_chunk_meta(second_page_reapplied));
    CHECK(bored::storage::page_header(first_page_reapplied).lsn == truncate_record.header.lsn);
    CHECK(bored::storage::page_header(second_page_reapplied).lsn == truncate_record.header.lsn);

    WalRecoveryPlan undo_plan{};
    undo_plan.undo.push_back(truncate_record);
    REQUIRE_FALSE(replayer.apply_undo(undo_plan));

    auto first_page_restored = std::span<const std::byte>(first_page.data(), first_page.size());
    auto second_page_restored = std::span<const std::byte>(second_page.data(), second_page.size());
    auto restored_first_meta = bored::storage::read_overflow_chunk_meta(first_page_restored);
    auto restored_second_meta = bored::storage::read_overflow_chunk_meta(second_page_restored);
    REQUIRE(restored_first_meta);
    REQUIRE(restored_second_meta);
    CHECK(restored_first_meta->chunk_length == first_meta.chunk_length);
    CHECK(restored_second_meta->chunk_length == second_meta.chunk_length);
    auto restored_first_payload = bored::storage::overflow_chunk_payload(first_page_restored, *restored_first_meta);
    auto restored_second_payload = bored::storage::overflow_chunk_payload(second_page_restored, *restored_second_meta);
    REQUIRE(restored_first_payload.size() == first_payload.size());
    REQUIRE(restored_second_payload.size() == second_payload.size());
    REQUIRE(std::equal(restored_first_payload.begin(), restored_first_payload.end(), first_payload.begin(), first_payload.end()));
    REQUIRE(std::equal(restored_second_payload.begin(), restored_second_payload.end(), second_payload.begin(), second_payload.end()));
}

TEST_CASE("WalReplayer undoes uncommitted update using before image")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_undo_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 7777U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::array<std::byte, 24> insert_payload{};
    insert_payload.fill(std::byte{0x55});
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, insert_payload, 9001U, insert_result));

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    const auto baseline_page = page_buffer;
    const auto baseline_free_bytes = fsm.current_free_bytes(page_id);

    std::array<std::byte, 32> updated_payload{};
    updated_payload.fill(std::byte{0xA7});
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, updated_payload, 9001U, update_result));

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.redo.size() == 1U);  // committed insert only
    REQUIRE(plan.undo.size() == 2U);
    REQUIRE(static_cast<WalRecordType>(plan.undo[0].header.type) == WalRecordType::TupleUpdate);
    REQUIRE(static_cast<WalRecordType>(plan.undo[1].header.type) == WalRecordType::TupleBeforeImage);

    FreeSpaceMap restored_fsm;
    WalReplayContext context{PageType::Table, &restored_fsm};
    context.set_page(page_id, std::span<const std::byte>(baseline_page.data(), baseline_page.size()));

    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    CAPTURE(replayer.last_undo_type());
    REQUIRE_FALSE(undo_error);

    auto replayed_page = context.get_page(page_id);
    const auto replayed_header = bored::storage::page_header(
        std::span<const std::byte>(replayed_page.data(), replayed_page.size()));
    REQUIRE(replayed_header.page_id == page_id);
    REQUIRE(replayed_header.tuple_count == 1U);

    auto restored_tuple = bored::storage::read_tuple(std::span<const std::byte>(replayed_page.data(), replayed_page.size()), insert_result.slot.index);
    REQUIRE(restored_tuple.size() == insert_payload.size());
    REQUIRE(std::equal(restored_tuple.begin(), restored_tuple.end(), insert_payload.begin()));

    CHECK(restored_fsm.current_free_bytes(page_id) == baseline_free_bytes);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer undoes overflow delete using before-image chunks")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_overflow_before_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 40404U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> payload(8192U);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>((index * 7U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 8080U, insert_result));
    REQUIRE(insert_result.used_overflow);

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 8080U, delete_result));

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    auto baseline_page = page_buffer;

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.undo_spans.size() == 1U);
    WalUndoWalker span_walker{plan};
    auto span_item = span_walker.next();
    REQUIRE(span_item);
    CHECK(span_item->owner_page_id == page_id);
    CHECK_FALSE(span_walker.next());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());
    INFO("before_image_prev_lsn=" << before_it->header.prev_lsn << " lsn=" << before_it->header.lsn);

    auto before_view = bored::storage::decode_wal_tuple_before_image(std::span<const std::byte>(before_it->payload.data(), before_it->payload.size()));
    REQUIRE(before_view);
    REQUIRE_FALSE(before_view->overflow_chunks.empty());
    REQUIRE(before_view->tuple_payload.size() == before_view->meta.tuple_length);
    CAPTURE(before_view->meta.slot_index);
    CAPTURE(before_view->meta.tuple_length);

    std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas;
    std::vector<std::vector<std::byte>> expected_chunk_payloads;
    expected_chunk_metas.reserve(before_view->overflow_chunks.size());
    expected_chunk_payloads.reserve(before_view->overflow_chunks.size());
    for (const auto& chunk_view : before_view->overflow_chunks) {
        expected_chunk_metas.push_back(chunk_view.meta);
        expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
    }

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(baseline_page.data(), baseline_page.size()));
    WalReplayer replayer{context};

    std::vector<int> redo_types;
    redo_types.reserve(plan.redo.size());
    for (const auto& record : plan.redo) {
        redo_types.push_back(static_cast<int>(record.header.type));
    }
    CAPTURE(redo_types);

    auto delete_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleDelete;
    });
    if (delete_it != plan.undo.end()) {
        auto delete_meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(delete_it->payload.data(), delete_it->payload.size()));
        CAPTURE(delete_meta ? delete_meta->slot_index : static_cast<std::uint16_t>(std::numeric_limits<std::uint16_t>::max()));
    }

    std::vector<int> undo_types;
    undo_types.reserve(plan.undo.size());
    std::vector<std::uint32_t> undo_page_ids;
    undo_page_ids.reserve(plan.undo.size());
    std::vector<std::uint16_t> undo_slot_indices;
    undo_slot_indices.reserve(plan.undo.size());
    for (const auto& record : plan.undo) {
        undo_types.push_back(static_cast<int>(record.header.type));
        undo_page_ids.push_back(record.header.page_id);
        auto type = static_cast<WalRecordType>(record.header.type);
        if (type == WalRecordType::TupleInsert || type == WalRecordType::TupleDelete) {
            auto meta = bored::storage::decode_wal_tuple_meta(std::span<const std::byte>(record.payload.data(), record.payload.size()));
            undo_slot_indices.push_back(meta ? meta->slot_index : std::numeric_limits<std::uint16_t>::max());
        } else {
            undo_slot_indices.push_back(std::numeric_limits<std::uint16_t>::max());
        }
    }
    CAPTURE(undo_types);
    CAPTURE(undo_page_ids);
    CAPTURE(undo_slot_indices);

    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    auto last_type = replayer.last_undo_type();
    CAPTURE(last_type.has_value());
    const int last_type_value = last_type ? static_cast<int>(*last_type) : -1;
    CAPTURE(last_type_value);
    REQUIRE_FALSE(undo_error);

    for (std::size_t index = 0; index < expected_chunk_metas.size(); ++index) {
        const auto& expected_meta = expected_chunk_metas[index];
        const auto& expected_payload = expected_chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto restored_meta = bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(page.data(), page.size()));
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(std::span<const std::byte>(page.data(), page.size()), *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
    }

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer undo overflow insert removes stub")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_overflow_insert_undo_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 50505U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> payload(8192U);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>((index * 11U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 0xDEADULL, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    const auto crash_snapshot = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    CAPTURE(plan.redo.size());
    CAPTURE(plan.undo.size());
    REQUIRE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_snapshot.data(), crash_snapshot.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto page_after = context.get_page(page_id);
    auto page_after_const = std::span<const std::byte>(page_after.data(), page_after.size());
    auto tuple_after = bored::storage::read_tuple(page_after_const, insert_result.slot.index);
    CAPTURE(tuple_after.size());
    CAPTURE(insert_result.inline_length);
    CAPTURE(insert_result.slot.index);

    REQUIRE(tuple_after.empty());

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill restores overflow before image")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_crash_drill_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 70707U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> initial_payload(12160U);
    for (std::size_t index = 0; index < initial_payload.size(); ++index) {
        initial_payload[index] = static_cast<std::byte>((index * 5U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, initial_payload, 0xBEEFULL, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE(insert_result.overflow_page_ids.size() >= 2U);

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    const auto baseline_page = page_buffer;
    const auto baseline_free_bytes = fsm.current_free_bytes(page_id);

    std::array<std::byte, 160> inline_payload{};
    inline_payload.fill(std::byte{0xDA});
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, inline_payload, 0xBEEFULL, update_result));

    const auto crash_page_image = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    CAPTURE(plan.redo.size());
    CAPTURE(plan.undo.size());
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    WalUndoWalker walker{plan};
    auto work_item = walker.next();
    REQUIRE(work_item);
    CHECK(work_item->owner_page_id == page_id);

    auto walker_pages = work_item->overflow_page_ids;
    std::sort(walker_pages.begin(), walker_pages.end());
    walker_pages.erase(std::unique(walker_pages.begin(), walker_pages.end()), walker_pages.end());

    std::vector<std::uint32_t> expected_touch;
    expected_touch.reserve(insert_result.overflow_page_ids.size() * 2U);
    expected_touch.insert(expected_touch.end(), insert_result.overflow_page_ids.begin(), insert_result.overflow_page_ids.end());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    auto before_view = bored::storage::decode_wal_tuple_before_image(std::span<const std::byte>(before_it->payload.data(), before_it->payload.size()));
    REQUIRE(before_view);
    REQUIRE(before_view->meta.page_id == page_id);
    REQUIRE_FALSE(before_view->overflow_chunks.empty());

    auto expected_tuple_payload = tuple_payload_vector(before_view->tuple_payload, before_view->meta.tuple_length);
    std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas;
    expected_chunk_metas.reserve(before_view->overflow_chunks.size());
    std::vector<std::vector<std::byte>> expected_chunk_payloads;
    expected_chunk_payloads.reserve(before_view->overflow_chunks.size());
    for (const auto& chunk_view : before_view->overflow_chunks) {
        expected_chunk_metas.push_back(chunk_view.meta);
        expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
        expected_touch.push_back(chunk_view.meta.overflow_page_id);
        if (chunk_view.meta.next_overflow_page_id != 0U) {
            expected_touch.push_back(chunk_view.meta.next_overflow_page_id);
        }
    }

    std::sort(expected_touch.begin(), expected_touch.end());
    expected_touch.erase(std::unique(expected_touch.begin(), expected_touch.end()), expected_touch.end());

    for (auto page : expected_touch) {
        CHECK(std::binary_search(walker_pages.begin(), walker_pages.end(), page));
    }
    CHECK_FALSE(walker.next());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page_image.data(), crash_page_image.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    REQUIRE_FALSE(undo_error);

    auto restored_page = context.get_page(page_id);
    auto restored_tuple = bored::storage::read_tuple(std::span<const std::byte>(restored_page.data(), restored_page.size()), insert_result.slot.index);
    REQUIRE(restored_tuple.size() == expected_tuple_payload.size());
    REQUIRE(std::equal(restored_tuple.begin(), restored_tuple.end(), expected_tuple_payload.begin(), expected_tuple_payload.end()));
    CHECK(replay_fsm.current_free_bytes(page_id) == baseline_free_bytes);

    for (std::size_t index = 0; index < expected_chunk_metas.size(); ++index) {
        const auto& expected_meta = expected_chunk_metas[index];
        const auto& expected_payload = expected_chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto restored_meta = bored::storage::read_overflow_chunk_meta(std::span<const std::byte>(page.data(), page.size()));
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(std::span<const std::byte>(page.data(), page.size()), *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
    }

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill undo walker enumerates overflow chains")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_undo_walker_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    struct ScenarioData final {
        std::uint32_t page_id = 0U;
        std::array<std::byte, bored::storage::kPageSize> crash_page{};
        std::vector<std::uint32_t> overflow_pages{};
        std::vector<std::byte> initial_payload{};
        std::uint16_t slot_index = 0U;
        std::uint64_t baseline_free_bytes = 0U;
        std::vector<std::byte> expected_inline_payload{};
        std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas{};
        std::vector<std::vector<std::byte>> expected_chunk_payloads{};
    };

    const std::array<std::uint32_t, 2U> page_ids{81818U, 82828U};
    std::vector<ScenarioData> scenarios;
    scenarios.reserve(page_ids.size());

    for (std::size_t index = 0; index < page_ids.size(); ++index) {
        const auto page_id = page_ids[index];

        alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
        auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
        REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

        ScenarioData scenario{};
        scenario.page_id = page_id;

        const std::size_t payload_length = 12032U + index * 256U;
        scenario.initial_payload.resize(payload_length);
        for (std::size_t byte_index = 0; byte_index < scenario.initial_payload.size(); ++byte_index) {
            const auto pattern = static_cast<unsigned int>((byte_index * (index + 3U)) & 0xFFU);
            scenario.initial_payload[byte_index] = static_cast<std::byte>(pattern);
        }

        PageManager::TupleInsertResult insert_result{};
        REQUIRE_FALSE(manager.insert_tuple(page_span,
                                           scenario.initial_payload,
                                           0xD00FULL + static_cast<std::uint64_t>(index),
                                           insert_result));
        REQUIRE(insert_result.used_overflow);
        REQUIRE(insert_result.overflow_page_ids.size() >= 2U);

        auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
        bored::storage::WalAppendResult commit_result{};
        REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

        scenario.slot_index = insert_result.slot.index;
        scenario.overflow_pages = insert_result.overflow_page_ids;
        scenario.baseline_free_bytes = fsm.current_free_bytes(page_id);

        std::array<std::byte, 320U> final_payload{};
        const auto fill_value = static_cast<unsigned char>(0x30 + static_cast<unsigned int>(index));
        final_payload.fill(std::byte{fill_value});

        PageManager::TupleUpdateResult update_result{};
        REQUIRE_FALSE(manager.update_tuple(page_span,
                                           insert_result.slot.index,
                                           std::span<const std::byte>(final_payload.data(), final_payload.size()),
                                           0xBADF00DULL + static_cast<std::uint64_t>(index),
                                           update_result));
        CHECK_FALSE(update_result.used_overflow);

        scenario.crash_page = page_buffer;

        scenarios.emplace_back(std::move(scenario));
    }

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.undo_spans.size() >= scenarios.size());
    WalUndoWalker walker{plan};

    std::size_t matched_spans = 0U;

    while (auto work_item = walker.next()) {
        auto scenario_it = std::find_if(scenarios.begin(), scenarios.end(), [&](const ScenarioData& data) {
            return data.page_id == work_item->owner_page_id;
        });

        if (scenario_it == scenarios.end()) {
            CHECK(work_item->overflow_page_ids.empty());
            continue;
        }

        ++matched_spans;

        auto& scenario = *scenario_it;

        auto actual_pages = work_item->overflow_page_ids;
        std::sort(actual_pages.begin(), actual_pages.end());
        actual_pages.erase(std::unique(actual_pages.begin(), actual_pages.end()), actual_pages.end());

        auto expected_pages = scenario.overflow_pages;
        std::sort(expected_pages.begin(), expected_pages.end());
        expected_pages.erase(std::unique(expected_pages.begin(), expected_pages.end()), expected_pages.end());

        CHECK(actual_pages == expected_pages);

        auto before_it = std::find_if(work_item->records.begin(),
                                      work_item->records.end(),
                                      [](const WalRecoveryRecord& record) {
                                          return static_cast<WalRecordType>(record.header.type) ==
                                                 WalRecordType::TupleBeforeImage;
                                      });
        REQUIRE(before_it != work_item->records.end());

        auto before_view = bored::storage::decode_wal_tuple_before_image(
            std::span<const std::byte>(before_it->payload.data(), before_it->payload.size()));
        REQUIRE(before_view);

        auto tuple_storage_span = std::span<const std::byte>(before_view->tuple_payload.data(),
                                                             before_view->tuple_payload.size());
        REQUIRE(tuple_storage_span.size() == before_view->meta.tuple_length);

        auto stub_span = tuple_storage_span.subspan(bored::storage::tuple_header_size());
        scenario.expected_inline_payload.assign(stub_span.begin(), stub_span.end());

        auto overflow_header = bored::storage::parse_overflow_tuple(stub_span);
        REQUIRE(overflow_header);
        REQUIRE(overflow_header->logical_length == scenario.initial_payload.size());

        auto inline_span = bored::storage::overflow_tuple_inline_payload(stub_span, *overflow_header);

        std::vector<std::byte> reconstructed_payload(overflow_header->logical_length);
        if (!inline_span.empty()) {
            std::copy(inline_span.begin(), inline_span.end(), reconstructed_payload.begin());
        }

        scenario.expected_chunk_metas.clear();
        scenario.expected_chunk_payloads.clear();
        scenario.expected_chunk_metas.reserve(before_view->overflow_chunks.size());
        scenario.expected_chunk_payloads.reserve(before_view->overflow_chunks.size());

        for (const auto& chunk_view : before_view->overflow_chunks) {
            scenario.expected_chunk_metas.push_back(chunk_view.meta);
            scenario.expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());

            REQUIRE(chunk_view.meta.chunk_offset + chunk_view.meta.chunk_length <= reconstructed_payload.size());
            std::copy(chunk_view.payload.begin(),
                      chunk_view.payload.end(),
                      reconstructed_payload.begin() + chunk_view.meta.chunk_offset);
        }

        if (reconstructed_payload != scenario.initial_payload) {
            CAPTURE(overflow_header->inline_length);
            CAPTURE(before_view->overflow_chunks.size());
            if (!before_view->overflow_chunks.empty()) {
                CAPTURE(before_view->overflow_chunks.front().meta.chunk_offset);
                CAPTURE(before_view->overflow_chunks.front().meta.chunk_length);
            }
            std::vector<std::uint16_t> prefix;
            const auto sample = std::min<std::size_t>(reconstructed_payload.size(), 32U);
            prefix.reserve(sample);
            for (std::size_t i = 0; i < sample; ++i) {
                prefix.push_back(static_cast<std::uint16_t>(reconstructed_payload[i]));
            }
            CAPTURE(prefix);
            CAPTURE(reconstructed_payload.size());
            CAPTURE(scenario.initial_payload.size());
            const auto mismatch = std::mismatch(reconstructed_payload.begin(),
                                                reconstructed_payload.end(),
                                                scenario.initial_payload.begin(),
                                                scenario.initial_payload.end());
            if (mismatch.first != reconstructed_payload.end() &&
                mismatch.second != scenario.initial_payload.end()) {
                CAPTURE(static_cast<std::uint16_t>(*mismatch.first));
                CAPTURE(static_cast<std::uint16_t>(*mismatch.second));
                CAPTURE(static_cast<std::uint32_t>(std::distance(reconstructed_payload.begin(), mismatch.first)));
            }
        }
        const bool size_matches = reconstructed_payload.size() == scenario.initial_payload.size();
        CHECK(size_matches);
        if (size_matches) {
            const auto mismatch = std::mismatch(reconstructed_payload.begin(),
                                                reconstructed_payload.end(),
                                                scenario.initial_payload.begin());
            CHECK(mismatch.first == reconstructed_payload.end());
        }
    }

    CHECK(matched_spans == scenarios.size());
    CHECK_FALSE(walker.next());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    for (const auto& scenario : scenarios) {
        context.set_page(scenario.page_id,
                         std::span<const std::byte>(scenario.crash_page.data(), scenario.crash_page.size()));
    }

    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    REQUIRE_FALSE(undo_error);

    for (const auto& scenario : scenarios) {
        auto page_span = context.get_page(scenario.page_id);
        auto tuple_span = bored::storage::read_tuple(
            std::span<const std::byte>(page_span.data(), page_span.size()),
            scenario.slot_index);
        std::vector<std::byte> restored(tuple_span.begin(), tuple_span.end());
        CHECK(restored == scenario.expected_inline_payload);
        CHECK(replay_fsm.current_free_bytes(scenario.page_id) == scenario.baseline_free_bytes);

        for (std::size_t index = 0; index < scenario.expected_chunk_metas.size(); ++index) {
            const auto& expected_meta = scenario.expected_chunk_metas[index];
            const auto& expected_payload = scenario.expected_chunk_payloads[index];
            auto overflow_page = context.get_page(expected_meta.overflow_page_id);
            auto restored_meta = bored::storage::read_overflow_chunk_meta(
                std::span<const std::byte>(overflow_page.data(), overflow_page.size()));
            REQUIRE(restored_meta);
            CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
            CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
            CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
            CHECK(restored_meta->chunk_length == expected_meta.chunk_length);

            auto restored_payload = bored::storage::overflow_chunk_payload(
                std::span<const std::byte>(overflow_page.data(), overflow_page.size()),
                *restored_meta);
            REQUIRE(restored_payload.size() == expected_payload.size());
            CHECK(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
        }
    }

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill unwinds layered overflow updates")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_crash_drill_layered_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 74747U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> initial_payload(12160U);
    for (std::size_t index = 0; index < initial_payload.size(); ++index) {
        initial_payload[index] = static_cast<std::byte>((index * 9U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, initial_payload, 0xA11CEULL, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE(insert_result.overflow_page_ids.size() >= 2U);

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    const auto baseline_page = page_buffer;
    const auto baseline_free_bytes = fsm.current_free_bytes(page_id);

    std::vector<std::byte> medium_payload(4096U);
    for (std::size_t index = 0; index < medium_payload.size(); ++index) {
        medium_payload[index] = static_cast<std::byte>((index * 5U) & 0xFFU);
    }

    PageManager::TupleUpdateResult first_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, medium_payload, 0xA11CEULL, first_update));
    CHECK_FALSE(first_update.used_overflow);

    std::array<std::byte, 192U> final_payload{};
    final_payload.fill(std::byte{0x42});

    PageManager::TupleUpdateResult second_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, std::span<const std::byte>(final_payload.data(), final_payload.size()), 0xA11CEULL, second_update));
    CHECK_FALSE(second_update.used_overflow);

    const auto crash_page_image = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.redo.size() >= 3U);
    CHECK(static_cast<WalRecordType>(plan.redo.front().header.type) == WalRecordType::TupleInsert);
    REQUIRE(plan.undo.size() == 5U);
    REQUIRE(plan.undo_spans.size() == 1U);
    const auto& span = plan.undo_spans.front();
    CHECK(span.owner_page_id == page_id);
    CHECK(span.count == plan.undo.size());

    std::vector<WalRecordType> undo_types;
    undo_types.reserve(plan.undo.size());
    for (const auto& record : plan.undo) {
        undo_types.push_back(static_cast<WalRecordType>(record.header.type));
    }

    const std::vector<WalRecordType> expected_types{
        WalRecordType::TupleUpdate,
        WalRecordType::TupleBeforeImage,
        WalRecordType::TupleUpdate,
        WalRecordType::TupleOverflowTruncate,
        WalRecordType::TupleBeforeImage
    };
    CHECK(undo_types == expected_types);

    WalUndoWalker walker{plan};
    auto work_item = walker.next();
    REQUIRE(work_item);
    CHECK(work_item->owner_page_id == page_id);
    CHECK(work_item->records.size() == plan.undo.size());

    std::optional<bored::storage::WalTupleBeforeImageView> baseline_before;
    for (const auto& record : work_item->records) {
        const auto type = static_cast<WalRecordType>(record.header.type);
        if (type != WalRecordType::TupleBeforeImage) {
            continue;
        }
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        auto before_view = bored::storage::decode_wal_tuple_before_image(payload);
        REQUIRE(before_view);
        if (!before_view->overflow_chunks.empty()) {
            baseline_before = before_view;
        }
    }

    REQUIRE(baseline_before.has_value());

    std::vector<std::uint32_t> expected_pages;
    expected_pages.reserve(baseline_before->overflow_chunks.size() * 2U);
    for (const auto& chunk_view : baseline_before->overflow_chunks) {
        if (chunk_view.meta.overflow_page_id != 0U) {
            expected_pages.push_back(chunk_view.meta.overflow_page_id);
        }
        if (chunk_view.meta.next_overflow_page_id != 0U) {
            expected_pages.push_back(chunk_view.meta.next_overflow_page_id);
        }
    }
    std::sort(expected_pages.begin(), expected_pages.end());
    expected_pages.erase(std::unique(expected_pages.begin(), expected_pages.end()), expected_pages.end());

    auto walker_pages = work_item->overflow_page_ids;
    std::sort(walker_pages.begin(), walker_pages.end());
    walker_pages.erase(std::unique(walker_pages.begin(), walker_pages.end()), walker_pages.end());
    CHECK(walker_pages == expected_pages);

    std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas;
    expected_chunk_metas.reserve(baseline_before->overflow_chunks.size());
    std::vector<std::vector<std::byte>> expected_chunk_payloads;
    expected_chunk_payloads.reserve(baseline_before->overflow_chunks.size());
    for (const auto& chunk_view : baseline_before->overflow_chunks) {
        expected_chunk_metas.push_back(chunk_view.meta);
        expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
    }

    auto expected_tuple_payload = tuple_payload_vector(baseline_before->tuple_payload, baseline_before->meta.tuple_length);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page_image.data(), crash_page_image.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    CAPTURE(replayer.last_undo_type());
    REQUIRE_FALSE(undo_error);

    auto restored_page = context.get_page(page_id);
    auto restored_tuple = bored::storage::read_tuple(
        std::span<const std::byte>(restored_page.data(), restored_page.size()),
        insert_result.slot.index);
    REQUIRE(restored_tuple.size() == expected_tuple_payload.size());
    REQUIRE(std::equal(restored_tuple.begin(), restored_tuple.end(), expected_tuple_payload.begin(), expected_tuple_payload.end()));
    CHECK(replay_fsm.current_free_bytes(page_id) == baseline_free_bytes);

    for (std::size_t index = 0; index < expected_chunk_metas.size(); ++index) {
        const auto& expected_meta = expected_chunk_metas[index];
        const auto& expected_payload = expected_chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto page_view = std::span<const std::byte>(page.data(), page.size());
        auto restored_meta = bored::storage::read_overflow_chunk_meta(page_view);
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(page_view, *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
    }

    CHECK_FALSE(walker.next());

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill restores multi-page overflow spans")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_crash_drill_multi_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id_a = 81818U;
    constexpr std::uint32_t page_id_b = 92929U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_a_buffer{};
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_b_buffer{};
    auto page_a_span = std::span<std::byte>(page_a_buffer.data(), page_a_buffer.size());
    auto page_b_span = std::span<std::byte>(page_b_buffer.data(), page_b_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_a_span, PageType::Table, page_id_a));
    REQUIRE_FALSE(manager.initialize_page(page_b_span, PageType::Table, page_id_b));

    std::vector<std::byte> payload_a(12160U);
    for (std::size_t index = 0; index < payload_a.size(); ++index) {
        payload_a[index] = static_cast<std::byte>((index * 3U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_a{};
    REQUIRE_FALSE(manager.insert_tuple(page_a_span, payload_a, 0xCAFEULL, insert_a));
    REQUIRE(insert_a.used_overflow);
    REQUIRE(insert_a.overflow_page_ids.size() >= 2U);

    std::vector<std::byte> payload_b(9216U);
    for (std::size_t index = 0; index < payload_b.size(); ++index) {
        payload_b[index] = static_cast<std::byte>((index * 7U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_b{};
    REQUIRE_FALSE(manager.insert_tuple(page_b_span, payload_b, 0xFADEULL, insert_b));
    REQUIRE(insert_b.used_overflow);
    REQUIRE(insert_b.overflow_page_ids.size() >= 2U);

    auto commit_header_a = make_commit_header(wal_writer, page_id_a, page_id_a + 1U, page_id_a);
    bored::storage::WalAppendResult commit_result_a{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header_a, commit_result_a));

    auto commit_header_b = make_commit_header(wal_writer, page_id_b, page_id_b + 1U, page_id_b);
    bored::storage::WalAppendResult commit_result_b{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header_b, commit_result_b));

    const auto baseline_page_a = page_a_buffer;
    const auto baseline_page_b = page_b_buffer;
    const auto baseline_free_bytes_a = fsm.current_free_bytes(page_id_a);
    const auto baseline_free_bytes_b = fsm.current_free_bytes(page_id_b);
    const auto baseline_fragment_count_a = fsm.current_fragment_count(page_id_a);
    const auto baseline_fragment_count_b = fsm.current_fragment_count(page_id_b);
    auto baseline_header_a = bored::storage::page_header(std::span<const std::byte>(baseline_page_a.data(), baseline_page_a.size()));
    auto baseline_header_b = bored::storage::page_header(std::span<const std::byte>(baseline_page_b.data(), baseline_page_b.size()));

    std::array<std::byte, 192> shrink_payload{};
    shrink_payload.fill(std::byte{0x42});
    PageManager::TupleUpdateResult update_a{};
    REQUIRE_FALSE(manager.update_tuple(page_a_span, insert_a.slot.index, shrink_payload, 0xCAFEULL, update_a));

    PageManager::TupleDeleteResult delete_b{};
    REQUIRE_FALSE(manager.delete_tuple(page_b_span, insert_b.slot.index, 0xFADEULL, delete_b));

    const auto crash_page_a = page_a_buffer;
    const auto crash_page_b = page_b_buffer;
    const auto crash_free_bytes_a = fsm.current_free_bytes(page_id_a);
    const auto crash_free_bytes_b = fsm.current_free_bytes(page_id_b);
    const auto crash_fragment_count_a = fsm.current_fragment_count(page_id_a);
    const auto crash_fragment_count_b = fsm.current_fragment_count(page_id_b);
    auto crash_header_a = bored::storage::page_header(std::span<const std::byte>(crash_page_a.data(), crash_page_a.size()));
    auto crash_header_b = bored::storage::page_header(std::span<const std::byte>(crash_page_b.data(), crash_page_b.size()));

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    CAPTURE(plan.redo.size());
    CAPTURE(plan.undo.size());
    REQUIRE(plan.redo.size() >= 2U);
    REQUIRE(plan.undo.size() >= 4U);

    WalUndoWalker walker{plan};
    struct ExpectedSpan final {
        std::uint32_t owner_page_id = 0U;
        std::uint16_t slot_index = std::numeric_limits<std::uint16_t>::max();
        std::vector<std::byte> tuple_payload{};
        std::vector<bored::storage::WalOverflowChunkMeta> chunk_metas{};
        std::vector<std::vector<std::byte>> chunk_payloads{};
        std::vector<std::uint32_t> walker_pages{};
    };

    std::vector<ExpectedSpan> spans;
    while (auto work_item = walker.next()) {
        ExpectedSpan span{};
        span.owner_page_id = work_item->owner_page_id;
        span.walker_pages = work_item->overflow_page_ids;
        std::sort(span.walker_pages.begin(), span.walker_pages.end());
        span.walker_pages.erase(std::unique(span.walker_pages.begin(), span.walker_pages.end()), span.walker_pages.end());

        for (const auto& record : work_item->records) {
            const auto type = static_cast<WalRecordType>(record.header.type);
            auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
            if (type == WalRecordType::TupleBeforeImage) {
                auto before_view = bored::storage::decode_wal_tuple_before_image(payload);
                REQUIRE(before_view);
                span.slot_index = before_view->meta.slot_index;
                auto before_payload = tuple_payload_view(before_view->tuple_payload, before_view->meta.tuple_length);
                span.tuple_payload.assign(before_payload.begin(), before_payload.end());
                for (const auto& chunk_view : before_view->overflow_chunks) {
                    span.chunk_metas.push_back(chunk_view.meta);
                    span.chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
                }
            }
        }

        REQUIRE(span.slot_index != std::numeric_limits<std::uint16_t>::max());
        spans.push_back(std::move(span));
    }

    REQUIRE(spans.size() == 2U);
    for (auto& span : spans) {
        std::vector<std::uint32_t> expected_pages;
        expected_pages.reserve(span.chunk_metas.size() * 2U);
        for (const auto& meta : span.chunk_metas) {
            expected_pages.push_back(meta.overflow_page_id);
            if (meta.next_overflow_page_id != 0U) {
                expected_pages.push_back(meta.next_overflow_page_id);
            }
        }
        std::sort(expected_pages.begin(), expected_pages.end());
        expected_pages.erase(std::unique(expected_pages.begin(), expected_pages.end()), expected_pages.end());
        CHECK(span.walker_pages == expected_pages);
    }

    CHECK(crash_header_a.fragment_count == crash_fragment_count_a);
    CHECK(crash_header_b.fragment_count == crash_fragment_count_b);
    REQUIRE(crash_fragment_count_b > baseline_fragment_count_b);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id_a, std::span<const std::byte>(crash_page_a.data(), crash_page_a.size()));
    context.set_page(page_id_b, std::span<const std::byte>(crash_page_b.data(), crash_page_b.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    auto undo_error = replayer.apply_undo(plan);
    CAPTURE(replayer.last_undo_type());
    REQUIRE_FALSE(undo_error);

    auto find_span = [&](std::uint32_t owner) -> const ExpectedSpan& {
        auto it = std::find_if(spans.begin(), spans.end(), [&](const ExpectedSpan& span) { return span.owner_page_id == owner; });
        REQUIRE(it != spans.end());
        return *it;
    };

    const auto& span_a = find_span(page_id_a);
    auto replay_page_a = context.get_page(page_id_a);
    auto tuple_a = bored::storage::read_tuple(std::span<const std::byte>(replay_page_a.data(), replay_page_a.size()), span_a.slot_index);
    REQUIRE(tuple_a.size() == span_a.tuple_payload.size());
    REQUIRE(std::equal(tuple_a.begin(), tuple_a.end(), span_a.tuple_payload.begin(), span_a.tuple_payload.end()));
    CHECK(replay_fsm.current_free_bytes(page_id_a) == baseline_free_bytes_a);
    CHECK(replay_fsm.current_fragment_count(page_id_a) == baseline_fragment_count_a);
    for (std::size_t index = 0; index < span_a.chunk_metas.size(); ++index) {
        const auto& expected_meta = span_a.chunk_metas[index];
        const auto& expected_payload = span_a.chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto page_view = std::span<const std::byte>(page.data(), page.size());
        auto restored_meta = bored::storage::read_overflow_chunk_meta(page_view);
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(page_view, *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
        auto overflow_header = bored::storage::page_header(page_view);
        CHECK(overflow_header.fragment_count == 0U);
        CHECK(replay_fsm.current_fragment_count(expected_meta.overflow_page_id) == 0U);
        CHECK(replay_fsm.current_free_bytes(expected_meta.overflow_page_id)
              == static_cast<std::uint16_t>(bored::storage::compute_free_bytes(overflow_header)));
    }

    const auto& span_b = find_span(page_id_b);
    auto replay_page_b = context.get_page(page_id_b);
    auto tuple_b = bored::storage::read_tuple(std::span<const std::byte>(replay_page_b.data(), replay_page_b.size()), span_b.slot_index);
    REQUIRE(tuple_b.size() == span_b.tuple_payload.size());
    REQUIRE(std::equal(tuple_b.begin(), tuple_b.end(), span_b.tuple_payload.begin(), span_b.tuple_payload.end()));
    CHECK(replay_fsm.current_free_bytes(page_id_b) == baseline_free_bytes_b);
    CHECK(replay_fsm.current_fragment_count(page_id_b) == baseline_fragment_count_b);
    for (std::size_t index = 0; index < span_b.chunk_metas.size(); ++index) {
        const auto& expected_meta = span_b.chunk_metas[index];
        const auto& expected_payload = span_b.chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto page_view = std::span<const std::byte>(page.data(), page.size());
        auto restored_meta = bored::storage::read_overflow_chunk_meta(page_view);
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto restored_payload = bored::storage::overflow_chunk_payload(page_view, *restored_meta);
        REQUIRE(restored_payload.size() == expected_payload.size());
        REQUIRE(std::equal(restored_payload.begin(), restored_payload.end(), expected_payload.begin(), expected_payload.end()));
        auto overflow_header = bored::storage::page_header(page_view);
        CHECK(overflow_header.fragment_count == 0U);
        CHECK(replay_fsm.current_fragment_count(expected_meta.overflow_page_id) == 0U);
        CHECK(replay_fsm.current_free_bytes(expected_meta.overflow_page_id)
              == static_cast<std::uint16_t>(bored::storage::compute_free_bytes(overflow_header)));
    }

    auto replay_header_a = bored::storage::page_header(std::span<const std::byte>(replay_page_a.data(), replay_page_a.size()));
    CHECK(replay_header_a.tuple_count == baseline_header_a.tuple_count);
    CHECK(replay_header_a.fragment_count == baseline_header_a.fragment_count);

    auto replay_header_b = bored::storage::page_header(std::span<const std::byte>(replay_page_b.data(), replay_page_b.size()));
    CHECK(replay_header_b.tuple_count == baseline_header_b.tuple_count);
    CHECK(replay_header_b.fragment_count == baseline_header_b.fragment_count);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill replays committed page and rolls back in-flight page")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_crash_multi_page_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * kWalBlockSize;
    config.buffer_size = 2U * kWalBlockSize;
    config.start_lsn = kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_a_id = 565656U;
    constexpr std::uint32_t page_b_id = 575757U;

    alignas(8) std::array<std::byte, kPageSize> page_a_buffer{};
    alignas(8) std::array<std::byte, kPageSize> page_b_buffer{};

    auto page_a_span = std::span<std::byte>(page_a_buffer.data(), page_a_buffer.size());
    auto page_b_span = std::span<std::byte>(page_b_buffer.data(), page_b_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_a_span, PageType::Table, page_a_id));
    REQUIRE_FALSE(manager.initialize_page(page_b_span, PageType::Table, page_b_id));

    std::vector<std::byte> committed_payload(384U);
    for (std::size_t index = 0; index < committed_payload.size(); ++index) {
        committed_payload[index] = static_cast<std::byte>((index * 13U) & 0xFFU);
    }

    PageManager::TupleInsertResult committed_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_a_span,
                                       std::span<const std::byte>(committed_payload.data(), committed_payload.size()),
                                       0xAA0001ULL,
                                       committed_insert));

    const auto committed_snapshot_a = page_a_buffer;
    const auto committed_free_bytes_a = fsm.current_free_bytes(page_a_id);

    auto commit_header_a = make_commit_header(wal_writer, page_a_id, page_a_id + 1U, page_a_id);
    WalAppendResult commit_result_a{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header_a, commit_result_a));

    const auto baseline_page_b = page_b_buffer;
    const auto baseline_free_bytes_b = fsm.current_free_bytes(page_b_id);
    const auto baseline_fragment_count_b = fsm.current_fragment_count(page_b_id);

    std::vector<std::byte> inflight_payload(256U);
    for (std::size_t index = 0; index < inflight_payload.size(); ++index) {
        inflight_payload[index] = static_cast<std::byte>((index * 7U) & 0xFFU);
    }

    PageManager::TupleInsertResult inflight_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_b_span,
                                       std::span<const std::byte>(inflight_payload.data(), inflight_payload.size()),
                                       0xBB0002ULL,
                                       inflight_insert));

    const auto crash_free_bytes_b = fsm.current_free_bytes(page_b_id);
    const auto crash_fragment_count_b = fsm.current_fragment_count(page_b_id);

    const auto crash_page_a = page_a_buffer;
    const auto crash_page_b = page_b_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    REQUIRE(plan.transactions.size() == 2U);

    auto find_transaction = [&](std::uint64_t id) -> const WalRecoveredTransaction* {
        const auto it = std::find_if(plan.transactions.begin(), plan.transactions.end(), [&](const WalRecoveredTransaction& txn) {
            return txn.transaction_id == id;
        });
        return it != plan.transactions.end() ? &(*it) : nullptr;
    };

    const auto* committed_tx = find_transaction(page_a_id);
    REQUIRE(committed_tx != nullptr);
    CHECK(committed_tx->state == WalRecoveredTransactionState::Committed);
    CHECK(committed_tx->commit_lsn == commit_header_a.commit_lsn);

    const auto* inflight_tx = find_transaction(page_b_id);
    REQUIRE(inflight_tx != nullptr);
    CHECK(inflight_tx->state == WalRecoveredTransactionState::InFlight);
    CHECK_FALSE(inflight_tx->commit_record.has_value());

    auto span_b = std::find_if(plan.undo_spans.begin(), plan.undo_spans.end(), [&](const WalUndoSpan& span) {
        return span.owner_page_id == page_b_id;
    });
    REQUIRE(span_b != plan.undo_spans.end());
    CHECK(span_b->count >= 1U);

    auto span_a = std::find_if(plan.undo_spans.begin(), plan.undo_spans.end(), [&](const WalUndoSpan& span) {
        return span.owner_page_id == page_a_id;
    });
    CHECK(span_a == plan.undo_spans.end());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_a_id, std::span<const std::byte>(crash_page_a.data(), crash_page_a.size()));
    context.set_page(page_b_id, std::span<const std::byte>(crash_page_b.data(), crash_page_b.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page_a = context.get_page(page_a_id);
    auto replay_span_a = std::span<const std::byte>(replay_page_a.data(), replay_page_a.size());
    auto directory_a = slot_directory(replay_span_a);
    REQUIRE(committed_insert.slot.index < directory_a.size());
    CHECK(directory_a[committed_insert.slot.index].length == committed_insert.slot.length);

    auto restored_tuple_a = read_tuple(replay_span_a, committed_insert.slot.index);
    REQUIRE(restored_tuple_a.size() == committed_payload.size());
    REQUIRE(std::equal(restored_tuple_a.begin(), restored_tuple_a.end(), committed_payload.begin(), committed_payload.end()));

    auto replay_header_a = page_header(replay_span_a);
    auto baseline_span_a = std::span<const std::byte>(committed_snapshot_a.data(), committed_snapshot_a.size());
    auto baseline_header_a = page_header(baseline_span_a);
    CHECK(replay_header_a.tuple_count == baseline_header_a.tuple_count);
    CHECK(replay_fsm.current_free_bytes(page_a_id) == committed_free_bytes_a);

    auto replay_page_b = context.get_page(page_b_id);
    auto replay_span_b = std::span<const std::byte>(replay_page_b.data(), replay_page_b.size());
    auto directory_b = slot_directory(replay_span_b);
    REQUIRE(inflight_insert.slot.index < directory_b.size());
    CHECK(directory_b[inflight_insert.slot.index].length == 0U);

    auto undone_tuple = read_tuple(replay_span_b, inflight_insert.slot.index);
    CHECK(undone_tuple.empty());

    auto replay_header_b = page_header(replay_span_b);
    auto baseline_span_b = std::span<const std::byte>(baseline_page_b.data(), baseline_page_b.size());
    auto baseline_header_b = page_header(baseline_span_b);
    auto crash_span_b = std::span<const std::byte>(crash_page_b.data(), crash_page_b.size());
    auto crash_header_b = page_header(crash_span_b);

    REQUIRE(crash_header_b.tuple_count == static_cast<std::uint16_t>(baseline_header_b.tuple_count + 1U));
    CHECK(baseline_fragment_count_b == 0U);
    CHECK(crash_fragment_count_b == baseline_fragment_count_b);
    CHECK(crash_free_bytes_b < baseline_free_bytes_b);
    CHECK(crash_header_b.fragment_count == crash_fragment_count_b);

    const auto expected_fragment_count_b = static_cast<std::uint16_t>(crash_fragment_count_b + 1U);
    // Undo leaves a fragment so free space metrics remain at their crash-time values.
    CHECK(replay_header_b.tuple_count == crash_header_b.tuple_count);
    CHECK(replay_header_b.free_start == crash_header_b.free_start);
    CHECK(replay_header_b.free_end == crash_header_b.free_end);
    CHECK(replay_header_b.fragment_count == expected_fragment_count_b);
    CHECK(replay_fsm.current_free_bytes(page_b_id) == crash_free_bytes_b);
    CHECK(replay_fsm.current_fragment_count(page_b_id) == expected_fragment_count_b);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill restores overflow tuple metadata")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_crash_drill_metadata_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 515151U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<std::byte> baseline_payload(10240U);
    for (std::size_t index = 0; index < baseline_payload.size(); ++index) {
        baseline_payload[index] = static_cast<std::byte>((index * 37U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, baseline_payload, 0x1BADF00DULL, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE(insert_result.overflow_page_ids.size() >= 2U);

    const auto baseline_page = page_buffer;
    const auto baseline_free_bytes = fsm.current_free_bytes(page_id);

    auto baseline_span = std::span<const std::byte>(baseline_page.data(), baseline_page.size());
    auto baseline_directory = bored::storage::slot_directory(baseline_span);

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    std::vector<std::byte> trimmed_payload(768U);
    for (std::size_t index = 0; index < trimmed_payload.size(); ++index) {
        trimmed_payload[index] = static_cast<std::byte>((index * 19U) & 0xFFU);
    }

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, trimmed_payload, 0x1BADF00DULL, update_result));
    REQUIRE_FALSE(update_result.used_overflow);

    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.undo.empty());

    auto span_it = std::find_if(plan.undo_spans.begin(), plan.undo_spans.end(), [&](const WalUndoSpan& span) {
        return span.owner_page_id == page_id;
    });
    REQUIRE(span_it != plan.undo_spans.end());

    WalUndoWalker walker{plan};
    std::optional<WalUndoWorkItem> matched;
    while (auto work_item = walker.next()) {
        if (work_item->owner_page_id == page_id) {
            matched = work_item;
            break;
        }
    }
    REQUIRE(matched);
    CHECK(matched->owner_page_id == page_id);

    const WalRecoveryRecord* before_record = nullptr;
    for (const auto& record : matched->records) {
        if (static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage) {
            before_record = &record;
            break;
        }
    }
    REQUIRE(before_record != nullptr);

    auto before_view = bored::storage::decode_wal_tuple_before_image(
        std::span<const std::byte>(before_record->payload.data(), before_record->payload.size()));
    REQUIRE(before_view);

    auto expected_tuple_payload = tuple_payload_vector(before_view->tuple_payload, before_view->meta.tuple_length);

    std::vector<bored::storage::WalOverflowChunkMeta> expected_chunk_metas;
    std::vector<std::vector<std::byte>> expected_chunk_payloads;
    expected_chunk_metas.reserve(before_view->overflow_chunks.size());
    expected_chunk_payloads.reserve(before_view->overflow_chunks.size());
    for (const auto& chunk_view : before_view->overflow_chunks) {
        expected_chunk_metas.push_back(chunk_view.meta);
        expected_chunk_payloads.emplace_back(chunk_view.payload.begin(), chunk_view.payload.end());
    }

    auto collect_page = [](std::vector<std::uint32_t>& pages, std::uint32_t page) {
        if (page != 0U) {
            pages.push_back(page);
        }
    };

    std::vector<std::uint32_t> expected_overflow_pages;
    for (const auto& chunk_meta : expected_chunk_metas) {
        collect_page(expected_overflow_pages, chunk_meta.overflow_page_id);
        collect_page(expected_overflow_pages, chunk_meta.next_overflow_page_id);
    }

    for (const auto& record : matched->records) {
        auto type = static_cast<WalRecordType>(record.header.type);
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());
        switch (type) {
        case WalRecordType::TupleOverflowChunk: {
            auto meta = bored::storage::decode_wal_overflow_chunk_meta(payload);
            if (meta) {
                collect_page(expected_overflow_pages, meta->overflow_page_id);
                collect_page(expected_overflow_pages, meta->next_overflow_page_id);
            }
            break;
        }
        case WalRecordType::TupleOverflowTruncate: {
            auto meta = bored::storage::decode_wal_overflow_truncate_meta(payload);
            if (meta) {
                collect_page(expected_overflow_pages, meta->first_overflow_page_id);
                auto chunk_views = bored::storage::decode_wal_overflow_truncate_chunks(payload, *meta);
                if (chunk_views) {
                    for (const auto& view : *chunk_views) {
                        collect_page(expected_overflow_pages, view.meta.overflow_page_id);
                        collect_page(expected_overflow_pages, view.meta.next_overflow_page_id);
                    }
                }
            }
            break;
        }
        default:
            break;
        }
    }

    std::sort(expected_overflow_pages.begin(), expected_overflow_pages.end());
    expected_overflow_pages.erase(std::remove(expected_overflow_pages.begin(), expected_overflow_pages.end(), 0U), expected_overflow_pages.end());
    expected_overflow_pages.erase(std::unique(expected_overflow_pages.begin(), expected_overflow_pages.end()), expected_overflow_pages.end());

    auto walker_pages = matched->overflow_page_ids;
    std::sort(walker_pages.begin(), walker_pages.end());
    walker_pages.erase(std::remove(walker_pages.begin(), walker_pages.end(), 0U), walker_pages.end());
    walker_pages.erase(std::unique(walker_pages.begin(), walker_pages.end()), walker_pages.end());
    CHECK(walker_pages == expected_overflow_pages);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = context.get_page(page_id);
    auto replay_view = std::span<const std::byte>(replay_page.data(), replay_page.size());
    auto replay_header = bored::storage::page_header(replay_view);
    auto baseline_header = bored::storage::page_header(baseline_span);

    CHECK(replay_header.lsn == before_view->previous_page_lsn);
    CHECK(replay_header.free_start == before_view->previous_free_start);
    CHECK(replay_header.free_end == baseline_header.free_end);
    CHECK(replay_header.tuple_count == baseline_header.tuple_count);
    CHECK(replay_fsm.current_free_bytes(page_id) == baseline_free_bytes);

    auto replay_directory = bored::storage::slot_directory(replay_view);
    REQUIRE(before_view->meta.slot_index < replay_directory.size());
    REQUIRE(before_view->meta.slot_index < baseline_directory.size());
    CHECK(replay_directory[before_view->meta.slot_index].offset == baseline_directory[before_view->meta.slot_index].offset);
    CHECK(replay_directory[before_view->meta.slot_index].length == baseline_directory[before_view->meta.slot_index].length);

    auto restored_tuple = bored::storage::read_tuple(replay_view, before_view->meta.slot_index);
    REQUIRE(restored_tuple.size() == expected_tuple_payload.size());
    REQUIRE(std::equal(restored_tuple.begin(), restored_tuple.end(), expected_tuple_payload.begin(), expected_tuple_payload.end()));

    for (std::size_t index = 0; index < expected_chunk_metas.size(); ++index) {
        const auto& expected_meta = expected_chunk_metas[index];
        const auto& expected_payload = expected_chunk_payloads[index];
        auto page = context.get_page(expected_meta.overflow_page_id);
        auto page_view = std::span<const std::byte>(page.data(), page.size());
        auto restored_meta = bored::storage::read_overflow_chunk_meta(page_view);
        REQUIRE(restored_meta);
        CHECK(restored_meta->overflow_page_id == expected_meta.overflow_page_id);
        CHECK(restored_meta->next_overflow_page_id == expected_meta.next_overflow_page_id);
        CHECK(restored_meta->chunk_index == expected_meta.chunk_index);
        CHECK(restored_meta->chunk_length == expected_meta.chunk_length);
        auto chunk_payload = bored::storage::overflow_chunk_payload(page_view, *restored_meta);
        REQUIRE(chunk_payload.size() == expected_payload.size());
        REQUIRE(std::equal(chunk_payload.begin(), chunk_payload.end(), expected_payload.begin(), expected_payload.end()));
    }

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill restores catalog tuple before image")
{
    using namespace bored::catalog;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_crash_drill_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = kCatalogTablesPageId;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Meta, page_id));

    CatalogTableDescriptor baseline_descriptor{};
    baseline_descriptor.tuple.xmin = 42U;
    baseline_descriptor.tuple.xmax = 0U;
    baseline_descriptor.tuple.visibility_flags = 0U;
    baseline_descriptor.relation_id = RelationId{9'001U};
    baseline_descriptor.schema_id = SchemaId{123U};
    baseline_descriptor.table_type = CatalogTableType::Heap;
    baseline_descriptor.root_page_id = 777U;
    baseline_descriptor.name = "events";
    auto baseline_payload = serialize_catalog_table(baseline_descriptor);

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span,
                                       std::span<const std::byte>(baseline_payload.data(), baseline_payload.size()),
                                       baseline_descriptor.relation_id.value,
                                       insert_result));
    const auto baseline_page = page_buffer;

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    CatalogTableDescriptor updated_descriptor = baseline_descriptor;
    updated_descriptor.tuple.xmin = baseline_descriptor.tuple.xmin + 10U;
    updated_descriptor.name = "events_crash";
    auto updated_payload = serialize_catalog_table(updated_descriptor);

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(updated_payload.data(), updated_payload.size()),
                                       baseline_descriptor.relation_id.value,
                                       update_result));
    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Meta, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = context.get_page(page_id);
    auto tuple_span = bored::storage::read_tuple(std::span<const std::byte>(replay_page.data(), replay_page.size()), insert_result.slot.index);
    auto restored_view = decode_catalog_table(tuple_span);
    REQUIRE(restored_view);
    CHECK(restored_view->name == baseline_descriptor.name);
    CHECK(restored_view->tuple.xmin == baseline_descriptor.tuple.xmin);
    CHECK(restored_view->root_page_id == baseline_descriptor.root_page_id);

    auto replay_span = std::span<const std::byte>(replay_page.data(), replay_page.size());
    auto baseline_span = std::span<const std::byte>(baseline_page.data(), baseline_page.size());
    CAPTURE(insert_result.slot.offset);
    CAPTURE(insert_result.slot.length);
    auto replay_header = bored::storage::page_header(replay_span);
    auto baseline_header = bored::storage::page_header(baseline_span);
    CHECK(replay_header.tuple_count == baseline_header.tuple_count);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill restores index descriptor before image")
{
    using namespace bored::catalog;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_index_crash_drill_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = kCatalogIndexesPageId;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Meta, page_id));

    CatalogIndexDescriptor pending_descriptor{};
    pending_descriptor.tuple.xmin = 91U;
    pending_descriptor.tuple.xmax = 0U;
    pending_descriptor.tuple.visibility_flags = 0x01U;
    pending_descriptor.index_id = IndexId{8'500U};
    pending_descriptor.relation_id = RelationId{4'200U};
    pending_descriptor.index_type = CatalogIndexType::BTree;
    pending_descriptor.root_page_id = 7'000U;
    pending_descriptor.max_fanout = 120U;
    pending_descriptor.comparator = "int64_ascending";
    pending_descriptor.name = "metrics_idx_pending";
    auto pending_payload = serialize_catalog_index(pending_descriptor);

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span,
                                       std::span<const std::byte>(pending_payload.data(), pending_payload.size()),
                                       pending_descriptor.index_id.value,
                                       insert_result));

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    const auto baseline_page = page_buffer;

    CatalogIndexDescriptor updated_descriptor = pending_descriptor;
    updated_descriptor.tuple.xmin = pending_descriptor.tuple.xmin + 5U;
    updated_descriptor.tuple.visibility_flags = 0x02U;
    updated_descriptor.root_page_id = pending_descriptor.root_page_id + 1U;
    updated_descriptor.max_fanout = pending_descriptor.max_fanout;
    updated_descriptor.comparator = pending_descriptor.comparator;
    updated_descriptor.name = "metrics_idx_ready";
    auto updated_payload = serialize_catalog_index(updated_descriptor);

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(updated_payload.data(), updated_payload.size()),
                                       pending_descriptor.index_id.value,
                                       update_result));

    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Meta, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = context.get_page(page_id);
    auto tuple_span = bored::storage::read_tuple(std::span<const std::byte>(replay_page.data(), replay_page.size()),
                                                 insert_result.slot.index);
    auto restored_view = decode_catalog_index(tuple_span);
    REQUIRE(restored_view);
    CHECK(restored_view->name == pending_descriptor.name);
    CHECK(restored_view->root_page_id == pending_descriptor.root_page_id);
    CHECK(restored_view->tuple.visibility_flags == pending_descriptor.tuple.visibility_flags);
    CHECK(restored_view->tuple.xmin == pending_descriptor.tuple.xmin);

    auto baseline_tuple_span = bored::storage::read_tuple(std::span<const std::byte>(baseline_page.data(), baseline_page.size()),
                                                         insert_result.slot.index);
    REQUIRE(baseline_tuple_span.size() == tuple_span.size());
    if (!std::equal(tuple_span.begin(), tuple_span.end(), baseline_tuple_span.begin(), baseline_tuple_span.end())) {
        const auto mismatch = std::mismatch(tuple_span.begin(), tuple_span.end(), baseline_tuple_span.begin(), baseline_tuple_span.end());
        const auto mismatch_index = std::distance(tuple_span.begin(), mismatch.first);
        UNSCOPED_INFO("tuple mismatch index=" << mismatch_index << " replay_byte="
                                                << std::to_integer<int>(*mismatch.first)
                                                << " baseline_byte=" << std::to_integer<int>(*mismatch.second));
    }
    CHECK(std::equal(tuple_span.begin(), tuple_span.end(), baseline_tuple_span.begin(), baseline_tuple_span.end()));

    auto replay_header = bored::storage::page_header(std::span<const std::byte>(replay_page.data(), replay_page.size()));
    auto baseline_header = bored::storage::page_header(std::span<const std::byte>(baseline_page.data(), baseline_page.size()));
    CHECK(replay_header.tuple_count == baseline_header.tuple_count);

    auto replay_span = std::span<const std::byte>(replay_page.data(), replay_page.size());
    auto baseline_span = std::span<const std::byte>(baseline_page.data(), baseline_page.size());
    const auto replay_header_meta = bored::storage::page_header(replay_span);
    const auto baseline_header_meta = bored::storage::page_header(baseline_span);
    CAPTURE(replay_span.size());
    CAPTURE(baseline_span.size());
    CAPTURE(replay_header_meta.lsn);
    CAPTURE(baseline_header_meta.lsn);
    CAPTURE(replay_header_meta.free_start);
    CAPTURE(baseline_header_meta.free_start);
    CAPTURE(replay_header_meta.free_end);
    CAPTURE(baseline_header_meta.free_end);
    CAPTURE(replay_header_meta.tuple_count);
    CAPTURE(baseline_header_meta.tuple_count);
    CAPTURE(replay_header_meta.fragment_count);
    CAPTURE(baseline_header_meta.fragment_count);
    auto mismatch = std::mismatch(replay_span.begin(), replay_span.end(), baseline_span.begin(), baseline_span.end());
    CAPTURE(mismatch.second == baseline_span.end());
    if (mismatch.first != replay_span.end()) {
        const auto mismatch_index = std::distance(replay_span.begin(), mismatch.first);
        CAPTURE(mismatch_index);
        const auto replay_byte = std::to_integer<int>(*mismatch.first);
        const auto baseline_byte = std::to_integer<int>(*mismatch.second);
        CAPTURE(replay_byte);
        CAPTURE(baseline_byte);
        UNSCOPED_INFO("mismatch_index=" << mismatch_index << " replay_byte=" << replay_byte
                                         << " baseline_byte=" << baseline_byte);
    } else if (mismatch.second != baseline_span.end()) {
        const auto mismatch_index = std::distance(baseline_span.begin(), mismatch.second);
        CAPTURE(mismatch_index);
        const auto baseline_byte = std::to_integer<int>(*mismatch.second);
        CAPTURE(baseline_byte);
        UNSCOPED_INFO("extra_baseline_byte index=" << mismatch_index << " value=" << baseline_byte);
    }
    REQUIRE(std::equal(replay_span.begin(), replay_span.end(), baseline_span.begin(), baseline_span.end()));

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill rolls back catalog id allocator counters")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_allocator_crash_drill_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t allocator_page_id = 11U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Meta, allocator_page_id));

    constexpr std::uint64_t allocator_row_id = 5'000U;
    CatalogAllocatorState baseline_state{};
    baseline_state.next_schema_id = 10'000U;
    baseline_state.next_table_id = 20'000U;
    baseline_state.next_index_id = 30'000U;
    baseline_state.next_column_id = 40'000U;

    std::vector<std::byte> baseline_payload(sizeof(CatalogAllocatorState));
    std::memcpy(baseline_payload.data(), &baseline_state, sizeof(CatalogAllocatorState));

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span,
                                       std::span<const std::byte>(baseline_payload.data(), baseline_payload.size()),
                                       allocator_row_id,
                                       insert_result));

    auto commit_header = make_commit_header(wal_writer, allocator_page_id, allocator_page_id + 1U, allocator_page_id);
    bored::storage::WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    const auto committed_page = page_buffer;

    CatalogAllocatorState updated_state = baseline_state;
    updated_state.next_schema_id += 7U;
    updated_state.next_table_id += 13U;
    updated_state.next_index_id += 17U;
    updated_state.next_column_id += 23U;

    std::vector<std::byte> updated_payload(sizeof(CatalogAllocatorState));
    std::memcpy(updated_payload.data(), &updated_state, sizeof(CatalogAllocatorState));

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(updated_payload.data(), updated_payload.size()),
                                       allocator_row_id,
                                       update_result));

    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Meta, &replay_fsm};
    context.set_page(allocator_page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));

    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = context.get_page(allocator_page_id);
    auto tuple_span = bored::storage::read_tuple(std::span<const std::byte>(replay_page.data(), replay_page.size()), insert_result.slot.index);
    REQUIRE(tuple_span.size() == sizeof(CatalogAllocatorState));

    CatalogAllocatorState restored_state{};
    std::memcpy(&restored_state, tuple_span.data(), tuple_span.size());

    CHECK(restored_state.next_schema_id == baseline_state.next_schema_id);
    CHECK(restored_state.next_table_id == baseline_state.next_table_id);
    CHECK(restored_state.next_index_id == baseline_state.next_index_id);
    CHECK(restored_state.next_column_id == baseline_state.next_column_id);

    auto replay_header = bored::storage::page_header(std::span<const std::byte>(replay_page.data(), replay_page.size()));
    auto committed_header = bored::storage::page_header(std::span<const std::byte>(committed_page.data(), committed_page.size()));
    CHECK(replay_header.tuple_count == committed_header.tuple_count);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill preserves committed allocator counters during concurrent updates")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_allocator_concurrent_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t allocator_page_id = 13U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Meta, allocator_page_id));

    constexpr std::uint64_t allocator_row_id = 7'000U;

    CatalogAllocatorState baseline_state{};
    baseline_state.next_schema_id = 110'000U;
    baseline_state.next_table_id = 210'000U;
    baseline_state.next_index_id = 310'000U;
    baseline_state.next_column_id = 410'000U;

    std::vector<std::byte> baseline_payload(sizeof(CatalogAllocatorState));
    std::memcpy(baseline_payload.data(), &baseline_state, sizeof(CatalogAllocatorState));

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span,
                                       std::span<const std::byte>(baseline_payload.data(), baseline_payload.size()),
                                       allocator_row_id,
                                       insert_result));

    auto commit_insert_header = make_commit_header(wal_writer, allocator_page_id, allocator_page_id + 1U, allocator_page_id);
    bored::storage::WalAppendResult commit_insert_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_insert_header, commit_insert_append));

    CatalogAllocatorState committed_state = baseline_state;
    committed_state.next_schema_id += 11U;
    committed_state.next_table_id += 13U;
    committed_state.next_index_id += 17U;
    committed_state.next_column_id += 19U;

    std::vector<std::byte> committed_payload(sizeof(CatalogAllocatorState));
    std::memcpy(committed_payload.data(), &committed_state, sizeof(CatalogAllocatorState));

    PageManager::TupleUpdateResult committed_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(committed_payload.data(), committed_payload.size()),
                                       allocator_row_id,
                                       committed_update));

    auto commit_update_header = make_commit_header(wal_writer, allocator_page_id, allocator_page_id + 1U, allocator_page_id);
    bored::storage::WalAppendResult commit_update_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_update_header, commit_update_append));

    const auto committed_page = page_buffer;

    CatalogAllocatorState inflight_state = committed_state;
    inflight_state.next_schema_id += 5U;
    inflight_state.next_table_id += 7U;
    inflight_state.next_index_id += 9U;
    inflight_state.next_column_id += 11U;

    std::vector<std::byte> inflight_payload(sizeof(CatalogAllocatorState));
    std::memcpy(inflight_payload.data(), &inflight_state, sizeof(CatalogAllocatorState));

    PageManager::TupleUpdateResult inflight_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(inflight_payload.data(), inflight_payload.size()),
                                       allocator_row_id,
                                       inflight_update));

    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    auto undo_has_before = std::any_of(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    CHECK(undo_has_before);
    for (const auto& record : plan.undo) {
        if (static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage) {
            INFO("before_image_prev_lsn=" << record.header.prev_lsn << " lsn=" << record.header.lsn);
            break;
        }
    }

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Meta, &replay_fsm};
    context.set_page(allocator_page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));

    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = context.get_page(allocator_page_id);
    auto tuple_span = bored::storage::read_tuple(std::span<const std::byte>(replay_page.data(), replay_page.size()), insert_result.slot.index);
    REQUIRE(tuple_span.size() == sizeof(CatalogAllocatorState));

    CatalogAllocatorState restored_state{};
    std::memcpy(&restored_state, tuple_span.data(), tuple_span.size());
    CHECK(restored_state.next_schema_id == committed_state.next_schema_id);
    CHECK(restored_state.next_table_id == committed_state.next_table_id);
    CHECK(restored_state.next_index_id == committed_state.next_index_id);
    CHECK(restored_state.next_column_id == committed_state.next_column_id);

    auto replay_header = bored::storage::page_header(std::span<const std::byte>(replay_page.data(), replay_page.size()));
    auto committed_header = bored::storage::page_header(std::span<const std::byte>(committed_page.data(), committed_page.size()));
    CHECK(replay_header.tuple_count == committed_header.tuple_count);

    auto replay_span = std::span<const std::byte>(replay_page.data(), replay_page.size());
    auto committed_span = std::span<const std::byte>(committed_page.data(), committed_page.size());
    const auto replay_header_committed = bored::storage::page_header(replay_span);
    const auto committed_header_meta = bored::storage::page_header(committed_span);
    CAPTURE(replay_span.size());
    CAPTURE(committed_span.size());
    CAPTURE(replay_header_committed.lsn);
    CAPTURE(committed_header_meta.lsn);
    CAPTURE(replay_header_committed.free_start);
    CAPTURE(committed_header_meta.free_start);
    CAPTURE(replay_header_committed.free_end);
    CAPTURE(committed_header_meta.free_end);
    CAPTURE(replay_header_committed.tuple_count);
    CAPTURE(committed_header_meta.tuple_count);
    CAPTURE(replay_header_committed.fragment_count);
    CAPTURE(committed_header_meta.fragment_count);
    auto mismatch = std::mismatch(replay_span.begin(), replay_span.end(), committed_span.begin(), committed_span.end());
    CAPTURE(mismatch.second == committed_span.end());
    if (mismatch.first != replay_span.end()) {
        const auto mismatch_index = std::distance(replay_span.begin(), mismatch.first);
        CAPTURE(mismatch_index);
        const auto replay_byte = std::to_integer<int>(*mismatch.first);
        const auto committed_byte = std::to_integer<int>(*mismatch.second);
        CAPTURE(replay_byte);
        CAPTURE(committed_byte);
        UNSCOPED_INFO("mismatch_index=" << mismatch_index << " replay_byte=" << replay_byte
                                         << " committed_byte=" << committed_byte);
    } else if (mismatch.second != committed_span.end()) {
        const auto mismatch_index = std::distance(committed_span.begin(), mismatch.second);
        CAPTURE(mismatch_index);
        const auto committed_byte = std::to_integer<int>(*mismatch.second);
        CAPTURE(committed_byte);
        UNSCOPED_INFO("extra_committed_byte index=" << mismatch_index << " value=" << committed_byte);
    }
    REQUIRE(std::equal(replay_span.begin(), replay_span.end(), committed_span.begin(), committed_span.end()));

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill detects catalog before image corruption")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_catalog_corruption_crash_drill_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 12U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Meta, page_id));

    constexpr std::uint64_t row_id = 6'000U;
    CatalogAllocatorState baseline_state{};
    baseline_state.next_schema_id = 50'000U;
    baseline_state.next_table_id = 60'000U;
    baseline_state.next_index_id = 70'000U;
    baseline_state.next_column_id = 80'000U;

    std::vector<std::byte> baseline_payload(sizeof(CatalogAllocatorState));
    std::memcpy(baseline_payload.data(), &baseline_state, sizeof(CatalogAllocatorState));

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span,
                                       std::span<const std::byte>(baseline_payload.data(), baseline_payload.size()),
                                       row_id,
                                       insert_result));

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    const auto committed_page = page_buffer;

    CatalogAllocatorState updated_state = baseline_state;
    updated_state.next_schema_id += 3U;
    updated_state.next_table_id += 5U;
    updated_state.next_index_id += 7U;
    updated_state.next_column_id += 11U;

    std::vector<std::byte> updated_payload(sizeof(CatalogAllocatorState));
    std::memcpy(updated_payload.data(), &updated_state, sizeof(CatalogAllocatorState));

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span,
                                       insert_result.slot.index,
                                       std::span<const std::byte>(updated_payload.data(), updated_payload.size()),
                                       row_id,
                                       update_result));

    const auto crash_page = page_buffer;

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE_FALSE(plan.redo.empty());
    REQUIRE_FALSE(plan.undo.empty());

    auto before_it = std::find_if(plan.undo.begin(), plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(before_it != plan.undo.end());

    WalRecoveryPlan corrupted_plan = plan;
    auto corrupt_it = std::find_if(corrupted_plan.undo.begin(), corrupted_plan.undo.end(), [](const WalRecoveryRecord& record) {
        return static_cast<WalRecordType>(record.header.type) == WalRecordType::TupleBeforeImage;
    });
    REQUIRE(corrupt_it != corrupted_plan.undo.end());
    REQUIRE(corrupt_it->payload.size() > sizeof(bored::storage::WalTupleBeforeImageHeader));
    corrupt_it->payload.resize(sizeof(bored::storage::WalTupleBeforeImageHeader) - 8U);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Meta, &replay_fsm};
    context.set_page(page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));

    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto undo_error = replayer.apply_undo(corrupted_plan);
    REQUIRE(undo_error);
    CHECK(undo_error == std::make_error_code(std::errc::invalid_argument));
    auto last_type = replayer.last_undo_type();
    REQUIRE(last_type);
    CHECK(*last_type == WalRecordType::TupleBeforeImage);

    auto mutated_page = context.get_page(page_id);
    auto mutated_span = bored::storage::read_tuple(std::span<const std::byte>(mutated_page.data(), mutated_page.size()), insert_result.slot.index);
    REQUIRE(mutated_span.size() == sizeof(CatalogAllocatorState));
    CatalogAllocatorState mutated_state{};
    std::memcpy(&mutated_state, mutated_span.data(), mutated_span.size());
    CHECK(mutated_state.next_table_id == updated_state.next_table_id);

    FreeSpaceMap clean_fsm;
    WalReplayContext clean_context{PageType::Meta, &clean_fsm};
    clean_context.set_page(page_id, std::span<const std::byte>(crash_page.data(), crash_page.size()));
    WalReplayer clean_replayer{clean_context};
    REQUIRE_FALSE(clean_replayer.apply_redo(plan));
    REQUIRE_FALSE(clean_replayer.apply_undo(plan));
    CHECK_FALSE(clean_replayer.last_undo_type());

    auto replay_page = clean_context.get_page(page_id);
    auto tuple_span = bored::storage::read_tuple(std::span<const std::byte>(replay_page.data(), replay_page.size()), insert_result.slot.index);
    REQUIRE(tuple_span.size() == sizeof(CatalogAllocatorState));
    CatalogAllocatorState restored_state{};
    std::memcpy(&restored_state, tuple_span.data(), tuple_span.size());

    CHECK(restored_state.next_schema_id == baseline_state.next_schema_id);
    CHECK(restored_state.next_table_id == baseline_state.next_table_id);
    CHECK(restored_state.next_index_id == baseline_state.next_index_id);
    CHECK(restored_state.next_column_id == baseline_state.next_column_id);

    auto replay_header = bored::storage::page_header(std::span<const std::byte>(replay_page.data(), replay_page.size()));
    auto committed_header = bored::storage::page_header(std::span<const std::byte>(committed_page.data(), committed_page.size()));
    CHECK(replay_header.tuple_count == committed_header.tuple_count);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReplayer replays page compaction")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_replay_compaction_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 6060U;
    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::array<std::byte, 40> first_tuple{};
    first_tuple.fill(std::byte{0x33});
    PageManager::TupleInsertResult first_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, first_tuple, 301U, first_insert));

    std::array<std::byte, 56> second_tuple{};
    second_tuple.fill(std::byte{0x44});
    PageManager::TupleInsertResult second_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, second_tuple, 302U, second_insert));

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, first_insert.slot.index, 301U, delete_result));

    PageManager::PageCompactionResult compaction_result{};
    REQUIRE_FALSE(manager.compact_page(page_span, compaction_result));
    REQUIRE(compaction_result.performed);

    auto commit_header = make_commit_header(wal_writer, page_id, page_id + 1U, page_id);
    bored::storage::WalAppendResult commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_result));

    const auto post_compaction_snapshot = page_buffer;

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    std::vector<WalRecordType> redo_types;
    redo_types.reserve(plan.redo.size());
    bool compaction_found = false;
    for (const auto& record : plan.redo) {
        auto type = static_cast<WalRecordType>(record.header.type);
        redo_types.push_back(type);
        if (type == WalRecordType::PageCompaction) {
            compaction_found = true;
        }
    }
    CAPTURE(redo_types);
    REQUIRE(compaction_found);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    WalReplayer replayer{context};

    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto replayed_page = context.get_page(page_id);
    auto expected_span = std::span<const std::byte>(post_compaction_snapshot.data(), post_compaction_snapshot.size());
    REQUIRE(std::equal(replayed_page.begin(), replayed_page.end(), expected_span.begin(), expected_span.end()));

    const auto& metadata = context.index_metadata();
    REQUIRE_FALSE(metadata.empty());
    bool refresh_seen = false;
    for (const auto& entry : metadata) {
        if (any(static_cast<WalIndexMaintenanceAction>(entry.index_action))) {
            refresh_seen = true;
        }
    }
    CHECK(refresh_seen);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal recovery replays mixed heap and index workloads across restart")
{
    using bored::storage::IndexBtreeLeafEntry;
    using bored::storage::IndexBtreePageHeader;
    using bored::storage::IndexBtreeSlotEntry;
    using bored::storage::IndexBtreeTuplePointer;
    using bored::storage::WalIndexSplitFlag;
    using bored::storage::WalIndexSplitHeader;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_mixed_heap_index_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t heap_page_id = 44'200U;
    constexpr std::uint32_t left_page_id = 55'100U;
    constexpr std::uint32_t right_page_id = 55'101U;
    constexpr std::uint64_t index_identifier = 77'700U;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> heap_page{};
    auto heap_span = std::span<std::byte>(heap_page.data(), heap_page.size());
    REQUIRE_FALSE(manager.initialize_page(heap_span, PageType::Table, heap_page_id));

    const auto baseline_heap = heap_page;

    struct RowEntry final {
        std::uint64_t row_id = 0U;
        std::uint64_t key = 0U;
        PageManager::TupleInsertResult insert{};
    };

    std::array<RowEntry, 4> rows{{
        RowEntry{9'001U, 10U, {}},
        RowEntry{9'002U, 20U, {}},
        RowEntry{9'003U, 30U, {}},
        RowEntry{9'004U, 40U, {}}
    }};

    for (auto& row : rows) {
        auto payload_str = std::string{"row_"} + std::to_string(row.key);
        std::vector<std::byte> payload(payload_str.size());
        std::memcpy(payload.data(), payload_str.data(), payload_str.size());
        REQUIRE_FALSE(manager.insert_tuple(heap_span,
                                           std::span<const std::byte>(payload.data(), payload.size()),
                                           row.row_id,
                                           row.insert));
    }

    auto make_key_bytes = [](std::uint64_t value) {
        std::array<std::byte, sizeof(std::uint64_t)> bytes{};
        std::memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    auto build_entries = [&](std::span<const RowEntry> source) {
        std::vector<IndexBtreeLeafEntry> entries;
        entries.reserve(source.size());
        for (const auto& row : source) {
            IndexBtreeLeafEntry entry{};
            entry.pointer.heap_page_id = heap_page_id;
            entry.pointer.heap_slot_id = row.insert.slot.index;
            auto key_bytes = make_key_bytes(row.key);
            entry.key.assign(key_bytes.begin(), key_bytes.end());
            entries.push_back(std::move(entry));
        }
        return entries;
    };

    auto left_entries = build_entries(std::span<const RowEntry>(rows.data(), 2U));
    auto right_entries = build_entries(std::span<const RowEntry>(rows.data() + 2U, 2U));

    auto build_slots_payload = [](const std::vector<IndexBtreeLeafEntry>& entries,
                                  std::vector<IndexBtreeSlotEntry>& out_slots,
                                  std::vector<std::byte>& out_payload) {
        constexpr std::size_t pointer_size = sizeof(IndexBtreeTuplePointer);
        out_slots.clear();
        out_payload.clear();
        out_slots.reserve(entries.size());
        out_payload.reserve(entries.size() * (pointer_size + sizeof(std::uint64_t)));
        for (const auto& entry : entries) {
            const auto* pointer_bytes = reinterpret_cast<const std::byte*>(&entry.pointer);
            out_payload.insert(out_payload.end(), pointer_bytes, pointer_bytes + pointer_size);
            const auto key_offset = static_cast<std::uint16_t>(kIndexPayloadBase + out_payload.size());
            out_payload.insert(out_payload.end(), entry.key.begin(), entry.key.end());

            IndexBtreeSlotEntry slot{};
            slot.key_offset = key_offset;
            slot.key_length = static_cast<std::uint16_t>(entry.key.size());
            out_slots.push_back(slot);
        }
    };

    std::vector<IndexBtreeSlotEntry> left_slots;
    std::vector<std::byte> left_payload;
    build_slots_payload(left_entries, left_slots, left_payload);

    std::vector<IndexBtreeSlotEntry> right_slots;
    std::vector<std::byte> right_payload;
    build_slots_payload(right_entries, right_slots, right_payload);

    auto pivot_key_buffer = make_key_bytes(rows[2].key);
    auto pivot_key = std::span<const std::byte>(pivot_key_buffer.data(), pivot_key_buffer.size());

    WalIndexSplitHeader split_header{};
    split_header.index_id = index_identifier;
    split_header.left_page_id = left_page_id;
    split_header.right_page_id = right_page_id;
    split_header.parent_page_id = 0U;
    split_header.right_sibling_page_id = 0U;
    split_header.level = 0U;
    split_header.flags = static_cast<std::uint16_t>(WalIndexSplitFlag::Leaf | WalIndexSplitFlag::Root);
    split_header.parent_insert_slot = 0U;
    split_header.pivot_key_length = static_cast<std::uint32_t>(pivot_key.size());
    split_header.left_slot_count = static_cast<std::uint32_t>(left_slots.size());
    split_header.right_slot_count = static_cast<std::uint32_t>(right_slots.size());
    split_header.left_payload_length = static_cast<std::uint32_t>(left_payload.size());
    split_header.right_payload_length = static_cast<std::uint32_t>(right_payload.size());

    const auto split_payload_size = bored::storage::wal_index_split_payload_size(left_slots.size(),
                                                                                left_payload.size(),
                                                                                right_slots.size(),
                                                                                right_payload.size(),
                                                                                pivot_key.size());
    std::vector<std::byte> split_payload(split_payload_size);
    REQUIRE(bored::storage::encode_wal_index_split(std::span<std::byte>(split_payload.data(), split_payload.size()),
                                                   split_header,
                                                   std::span<const IndexBtreeSlotEntry>(left_slots.data(), left_slots.size()),
                                                   std::span<const std::byte>(left_payload.data(), left_payload.size()),
                                                   std::span<const IndexBtreeSlotEntry>(right_slots.data(), right_slots.size()),
                                                   std::span<const std::byte>(right_payload.data(), right_payload.size()),
                                                   pivot_key));

    WalRecordDescriptor split_descriptor{};
    split_descriptor.type = WalRecordType::IndexSplit;
    split_descriptor.page_id = heap_page_id;
    split_descriptor.payload = std::span<const std::byte>(split_payload.data(), split_payload.size());

    bored::storage::WalAppendResult split_append{};
    REQUIRE_FALSE(wal_writer->append_record(split_descriptor, split_append));

    auto commit_header = make_commit_header(wal_writer, heap_page_id, heap_page_id + 1U, heap_page_id);
    bored::storage::WalAppendResult commit_append{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, commit_append));

    const auto committed_heap_image = heap_page;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> expected_left_page{};
    auto expected_left_span = std::span<std::byte>(expected_left_page.data(), expected_left_page.size());
    REQUIRE_FALSE(bored::storage::initialize_index_page(expected_left_span, split_header.left_page_id, split_header.level, true));
    REQUIRE(bored::storage::rebuild_index_leaf_page(expected_left_span,
                                                    std::span<const IndexBtreeLeafEntry>(left_entries.data(), left_entries.size()),
                                                    split_append.lsn));
    auto& expected_left_index = *reinterpret_cast<IndexBtreePageHeader*>(expected_left_page.data() + kIndexHeaderOffset);
    expected_left_index.right_sibling_page_id = split_header.right_page_id;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> expected_right_page{};
    auto expected_right_span = std::span<std::byte>(expected_right_page.data(), expected_right_page.size());
    REQUIRE_FALSE(bored::storage::initialize_index_page(expected_right_span, split_header.right_page_id, split_header.level, true));
    REQUIRE(bored::storage::rebuild_index_leaf_page(expected_right_span,
                                                    std::span<const IndexBtreeLeafEntry>(right_entries.data(), right_entries.size()),
                                                    split_append.lsn));
    auto& expected_right_index = *reinterpret_cast<IndexBtreePageHeader*>(expected_right_page.data() + kIndexHeaderOffset);
    expected_right_index.right_sibling_page_id = split_header.right_sibling_page_id;

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    bool split_seen = false;
    for (const auto& record : plan.redo) {
        if (static_cast<WalRecordType>(record.header.type) == WalRecordType::IndexSplit) {
            split_seen = true;
            break;
        }
    }
    REQUIRE(split_seen);

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};
    context.set_page(heap_page_id, std::span<const std::byte>(baseline_heap.data(), baseline_heap.size()));
    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto replay_heap = context.get_page(heap_page_id);
    auto expected_heap_span = std::span<const std::byte>(committed_heap_image.data(), committed_heap_image.size());
    REQUIRE(std::equal(replay_heap.begin(), replay_heap.end(), expected_heap_span.begin(), expected_heap_span.end()));

    auto replay_left = context.get_page(split_header.left_page_id);
    auto expected_left_const = std::span<const std::byte>(expected_left_page.data(), expected_left_page.size());
    REQUIRE(std::equal(replay_left.begin(), replay_left.end(), expected_left_const.begin(), expected_left_const.end()));

    auto replay_right = context.get_page(split_header.right_page_id);
    auto expected_right_const = std::span<const std::byte>(expected_right_page.data(), expected_right_page.size());
    REQUIRE(std::equal(replay_right.begin(), replay_right.end(), expected_right_const.begin(), expected_right_const.end()));

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill rehydrates checkpointed heap and index pages")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_heap_index_crash_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 128U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);

    constexpr std::uint32_t heap_page_id = 81'000U;
    constexpr std::uint32_t index_page_id = 91'000U;
    constexpr std::uint64_t checkpoint_id = 11'500U;
    constexpr std::uint64_t index_identifier = 61'500U;
    constexpr std::uint64_t checkpoint_lsn = bored::storage::kWalBlockSize * 6U;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> heap_snapshot{};
    auto heap_span = std::span<std::byte>(heap_snapshot.data(), heap_snapshot.size());
    REQUIRE(bored::storage::initialize_page(heap_span, PageType::Table, heap_page_id, checkpoint_lsn));

    struct HeapTuple final {
        std::string label{};
        std::uint64_t key = 0U;
        std::uint16_t slot = 0U;
    };

    std::array<HeapTuple, 3> tuples{{
        HeapTuple{"order_100", 100U, 0U},
        HeapTuple{"order_200", 200U, 0U},
        HeapTuple{"order_300", 300U, 0U}
    }};

    for (auto& tuple : tuples) {
        std::vector<std::byte> payload(tuple.label.size());
        std::memcpy(payload.data(), tuple.label.data(), tuple.label.size());
        auto slot = bored::storage::append_tuple(heap_span,
                                                 std::span<const std::byte>(payload.data(), payload.size()),
                                                 checkpoint_lsn);
        REQUIRE(slot);
        tuple.slot = slot->index;
    }

    alignas(8) std::array<std::byte, bored::storage::kPageSize> index_snapshot{};
    auto index_span = std::span<std::byte>(index_snapshot.data(), index_snapshot.size());
    REQUIRE_FALSE(bored::storage::initialize_index_page(index_span, index_page_id, 0U, true, checkpoint_lsn, nullptr));

    std::vector<bored::storage::IndexBtreeLeafEntry> leaf_entries;
    leaf_entries.reserve(tuples.size());
    for (const auto& tuple : tuples) {
        bored::storage::IndexBtreeLeafEntry entry{};
        entry.pointer.heap_page_id = heap_page_id;
        entry.pointer.heap_slot_id = tuple.slot;
        std::array<std::byte, sizeof(tuple.key)> key_buffer{};
        std::memcpy(key_buffer.data(), &tuple.key, sizeof(tuple.key));
        entry.key.assign(key_buffer.begin(), key_buffer.end());
        leaf_entries.push_back(std::move(entry));
    }

    REQUIRE(bored::storage::rebuild_index_leaf_page(index_span,
                                                    std::span<const bored::storage::IndexBtreeLeafEntry>(leaf_entries.data(), leaf_entries.size()),
                                                    checkpoint_lsn));

    bored::storage::CheckpointPageSnapshot heap_page_snapshot{};
    heap_page_snapshot.entry.page_id = heap_page_id;
    heap_page_snapshot.entry.page_lsn = checkpoint_lsn;
    heap_page_snapshot.page_type = PageType::Table;
    heap_page_snapshot.image.assign(heap_snapshot.begin(), heap_snapshot.end());

    bored::storage::CheckpointPageSnapshot index_page_snapshot{};
    index_page_snapshot.entry.page_id = index_page_id;
    index_page_snapshot.entry.page_lsn = checkpoint_lsn;
    index_page_snapshot.page_type = PageType::Index;
    index_page_snapshot.image.assign(index_snapshot.begin(), index_snapshot.end());

    std::array<bored::storage::CheckpointPageSnapshot, 2> snapshots{{heap_page_snapshot, index_page_snapshot}};

    bored::storage::CheckpointImageStore image_store{wal_dir / "checkpoints"};
    auto persist_ec = image_store.persist(checkpoint_id,
                                          std::span<const bored::storage::CheckpointPageSnapshot>(snapshots.data(), snapshots.size()));
    REQUIRE_FALSE(persist_ec);

    std::array<bored::storage::WalCheckpointDirtyPageEntry, 2> dirty_pages{{
        bored::storage::WalCheckpointDirtyPageEntry{.page_id = heap_page_id, .reserved = 0U, .page_lsn = checkpoint_lsn},
        bored::storage::WalCheckpointDirtyPageEntry{.page_id = index_page_id, .reserved = 0U, .page_lsn = checkpoint_lsn}
    }};

    std::array<bored::storage::WalCheckpointIndexEntry, 1> index_entries{{
        bored::storage::WalCheckpointIndexEntry{.index_id = index_identifier, .high_water_lsn = checkpoint_lsn}
    }};

    bored::storage::CheckpointManager checkpoint_manager{wal_writer};
    bored::storage::WalAppendResult checkpoint_append{};
    auto emit_ec = checkpoint_manager.emit_checkpoint(checkpoint_id,
                                                      checkpoint_lsn,
                                                      checkpoint_lsn,
                                                      std::span<const bored::storage::WalCheckpointDirtyPageEntry>(dirty_pages.data(), dirty_pages.size()),
                                                      std::span<const bored::storage::WalCheckpointTxnEntry>{},
                                                      std::span<const bored::storage::WalCheckpointIndexEntry>(index_entries.data(), index_entries.size()),
                                                      checkpoint_append);
    REQUIRE_FALSE(emit_ec);
    REQUIRE(checkpoint_append.written_bytes > 0U);

    REQUIRE_FALSE(wal_writer->flush());
    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, "wal", ".seg", nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));
    REQUIRE(plan.checkpoint_id == checkpoint_id);
    REQUIRE(plan.checkpoint_page_snapshots.size() == snapshots.size());
    REQUIRE(plan.checkpoint_index_metadata.size() == index_entries.size());

    // The replayer seeds checkpoint snapshots up front and ignores checkpoint records, so drop them here.
    plan.redo.erase(std::remove_if(plan.redo.begin(),
                                   plan.redo.end(),
                                   [](const WalRecoveryRecord& record) {
                                       return static_cast<WalRecordType>(record.header.type) == WalRecordType::Checkpoint;
                                   }),
                    plan.redo.end());

    REQUIRE(plan.redo.empty());

    FreeSpaceMap replay_fsm;
    WalReplayContext context{PageType::Table, &replay_fsm};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> crash_heap{};
    crash_heap.fill(std::byte{0xAB});
    context.set_page(heap_page_id, std::span<const std::byte>(crash_heap.data(), crash_heap.size()));

    alignas(8) std::array<std::byte, bored::storage::kPageSize> crash_index{};
    crash_index.fill(std::byte{0xCD});
    context.set_page(index_page_id, std::span<const std::byte>(crash_index.data(), crash_index.size()));

    WalReplayer replayer{context};
    REQUIRE_FALSE(replayer.apply_redo(plan));

    auto restored_heap = context.get_page(heap_page_id);
    auto expected_heap = std::span<const std::byte>(heap_snapshot.data(), heap_snapshot.size());
    REQUIRE(std::equal(restored_heap.begin(), restored_heap.end(), expected_heap.begin(), expected_heap.end()));

    auto restored_index = context.get_page(index_page_id);
    auto expected_index = std::span<const std::byte>(index_snapshot.data(), index_snapshot.size());
    REQUIRE(std::equal(restored_index.begin(), restored_index.end(), expected_index.begin(), expected_index.end()));

    const auto& metadata = context.checkpoint_index_metadata();
    REQUIRE(metadata.size() == index_entries.size());
    CHECK(metadata.front().index_id == index_identifier);
    CHECK(metadata.front().high_water_lsn == checkpoint_lsn);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill rehydrates spool SELECT worktables")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_spool_select_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * kWalBlockSize;
    config.buffer_size = 2U * kWalBlockSize;
    config.start_lsn = kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 51001U;
    alignas(8) std::array<std::byte, kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<SpoolTestRow> baseline_rows;
    baseline_rows.reserve(3U);
    for (std::uint64_t index = 0U; index < 3U; ++index) {
        const auto row_id = 10'000U + index;
        std::vector<std::byte> payload(24U + index * 8U);
        for (std::size_t pos = 0; pos < payload.size(); ++pos) {
            payload[pos] = static_cast<std::byte>((index + 1U) * (pos + 3U));
        }
        PageManager::TupleInsertResult insert_result{};
        REQUIRE_FALSE(manager.insert_tuple(page_span,
                                           std::span<const std::byte>(payload.data(), payload.size()),
                                           row_id,
                                           insert_result));
        baseline_rows.push_back(SpoolTestRow{row_id, insert_result.slot.index, std::move(payload)});
    }

    auto insert_commit = make_commit_header(wal_writer, 4000U, 4001U, 4000U);
    WalAppendResult insert_commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(insert_commit, insert_commit_result));
    REQUIRE_FALSE(wal_writer->flush());

    std::vector<std::uint16_t> slot_order;
    slot_order.reserve(baseline_rows.size());
    for (const auto& row : baseline_rows) {
        slot_order.push_back(row.slot_index);
    }

    auto baseline_tuple_rows = collect_page_rows(std::span<const std::byte>(page_span.data(), page_span.size()), slot_order);

    bored::executor::WorkTableRegistry baseline_registry;
    bored::executor::SpoolExecutor::Config baseline_config{};
    baseline_config.reserve_rows = baseline_tuple_rows.size();
    baseline_config.worktable_registry = &baseline_registry;
    baseline_config.worktable_id = 0x5A5A5A5AU;

    std::size_t baseline_child_reads = 0U;
    auto baseline_child = std::make_unique<VectorChild>(&baseline_tuple_rows, &baseline_child_reads);
    auto baseline_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(baseline_child), baseline_config);

    bored::executor::ExecutorContext baseline_context{};
    bored::txn::Snapshot baseline_snapshot{};
    baseline_snapshot.read_lsn = insert_commit.commit_lsn;
    baseline_context.set_snapshot(baseline_snapshot);

    std::vector<std::vector<std::byte>> materialized_rows;
    bored::executor::TupleBuffer tuple_buffer{};

    baseline_spool->open(baseline_context);
    while (baseline_spool->next(baseline_context, tuple_buffer)) {
        auto span = tuple_buffer.span();
        materialized_rows.emplace_back(span.begin(), span.end());
        tuple_buffer.reset();
    }
    baseline_spool->close(baseline_context);

    CHECK(baseline_child_reads == baseline_tuple_rows.size());
    REQUIRE(materialized_rows == baseline_tuple_rows);

    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, config.file_prefix, config.file_extension, nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    WalReplayContext replay_context{PageType::Table, nullptr};
    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = replay_context.get_page(page_id);
    auto replay_span = std::span<const std::byte>(replay_page.data(), replay_page.size());
    auto recovered_rows = collect_page_rows(replay_span, slot_order);
    REQUIRE(recovered_rows == baseline_tuple_rows);

    bored::executor::WorkTableRegistry recovered_registry;
    bored::executor::SpoolExecutor::Config recovered_config{};
    recovered_config.reserve_rows = recovered_rows.size();
    recovered_config.worktable_registry = &recovered_registry;
    recovered_config.worktable_id = 0x5A5A5A5AU;

    std::size_t recovered_child_reads = 0U;
    auto recovered_child = std::make_unique<VectorChild>(&recovered_rows, &recovered_child_reads);
    auto recovered_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(recovered_child), recovered_config);

    bored::executor::ExecutorContext recovered_context{};
    recovered_context.set_snapshot(baseline_snapshot);

    std::vector<std::vector<std::byte>> rehydrated_rows;
    bored::executor::TupleBuffer recovered_buffer{};

    recovered_spool->open(recovered_context);
    while (recovered_spool->next(recovered_context, recovered_buffer)) {
        auto span = recovered_buffer.span();
        rehydrated_rows.emplace_back(span.begin(), span.end());
        recovered_buffer.reset();
    }
    recovered_spool->close(recovered_context);

    CHECK(recovered_child_reads == recovered_rows.size());
    REQUIRE(rehydrated_rows == recovered_rows);

    std::size_t cached_child_reads = 0U;
    auto cached_child = std::make_unique<VectorChild>(&recovered_rows, &cached_child_reads);
    auto cached_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(cached_child), recovered_config);

    std::vector<std::vector<std::byte>> cached_rows;
    bored::executor::TupleBuffer cached_buffer{};
    cached_spool->open(recovered_context);
    while (cached_spool->next(recovered_context, cached_buffer)) {
        auto span = cached_buffer.span();
        cached_rows.emplace_back(span.begin(), span.end());
        cached_buffer.reset();
    }
    cached_spool->close(recovered_context);

    CHECK(cached_child_reads == 0U);
    REQUIRE(cached_rows == recovered_rows);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill rehydrates spool UPDATE worktables")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_spool_update_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * kWalBlockSize;
    config.buffer_size = 2U * kWalBlockSize;
    config.start_lsn = kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 52001U;
    alignas(8) std::array<std::byte, kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<SpoolTestRow> rows;
    rows.reserve(3U);
    std::unordered_map<std::uint64_t, std::uint16_t> slot_map;
    std::unordered_map<std::uint64_t, std::vector<std::byte>> expected_payloads;

    for (std::uint64_t index = 0U; index < 3U; ++index) {
        const auto row_id = 20'000U + index;
        std::vector<std::byte> payload(32U + index * 4U);
        for (std::size_t pos = 0; pos < payload.size(); ++pos) {
            payload[pos] = static_cast<std::byte>((pos + 7U) ^ (index + 3U));
        }
        PageManager::TupleInsertResult insert_result{};
        REQUIRE_FALSE(manager.insert_tuple(page_span,
                                           std::span<const std::byte>(payload.data(), payload.size()),
                                           row_id,
                                           insert_result));
        rows.push_back(SpoolTestRow{row_id, insert_result.slot.index, payload});
        slot_map[row_id] = insert_result.slot.index;
        expected_payloads[row_id] = rows.back().payload;
    }

    auto insert_commit = make_commit_header(wal_writer, 5000U, 5001U, 5000U);
    WalAppendResult insert_commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(insert_commit, insert_commit_result));

    std::vector<std::vector<std::byte>> update_payloads;
    update_payloads.reserve(rows.size());
    for (std::size_t idx = 0U; idx < rows.size(); ++idx) {
        auto payload = rows[idx].payload;
        for (std::size_t pos = 0; pos < payload.size(); ++pos) {
            payload[pos] = static_cast<std::byte>((payload[pos] ^ std::byte{0x5A}) + static_cast<std::byte>(idx + 1U));
        }
        expected_payloads[rows[idx].row_id] = payload;
        update_payloads.push_back(std::move(payload));
    }

    std::vector<std::vector<std::byte>> update_rows;
    update_rows.reserve(rows.size());
    for (std::size_t idx = 0U; idx < rows.size(); ++idx) {
        update_rows.push_back(make_update_tuple_row(rows[idx].row_id,
                                                    std::span<const std::byte>(update_payloads[idx].data(),
                                                                               update_payloads[idx].size())));
    }

    bored::executor::WorkTableRegistry registry;
    bored::executor::SpoolExecutor::Config spool_config{};
    spool_config.reserve_rows = update_rows.size();
    spool_config.worktable_registry = &registry;
    spool_config.worktable_id = 0x6B6B6B6BU;

    std::size_t child_reads = 0U;
    auto child = std::make_unique<VectorChild>(&update_rows, &child_reads);
    auto spool_node = std::make_unique<bored::executor::SpoolExecutor>(std::move(child), spool_config);

    PageUpdateTarget update_target{&manager, page_span, &slot_map, &expected_payloads};
    bored::executor::UpdateExecutor::Config update_config{};
    update_config.target = &update_target;

    bored::executor::UpdateExecutor update_executor{std::move(spool_node), update_config};

    bored::executor::ExecutorContext context{};
    bored::txn::Snapshot snapshot{};
    snapshot.read_lsn = wal_writer->next_lsn();
    context.set_snapshot(snapshot);

    bored::executor::TupleBuffer sink{};
    update_executor.open(context);
    while (update_executor.next(context, sink)) {
        sink.reset();
    }
    update_executor.close(context);

    CHECK(child_reads == update_rows.size());
    CHECK(update_target.updated_count() == update_rows.size());

    auto update_commit = make_commit_header(wal_writer, 5001U, 5002U, 5001U);
    WalAppendResult update_commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(update_commit, update_commit_result));
    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, config.file_prefix, config.file_extension, nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    WalReplayContext replay_context{PageType::Table, nullptr};
    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = replay_context.get_page(page_id);
    auto replay_span = std::span<const std::byte>(replay_page.data(), replay_page.size());

    std::vector<std::uint16_t> slot_order;
    slot_order.reserve(rows.size());
    for (const auto& row : rows) {
        auto it = slot_map.find(row.row_id);
        REQUIRE(it != slot_map.end());
        slot_order.push_back(it->second);
        auto tuple_span = bored::storage::read_tuple(replay_span, it->second);
        auto payload = tuple_payload_vector(tuple_span);
        const auto expected_it = expected_payloads.find(row.row_id);
        REQUIRE(expected_it != expected_payloads.end());
        REQUIRE(payload == expected_it->second);
    }

    auto recovered_rows = collect_page_rows(replay_span, slot_order);

    bored::executor::WorkTableRegistry recovered_registry;
    bored::executor::SpoolExecutor::Config recovered_config{};
    recovered_config.reserve_rows = recovered_rows.size();
    recovered_config.worktable_registry = &recovered_registry;
    recovered_config.worktable_id = 0x6B6B6B6BU;

    std::size_t recovered_child_reads = 0U;
    auto recovered_child = std::make_unique<VectorChild>(&recovered_rows, &recovered_child_reads);
    auto recovered_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(recovered_child), recovered_config);

    bored::executor::ExecutorContext recovered_context{};
    snapshot.read_lsn = update_commit.commit_lsn;
    recovered_context.set_snapshot(snapshot);

    std::vector<std::vector<std::byte>> rehydrated_rows;
    bored::executor::TupleBuffer recovered_buffer{};

    recovered_spool->open(recovered_context);
    while (recovered_spool->next(recovered_context, recovered_buffer)) {
        auto span = recovered_buffer.span();
        rehydrated_rows.emplace_back(span.begin(), span.end());
        recovered_buffer.reset();
    }
    recovered_spool->close(recovered_context);

    CHECK(recovered_child_reads == recovered_rows.size());
    REQUIRE(rehydrated_rows == recovered_rows);

    std::size_t cached_child_reads = 0U;
    auto cached_child = std::make_unique<VectorChild>(&recovered_rows, &cached_child_reads);
    auto cached_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(cached_child), recovered_config);

    std::vector<std::vector<std::byte>> cached_rows;
    bored::executor::TupleBuffer cached_buffer{};
    cached_spool->open(recovered_context);
    while (cached_spool->next(recovered_context, cached_buffer)) {
        auto span = cached_buffer.span();
        cached_rows.emplace_back(span.begin(), span.end());
        cached_buffer.reset();
    }
    cached_spool->close(recovered_context);

    CHECK(cached_child_reads == 0U);
    REQUIRE(cached_rows == recovered_rows);

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("Wal crash drill rehydrates spool DELETE worktables")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_spool_delete_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 4U * kWalBlockSize;
    config.buffer_size = 2U * kWalBlockSize;
    config.start_lsn = kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    constexpr std::uint32_t page_id = 53001U;
    alignas(8) std::array<std::byte, kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::vector<SpoolTestRow> rows;
    rows.reserve(3U);
    std::unordered_map<std::uint64_t, std::uint16_t> slot_map;

    for (std::uint64_t index = 0U; index < 3U; ++index) {
        const auto row_id = 30'000U + index;
        std::vector<std::byte> payload(28U + index * 6U);
        for (std::size_t pos = 0; pos < payload.size(); ++pos) {
            payload[pos] = static_cast<std::byte>((pos + 11U) * (index + 5U));
        }
        PageManager::TupleInsertResult insert_result{};
        REQUIRE_FALSE(manager.insert_tuple(page_span,
                                           std::span<const std::byte>(payload.data(), payload.size()),
                                           row_id,
                                           insert_result));
        rows.push_back(SpoolTestRow{row_id, insert_result.slot.index, std::move(payload)});
        slot_map[row_id] = insert_result.slot.index;
    }

    auto insert_commit = make_commit_header(wal_writer, 6000U, 6001U, 6000U);
    WalAppendResult insert_commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(insert_commit, insert_commit_result));

    std::vector<std::vector<std::byte>> delete_rows;
    delete_rows.reserve(rows.size());
    for (const auto& row : rows) {
        delete_rows.push_back(make_delete_tuple_row(row.row_id));
    }

    bored::executor::WorkTableRegistry registry;
    bored::executor::SpoolExecutor::Config spool_config{};
    spool_config.reserve_rows = delete_rows.size();
    spool_config.worktable_registry = &registry;
    spool_config.worktable_id = 0x7C7C7C7CU;

    std::size_t child_reads = 0U;
    auto child = std::make_unique<VectorChild>(&delete_rows, &child_reads);
    auto spool_node = std::make_unique<bored::executor::SpoolExecutor>(std::move(child), spool_config);

    PageDeleteTarget delete_target{&manager, page_span, &slot_map};
    bored::executor::DeleteExecutor::Config delete_config{};
    delete_config.target = &delete_target;

    bored::executor::DeleteExecutor delete_executor{std::move(spool_node), delete_config};

    bored::executor::ExecutorContext context{};
    bored::txn::Snapshot snapshot{};
    snapshot.read_lsn = wal_writer->next_lsn();
    context.set_snapshot(snapshot);

    bored::executor::TupleBuffer sink{};
    delete_executor.open(context);
    while (delete_executor.next(context, sink)) {
        sink.reset();
    }
    delete_executor.close(context);

    CHECK(child_reads == delete_rows.size());
    CHECK(delete_target.deleted_count() == delete_rows.size());

    auto delete_commit = make_commit_header(wal_writer, 6001U, 6002U, 6001U);
    WalAppendResult delete_commit_result{};
    REQUIRE_FALSE(wal_writer->append_commit_record(delete_commit, delete_commit_result));
    REQUIRE_FALSE(manager.flush_wal());
    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();

    WalRecoveryDriver driver{wal_dir, config.file_prefix, config.file_extension, nullptr, wal_dir / "checkpoints"};
    WalRecoveryPlan plan{};
    REQUIRE_FALSE(driver.build_plan(plan));

    WalReplayContext replay_context{PageType::Table, nullptr};
    WalReplayer replayer{replay_context};
    REQUIRE_FALSE(replayer.apply_redo(plan));
    REQUIRE_FALSE(replayer.apply_undo(plan));

    auto replay_page = replay_context.get_page(page_id);
    auto header = bored::storage::page_header(std::span<const std::byte>(replay_page.data(), replay_page.size()));
    CHECK(header.tuple_count == 0U);

    std::vector<std::vector<std::byte>> recovered_rows;
    bored::executor::WorkTableRegistry recovered_registry;
    bored::executor::SpoolExecutor::Config recovered_config{};
    recovered_config.reserve_rows = 0U;
    recovered_config.worktable_registry = &recovered_registry;
    recovered_config.worktable_id = 0x7C7C7C7CU;

    bored::executor::ExecutorContext recovered_context{};
    snapshot.read_lsn = delete_commit.commit_lsn;
    recovered_context.set_snapshot(snapshot);

    std::size_t recovered_child_reads = 0U;
    auto recovered_child = std::make_unique<VectorChild>(&recovered_rows, &recovered_child_reads);
    auto recovered_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(recovered_child), recovered_config);

    bored::executor::TupleBuffer recovered_buffer{};
    recovered_spool->open(recovered_context);
    while (recovered_spool->next(recovered_context, recovered_buffer)) {
        recovered_buffer.reset();
    }
    recovered_spool->close(recovered_context);

    CHECK(recovered_child_reads == 0U);

    std::size_t cached_child_reads = 0U;
    auto cached_child = std::make_unique<VectorChild>(&recovered_rows, &cached_child_reads);
    auto cached_spool = std::make_unique<bored::executor::SpoolExecutor>(std::move(cached_child), recovered_config);
    bored::executor::TupleBuffer cached_buffer{};
    cached_spool->open(recovered_context);
    while (cached_spool->next(recovered_context, cached_buffer)) {
        cached_buffer.reset();
    }
    cached_spool->close(recovered_context);

    CHECK(cached_child_reads == 0U);

    (void)std::filesystem::remove_all(wal_dir);
}

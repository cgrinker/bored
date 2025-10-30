#include "bored/storage/page_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_page_registry.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_format.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_reader.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <cstring>
#include <span>
#include <unordered_map>
#include <vector>

using bored::storage::align_up_to_block;
using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::CheckpointPageRegistry;
using bored::storage::FreeSpaceMap;
using bored::storage::PageManager;
using bored::storage::PageLatchMode;
using bored::storage::PageType;
using bored::storage::TupleSlot;
using bored::storage::WalAppendResult;
using bored::storage::WalCheckpointDirtyPageEntry;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalSegmentHeader;
using bored::storage::WalTupleMeta;
using bored::storage::WalTupleUpdateMeta;
using bored::storage::WalOverflowTruncateMeta;
using bored::storage::WalOverflowChunkMeta;
using bored::storage::WalCompactionEntry;
using bored::storage::WalCompactionView;
using bored::storage::WalIndexMaintenanceAction;
using bored::storage::any;
using bored::storage::WalReader;
using bored::storage::TupleHeader;
using bored::storage::tuple_header_size;
using bored::storage::tuple_storage_length;
using bored::txn::TransactionIdAllocatorStub;
using bored::txn::TransactionManager;

namespace {

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
    (void)std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

std::vector<std::byte> read_file_bytes(const std::filesystem::path& path)
{
    std::ifstream stream(path, std::ios::binary);
    REQUIRE(stream.good());
    stream.seekg(0, std::ios::end);
    const auto size = static_cast<std::size_t>(stream.tellg());
    stream.seekg(0, std::ios::beg);
    std::vector<std::byte> buffer(size);
    stream.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
    REQUIRE(stream.good());
    return buffer;
}

std::span<const std::byte> wal_payload_view(const WalRecordHeader& header, const std::byte* base)
{
    const auto payload_size = header.total_length - static_cast<std::uint32_t>(sizeof(WalRecordHeader));
    return {base + sizeof(WalRecordHeader), payload_size};
}

std::span<const std::byte> tuple_payload_view(std::span<const std::byte> storage, std::size_t tuple_length)
{
    const auto header_size = tuple_header_size();
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

TupleHeader decode_tuple_header(std::span<const std::byte> storage)
{
    TupleHeader header{};
    if (storage.size() >= tuple_header_size()) {
        std::memcpy(&header, storage.data(), tuple_header_size());
    }
    return header;
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

}  // namespace

TEST_CASE("PageManager insert tuple logs WAL record")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_insert_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    auto init_ec = manager.initialize_page(page_span, PageType::Table, 100U);
    REQUIRE_FALSE(init_ec);

    const std::array<std::byte, 5> tuple{std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'}, std::byte{'o'}};
    PageManager::TupleInsertResult insert_result{};
    auto insert_ec = manager.insert_tuple(page_span, tuple, 1234U, insert_result);
    REQUIRE_FALSE(insert_ec);

    const auto& header = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    REQUIRE(header.lsn == insert_result.wal.lsn);
    REQUIRE(insert_result.slot.length == tuple_storage_length(tuple.size()));

    REQUIRE_FALSE(manager.flush_wal());

    auto segment_path = wal_writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));

    auto bytes = read_file_bytes(segment_path);
    REQUIRE(bytes.size() >= bored::storage::kWalBlockSize * 2U);

    const auto* segment_header = reinterpret_cast<const WalSegmentHeader*>(bytes.data());
    REQUIRE(segment_header->start_lsn == wal_config.start_lsn);
    REQUIRE(segment_header->end_lsn == insert_result.wal.lsn + insert_result.wal.written_bytes);

    const auto* record_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize);
    REQUIRE(static_cast<WalRecordType>(record_header->type) == WalRecordType::TupleInsert);
    REQUIRE(record_header->page_id == header.page_id);
    REQUIRE(record_header->lsn == insert_result.wal.lsn);

    auto payload = wal_payload_view(*record_header, bytes.data() + bored::storage::kWalBlockSize);
    auto meta = bored::storage::decode_wal_tuple_meta(payload);
    REQUIRE(meta);
    REQUIRE(meta->page_id == header.page_id);
    REQUIRE(meta->slot_index == insert_result.slot.index);
    REQUIRE(meta->tuple_length == insert_result.slot.length);

    auto tuple_storage = bored::storage::wal_tuple_payload(payload, *meta);
    REQUIRE(tuple_storage.size() == tuple_storage_length(tuple.size()));
    auto header_view = decode_tuple_header(tuple_storage);
    REQUIRE(header_view.created_transaction_id == 0U);
    REQUIRE(header_view.deleted_transaction_id == 0U);
    auto logical_payload = tuple_payload_view(tuple_storage);
    REQUIRE(logical_payload.size() == tuple.size());
    REQUIRE(std::equal(logical_payload.begin(), logical_payload.end(), tuple.begin(), tuple.end()));

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager delete tuple logs WAL record")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_delete_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    auto init_ec = manager.initialize_page(page_span, PageType::Table, 77U);
    REQUIRE_FALSE(init_ec);

    const std::array<std::byte, 3> tuple{std::byte{'f'}, std::byte{'o'}, std::byte{'o'}};
    PageManager::TupleInsertResult insert_result{};
    auto insert_ec = manager.insert_tuple(page_span, tuple, 9000U, insert_result);
    REQUIRE_FALSE(insert_ec);

    PageManager::TupleDeleteResult delete_result{};
    auto ec = manager.delete_tuple(page_span, insert_result.slot.index, 9000U, delete_result);
    REQUIRE_FALSE(ec);

    REQUIRE_FALSE(manager.flush_wal());

    auto segment_path = wal_writer->segment_path(0U);
    auto bytes = read_file_bytes(segment_path);

    const auto* first_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize);
    auto first_aligned = align_up_to_block(first_header->total_length);

    const auto* second_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize + first_aligned);
    REQUIRE(static_cast<WalRecordType>(second_header->type) == WalRecordType::TupleBeforeImage);
    REQUIRE(second_header->prev_lsn == first_header->lsn);

    auto second_payload = wal_payload_view(*second_header,
                                           bytes.data() + bored::storage::kWalBlockSize + first_aligned);
    auto before_view = bored::storage::decode_wal_tuple_before_image(second_payload);
    REQUIRE(before_view);
    REQUIRE(before_view->meta.slot_index == insert_result.slot.index);
    REQUIRE(before_view->meta.page_id == 77U);
    REQUIRE(before_view->tuple_payload.size() == tuple_storage_length(tuple.size()));
    auto before_payload = tuple_payload_view(before_view->tuple_payload, before_view->meta.tuple_length);
    REQUIRE(before_payload.size() == tuple.size());
    REQUIRE(std::equal(before_payload.begin(), before_payload.end(), tuple.begin(), tuple.end()));
    REQUIRE(before_view->overflow_chunks.empty());

    auto second_aligned = align_up_to_block(second_header->total_length);

    const auto* third_header = reinterpret_cast<const WalRecordHeader*>(
        bytes.data() + bored::storage::kWalBlockSize + first_aligned + second_aligned);
    REQUIRE(static_cast<WalRecordType>(third_header->type) == WalRecordType::TupleDelete);
    REQUIRE(third_header->lsn == delete_result.wal.lsn);
    REQUIRE(third_header->prev_lsn == second_header->lsn);

    auto payload = wal_payload_view(*third_header,
                                    bytes.data() + bored::storage::kWalBlockSize + first_aligned + second_aligned);
    auto meta = bored::storage::decode_wal_tuple_meta(payload);
    REQUIRE(meta);
    REQUIRE(meta->slot_index == insert_result.slot.index);
    REQUIRE(meta->page_id == 77U);

    REQUIRE(bored::storage::read_tuple(std::span<const std::byte>(page_span.data(), page_span.size()),
                                       insert_result.slot.index)
                .empty());

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager registers dirty pages with checkpoint registry")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_checkpoint_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    CheckpointPageRegistry checkpoint_registry;

    PageManager::Config manager_config{};
    manager_config.checkpoint_registry = &checkpoint_registry;

    PageManager manager{&fsm, wal_writer, manager_config};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    constexpr std::uint32_t page_id = 501U;
    auto init_ec = manager.initialize_page(page_span, PageType::Table, page_id);
    REQUIRE_FALSE(init_ec);

    const std::array<std::byte, 8> inline_tuple{
        std::byte{'c'}, std::byte{'h'}, std::byte{'e'}, std::byte{'c'},
        std::byte{'k'}, std::byte{'p'}, std::byte{'t'}, std::byte{'1'}};

    PageManager::TupleInsertResult inline_result{};
    auto inline_ec = manager.insert_tuple(page_span, inline_tuple, 1U, inline_result);
    REQUIRE_FALSE(inline_ec);

    std::vector<WalCheckpointDirtyPageEntry> entries;
    checkpoint_registry.snapshot_into(entries);
    std::unordered_map<std::uint32_t, std::uint64_t> page_map;
    for (const auto& entry : entries) {
        page_map.emplace(entry.page_id, entry.page_lsn);
    }

    REQUIRE(page_map.size() == 1U);
    REQUIRE(page_map.at(page_id) == inline_result.wal.lsn);

    checkpoint_registry.clear();
    entries.clear();
    page_map.clear();

    alignas(8) std::array<std::byte, bored::storage::kPageSize> overflow_page_buffer{};
    auto overflow_span = std::span<std::byte>(overflow_page_buffer.data(), overflow_page_buffer.size());
    constexpr std::uint32_t overflow_page_id = 777U;
    auto overflow_init = manager.initialize_page(overflow_span, PageType::Table, overflow_page_id);
    REQUIRE_FALSE(overflow_init);

    std::vector<std::byte> overflow_payload(9000U, std::byte{'X'});
    PageManager::TupleInsertResult overflow_result{};
    auto overflow_ec = manager.insert_tuple(overflow_span,
                                            std::span<const std::byte>(overflow_payload.data(), overflow_payload.size()),
                                            2U,
                                            overflow_result);
    REQUIRE_FALSE(overflow_ec);
    REQUIRE(overflow_result.used_overflow);

    checkpoint_registry.snapshot_into(entries);
    for (const auto& entry : entries) {
        page_map[entry.page_id] = entry.page_lsn;
    }

    REQUIRE(page_map.contains(overflow_page_id));
    REQUIRE(page_map.at(overflow_page_id) == overflow_result.wal.lsn);
    REQUIRE(page_map.size() == overflow_result.overflow_page_ids.size() + 1U);
    for (auto overflow_page_id : overflow_result.overflow_page_ids) {
        REQUIRE(page_map.contains(overflow_page_id));
        REQUIRE(page_map.at(overflow_page_id) >= overflow_result.wal.lsn);
    }

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager update tuple logs WAL record")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_update_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    auto init_ec = manager.initialize_page(page_span, PageType::Table, 501U);
    REQUIRE_FALSE(init_ec);

    const std::array<std::byte, 4> original{std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'a'}};
    PageManager::TupleInsertResult insert_result{};
    auto insert_ec = manager.insert_tuple(page_span, original, 6000U, insert_result);
    REQUIRE_FALSE(insert_ec);

    const std::array<std::byte, 9> updated{std::byte{'u'}, std::byte{'p'}, std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'e'}, std::byte{'d'}, std::byte{'!'}, std::byte{'!'}};
    PageManager::TupleUpdateResult update_result{};
    auto update_ec = manager.update_tuple(page_span, insert_result.slot.index, updated, 6000U, update_result);
    REQUIRE_FALSE(update_ec);
    REQUIRE(update_result.slot.index == insert_result.slot.index);
    REQUIRE(update_result.slot.length == tuple_storage_length(updated.size()));
    REQUIRE(update_result.old_length == original.size());

    auto tuple_view = bored::storage::read_tuple(std::span<const std::byte>(page_span.data(), page_span.size()), update_result.slot.index);
    REQUIRE(tuple_view.size() == updated.size());
    REQUIRE(std::equal(tuple_view.begin(), tuple_view.end(), updated.begin(), updated.end()));

    REQUIRE_FALSE(manager.flush_wal());

    auto segment_path = wal_writer->segment_path(0U);
    auto bytes = read_file_bytes(segment_path);

    const auto* first_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize);
    auto first_aligned = align_up_to_block(first_header->total_length);

    const auto* second_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize + first_aligned);
    REQUIRE(static_cast<WalRecordType>(second_header->type) == WalRecordType::TupleBeforeImage);
    REQUIRE(second_header->prev_lsn == first_header->lsn);
    REQUIRE(second_header->page_id == 501U);

    auto second_payload = wal_payload_view(*second_header, bytes.data() + bored::storage::kWalBlockSize + first_aligned);
    auto before_view = bored::storage::decode_wal_tuple_before_image(second_payload);
    REQUIRE(before_view);
    REQUIRE(before_view->meta.slot_index == insert_result.slot.index);
    REQUIRE(before_view->meta.page_id == 501U);
    REQUIRE(before_view->meta.tuple_length == tuple_storage_length(original.size()));
    REQUIRE(before_view->tuple_payload.size() == tuple_storage_length(original.size()));
    auto before_payload = tuple_payload_view(before_view->tuple_payload, before_view->meta.tuple_length);
    REQUIRE(before_payload.size() == original.size());
    REQUIRE(std::equal(before_payload.begin(), before_payload.end(), original.begin(), original.end()));
    REQUIRE(before_view->overflow_chunks.empty());

    auto second_aligned = align_up_to_block(second_header->total_length);

    const auto* third_header = reinterpret_cast<const WalRecordHeader*>(
        bytes.data() + bored::storage::kWalBlockSize + first_aligned + second_aligned);
    REQUIRE(static_cast<WalRecordType>(third_header->type) == WalRecordType::TupleUpdate);
    REQUIRE(third_header->prev_lsn == second_header->lsn);
    REQUIRE(third_header->page_id == 501U);

    auto payload = wal_payload_view(*third_header,
                                    bytes.data() + bored::storage::kWalBlockSize + first_aligned + second_aligned);
    auto meta = bored::storage::decode_wal_tuple_update_meta(payload);
    REQUIRE(meta);
    REQUIRE(meta->base.page_id == 501U);
    REQUIRE(meta->base.slot_index == insert_result.slot.index);
    REQUIRE(meta->base.tuple_length == tuple_storage_length(updated.size()));
    REQUIRE(meta->old_length == original.size());

    auto payload_bytes = bored::storage::wal_tuple_update_payload(payload, *meta);
    REQUIRE(payload_bytes.size() == tuple_storage_length(updated.size()));
    auto update_payload = tuple_payload_view(payload_bytes, meta->base.tuple_length);
    REQUIRE(update_payload.size() == updated.size());
    REQUIRE(std::equal(update_payload.begin(), update_payload.end(), updated.begin(), updated.end()));

    auto update_header = decode_tuple_header(payload_bytes);
    CHECK(update_header.undo_next_lsn == second_header->lsn);

    auto tuple_storage_view = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), update_result.slot.index);
    auto header_view = decode_tuple_header(tuple_storage_view);
    CHECK(header_view.undo_next_lsn == second_header->lsn);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort undoes inline insert", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_insert_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 501U));

    TransactionIdAllocatorStub allocator{10U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    const std::array<std::byte, 4> payload{std::byte{'t'}, std::byte{'e'}, std::byte{'s'}, std::byte{'t'}};
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 123U, insert_result, TupleHeader{}, &ctx));

    auto directory_before = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    REQUIRE(directory_before[insert_result.slot.index].length != 0U);

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto directory_after = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(directory_after[insert_result.slot.index].length == 0U);

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort restores deleted tuple", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_delete_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 601U));

    const std::array<std::byte, 3> base_payload{std::byte{'f'}, std::byte{'o'}, std::byte{'o'}};
    PageManager::TupleInsertResult base_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, base_payload, 321U, base_insert));

    TransactionIdAllocatorStub allocator{20U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, base_insert.slot.index, 321U, delete_result, &ctx));

    auto directory_after_delete = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    REQUIRE(directory_after_delete[base_insert.slot.index].length == 0U);

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto restored_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), base_insert.slot.index);
    REQUIRE(restored_storage.size() == bored::storage::tuple_storage_length(base_payload.size()));
    auto restored_payload = tuple_payload_view(restored_storage);
    REQUIRE(restored_payload.size() == base_payload.size());
    CHECK(std::equal(restored_payload.begin(), restored_payload.end(), base_payload.begin(), base_payload.end()));

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort reverts tuple update", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_update_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 701U));

    const std::array<std::byte, 3> original_payload{std::byte{'b'}, std::byte{'a'}, std::byte{'r'}};
    PageManager::TupleInsertResult base_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, original_payload, 555U, base_insert));

    TransactionIdAllocatorStub allocator{30U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    const std::array<std::byte, 3> updated_payload{std::byte{'b'}, std::byte{'a'}, std::byte{'z'}};
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, base_insert.slot.index, updated_payload, 555U, update_result, &ctx));

    auto updated_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), base_insert.slot.index);
    auto updated_view = tuple_payload_view(updated_storage);
    REQUIRE(std::equal(updated_view.begin(), updated_view.end(), updated_payload.begin(), updated_payload.end()));

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto restored_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), base_insert.slot.index);
    auto restored_view = tuple_payload_view(restored_storage);
    REQUIRE(restored_view.size() == original_payload.size());
    CHECK(std::equal(restored_view.begin(), restored_view.end(), original_payload.begin(), original_payload.end()));

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort unwinds stacked tuple updates", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_stacked_update_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 702U));

    const std::array<std::byte, 5> base_payload{std::byte{'a'}, std::byte{'l'}, std::byte{'p'}, std::byte{'h'}, std::byte{'a'}};
    PageManager::TupleInsertResult base_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, base_payload, 444U, base_insert));

    TransactionIdAllocatorStub allocator{31U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    const std::array<std::byte, 5> first_payload{std::byte{'b'}, std::byte{'e'}, std::byte{'t'}, std::byte{'a'}, std::byte{'1'}};
    PageManager::TupleUpdateResult first_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span, base_insert.slot.index, first_payload, 444U, first_update, &ctx));

    const std::array<std::byte, 5> second_payload{std::byte{'g'}, std::byte{'a'}, std::byte{'m'}, std::byte{'m'}, std::byte{'a'}};
    PageManager::TupleUpdateResult second_update{};
    REQUIRE_FALSE(manager.update_tuple(page_span, first_update.slot.index, second_payload, 444U, second_update, &ctx));

    auto staged_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), second_update.slot.index);
    auto staged_view = tuple_payload_view(staged_storage);
    REQUIRE(staged_view.size() == second_payload.size());
    CHECK(std::equal(staged_view.begin(), staged_view.end(), second_payload.begin(), second_payload.end()));

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto restored_storage = bored::storage::read_tuple_storage(std::span<const std::byte>(page_span.data(), page_span.size()), base_insert.slot.index);
    auto restored_view = tuple_payload_view(restored_storage);
    REQUIRE(restored_view.size() == base_payload.size());
    CHECK(std::equal(restored_view.begin(), restored_view.end(), base_payload.begin(), base_payload.end()));

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort undoes overflow insert", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_overflow_insert_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 7001U));

    std::vector<std::byte> big_payload(9000U);
    for (std::size_t index = 0; index < big_payload.size(); ++index) {
        big_payload[index] = static_cast<std::byte>(index & 0xFFU);
    }

    TransactionIdAllocatorStub allocator{33U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, big_payload, 5555U, insert_result, TupleHeader{}, &ctx));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    auto header_before = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    REQUIRE(bored::storage::has_flag(header_before, bored::storage::PageFlag::HasOverflow));

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto header_after = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK_FALSE(bored::storage::has_flag(header_after, bored::storage::PageFlag::HasOverflow));

    auto directory_after = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(directory_after[insert_result.slot.index].length == 0U);

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort restores overflow delete", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_overflow_delete_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 7002U));

    std::vector<std::byte> big_payload(8192U);
    for (std::size_t index = 0; index < big_payload.size(); ++index) {
        big_payload[index] = static_cast<std::byte>((index * 13U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, big_payload, 7777U, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    TransactionIdAllocatorStub allocator{44U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 7777U, delete_result, &ctx));

    auto directory_after_delete = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    REQUIRE(directory_after_delete[insert_result.slot.index].length == 0U);

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto directory_after_abort = bored::storage::slot_directory(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(directory_after_abort[insert_result.slot.index].length != 0U);

    auto tuple_payload_view = bored::storage::read_tuple(std::span<const std::byte>(page_span.data(), page_span.size()), insert_result.slot.index);
    auto overflow_header = bored::storage::parse_overflow_tuple(tuple_payload_view);
    REQUIRE(overflow_header);
    CHECK(overflow_header->chunk_count == insert_result.overflow_page_ids.size());
    if (!insert_result.overflow_page_ids.empty()) {
        CHECK(overflow_header->first_overflow_page_id == insert_result.overflow_page_ids.front());
    }

    auto inline_payload = bored::storage::overflow_tuple_inline_payload(tuple_payload_view, *overflow_header);
    REQUIRE(inline_payload.size() == insert_result.inline_length);
    REQUIRE(inline_payload.size() <= big_payload.size());
    REQUIRE(std::equal(inline_payload.begin(), inline_payload.end(), big_payload.begin(), big_payload.begin() + inline_payload.size()));

    auto header_after_abort = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(bored::storage::has_flag(header_after_abort, bored::storage::PageFlag::HasOverflow));

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort reverts overflow update", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_overflow_update_abort_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 7003U));

    std::vector<std::byte> big_payload(9100U);
    for (std::size_t index = 0; index < big_payload.size(); ++index) {
        big_payload[index] = static_cast<std::byte>((index * 19U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, big_payload, 8888U, insert_result));
    REQUIRE(insert_result.used_overflow);

    TransactionIdAllocatorStub allocator{55U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    const std::array<std::byte, 32> compact_payload{
        std::byte{'c'}, std::byte{'o'}, std::byte{'m'}, std::byte{'p'},
        std::byte{'a'}, std::byte{'c'}, std::byte{'t'}, std::byte{' '},
        std::byte{'p'}, std::byte{'a'}, std::byte{'y'}, std::byte{'l'},
        std::byte{'o'}, std::byte{'a'}, std::byte{'d'}, std::byte{' '},
        std::byte{'r'}, std::byte{'e'}, std::byte{'p'}, std::byte{'l'},
        std::byte{'a'}, std::byte{'c'}, std::byte{'e'}, std::byte{'s'},
        std::byte{' '}, std::byte{'o'}, std::byte{'v'}, std::byte{'e'},
        std::byte{'r'}, std::byte{'f'}, std::byte{'l'}, std::byte{'o'}};

    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, compact_payload, 8888U, update_result, &ctx));

    auto tuple_after_update = bored::storage::read_tuple(std::span<const std::byte>(page_span.data(), page_span.size()), update_result.slot.index);
    REQUIRE(tuple_after_update.size() == compact_payload.size());
    REQUIRE(std::equal(tuple_after_update.begin(), tuple_after_update.end(), compact_payload.begin(), compact_payload.end()));

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);

    auto tuple_after_abort = bored::storage::read_tuple(std::span<const std::byte>(page_span.data(), page_span.size()), insert_result.slot.index);
    auto overflow_header = bored::storage::parse_overflow_tuple(tuple_after_abort);
    REQUIRE(overflow_header);
    CHECK(overflow_header->chunk_count == insert_result.overflow_page_ids.size());

    auto inline_payload = bored::storage::overflow_tuple_inline_payload(tuple_after_abort, *overflow_header);
    REQUIRE(inline_payload.size() == insert_result.inline_length);
    REQUIRE(inline_payload.size() <= big_payload.size());
    REQUIRE(std::equal(inline_payload.begin(), inline_payload.end(), big_payload.begin(), big_payload.begin() + inline_payload.size()));

    auto header_after_abort = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(bored::storage::has_flag(header_after_abort, bored::storage::PageFlag::HasOverflow));

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager abort invokes latch release callbacks", "[txn]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_abort_release_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    std::atomic<int> release_calls{0};
    PageManager::Config config{};
    config.latch_callbacks.acquire = [](std::uint32_t, PageLatchMode) -> std::error_code {
        return {};
    };
    config.latch_callbacks.release = [&release_calls](std::uint32_t, PageLatchMode) {
        release_calls.fetch_add(1, std::memory_order_relaxed);
    };

    PageManager manager{&fsm, wal_writer, config};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 9101U));

    TransactionIdAllocatorStub allocator{77U};
    TransactionManager txn_manager{allocator};
    auto ctx = txn_manager.begin();

    const std::array<std::byte, 4> payload{std::byte{'l'}, std::byte{'a'}, std::byte{'t'}, std::byte{'c'}};
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, payload, 991U, insert_result, TupleHeader{}, &ctx));

    const auto before_abort = release_calls.load(std::memory_order_relaxed);
    REQUIRE(before_abort >= 1);

    txn_manager.abort(ctx);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);
    const auto after_abort = release_calls.load(std::memory_order_relaxed);
    CHECK(after_abort > before_abort);

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager delete overflow tuple logs truncate record")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_overflow_delete_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 2025U));

    std::vector<std::byte> big_payload(9000U);
    for (std::size_t index = 0; index < big_payload.size(); ++index) {
        big_payload[index] = static_cast<std::byte>(index & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, big_payload, 4242U, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 4242U, delete_result));

    REQUIRE_FALSE(manager.flush_wal());

    std::vector<int> record_types;
    bool truncate_found = false;

    for (std::uint32_t segment_id = 0U;; ++segment_id) {
        auto segment_path = wal_writer->segment_path(segment_id);
        if (!std::filesystem::exists(segment_path)) {
            break;
        }

        auto bytes = read_file_bytes(segment_path);
        auto offset = bored::storage::kWalBlockSize;
        while (offset + sizeof(WalRecordHeader) <= bytes.size()) {
            const auto* header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + offset);
            if (header->total_length == 0U) {
                break;
            }

            const auto type = static_cast<WalRecordType>(header->type);
            record_types.push_back(static_cast<int>(type));

            auto record_payload = wal_payload_view(*header, reinterpret_cast<const std::byte*>(header));

            if (type == WalRecordType::TupleBeforeImage) {
                auto before_view = bored::storage::decode_wal_tuple_before_image(record_payload);
                REQUIRE(before_view);
                REQUIRE(before_view->meta.page_id == 2025U);
                REQUIRE(before_view->overflow_chunks.size() == insert_result.overflow_page_ids.size());
                for (std::size_t index = 0; index < before_view->overflow_chunks.size(); ++index) {
                    const auto& chunk_view = before_view->overflow_chunks[index];
                    REQUIRE(chunk_view.meta.overflow_page_id == insert_result.overflow_page_ids[index]);
                    REQUIRE(chunk_view.payload.size() == chunk_view.meta.chunk_length);
                }
            }

            if (!truncate_found && type == WalRecordType::TupleOverflowTruncate) {
                auto truncate_meta = bored::storage::decode_wal_overflow_truncate_meta(record_payload);
                REQUIRE(truncate_meta);
                CHECK(truncate_meta->owner.page_id == 2025U);
                CHECK(truncate_meta->released_page_count == insert_result.overflow_page_ids.size());

                auto chunk_views = bored::storage::decode_wal_overflow_truncate_chunks(record_payload, *truncate_meta);
                REQUIRE(chunk_views);
                REQUIRE(chunk_views->size() == insert_result.overflow_page_ids.size());
                for (std::size_t index = 0; index < chunk_views->size(); ++index) {
                    const auto& view = chunk_views->at(index);
                    REQUIRE(view.payload.size() == view.meta.chunk_length);
                    REQUIRE(view.meta.overflow_page_id == insert_result.overflow_page_ids[index]);
                }

                truncate_found = true;
            }

            offset += align_up_to_block(header->total_length);
        }
    }

    CAPTURE(record_types);
    REQUIRE(truncate_found);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager insert overflow tuple logs before-image chunks")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_overflow_insert_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;
    PageManager manager{&fsm, wal_writer};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 3131U));

    std::vector<std::byte> big_payload(8192U);
    for (std::size_t index = 0; index < big_payload.size(); ++index) {
        big_payload[index] = static_cast<std::byte>((index * 37U) & 0xFFU);
    }

    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, big_payload, 1212U, insert_result));
    REQUIRE(insert_result.used_overflow);
    REQUIRE_FALSE(insert_result.overflow_page_ids.empty());

    REQUIRE_FALSE(manager.flush_wal());

    const WalRecordHeader* before_header = nullptr;
    bored::storage::WalTupleBeforeImageView before_snapshot{};
    bool have_before_snapshot = false;
    std::size_t chunk_records = 0U;
    bool saw_insert = false;
    std::vector<int> record_types;
    std::uint64_t previous_lsn = 0U;
    bool have_previous_lsn = false;

    for (std::uint32_t segment_id = 0U;; ++segment_id) {
        auto segment_path = wal_writer->segment_path(segment_id);
        if (!std::filesystem::exists(segment_path)) {
            break;
        }

        auto bytes = read_file_bytes(segment_path);
        auto offset = bored::storage::kWalBlockSize;
        while (offset + sizeof(WalRecordHeader) <= bytes.size()) {
            const auto* header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + offset);
            if (header->total_length == 0U) {
                break;
            }

            auto payload_span = wal_payload_view(*header, reinterpret_cast<const std::byte*>(header));
            const auto type = static_cast<WalRecordType>(header->type);
            record_types.push_back(static_cast<int>(type));

            if (have_previous_lsn) {
                CHECK(header->prev_lsn == previous_lsn);
            }

            if (type == WalRecordType::TupleInsert) {
                saw_insert = true;
                auto meta = bored::storage::decode_wal_tuple_meta(payload_span);
                REQUIRE(meta);
                CHECK(meta->page_id == 3131U);
                CHECK(meta->slot_index == insert_result.slot.index);
            } else if (type == WalRecordType::TupleOverflowChunk) {
                ++chunk_records;
            } else if (type == WalRecordType::TupleBeforeImage) {
                auto before_view = bored::storage::decode_wal_tuple_before_image(payload_span);
                REQUIRE(before_view);
                before_snapshot = *before_view;
                before_header = header;
                have_before_snapshot = true;
            }

            previous_lsn = header->lsn;
            have_previous_lsn = true;
            offset += align_up_to_block(header->total_length);
        }
    }

    CAPTURE(record_types);
    CAPTURE(chunk_records);
    CAPTURE(insert_result.inline_length);
    CAPTURE(insert_result.overflow_page_ids.size());

    REQUIRE(saw_insert);
    REQUIRE(have_before_snapshot);
    REQUIRE(before_header != nullptr);
    REQUIRE(chunk_records > 0U);
    REQUIRE(chunk_records == insert_result.overflow_page_ids.size());
    REQUIRE(before_snapshot.meta.page_id == 3131U);
    REQUIRE(before_snapshot.meta.slot_index == insert_result.slot.index);
    const auto header_size = bored::storage::overflow_tuple_header_size();
    const auto expected_stub_storage = tuple_storage_length(header_size + insert_result.inline_length);
    CHECK(before_snapshot.meta.tuple_length == expected_stub_storage);
    REQUIRE(before_snapshot.tuple_payload.size() == expected_stub_storage);
    auto stub_payload = tuple_payload_view(before_snapshot.tuple_payload, before_snapshot.meta.tuple_length);
    REQUIRE(stub_payload.size() == header_size + insert_result.inline_length);
    REQUIRE(before_snapshot.overflow_chunks.size() == chunk_records);

    if (insert_result.inline_length > 0U) {
        auto inline_view = stub_payload.subspan(header_size);
        auto expected_inline = std::span<const std::byte>(big_payload.data(), insert_result.inline_length);
        REQUIRE(std::equal(inline_view.begin(), inline_view.end(), expected_inline.begin(), expected_inline.end()));
    }

    std::size_t expected_offset = insert_result.inline_length;
    for (std::size_t index = 0; index < before_snapshot.overflow_chunks.size(); ++index) {
        const auto& chunk_view = before_snapshot.overflow_chunks[index];
        REQUIRE(chunk_view.meta.chunk_index == index);
        REQUIRE(chunk_view.meta.overflow_page_id == insert_result.overflow_page_ids[index]);
        REQUIRE(chunk_view.payload.size() == chunk_view.meta.chunk_length);
        REQUIRE(expected_offset + chunk_view.meta.chunk_length <= big_payload.size());
        auto expected_payload = std::span<const std::byte>(big_payload.data() + expected_offset, chunk_view.meta.chunk_length);
        REQUIRE(std::equal(expected_payload.begin(), expected_payload.end(), chunk_view.payload.begin(), chunk_view.payload.end()));
        expected_offset += chunk_view.meta.chunk_length;
    }
    REQUIRE(expected_offset == big_payload.size());

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager invokes latch callbacks for tuple mutations")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_latch_hooks_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    std::vector<std::uint32_t> acquired_pages;
    std::vector<PageLatchMode> acquired_modes;
    std::vector<std::uint32_t> released_pages;
    std::vector<PageLatchMode> released_modes;

    PageManager::Config config{};
    config.latch_callbacks.acquire = [&](std::uint32_t page_id, PageLatchMode mode) -> std::error_code {
        acquired_pages.push_back(page_id);
        acquired_modes.push_back(mode);
        return {};
    };
    config.latch_callbacks.release = [&](std::uint32_t page_id, PageLatchMode mode) {
        released_pages.push_back(page_id);
        released_modes.push_back(mode);
    };

    PageManager manager{&fsm, wal_writer, config};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 606U));

    acquired_pages.clear();
    acquired_modes.clear();
    released_pages.clear();
    released_modes.clear();

    const std::array<std::byte, 8> tuple{std::byte{'c'}, std::byte{'o'}, std::byte{'n'}, std::byte{'c'}, std::byte{'u'}, std::byte{'r'}, std::byte{'r'}, std::byte{'y'}};
    PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, tuple, 42U, insert_result));

    REQUIRE(acquired_pages == std::vector<std::uint32_t>{606U});
    REQUIRE(acquired_modes == std::vector<PageLatchMode>{PageLatchMode::Exclusive});
    REQUIRE(released_pages == acquired_pages);
    REQUIRE(released_modes == acquired_modes);

    acquired_pages.clear();
    acquired_modes.clear();
    released_pages.clear();
    released_modes.clear();

    const std::array<std::byte, 12> updated{std::byte{'l'}, std::byte{'a'}, std::byte{'t'}, std::byte{'c'}, std::byte{'h'}, std::byte{'e'}, std::byte{'d'}, std::byte{'_'}, std::byte{'d'}, std::byte{'a'}, std::byte{'t'}, std::byte{'a'}};
    PageManager::TupleUpdateResult update_result{};
    REQUIRE_FALSE(manager.update_tuple(page_span, insert_result.slot.index, updated, 42U, update_result));

    REQUIRE(acquired_pages == std::vector<std::uint32_t>{606U});
    REQUIRE(acquired_modes == std::vector<PageLatchMode>{PageLatchMode::Exclusive});
    REQUIRE(released_pages == acquired_pages);
    REQUIRE(released_modes == acquired_modes);

    acquired_pages.clear();
    acquired_modes.clear();
    released_pages.clear();
    released_modes.clear();

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, insert_result.slot.index, 42U, delete_result));

    REQUIRE(acquired_pages == std::vector<std::uint32_t>{606U});
    REQUIRE(acquired_modes == std::vector<PageLatchMode>{PageLatchMode::Exclusive});
    REQUIRE(released_pages == acquired_pages);
    REQUIRE(released_modes == acquired_modes);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager propagates latch acquire failures")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_latch_failure_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    PageManager::Config config{};
    std::size_t acquire_calls = 0U;
    std::size_t release_calls = 0U;
    bool fail_next_acquire = false;
    const auto expected_error = std::make_error_code(std::errc::resource_unavailable_try_again);

    config.latch_callbacks.acquire = [&](std::uint32_t, PageLatchMode) -> std::error_code {
        ++acquire_calls;
        if (fail_next_acquire) {
            fail_next_acquire = false;
            return expected_error;
        }
        return {};
    };
    config.latch_callbacks.release = [&](std::uint32_t, PageLatchMode) {
        ++release_calls;
    };

    PageManager manager{&fsm, wal_writer, config};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 707U));

    acquire_calls = 0U;
    release_calls = 0U;
    fail_next_acquire = true;

    const std::array<std::byte, 4> tuple{std::byte{'f'}, std::byte{'a'}, std::byte{'i'}, std::byte{'l'}};
    PageManager::TupleInsertResult insert_result{};
    auto acquire_error = manager.insert_tuple(page_span, tuple, 88U, insert_result);
    REQUIRE(acquire_error == expected_error);
    REQUIRE(acquire_calls == 1U);
    REQUIRE(release_calls == 0U);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("PageManager compacts fragmented page and logs metadata")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_page_manager_compact_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    std::vector<WalCompactionEntry> observed_metadata;

    PageManager::Config config{};
    config.index_metadata_callback = [&](const WalCompactionEntry& entry) {
        observed_metadata.push_back(entry);
    };

    PageManager manager{&fsm, wal_writer, config};

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    constexpr std::uint32_t page_id = 900U;
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, page_id));

    std::array<std::byte, 48> first_tuple{};
    first_tuple.fill(std::byte{0x1});
    PageManager::TupleInsertResult first_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, first_tuple, 10U, first_insert));

    std::array<std::byte, 64> second_tuple{};
    second_tuple.fill(std::byte{0x2});
    PageManager::TupleInsertResult second_insert{};
    REQUIRE_FALSE(manager.insert_tuple(page_span, second_tuple, 11U, second_insert));

    const auto before_offset = second_insert.slot.offset;

    PageManager::TupleDeleteResult delete_result{};
    REQUIRE_FALSE(manager.delete_tuple(page_span, first_insert.slot.index, 10U, delete_result));

    PageManager::PageCompactionResult compaction_result{};
    REQUIRE_FALSE(manager.compact_page(page_span, compaction_result));
    REQUIRE(compaction_result.performed);
    REQUIRE(compaction_result.relocations.size() == 1U);
    const auto& relocation = compaction_result.relocations.front();
    CHECK(relocation.slot_index == second_insert.slot.index);
    CHECK(relocation.old_offset == before_offset);
    CHECK(relocation.new_offset == static_cast<std::uint32_t>(sizeof(bored::storage::PageHeader)));
    CHECK(any(static_cast<WalIndexMaintenanceAction>(relocation.index_action)));

    const auto post_header = bored::storage::page_header(std::span<const std::byte>(page_span.data(), page_span.size()));
    CHECK(post_header.fragment_count == 0U);
    CHECK(post_header.free_start == relocation.new_offset + relocation.length);

    REQUIRE(observed_metadata.size() == 1U);
    CHECK(observed_metadata.front().slot_index == relocation.slot_index);
    CHECK(static_cast<WalIndexMaintenanceAction>(observed_metadata.front().index_action) == WalIndexMaintenanceAction::RefreshPointers);

    REQUIRE_FALSE(manager.flush_wal());

    auto segment_path = wal_writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));
    WalReader reader{wal_dir};
    bool found_compaction = false;
    auto visit_ec = reader.for_each_record([&](const auto&, const WalRecordHeader& header, std::span<const std::byte> payload) {
        const auto type = static_cast<WalRecordType>(header.type);
        if (type != WalRecordType::PageCompaction) {
            return true;
        }
        auto decoded = bored::storage::decode_wal_compaction(payload);
        REQUIRE(decoded);
        REQUIRE(decoded->header.entry_count == compaction_result.relocations.size());
        REQUIRE_FALSE(decoded->entries.empty());
        REQUIRE(decoded->entries.front().new_offset == relocation.new_offset);
        found_compaction = true;
        return false;
    });
    REQUIRE_FALSE(visit_ec);
    CHECK(found_compaction);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

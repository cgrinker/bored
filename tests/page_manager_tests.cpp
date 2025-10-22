#include "bored/storage/page_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_format.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <vector>

using bored::storage::align_up_to_block;
using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FreeSpaceMap;
using bored::storage::PageManager;
using bored::storage::PageType;
using bored::storage::TupleSlot;
using bored::storage::WalAppendResult;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalSegmentHeader;
using bored::storage::WalTupleMeta;
using bored::storage::WalTupleUpdateMeta;

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
    REQUIRE(insert_result.slot.length == tuple.size());

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

    auto tuple_payload = bored::storage::wal_tuple_payload(payload, *meta);
    REQUIRE(tuple_payload.size() == tuple.size());
    REQUIRE(std::equal(tuple_payload.begin(), tuple_payload.end(), tuple.begin(), tuple.end()));

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
    auto before_meta = bored::storage::decode_wal_tuple_meta(second_payload);
    REQUIRE(before_meta);
    REQUIRE(before_meta->slot_index == insert_result.slot.index);
    REQUIRE(before_meta->page_id == 77U);
    auto before_payload = bored::storage::wal_tuple_payload(second_payload, *before_meta);
    REQUIRE(before_payload.size() == tuple.size());
    REQUIRE(std::equal(before_payload.begin(), before_payload.end(), tuple.begin(), tuple.end()));

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
    REQUIRE(update_result.slot.length == updated.size());
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
    auto before_meta = bored::storage::decode_wal_tuple_meta(second_payload);
    REQUIRE(before_meta);
    REQUIRE(before_meta->slot_index == insert_result.slot.index);
    REQUIRE(before_meta->page_id == 501U);
    REQUIRE(before_meta->tuple_length == original.size());
    auto before_payload = bored::storage::wal_tuple_payload(second_payload, *before_meta);
    REQUIRE(before_payload.size() == original.size());
    REQUIRE(std::equal(before_payload.begin(), before_payload.end(), original.begin(), original.end()));

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
    REQUIRE(meta->base.tuple_length == updated.size());
    REQUIRE(meta->old_length == original.size());

    auto payload_bytes = bored::storage::wal_tuple_update_payload(payload, *meta);
    REQUIRE(payload_bytes.size() == updated.size());
    REQUIRE(std::equal(payload_bytes.begin(), payload_bytes.end(), updated.begin(), updated.end()));

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

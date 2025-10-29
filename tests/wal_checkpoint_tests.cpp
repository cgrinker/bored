#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::CheckpointManager;
using bored::storage::WalCheckpointDirtyPageEntry;
using bored::storage::WalCheckpointIndexEntry;
using bored::storage::WalCheckpointTxnEntry;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalSegmentHeader;
using bored::storage::WalWriter;

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

TEST_CASE("CheckpointManager emits checkpoint record")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_manager_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    CheckpointManager checkpoint_manager{wal_writer};

    std::array<WalCheckpointDirtyPageEntry, 2> dirty_pages{{
        WalCheckpointDirtyPageEntry{.page_id = 101U, .reserved = 0U, .page_lsn = 4096U},
        WalCheckpointDirtyPageEntry{.page_id = 202U, .reserved = 0U, .page_lsn = 8192U}
    }};

    std::array<WalCheckpointTxnEntry, 1> active_transactions{{
        WalCheckpointTxnEntry{.transaction_id = 55U, .state = 1U, .last_lsn = 16384U}
    }};

    std::array<WalCheckpointIndexEntry, 2> index_metadata{{
        WalCheckpointIndexEntry{.index_id = 77U, .high_water_lsn = 20480U},
        WalCheckpointIndexEntry{.index_id = 88U, .high_water_lsn = 24576U}
    }};

    bored::storage::WalAppendResult append_result{};
    auto ec = checkpoint_manager.emit_checkpoint(9001U,
                                                 12000U,
                                                 11000U,
                                                 dirty_pages,
                                                 active_transactions,
                                                 index_metadata,
                                                 append_result);
    REQUIRE_FALSE(ec);
    REQUIRE(append_result.written_bytes > 0U);

    REQUIRE_FALSE(wal_writer->flush());

    auto segment_path = wal_writer->segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));

    auto bytes = read_file_bytes(segment_path);
    REQUIRE(bytes.size() >= bored::storage::kWalBlockSize * 2U);

    const auto* segment_header = reinterpret_cast<const WalSegmentHeader*>(bytes.data());
    REQUIRE(segment_header->start_lsn == wal_config.start_lsn);
    REQUIRE(segment_header->start_lsn <= append_result.lsn);

    const auto* record_header = reinterpret_cast<const WalRecordHeader*>(bytes.data() + bored::storage::kWalBlockSize);
    REQUIRE(static_cast<WalRecordType>(record_header->type) == WalRecordType::Checkpoint);
    REQUIRE(record_header->page_id == 0U);
    REQUIRE(record_header->lsn == append_result.lsn);

    auto payload = wal_payload_view(*record_header, bytes.data() + bored::storage::kWalBlockSize);
    auto checkpoint_view = bored::storage::decode_wal_checkpoint(payload);
    REQUIRE(checkpoint_view);
    REQUIRE(checkpoint_view->header.checkpoint_id == 9001U);
    REQUIRE(checkpoint_view->header.redo_lsn == 12000U);
    REQUIRE(checkpoint_view->header.undo_lsn == 11000U);
    REQUIRE(checkpoint_view->dirty_pages.size() == dirty_pages.size());
    REQUIRE(checkpoint_view->active_transactions.size() == active_transactions.size());
    REQUIRE(checkpoint_view->index_metadata.size() == index_metadata.size());

    for (std::size_t i = 0; i < dirty_pages.size(); ++i) {
        REQUIRE(checkpoint_view->dirty_pages[i].page_id == dirty_pages[i].page_id);
        REQUIRE(checkpoint_view->dirty_pages[i].page_lsn == dirty_pages[i].page_lsn);
    }

    for (std::size_t i = 0; i < active_transactions.size(); ++i) {
        REQUIRE(checkpoint_view->active_transactions[i].transaction_id == active_transactions[i].transaction_id);
        REQUIRE(checkpoint_view->active_transactions[i].state == active_transactions[i].state);
        REQUIRE(checkpoint_view->active_transactions[i].last_lsn == active_transactions[i].last_lsn);
    }

    for (std::size_t i = 0; i < index_metadata.size(); ++i) {
        REQUIRE(checkpoint_view->index_metadata[i].index_id == index_metadata[i].index_id);
        REQUIRE(checkpoint_view->index_metadata[i].high_water_lsn == index_metadata[i].high_water_lsn);
    }

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

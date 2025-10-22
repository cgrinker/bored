#include "bored/storage/wal_reader.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <vector>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::WalAppendResult;
using bored::storage::WalReader;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;

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
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

}  // namespace

TEST_CASE("WalReader enumerates and validates segments")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_reader_enum_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize / 2U;
    config.flush_on_commit = false;

    WalWriter writer{io, config};

    std::array<std::byte, 128> payload_a{};
    payload_a.fill(std::byte{0xAA});

    std::array<std::byte, 64> payload_b{};
    payload_b.fill(std::byte{0xBB});

    WalAppendResult first{};
    WalAppendResult second{};

    bored::storage::WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 321U;

    descriptor.payload = payload_a;
    REQUIRE_FALSE(writer.append_record(descriptor, first));

    descriptor.payload = payload_b;
    REQUIRE_FALSE(writer.append_record(descriptor, second));

    REQUIRE_FALSE(writer.notify_commit());
    REQUIRE_FALSE(writer.close());

    WalReader reader{wal_dir};
    std::vector<bored::storage::WalSegmentView> segments;
    auto ec = reader.enumerate_segments(segments);
    REQUIRE_FALSE(ec);
    REQUIRE_FALSE(segments.empty());
    REQUIRE(segments.front().header.start_lsn == config.start_lsn);
    REQUIRE(segments.back().header.end_lsn == second.lsn + second.written_bytes);

    std::vector<bored::storage::WalRecordView> records;
    std::vector<std::size_t> sizes;
    for (const auto& seg : segments) {
        ec = reader.read_records(seg, records);
        REQUIRE_FALSE(ec);
        for (const auto& view : records) {
            sizes.push_back(view.payload.size());
        }
    }

    REQUIRE(sizes.size() == 2U);
    REQUIRE(sizes[0] == payload_a.size());
    REQUIRE(sizes[1] == payload_b.size());

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("WalReader for_each_record streams across segments")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_wal_reader_stream_");

    WalWriterConfig config{};
    config.directory = wal_dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;
    config.size_flush_threshold = bored::storage::kWalBlockSize / 4U;
    config.flush_on_commit = false;

    WalWriter writer{io, config};

    std::array<std::byte, 512> payload{};
    payload.fill(std::byte{0x5A});

    bored::storage::WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 999U;
    descriptor.payload = payload;

    std::vector<WalAppendResult> appends;
    for (int i = 0; i < 8; ++i) {
        WalAppendResult result{};
        REQUIRE_FALSE(writer.append_record(descriptor, result));
        appends.push_back(result);
    }

    REQUIRE_FALSE(writer.close());

    WalReader reader{wal_dir};

    std::size_t seen = 0U;
    auto ec = reader.for_each_record([&](const bored::storage::WalSegmentView& segment,
                                         const WalRecordHeader& header,
                                         std::span<const std::byte> record_payload) {
        REQUIRE(static_cast<WalRecordType>(header.type) == WalRecordType::TupleInsert);
        REQUIRE(record_payload.size() == payload.size());
        ++seen;
        (void)segment;
        return true;
    });
    REQUIRE_FALSE(ec);
    REQUIRE(seen == appends.size());

    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

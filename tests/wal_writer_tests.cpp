#include "bored/storage/wal_writer.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checksum.hpp"

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
using bored::storage::WalAppendResult;
using bored::storage::WalRecordDescriptor;
using bored::storage::WalRecordFlag;
using bored::storage::WalRecordHeader;
using bored::storage::WalRecordType;
using bored::storage::WalSegmentHeader;
using bored::storage::WalWriter;
using bored::storage::WalWriterConfig;

namespace {

std::shared_ptr<AsyncIo> make_async_io()
{
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 8U;
    auto instance = bored::storage::create_async_io(config);
    return std::shared_ptr<AsyncIo>(std::move(instance));
}

std::filesystem::path make_temp_dir(const std::string& name)
{
    auto root = std::filesystem::temp_directory_path();
    auto dir = root / (name + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(dir);
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

}  // namespace

TEST_CASE("WalWriter writes aligned record and updates headers")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_writer_");

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 4U * bored::storage::kWalBlockSize;
    config.buffer_size = 2U * bored::storage::kWalBlockSize;
    config.start_lsn = 1024U;

    WalWriter writer{io, config};

    std::array<std::byte, 64> payload{};
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::byte>(index & 0xFFU);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleInsert;
    descriptor.page_id = 42U;
    descriptor.flags = WalRecordFlag::None;
    descriptor.payload = payload;

    WalAppendResult append_result{};
    auto ec = writer.append_record(descriptor, append_result);
    REQUIRE_FALSE(ec);

    ec = writer.flush();
    REQUIRE_FALSE(ec);

    ec = writer.close();
    REQUIRE_FALSE(ec);

    auto segment_path = writer.segment_path(0U);
    REQUIRE(std::filesystem::exists(segment_path));

    auto bytes = read_file_bytes(segment_path);
    REQUIRE(bytes.size() == 2U * bored::storage::kWalBlockSize);

    WalSegmentHeader segment_header{};
    std::memcpy(&segment_header, bytes.data(), sizeof(segment_header));
    REQUIRE(segment_header.segment_id == 0U);
    REQUIRE(segment_header.start_lsn == config.start_lsn);
    REQUIRE(segment_header.end_lsn == append_result.lsn + append_result.written_bytes);

    WalRecordHeader record_header{};
    std::memcpy(&record_header, bytes.data() + bored::storage::kWalBlockSize, sizeof(record_header));

    REQUIRE(record_header.total_length == append_result.total_length);
    REQUIRE(record_header.lsn == append_result.lsn);
    REQUIRE(record_header.prev_lsn == 0U);
    REQUIRE(record_header.page_id == descriptor.page_id);
    REQUIRE((record_header.flags & static_cast<std::uint16_t>(WalRecordFlag::HasPayload)) != 0U);

    auto payload_span = std::span<const std::byte>(bytes.data() + bored::storage::kWalBlockSize + sizeof(WalRecordHeader), payload.size());
    REQUIRE(std::equal(payload_span.begin(), payload_span.end(), payload.begin(), payload.end()));
    REQUIRE(bored::storage::verify_wal_checksum(record_header, payload_span));

    io->shutdown();
    std::filesystem::remove_all(dir);
}

TEST_CASE("WalWriter rotates segments when full")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_rotate_");

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 2U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    std::array<std::byte, 16> payload{};
    payload.fill(std::byte{0xAB});

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::TupleDelete;
    descriptor.page_id = 11U;
    descriptor.payload = payload;

    WalAppendResult first{};
    auto ec = writer.append_record(descriptor, first);
    REQUIRE_FALSE(ec);

    WalAppendResult second{};
    ec = writer.append_record(descriptor, second);
    REQUIRE_FALSE(ec);

    ec = writer.close();
    REQUIRE_FALSE(ec);

    auto first_segment = writer.segment_path(0U);
    auto second_segment = writer.segment_path(1U);

    REQUIRE(std::filesystem::exists(first_segment));
    REQUIRE(std::filesystem::exists(second_segment));

    auto first_bytes = read_file_bytes(first_segment);
    auto second_bytes = read_file_bytes(second_segment);

    WalSegmentHeader first_header{};
    std::memcpy(&first_header, first_bytes.data(), sizeof(first_header));
    REQUIRE(first_header.end_lsn == first.lsn + first.written_bytes);

    WalSegmentHeader second_header{};
    std::memcpy(&second_header, second_bytes.data(), sizeof(second_header));
    REQUIRE(second_header.start_lsn == first_header.end_lsn);
    REQUIRE(second_header.end_lsn == second.lsn + second.written_bytes);

    WalRecordHeader second_record_header{};
    std::memcpy(&second_record_header, second_bytes.data() + bored::storage::kWalBlockSize, sizeof(second_record_header));
    REQUIRE(second_record_header.prev_lsn == first.lsn);

    io->shutdown();
    std::filesystem::remove_all(dir);
}

TEST_CASE("WalWriter grows buffer for large records")
{
    auto io = make_async_io();
    auto dir = make_temp_dir("bored_wal_large_");

    WalWriterConfig config{};
    config.directory = dir;
    config.segment_size = 8U * bored::storage::kWalBlockSize;
    config.buffer_size = bored::storage::kWalBlockSize;

    WalWriter writer{io, config};

    const std::size_t payload_size = 2U * bored::storage::kWalBlockSize;
    std::vector<std::byte> payload(payload_size, std::byte{0x5C});

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::PageImage;
    descriptor.page_id = 999U;
    descriptor.payload = payload;

    WalAppendResult result{};
    auto ec = writer.append_record(descriptor, result);
    REQUIRE_FALSE(ec);
    REQUIRE(result.written_bytes > config.buffer_size);

    ec = writer.close();
    REQUIRE_FALSE(ec);

    auto segment = writer.segment_path(0U);
    REQUIRE(std::filesystem::exists(segment));
    auto bytes = read_file_bytes(segment);
    REQUIRE(bytes.size() == bored::storage::kWalBlockSize + result.written_bytes);

    io->shutdown();
    std::filesystem::remove_all(dir);
}

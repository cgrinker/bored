#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <filesystem>
#include <string_view>
#include <system_error>

using bored::storage::AsyncIo;
using bored::storage::AsyncIoConfig;
using bored::storage::FileClass;
using bored::storage::IoFlag;
using bored::storage::ReadRequest;
using bored::storage::WriteRequest;

namespace {

std::filesystem::path temp_file_path()
{
    auto path = std::filesystem::temp_directory_path() / "bored_async_io_test.bin";
    std::filesystem::remove(path);
    return path;
}

}  // namespace

TEST_CASE("Thread pool async IO writes and reads data")
{
    const auto path = temp_file_path();
    auto io = bored::storage::create_async_io(AsyncIoConfig{.worker_threads = 2U, .queue_depth = 8U});

    constexpr std::string_view payload = "async-io";
    std::array<std::byte, payload.size()> write_buffer{};
    std::transform(payload.begin(), payload.end(), write_buffer.begin(), [](char ch) {
        return std::byte(static_cast<unsigned char>(ch));
    });

    WriteRequest write_req{};
    write_req.path = path;
    write_req.offset = 0U;
    write_req.file_class = FileClass::WriteAheadLog;
    write_req.data = write_buffer.data();
    write_req.size = write_buffer.size();
    write_req.flags = IoFlag::Dsync;

    auto write_result = io->submit_write(write_req).get();
    REQUIRE_FALSE(write_result.status);
    REQUIRE(write_result.bytes_transferred == write_buffer.size());

    auto flush_result = io->flush(FileClass::WriteAheadLog).get();
    REQUIRE_FALSE(flush_result.status);

    std::array<std::byte, payload.size()> read_buffer{};
    ReadRequest read_req{};
    read_req.path = path;
    read_req.offset = 0U;
    read_req.file_class = FileClass::WriteAheadLog;
    read_req.data = read_buffer.data();
    read_req.size = read_buffer.size();

    auto read_result = io->submit_read(read_req).get();
    REQUIRE_FALSE(read_result.status);
    REQUIRE(read_result.bytes_transferred == read_buffer.size());
    REQUIRE(std::equal(read_buffer.begin(), read_buffer.end(), write_buffer.begin(), write_buffer.end()));

    io->shutdown();
    std::filesystem::remove(path);
}

TEST_CASE("Async IO returns error when file missing")
{
    auto io = bored::storage::create_async_io();
    ReadRequest request{};
    request.path = std::filesystem::temp_directory_path() / "bored_async_io_missing.bin";
    request.offset = 0U;
    std::array<std::byte, 4> buffer{};
    request.data = buffer.data();
    request.size = buffer.size();

    auto future = io->submit_read(request);
    auto result = future.get();
    REQUIRE(result.status == std::make_error_code(std::errc::no_such_file_or_directory));
    io->shutdown();
}

#include "bored/storage/async_io.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <string_view>
#include <system_error>
#include <vector>
#include <memory>
#include <future>
#include <iostream>
#include <thread>
#include <atomic>

#if defined(_WIN32)
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    define WIN32_LEAN_AND_MEAN
#    include <windows.h>
#    if defined(__has_include)
#        if __has_include(<ioringapi.h>)
#            include <ioringapi.h>
#            define BORED_TEST_HAVE_IORING 1
#        else
#            define BORED_TEST_HAVE_IORING 0
#        endif
#    else
#        define BORED_TEST_HAVE_IORING 0
#    endif
#else
#    define BORED_TEST_HAVE_IORING 0
#endif

using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;
using bored::storage::FileClass;
using bored::storage::IoFlag;
using bored::storage::ReadRequest;
using bored::storage::WriteRequest;

namespace {

std::filesystem::path temp_file_path()
{
    auto path = std::filesystem::temp_directory_path() / "bored_async_io_ioring_test.bin";
    std::filesystem::remove(path);
    return path;
}

}  // namespace

#if BORED_TEST_HAVE_IORING
TEST_CASE("Windows IoRing backend writes and reads data")
{
    UNSCOPED_INFO("Starting Windows IoRing AsyncIo integration test");
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::WindowsIoRing;
    config.queue_depth = 32U;
    config.worker_threads = 0U;

    std::unique_ptr<bored::storage::AsyncIo> io;
    try {
        UNSCOPED_INFO("Craeting Windows IoRing AsyncIo instance");
        io = bored::storage::create_async_io(config);
    } catch (const std::system_error& error) {
        INFO("Windows IoRing backend unavailable: " << error.code() << " " << error.what());
        return;
    }

    if (!io) {
        INFO("create_async_io returned nullptr; skipping IoRing integration test");
        return;
    }

    const auto path = temp_file_path();

    constexpr std::string_view payload = "windows-ioring-integration";
    std::vector<std::byte> write_buffer(payload.size());
    std::transform(payload.begin(), payload.end(), write_buffer.begin(), [](char ch) {
        return std::byte(static_cast<unsigned char>(ch));
    });

    const auto wait_ready = [&](auto& future, std::string_view step) {
        constexpr auto timeout = std::chrono::seconds{5};
        std::cout << "[ioring-test] waiting up to " << timeout.count() << "s for " << step << std::endl;
        const auto status = future.wait_for(timeout);
        if(status != std::future_status::ready) {
            std::cout << "[ioring-test] timeout while waiting for " << step << std::endl;
            std::filesystem::remove(path);
            abort();
        }
        
        std::cout << "[ioring-test] " << step << " completed" << std::endl;
    };

    WriteRequest write_req{};
    write_req.path = path;
    write_req.offset = 0U;
    write_req.file_class = FileClass::Data;
    write_req.data = write_buffer.data();
    write_req.size = write_buffer.size();
    write_req.flags = IoFlag::Dsync;

    std::cout << "[ioring-test] submit write" << std::endl;
    auto write_future = io->submit_write(write_req);
    wait_ready(write_future, "write submission");
    auto write_result = write_future.get();
    REQUIRE_FALSE(write_result.status);
    REQUIRE(write_result.bytes_transferred == write_buffer.size());

    std::cout << "[ioring-test] submit flush" << std::endl;
    auto flush_future = io->flush(FileClass::Data);
    wait_ready(flush_future, "flush");
    auto flush_result = flush_future.get();
    REQUIRE_FALSE(flush_result.status);

    std::vector<std::byte> read_buffer(write_buffer.size());
    ReadRequest read_req{};
    read_req.path = path;
    read_req.offset = 0U;
    read_req.file_class = FileClass::Data;
    read_req.data = read_buffer.data();
    read_req.size = read_buffer.size();

    std::cout << "[ioring-test] submit read" << std::endl;
    auto read_future = io->submit_read(read_req);
    wait_ready(read_future, "read submission");
    auto read_result = read_future.get();
    REQUIRE_FALSE(read_result.status);
    REQUIRE(read_result.bytes_transferred == read_buffer.size());
    REQUIRE(std::equal(read_buffer.begin(), read_buffer.end(), write_buffer.begin(), write_buffer.end()));

    std::atomic<bool> shutdown_finished{false};
    std::thread watchdog([&shutdown_finished]() {
        constexpr auto timeout = std::chrono::seconds{5};
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!shutdown_finished.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                std::cout << "[ioring-test] shutdown watchdog fired" << std::endl;
                std::abort();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
    });

    std::cout << "[ioring-test] calling shutdown" << std::endl;
    io->shutdown();
    shutdown_finished.store(true, std::memory_order_release);
    watchdog.join();
    std::cout << "[ioring-test] shutdown returned" << std::endl;
    std::filesystem::remove(path);
    std::cout << "[ioring-test] removed temp file" << std::endl;
}
#endif

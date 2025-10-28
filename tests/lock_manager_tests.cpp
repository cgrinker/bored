#include "bored/storage/lock_manager.hpp"

#include "bored/txn/transaction_manager.hpp"

#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <span>
#include <system_error>
#include <thread>
#include <vector>

namespace {

std::shared_ptr<bored::storage::AsyncIo> make_async_io()
{
    using namespace bored::storage;
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 8U;
    auto instance = create_async_io(config);
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

}  // namespace

TEST_CASE("LockManager allows concurrent shared holders")
{
    bored::storage::LockManager manager{};
    const std::uint32_t page_id = 42U;

    std::promise<void> held_promise;
    std::promise<void> release_promise;

    std::thread shared_holder([&]() {
        auto ec = manager.acquire(page_id, bored::storage::PageLatchMode::Shared);
        REQUIRE_FALSE(ec);
        held_promise.set_value();
        release_promise.get_future().wait();
        manager.release(page_id, bored::storage::PageLatchMode::Shared);
    });

    held_promise.get_future().wait();

    auto ec = manager.acquire(page_id, bored::storage::PageLatchMode::Shared);
    REQUIRE_FALSE(ec);
    manager.release(page_id, bored::storage::PageLatchMode::Shared);

    release_promise.set_value();
    shared_holder.join();
}

TEST_CASE("LockManager releases transactional locks on abort")
{
    using namespace bored::storage;

    bored::txn::TransactionIdAllocatorStub allocator{10U};
    bored::txn::TransactionManager txn_manager{allocator};

    LockManager manager{};
    auto txn = txn_manager.begin();
    REQUIRE(txn.state() == bored::txn::TransactionState::Active);

    REQUIRE_FALSE(manager.acquire(99U, PageLatchMode::Exclusive, &txn));

    auto contention = std::async(std::launch::async, [&manager]() {
        return manager.acquire(99U, PageLatchMode::Shared);
    });

    auto contention_status = contention.get();
    REQUIRE(contention_status == std::make_error_code(std::errc::resource_unavailable_try_again));

    txn_manager.abort(txn);

    auto retry = manager.acquire(99U, PageLatchMode::Shared);
    REQUIRE_FALSE(retry);
    manager.release(99U, PageLatchMode::Shared);
}

TEST_CASE("LockManager rejects conflicting shared when exclusive held")
{
    bored::storage::LockManager manager{};
    const std::uint32_t page_id = 506U;

    std::promise<void> held_promise;
    std::promise<void> release_promise;

    std::thread exclusive_holder([&]() {
        auto ec = manager.acquire(page_id, bored::storage::PageLatchMode::Exclusive);
        REQUIRE_FALSE(ec);
        held_promise.set_value();
        release_promise.get_future().wait();
        manager.release(page_id, bored::storage::PageLatchMode::Exclusive);
    });

    held_promise.get_future().wait();

    auto ec = manager.acquire(page_id, bored::storage::PageLatchMode::Shared);
    REQUIRE(ec == std::make_error_code(std::errc::resource_unavailable_try_again));

    release_promise.set_value();
    exclusive_holder.join();
}

TEST_CASE("PageManager surfaces lock contention via LockManager callbacks")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_lock_manager_page_manager_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    FreeSpaceMap fsm;

    LockManager lock_manager{};
    PageManager::Config config{};
    config.latch_callbacks = lock_manager.page_latch_callbacks();

    PageManager manager{&fsm, wal_writer, config};

    alignas(8) std::array<std::byte, kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(manager.initialize_page(page_span, PageType::Table, 88U));

    std::promise<void> held_promise;
    std::promise<void> release_promise;

    std::thread blocker([&]() {
        auto ec = lock_manager.acquire(88U, PageLatchMode::Exclusive);
        REQUIRE_FALSE(ec);
        held_promise.set_value();
        release_promise.get_future().wait();
        lock_manager.release(88U, PageLatchMode::Exclusive);
    });

    held_promise.get_future().wait();

    const std::array<std::byte, 5> tuple{std::byte{'l'}, std::byte{'o'}, std::byte{'c'}, std::byte{'k'}, std::byte{'d'}};
    PageManager::TupleInsertResult insert_result{};
    auto conflict = manager.insert_tuple(page_span, tuple, 123U, insert_result);
    REQUIRE(conflict == std::make_error_code(std::errc::resource_unavailable_try_again));

    release_promise.set_value();
    blocker.join();

    PageManager::TupleInsertResult retry_result{};
    auto retry = manager.insert_tuple(page_span, tuple, 123U, retry_result);
    REQUIRE_FALSE(retry);

    REQUIRE_FALSE(manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

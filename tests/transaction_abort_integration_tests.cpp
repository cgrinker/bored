#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/lock_manager.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

using bored::storage::AsyncIo;
using bored::storage::AsyncIoBackend;
using bored::storage::AsyncIoConfig;

std::shared_ptr<AsyncIo> make_async_io()
{
    AsyncIoConfig config{};
    config.backend = AsyncIoBackend::ThreadPool;
    config.worker_threads = 2U;
    config.queue_depth = 8U;
    auto instance = bored::storage::create_async_io(config);
    return std::shared_ptr<AsyncIo>(std::move(instance));
}

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto base = std::filesystem::temp_directory_path();
    auto path = base / (prefix + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    (void)std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

std::vector<std::byte> to_bytes(std::string_view text)
{
    std::vector<std::byte> buffer(text.size());
    if (!text.empty()) {
        std::memcpy(buffer.data(), text.data(), text.size());
    }
    return buffer;
}

}  // namespace

TEST_CASE("Transaction abort rolls back storage and catalog mutations with lock cleanup", "[txn][integration]")
{
    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_txn_abort_integration_");

    bored::storage::WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * bored::storage::kWalBlockSize;
    wal_config.buffer_size = 2U * bored::storage::kWalBlockSize;

    auto wal_writer = std::make_shared<bored::storage::WalWriter>(io, wal_config);
    bored::storage::FreeSpaceMap fsm;
    bored::storage::LockManager lock_manager{};

    std::vector<std::pair<std::uint32_t, bored::storage::PageLatchMode>> acquire_log;
    std::vector<std::pair<std::uint32_t, bored::storage::PageLatchMode>> release_log;

    bored::storage::PageManager::Config config{};
    config.latch_callbacks.acquire = [&](std::uint32_t page_id, bored::storage::PageLatchMode mode) -> std::error_code {
        acquire_log.emplace_back(page_id, mode);
        return lock_manager.acquire(page_id, mode);
    };
    config.latch_callbacks.release = [&](std::uint32_t page_id, bored::storage::PageLatchMode mode) {
        release_log.emplace_back(page_id, mode);
        lock_manager.release(page_id, mode);
    };

    bored::storage::PageManager page_manager{&fsm, wal_writer, config};
    const std::uint32_t data_page_id = 9100U;

    alignas(8) std::array<std::byte, bored::storage::kPageSize> page_buffer{};
    auto page_span = std::span<std::byte>(page_buffer.data(), page_buffer.size());
    REQUIRE_FALSE(page_manager.initialize_page(page_span, bored::storage::PageType::Table, data_page_id));

    const auto releases_after_initialize = release_log.size();

    bored::txn::TransactionIdAllocatorStub allocator{6'000U};
    bored::txn::TransactionManager txn_manager{allocator};
    auto txn = txn_manager.begin();
    REQUIRE(txn.state() == bored::txn::TransactionState::Active);

    bored::catalog::CatalogTransactionConfig catalog_cfg{};
    catalog_cfg.transaction_manager = &txn_manager;
    catalog_cfg.transaction_context = &txn;
    bored::catalog::CatalogTransaction catalog_tx{catalog_cfg};

    bored::catalog::CatalogMutatorConfig mutator_cfg{};
    mutator_cfg.transaction = &catalog_tx;
    bored::catalog::CatalogMutator mutator{mutator_cfg};

    const auto relation_id = bored::catalog::kCatalogSchemasRelationId;
    const std::uint64_t row_id = 4'242U;
    auto descriptor = bored::catalog::CatalogTupleBuilder::for_insert(catalog_tx);
    auto catalog_payload = to_bytes("rollback_schema");

    mutator.stage_insert(relation_id, row_id, descriptor, catalog_payload);
    REQUIRE(mutator.staged_mutations().size() == 1U);

    REQUIRE_FALSE(lock_manager.acquire(data_page_id, bored::storage::PageLatchMode::Exclusive, &txn));

    const std::array<std::byte, 6U> tuple_payload{
        std::byte{'r'}, std::byte{'o'}, std::byte{'l'}, std::byte{'l'}, std::byte{'b'}, std::byte{'k'}
    };
    bored::storage::PageManager::TupleInsertResult insert_result{};
    REQUIRE_FALSE(page_manager.insert_tuple(page_span,
                                            std::span<const std::byte>(tuple_payload.data(), tuple_payload.size()),
                                            99U,
                                            insert_result,
                                            bored::storage::TupleHeader{},
                                            &txn));

    REQUIRE(insert_result.slot.length != 0U);
    auto page_const = std::span<const std::byte>(page_span.data(), page_span.size());
    auto slot_directory = bored::storage::slot_directory(page_const);
    REQUIRE(slot_directory[insert_result.slot.index].length == insert_result.slot.length);

    auto header_after_insert = bored::storage::page_header(page_const);
    CHECK(header_after_insert.tuple_count == 1U);
    const auto insert_fragment_count = header_after_insert.fragment_count;
    REQUIRE(release_log.size() == releases_after_initialize + 1U);

    const auto releases_before_abort = release_log.size();

    txn_manager.abort(txn);
    CHECK(txn.state() == bored::txn::TransactionState::Aborted);
    CHECK(catalog_tx.is_aborted());
    CHECK(mutator.staged_mutations().empty());
    CHECK_FALSE(mutator.has_published_batch());

    auto header_after_abort = bored::storage::page_header(page_const);
    CHECK(header_after_abort.fragment_count == static_cast<std::uint16_t>(insert_fragment_count + 1U));
    auto slot_directory_after_abort = bored::storage::slot_directory(page_const);
    CHECK(slot_directory_after_abort[insert_result.slot.index].length == 0U);

    REQUIRE(release_log.size() >= releases_before_abort + 2U);
    const auto& abort_release = release_log.back();
    CHECK(abort_release.first == data_page_id);
    CHECK(abort_release.second == bored::storage::PageLatchMode::Exclusive);

    auto reacquire_future = std::async(std::launch::async, [&lock_manager, data_page_id]() {
        auto ec = lock_manager.acquire(data_page_id, bored::storage::PageLatchMode::Exclusive);
        if (!ec) {
            lock_manager.release(data_page_id, bored::storage::PageLatchMode::Exclusive);
        }
        return ec;
    });
    auto reacquire_ec = reacquire_future.get();
    REQUIRE_FALSE(reacquire_ec);

    REQUIRE_FALSE(page_manager.close_wal());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

#include "bored/storage/checkpoint_coordinator.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

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

TEST_CASE("CheckpointCoordinator pins retention while checkpoint active")
{
    using namespace bored::storage;
    using namespace bored::txn;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_coord_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 2U * kWalBlockSize;
    wal_config.buffer_size = kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    WalRetentionManager retention_manager{wal_config.directory,
                                          wal_config.file_prefix,
                                          wal_config.file_extension,
                                          wal_writer->durability_horizon()};

    TransactionIdAllocatorStub allocator{10U};
    TransactionManager transaction_manager{allocator};

    WalRecordDescriptor record{};
    record.type = WalRecordType::TupleInsert;
    record.page_id = 55U;

    WalAppendResult first_append{};
    REQUIRE_FALSE(wal_writer->append_record(record, first_append));

    WalAppendResult second_append{};
    REQUIRE_FALSE(wal_writer->append_record(record, second_append));
    REQUIRE(second_append.segment_id > first_append.segment_id);

    REQUIRE_FALSE(wal_writer->flush());

    CheckpointCoordinator coordinator{checkpoint_manager, transaction_manager, retention_manager};
    CheckpointCoordinator::ActiveCheckpoint checkpoint{};
    REQUIRE_FALSE(coordinator.begin_checkpoint(1U, checkpoint));
    CHECK(checkpoint.transaction_fence.active());

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = first_append.lsn;
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 77U, .reserved = 0U, .page_lsn = first_append.lsn}
        };
        snapshot.active_transactions = {
            WalCheckpointTxnEntry{.transaction_id = static_cast<std::uint32_t>(transaction_manager.next_transaction_id()),
                                   .state = 1U,
                                   .last_lsn = first_append.lsn}
        };
        return {};
    };

    REQUIRE_FALSE(coordinator.prepare_checkpoint(provider, checkpoint));
    REQUIRE(checkpoint.snapshot.dirty_pages.size() == 1U);
    REQUIRE(checkpoint.snapshot.active_transactions.size() == 1U);

    WalRetentionConfig retention_config{};
    retention_config.retention_segments = 1U;

    WalRetentionStats pinned_stats{};
    REQUIRE_FALSE(retention_manager.apply(retention_config, second_append.segment_id + 1U, &pinned_stats));
    CHECK(pinned_stats.scanned_segments == 0U);

    auto first_segment_path = wal_writer->segment_path(first_append.segment_id);
    auto second_segment_path = wal_writer->segment_path(second_append.segment_id);
    CHECK(std::filesystem::exists(first_segment_path));
    CHECK(std::filesystem::exists(second_segment_path));

    WalAppendResult checkpoint_result{};
    REQUIRE_FALSE(coordinator.commit_checkpoint(checkpoint, checkpoint_result));
    CHECK_FALSE(checkpoint.transaction_fence.active());
    CHECK(checkpoint_result.segment_id >= second_append.segment_id);

    WalRetentionStats post_stats{};
    REQUIRE_FALSE(retention_manager.apply(retention_config, second_append.segment_id + 1U, &post_stats));
    CHECK(post_stats.pruned_segments >= 1U);
    CHECK_FALSE(std::filesystem::exists(first_segment_path));
    CHECK(std::filesystem::exists(second_segment_path));

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

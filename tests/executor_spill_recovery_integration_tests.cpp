#include "bored/executor/executor_temp_resource_manager.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/checkpoint_scheduler.hpp"
#include "bored/storage/temp_resource_registry.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>

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

TEST_CASE("Executor spill retention and recovery purge staged artifacts", "[executor][integration]")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_executor_spill_integration_");

    bored::executor::ExecutorTempResourceManager manager{
        bored::executor::ExecutorTempResourceManager::Config{
            .base_directory = wal_dir / "spill",
        }
    };

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;
    wal_config.size_flush_threshold = kWalBlockSize;
    wal_config.retention.retention_segments = 1U;
    wal_config.temp_resource_registry = &manager.registry();

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::seconds(300);
    scheduler_config.dirty_page_trigger = 0U;
    scheduler_config.active_transaction_trigger = 0U;
    scheduler_config.lsn_gap_trigger = kWalBlockSize;
    scheduler_config.retention = wal_config.retention;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Abort;
    descriptor.page_id = 42U;

    WalAppendResult append_result{};
    REQUIRE_FALSE(wal_writer->append_record(descriptor, append_result));
    REQUIRE_FALSE(wal_writer->flush());

    std::filesystem::path spill_dir;
    REQUIRE_FALSE(manager.create_spill_directory("hash_join", spill_dir));
    auto spill_file = spill_dir / "spill.tmp";
    {
        std::ofstream stream{spill_file, std::ios::binary};
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> checkpoint_result;
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = wal_writer->next_lsn();
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(now, provider, true, checkpoint_result));
    REQUIRE(checkpoint_result.has_value());
    CHECK_FALSE(std::filesystem::exists(spill_file));

    {
        std::ofstream stream{spill_file, std::ios::binary};
        stream << "post-checkpoint spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();

    WalRecoveryDriver recovery_driver{wal_dir,
                                      wal_config.file_prefix,
                                      wal_config.file_extension,
                                      &manager.registry(),
                                      wal_dir / "checkpoints"};
    WalRecoveryPlan recovery_plan{};
    REQUIRE_FALSE(recovery_driver.build_plan(recovery_plan));
    REQUIRE(recovery_plan.temp_resource_registry == &manager.registry());

    WalReplayContext replay_context{};
    WalReplayer replayer{replay_context};
    auto redo_ec = replayer.apply_redo(recovery_plan);
    REQUIRE((!redo_ec || redo_ec == std::make_error_code(std::errc::not_supported)));
    REQUIRE_FALSE(replayer.apply_undo(recovery_plan));

    CHECK(std::filesystem::exists(spill_dir));
    CHECK_FALSE(std::filesystem::exists(spill_file));

    (void)std::filesystem::remove_all(wal_dir);
}

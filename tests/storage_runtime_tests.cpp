#include "bored/storage/storage_runtime.hpp"

#include "bored/executor/executor_temp_resource_manager.hpp"
#include "bored/storage/index_retention.hpp"
#include "bored/storage/index_retention_pruner.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/temp_resource_registry.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_telemetry_registry.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <system_error>
#include <vector>

namespace {

std::filesystem::path make_temp_dir(const std::string& prefix)
{
    auto root = std::filesystem::temp_directory_path();
    auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dir = root / (prefix + std::to_string(timestamp));
    (void)std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

}  // namespace

TEST_CASE("StorageRuntime wires temp cleanup into checkpoint and recovery", "[storage][runtime]")
{
    using namespace bored::storage;

    StorageTelemetryRegistry storage_registry;
    WalTelemetryRegistry wal_registry;

    auto wal_dir = make_temp_dir("bored_storage_runtime_");

    StorageRuntimeConfig config{};
    config.async_io.backend = AsyncIoBackend::ThreadPool;
    config.async_io.worker_threads = 2U;
    config.async_io.queue_depth = 8U;
    config.wal_telemetry_registry = &wal_registry;
    config.storage_telemetry_registry = &storage_registry;

    config.temp_manager.base_directory = wal_dir / "spill";

    config.wal_writer.directory = wal_dir;
    config.wal_writer.segment_size = 4U * kWalBlockSize;
    config.wal_writer.buffer_size = 2U * kWalBlockSize;
    config.wal_writer.size_flush_threshold = kWalBlockSize;
    config.wal_writer.start_lsn = kWalBlockSize;
    config.wal_writer.retention.retention_segments = 1U;
    config.wal_writer.telemetry_identifier = "runtime_wal";
    config.wal_writer.temp_cleanup_telemetry_identifier = "runtime_temp_cleanup";

    config.checkpoint_scheduler.min_interval = std::chrono::seconds(300);
    config.checkpoint_scheduler.dirty_page_trigger = 0U;
    config.checkpoint_scheduler.active_transaction_trigger = 0U;
    config.checkpoint_scheduler.lsn_gap_trigger = kWalBlockSize;
    config.checkpoint_scheduler.flush_after_emit = true;
    config.checkpoint_scheduler.retention = config.wal_writer.retention;
    config.checkpoint_scheduler.telemetry_identifier = "runtime_checkpoint";
    config.checkpoint_scheduler.retention_telemetry_identifier = "runtime_retention";

    StorageRuntime runtime{config};
    auto init_ec = runtime.initialize();
    REQUIRE_FALSE(init_ec);

    REQUIRE(runtime.wal_writer());
    REQUIRE(runtime.checkpoint_scheduler());
    CHECK(runtime.wal_writer_config().temp_resource_registry == &runtime.temp_registry());

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Abort;
    descriptor.page_id = 7U;

    WalAppendResult append_result{};
    REQUIRE_FALSE(runtime.wal_writer()->append_record(descriptor, append_result));
    REQUIRE_FALSE(runtime.wal_writer()->flush());

    auto& manager = runtime.temp_manager();
    std::filesystem::path spill_dir;
    REQUIRE_FALSE(manager.create_spill_directory("hash_join", spill_dir));
    const auto spill_file = spill_dir / "spill.tmp";
    {
        std::ofstream stream{spill_file, std::ios::binary};
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = runtime.wal_writer()->next_lsn();
        snapshot.undo_lsn = runtime.wal_writer()->next_lsn();
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> checkpoint_result;
    REQUIRE_FALSE(runtime.checkpoint_scheduler()->maybe_run(now, provider, true, checkpoint_result));
    REQUIRE(checkpoint_result.has_value());
    CHECK(checkpoint_result->lsn > 0U);
    CHECK_FALSE(std::filesystem::exists(spill_file));

    std::filesystem::path crash_spill_dir;
    REQUIRE_FALSE(manager.create_spill_directory("crash", crash_spill_dir));
    const auto crash_spill_file = crash_spill_dir / "spill.tmp";
    {
        std::ofstream stream{crash_spill_file, std::ios::binary};
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(crash_spill_file));

    const auto temp_cleanup_snapshot = storage_registry.aggregate_temp_cleanup();
    CHECK(temp_cleanup_snapshot.invocations > 0U);

    runtime.shutdown();

    auto recovery_driver = runtime.make_recovery_driver();
    WalRecoveryPlan recovery_plan{};
    REQUIRE_FALSE(recovery_driver.build_plan(recovery_plan));
    REQUIRE(recovery_plan.temp_resource_registry == &runtime.temp_registry());

    WalReplayContext replay_context{};
    WalReplayer replayer{replay_context};
    auto redo_ec = replayer.apply_redo(recovery_plan);
    REQUIRE((!redo_ec || redo_ec == std::make_error_code(std::errc::not_supported)));
    REQUIRE_FALSE(replayer.apply_undo(recovery_plan));

    CHECK_FALSE(std::filesystem::exists(crash_spill_file));

    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("StorageRuntime dispatches index retention candidates via pruner", "[storage][runtime]")
{
    using namespace bored::storage;

    StorageTelemetryRegistry storage_registry;
    WalTelemetryRegistry wal_registry;

    auto wal_dir = make_temp_dir("bored_storage_runtime_retention_");

    IndexRetentionPruner::Config pruner_config{};
    std::vector<IndexRetentionCandidate> invoked{};
    pruner_config.prune_callback = [&invoked](const IndexRetentionCandidate& candidate) -> std::error_code {
        invoked.push_back(candidate);
        return {};
    };

    StorageRuntimeConfig config{};
    config.async_io.backend = AsyncIoBackend::ThreadPool;
    config.async_io.worker_threads = 1U;
    config.async_io.queue_depth = 4U;
    config.wal_telemetry_registry = &wal_registry;
    config.storage_telemetry_registry = &storage_registry;
    config.temp_manager.base_directory = wal_dir / "spill";
    config.wal_writer.directory = wal_dir;
    config.wal_writer.segment_size = 4U * kWalBlockSize;
    config.wal_writer.buffer_size = 2U * kWalBlockSize;
    config.wal_writer.size_flush_threshold = kWalBlockSize;
    config.wal_writer.start_lsn = kWalBlockSize;
    config.wal_writer.retention.retention_segments = 1U;
    config.wal_writer.telemetry_identifier = "runtime_retention_wal";
    config.index_retention.retention_window = std::chrono::milliseconds{0};
    config.index_retention_pruner = pruner_config;

    config.checkpoint_scheduler.min_interval = std::chrono::seconds(300);
    config.checkpoint_scheduler.dirty_page_trigger = 0U;
    config.checkpoint_scheduler.active_transaction_trigger = 0U;
    config.checkpoint_scheduler.lsn_gap_trigger = kWalBlockSize;
    config.checkpoint_scheduler.flush_after_emit = true;
    config.checkpoint_scheduler.retention = config.wal_writer.retention;
    config.checkpoint_scheduler.telemetry_identifier = "runtime_retention_checkpoint";

    StorageRuntime runtime{config};
    REQUIRE_FALSE(runtime.initialize());

    auto manager = runtime.index_retention_manager();
    REQUIRE(manager != nullptr);

    IndexRetentionCandidate candidate{};
    candidate.index_id = 333U;
    candidate.page_id = 77U;
    candidate.level = 0U;
    candidate.prune_lsn = 1U;
    manager->schedule_candidate(candidate);

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = 100U;
        snapshot.undo_lsn = 100U;
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> checkpoint_result;
    REQUIRE_FALSE(runtime.checkpoint_scheduler()->maybe_run(now, provider, true, checkpoint_result));
    REQUIRE(checkpoint_result.has_value());
    CHECK(checkpoint_result->lsn > 0U);

    auto retention_snapshot = manager->telemetry_snapshot();
    INFO("dispatched=" << retention_snapshot.dispatched_candidates << ", pruned=" << retention_snapshot.pruned_candidates << ", pending=" << retention_snapshot.pending_candidates);
    CHECK(retention_snapshot.dispatched_candidates == 1U);

    REQUIRE(invoked.size() == 1U);
    CHECK(invoked.front().index_id == candidate.index_id);
    CHECK(invoked.front().page_id == candidate.page_id);

    runtime.shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("StorageRuntime dispatches index retention candidates via executor fallback", "[storage][runtime]")
{
    using namespace bored::storage;

    StorageTelemetryRegistry storage_registry;
    WalTelemetryRegistry wal_registry;

    auto wal_dir = make_temp_dir("bored_storage_runtime_retention_executor_");

    StorageRuntimeConfig config{};
    config.async_io.backend = AsyncIoBackend::ThreadPool;
    config.async_io.worker_threads = 1U;
    config.async_io.queue_depth = 4U;
    config.wal_telemetry_registry = &wal_registry;
    config.storage_telemetry_registry = &storage_registry;
    config.temp_manager.base_directory = wal_dir / "spill";
    config.wal_writer.directory = wal_dir;
    config.wal_writer.segment_size = 4U * kWalBlockSize;
    config.wal_writer.buffer_size = 2U * kWalBlockSize;
    config.wal_writer.size_flush_threshold = kWalBlockSize;
    config.wal_writer.start_lsn = kWalBlockSize;
    config.wal_writer.retention.retention_segments = 1U;
    config.wal_writer.telemetry_identifier = "runtime_retention_executor_wal";
    config.index_retention.retention_window = std::chrono::milliseconds{0};

    std::vector<IndexRetentionCandidate> invoked{};
    config.index_retention_pruner = IndexRetentionPruner::Config{};
    config.index_retention_executor = IndexRetentionExecutor::Config{};
    config.index_retention_executor->prune_callback = [&invoked](const IndexRetentionCandidate& candidate) -> std::error_code {
        invoked.push_back(candidate);
        return {};
    };

    config.checkpoint_scheduler.min_interval = std::chrono::seconds(300);
    config.checkpoint_scheduler.dirty_page_trigger = 0U;
    config.checkpoint_scheduler.active_transaction_trigger = 0U;
    config.checkpoint_scheduler.lsn_gap_trigger = kWalBlockSize;
    config.checkpoint_scheduler.flush_after_emit = true;
    config.checkpoint_scheduler.retention = config.wal_writer.retention;
    config.checkpoint_scheduler.telemetry_identifier = "runtime_retention_executor_checkpoint";

    StorageRuntime runtime{config};
    REQUIRE_FALSE(runtime.initialize());

    auto manager = runtime.index_retention_manager();
    REQUIRE(manager != nullptr);

    IndexRetentionCandidate candidate{};
    candidate.index_id = 901U;
    candidate.page_id = 11U;
    candidate.level = 2U;
    candidate.prune_lsn = 3U;
    manager->schedule_candidate(candidate);

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = 100U;
        snapshot.undo_lsn = 100U;
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> checkpoint_result;
    REQUIRE_FALSE(runtime.checkpoint_scheduler()->maybe_run(now, provider, true, checkpoint_result));
    REQUIRE(checkpoint_result.has_value());
    CHECK(checkpoint_result->lsn > 0U);

    auto retention_snapshot = manager->telemetry_snapshot();
    INFO("dispatched=" << retention_snapshot.dispatched_candidates << ", pruned=" << retention_snapshot.pruned_candidates << ", pending=" << retention_snapshot.pending_candidates);
    CHECK(retention_snapshot.dispatched_candidates == 1U);

    REQUIRE(invoked.size() == 1U);
    CHECK(invoked.front().index_id == candidate.index_id);
    CHECK(invoked.front().page_id == candidate.page_id);

    runtime.shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("StorageRuntime registers storage control handlers", "[storage][runtime]")
{
    using namespace bored::storage;

    StorageTelemetryRegistry storage_registry;
    WalTelemetryRegistry wal_registry;

    auto wal_dir = make_temp_dir("bored_storage_runtime_control_");

    StorageRuntimeConfig config{};
    config.async_io.backend = AsyncIoBackend::ThreadPool;
    config.async_io.worker_threads = 2U;
    config.async_io.queue_depth = 8U;
    config.wal_telemetry_registry = &wal_registry;
    config.storage_telemetry_registry = &storage_registry;
    config.control_telemetry_identifier = "runtime_control";

    config.temp_manager.base_directory = wal_dir / "spill";

    config.wal_writer.directory = wal_dir;
    config.wal_writer.segment_size = 4U * kWalBlockSize;
    config.wal_writer.buffer_size = 2U * kWalBlockSize;
    config.wal_writer.size_flush_threshold = kWalBlockSize;
    config.wal_writer.start_lsn = kWalBlockSize;
    config.wal_writer.retention.retention_segments = 1U;
    config.wal_writer.telemetry_identifier = "runtime_control_wal";
    config.wal_writer.temp_cleanup_telemetry_identifier = "runtime_control_temp";

    config.checkpoint_scheduler.min_interval = std::chrono::seconds(300);
    config.checkpoint_scheduler.dirty_page_trigger = 0U;
    config.checkpoint_scheduler.active_transaction_trigger = 0U;
    config.checkpoint_scheduler.lsn_gap_trigger = kWalBlockSize;
    config.checkpoint_scheduler.flush_after_emit = true;
    config.checkpoint_scheduler.retention = config.wal_writer.retention;
    config.checkpoint_scheduler.telemetry_identifier = "runtime_control_checkpoint";
    config.checkpoint_scheduler.retention_telemetry_identifier = "runtime_control_retention";

    StorageRuntime* runtime_ptr = nullptr;
    config.checkpoint_snapshot_provider = [&runtime_ptr](CheckpointSnapshot& snapshot) -> std::error_code {
        if (runtime_ptr == nullptr) {
            return std::make_error_code(std::errc::not_connected);
        }
        auto writer = runtime_ptr->wal_writer();
        if (!writer) {
            return std::make_error_code(std::errc::not_connected);
        }
        snapshot.redo_lsn = writer->next_lsn();
        snapshot.undo_lsn = writer->next_lsn();
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    StorageRuntime runtime{config};
    runtime_ptr = &runtime;
    REQUIRE_FALSE(runtime.initialize());

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Abort;
    descriptor.page_id = 42U;

    WalAppendResult append_result{};
    REQUIRE_FALSE(runtime.wal_writer()->append_record(descriptor, append_result));
    REQUIRE_FALSE(runtime.wal_writer()->flush());

    auto handlers = get_global_storage_control_handlers();
    REQUIRE(static_cast<bool>(handlers.checkpoint));
    REQUIRE(static_cast<bool>(handlers.retention));
    REQUIRE(static_cast<bool>(handlers.recovery));

    StorageCheckpointRequest checkpoint_request{};
    checkpoint_request.force = true;
    checkpoint_request.dry_run = false;
    REQUIRE_FALSE(handlers.checkpoint(checkpoint_request));

    StorageRetentionRequest retention_request{};
    retention_request.include_index_retention = true;
    REQUIRE_FALSE(handlers.retention(retention_request));

    StorageRecoveryRequest recovery_request{};
    recovery_request.run_redo = false;
    recovery_request.run_undo = false;
    recovery_request.cleanup_only = true;
    REQUIRE_FALSE(handlers.recovery(recovery_request));

    auto control_snapshot = storage_registry.aggregate_control();
    CHECK(control_snapshot.checkpoint.attempts == 1U);
    CHECK(control_snapshot.retention.attempts == 1U);
    CHECK(control_snapshot.recovery.attempts == 1U);

    runtime.shutdown();

    handlers = get_global_storage_control_handlers();
    CHECK_FALSE(static_cast<bool>(handlers.checkpoint));
    CHECK_FALSE(static_cast<bool>(handlers.retention));
    CHECK_FALSE(static_cast<bool>(handlers.recovery));

    control_snapshot = storage_registry.aggregate_control();
    CHECK(control_snapshot.checkpoint.attempts == 0U);

    (void)std::filesystem::remove_all(wal_dir);
}

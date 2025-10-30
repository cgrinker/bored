#include "bored/storage/checkpoint_scheduler.hpp"

#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/checkpoint_coordinator.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/temp_resource_registry.hpp"
#include "bored/storage/wal_retention.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <atomic>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <thread>
#include <fstream>

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

TEST_CASE("CheckpointScheduler emits checkpoint when dirty page trigger reached")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_dirty_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::seconds(300);
    scheduler_config.dirty_page_trigger = 2U;
    scheduler_config.active_transaction_trigger = 10U;
    scheduler_config.lsn_gap_trigger = kWalBlockSize;
    scheduler_config.retention.retention_segments = 1U;

    std::size_t retention_calls = 0U;
    WalRetentionConfig captured_retention{};
    std::vector<std::uint64_t> captured_segments{};

    CheckpointScheduler scheduler{checkpoint_manager,
                                  scheduler_config,
                                  [&](const WalRetentionConfig& config, std::uint64_t segment_id, WalRetentionStats* stats) -> std::error_code {
                                      ++retention_calls;
                                      captured_retention = config;
                                      captured_segments.push_back(segment_id);
                                      if (stats) {
                                          stats->scanned_segments = 7U;
                                          stats->candidate_segments = 5U;
                                          stats->pruned_segments = 3U;
                                          stats->archived_segments = 1U;
                                      }
                                      return {};
                                  }};

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> result;

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = wal_writer->next_lsn();
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 11U, .reserved = 0U, .page_lsn = 1024U},
            WalCheckpointDirtyPageEntry{.page_id = 19U, .reserved = 0U, .page_lsn = 2048U}
        };
        snapshot.active_transactions = {
            WalCheckpointTxnEntry{.transaction_id = 7U, .state = 1U, .last_lsn = 512U}
        };
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(now, provider, false, result));
    REQUIRE(result.has_value());
    REQUIRE(scheduler.last_checkpoint_id() == 1U);
    REQUIRE(scheduler.next_checkpoint_id() == 2U);

    std::optional<WalAppendResult> second_result;
    REQUIRE_FALSE(scheduler.maybe_run(now, provider, false, second_result));
    REQUIRE(second_result.has_value());
    REQUIRE(scheduler.last_checkpoint_id() == 2U);
    REQUIRE(scheduler.next_checkpoint_id() == 3U);
    REQUIRE(retention_calls == 2U);
    REQUIRE(captured_segments.size() == 2U);
    REQUIRE(captured_segments.front() == result->segment_id);
    REQUIRE(captured_segments.back() == second_result->segment_id);
    REQUIRE(captured_retention.retention_segments == scheduler_config.retention.retention_segments);

    const auto checkpoint_metrics = scheduler.telemetry_snapshot();
    REQUIRE(checkpoint_metrics.invocations == 2U);
    REQUIRE(checkpoint_metrics.emitted_checkpoints == 2U);
    REQUIRE(checkpoint_metrics.trigger_first == 1U);
    REQUIRE(checkpoint_metrics.trigger_dirty == 1U);
    REQUIRE(checkpoint_metrics.last_checkpoint_id == scheduler.last_checkpoint_id());

    const auto retention_metrics = scheduler.retention_telemetry_snapshot();
    REQUIRE(retention_metrics.invocations == 2U);
    REQUIRE(retention_metrics.failures == 0U);
    REQUIRE(retention_metrics.scanned_segments == 14U);
    REQUIRE(retention_metrics.candidate_segments == 10U);
    REQUIRE(retention_metrics.pruned_segments == 6U);
    REQUIRE(retention_metrics.archived_segments == 2U);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler honors interval and lsn gap triggers")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_lsn_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::seconds(300);
    scheduler_config.dirty_page_trigger = 1000U;
    scheduler_config.active_transaction_trigger = 1000U;
    scheduler_config.lsn_gap_trigger = kWalBlockSize;
    scheduler_config.retention = {};

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    const auto time_zero = std::chrono::steady_clock::time_point{};

    std::optional<WalAppendResult> first_result;
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = wal_writer->next_lsn();
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(time_zero, provider, false, first_result));
    REQUIRE(first_result.has_value());

    std::optional<WalAppendResult> suppressed_result;
    REQUIRE_FALSE(scheduler.maybe_run(time_zero, provider, false, suppressed_result));
    REQUIRE_FALSE(suppressed_result.has_value());

    bored::storage::WalCommitHeader commit_header{};
    commit_header.transaction_id = 1U;
    commit_header.commit_lsn = wal_writer->next_lsn();
    commit_header.next_transaction_id = 2U;
    commit_header.oldest_active_transaction_id = 0U;
    commit_header.oldest_active_commit_lsn = commit_header.commit_lsn;

    WalAppendResult dummy{};
    REQUIRE_FALSE(wal_writer->append_commit_record(commit_header, dummy));

    std::optional<WalAppendResult> lsn_result;
    REQUIRE_FALSE(scheduler.maybe_run(time_zero, provider, false, lsn_result));
    REQUIRE(lsn_result.has_value());
    REQUIRE(scheduler.last_checkpoint_id() == 2U);

    const auto telemetry = scheduler.telemetry_snapshot();
    REQUIRE(telemetry.invocations == 3U);
    REQUIRE(telemetry.emitted_checkpoints == 2U);
    REQUIRE(telemetry.skipped_runs == 1U);
    REQUIRE(telemetry.trigger_first == 1U);
    REQUIRE(telemetry.trigger_lsn_gap == 1U);
    REQUIRE(telemetry.last_checkpoint_id == scheduler.last_checkpoint_id());

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler default retention hook invokes WalWriter retention")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_default_");

    TempResourceRegistry temp_registry;

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;
    wal_config.retention.retention_segments = 0U;
    wal_config.temp_resource_registry = &temp_registry;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::seconds(300);
    scheduler_config.dirty_page_trigger = 0U;
    scheduler_config.retention.retention_segments = 1U;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    auto spill_dir = wal_dir / "spill";
    std::filesystem::create_directories(spill_dir);
    auto spill_file = spill_dir / "executor.tmp";
    {
        std::ofstream stream(spill_file, std::ios::binary);
        stream << "spill";
    }
    REQUIRE(std::filesystem::exists(spill_file));
    temp_registry.register_directory(spill_dir);

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> result;
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = wal_writer->next_lsn();
        snapshot.dirty_pages.clear();
        snapshot.active_transactions.clear();
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(now, provider, true, result));
    REQUIRE(result.has_value());
    CHECK_FALSE(std::filesystem::exists(spill_file));

    const auto telemetry = scheduler.retention_telemetry_snapshot();
    REQUIRE(telemetry.invocations == 1U);
    REQUIRE(telemetry.failures == 0U);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler dry run tracks coordinator phases")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_dry_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    WalRetentionManager retention_manager{wal_config.directory,
                                          wal_config.file_prefix,
                                          wal_config.file_extension,
                                          wal_writer->durability_horizon()};

    bored::txn::TransactionIdAllocatorStub allocator{1U};
    bored::txn::TransactionManager transaction_manager{allocator};

    CheckpointCoordinator coordinator{checkpoint_manager, transaction_manager, retention_manager};

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::seconds(0);
    scheduler_config.flush_after_emit = false;
    scheduler_config.dry_run_only = true;
    scheduler_config.coordinator = &coordinator;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    const auto now = std::chrono::steady_clock::now();
    std::optional<WalAppendResult> result;
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = snapshot.redo_lsn;
        return {};
    };

    REQUIRE_FALSE(scheduler.maybe_run(now, provider, true, result));
    CHECK_FALSE(result.has_value());

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.coordinator_begin_calls == 1U);
    CHECK(telemetry.coordinator_prepare_calls == 1U);
    CHECK(telemetry.coordinator_commit_calls == 0U);
    CHECK(telemetry.coordinator_dry_runs == 1U);
    CHECK(telemetry.emitted_checkpoints == 0U);
    CHECK(telemetry.retention_invocations == 0U);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler throttles checkpoint IO budget")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_throttle_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::milliseconds{0};
    scheduler_config.dirty_page_trigger = 1U;
    scheduler_config.active_transaction_trigger = 1000U;
    scheduler_config.lsn_gap_trigger = 0U;
    scheduler_config.flush_after_emit = false;
    scheduler_config.io_target_bytes_per_second = 4U * kWalBlockSize;
    scheduler_config.io_burst_bytes = 4U * kWalBlockSize;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = snapshot.redo_lsn;
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 17U, .reserved = 0U, .page_lsn = snapshot.redo_lsn}
        };
        snapshot.active_transactions.clear();
        snapshot.index_metadata.clear();
        return {};
    };

    const auto base_time = std::chrono::steady_clock::now();

    std::optional<WalAppendResult> first_result;
    REQUIRE_FALSE(scheduler.maybe_run(base_time, provider, false, first_result));
    REQUIRE(first_result.has_value());

    auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 1U);
    CHECK(telemetry.io_throttle_deferrals == 0U);
    CHECK(telemetry.io_throttle_bytes_consumed >= first_result->written_bytes);

    std::optional<WalAppendResult> deferred_result;
    REQUIRE_FALSE(scheduler.maybe_run(base_time, provider, false, deferred_result));
    REQUIRE_FALSE(deferred_result.has_value());

    telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 1U);
    CHECK(telemetry.io_throttle_deferrals == 1U);
    CHECK(telemetry.io_throttle_budget < scheduler_config.io_target_bytes_per_second);

    const auto later_time = base_time + std::chrono::seconds{2};
    std::optional<WalAppendResult> second_result;
    REQUIRE_FALSE(scheduler.maybe_run(later_time, provider, false, second_result));
    REQUIRE(second_result.has_value());

    telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 2U);
    CHECK(telemetry.io_throttle_deferrals == 1U);
    CHECK(telemetry.io_throttle_bytes_consumed >= first_result->written_bytes + second_result->written_bytes);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler overlaps checkpoint with concurrent transactions")
{
    using namespace bored::storage;
    using namespace std::chrono_literals;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_concurrent_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    WalRetentionManager retention_manager{wal_config.directory,
                                          wal_config.file_prefix,
                                          wal_config.file_extension,
                                          wal_writer->durability_horizon()};

    bored::txn::TransactionIdAllocatorStub allocator{120U};
    bored::txn::TransactionManager transaction_manager{allocator};

    CheckpointCoordinator coordinator{checkpoint_manager, transaction_manager, retention_manager};

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::milliseconds{0};
    scheduler_config.flush_after_emit = false;
    scheduler_config.coordinator = &coordinator;
    scheduler_config.dirty_page_trigger = 1U;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    auto active_txn = transaction_manager.begin();
    REQUIRE(active_txn.id() == 120U);

    std::promise<void> provider_ready_promise;
    auto provider_ready_future = provider_ready_promise.get_future();
    std::promise<void> release_provider_promise;
    auto release_provider_future = release_provider_promise.get_future();

    std::promise<void> worker_attempt_promise;
    auto worker_attempt_future = worker_attempt_promise.get_future();

    auto worker_future = std::async(std::launch::async, [&]() -> std::pair<std::uint64_t, std::chrono::nanoseconds> {
        provider_ready_future.wait();
        const auto block_start = std::chrono::steady_clock::now();
        worker_attempt_promise.set_value();
        auto ctx = transaction_manager.begin();
        const auto block_end = std::chrono::steady_clock::now();
        const auto id = ctx.id();
        transaction_manager.commit(ctx);
        return std::make_pair(id, std::chrono::duration_cast<std::chrono::nanoseconds>(block_end - block_start));
    });

    std::atomic<int> provider_calls{0};
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = snapshot.redo_lsn;
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 42U, .reserved = 0U, .page_lsn = snapshot.redo_lsn}
        };
        snapshot.active_transactions = {
            WalCheckpointTxnEntry{.transaction_id = static_cast<std::uint32_t>(active_txn.id()),
                                   .state = 1U,
                                   .last_lsn = snapshot.redo_lsn}
        };
        snapshot.index_metadata.clear();
        const auto call_index = provider_calls.fetch_add(1);
        if (call_index == 0) {
            return {};
        }
        if (call_index == 1) {
            provider_ready_promise.set_value();
            release_provider_future.wait();
        }
        return {};
    };

    std::optional<WalAppendResult> result;
    auto scheduler_future = std::async(std::launch::async, [&]() {
        return scheduler.maybe_run(std::chrono::steady_clock::now(), provider, false, result);
    });

    worker_attempt_future.wait();
    auto worker_block_status = worker_future.wait_for(25ms);
    CHECK(worker_block_status == std::future_status::timeout);

    release_provider_promise.set_value();

    auto scheduler_status = scheduler_future.wait_for(250ms);
    REQUIRE(scheduler_status == std::future_status::ready);
    auto scheduler_ec = scheduler_future.get();
    REQUIRE_FALSE(scheduler_ec);
    REQUIRE(result.has_value());

    auto worker_resume_status = worker_future.wait_for(250ms);
    REQUIRE(worker_resume_status == std::future_status::ready);
    const auto worker_result = worker_future.get();
    const auto resumed_id = worker_result.first;
    const auto blocked_duration = worker_result.second;
    CHECK(resumed_id == active_txn.id() + 1U);
    CHECK(blocked_duration > std::chrono::nanoseconds::zero());
    CHECK(blocked_duration < 50ms);

    transaction_manager.commit(active_txn);

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 1U);
    CHECK(telemetry.coordinator_begin_calls == 1U);
    CHECK(telemetry.coordinator_prepare_calls == 1U);
    CHECK(telemetry.coordinator_commit_calls == 1U);
    CHECK(telemetry.last_checkpoint_duration_ns > 0U);
    CHECK(telemetry.total_checkpoint_duration_ns >= telemetry.last_checkpoint_duration_ns);
    CHECK(telemetry.max_checkpoint_duration_ns >= telemetry.last_checkpoint_duration_ns);
    CHECK(telemetry.last_fence_duration_ns > 0U);
    CHECK(telemetry.total_fence_duration_ns >= telemetry.last_fence_duration_ns);
    CHECK(telemetry.max_fence_duration_ns >= telemetry.last_fence_duration_ns);
    const auto blocked_ns = static_cast<std::uint64_t>(blocked_duration.count());
    CHECK(blocked_ns <= telemetry.last_fence_duration_ns + 100000U);
    CHECK(telemetry.blocked_transactions >= 1U);
    CHECK(telemetry.last_blocked_duration_ns > 0U);
    CHECK(telemetry.total_blocked_duration_ns >= telemetry.last_blocked_duration_ns);
    CHECK(telemetry.max_blocked_duration_ns >= telemetry.last_blocked_duration_ns);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler telemetry tracks queue depth")
{
    using namespace bored::storage;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_queue_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::milliseconds(1);
    scheduler_config.dirty_page_trigger = 1U;
    scheduler_config.active_transaction_trigger = 100U;
    scheduler_config.lsn_gap_trigger = kWalBlockSize;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    std::promise<void> first_provider_entered;
    auto first_entered_future = first_provider_entered.get_future();
    std::promise<void> release_first_provider;
    auto release_future = release_first_provider.get_future().share();
    std::atomic<int> provider_calls{0};

    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = snapshot.redo_lsn;
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 7U, .reserved = 0U, .page_lsn = 4096U}
        };
        snapshot.active_transactions.clear();

        const auto call_index = provider_calls.fetch_add(1, std::memory_order_acq_rel);
        if (call_index == 0) {
            first_provider_entered.set_value();
            release_future.wait();
        }
        return {};
    };

    std::optional<WalAppendResult> first_result;
    auto first_future = std::async(std::launch::async, [&] {
        return scheduler.maybe_run(std::chrono::steady_clock::now(), provider, false, first_result);
    });

    first_entered_future.wait();

    std::optional<WalAppendResult> second_result;
    auto second_future = std::async(std::launch::async, [&] {
        return scheduler.maybe_run(std::chrono::steady_clock::now(), provider, true, second_result);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    release_first_provider.set_value();

    REQUIRE_FALSE(first_future.get());
    REQUIRE(first_result.has_value());
    REQUIRE_FALSE(second_future.get());
    REQUIRE(second_result.has_value());

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 2U);
    CHECK(telemetry.queue_waits == 1U);
    CHECK(telemetry.max_queue_depth >= 2U);
    CHECK(telemetry.last_queue_depth == 2U);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

TEST_CASE("CheckpointScheduler telemetry reports checkpoint duration trends")
{
    using namespace bored::storage;
    using namespace std::chrono_literals;

    auto io = make_async_io();
    auto wal_dir = make_temp_dir("bored_checkpoint_scheduler_trends_");

    WalWriterConfig wal_config{};
    wal_config.directory = wal_dir;
    wal_config.segment_size = 4U * kWalBlockSize;
    wal_config.buffer_size = 2U * kWalBlockSize;

    auto wal_writer = std::make_shared<WalWriter>(io, wal_config);
    auto checkpoint_manager = std::make_shared<CheckpointManager>(wal_writer);

    WalRetentionManager retention_manager{wal_config.directory,
                                          wal_config.file_prefix,
                                          wal_config.file_extension,
                                          wal_writer->durability_horizon()};

    bored::txn::TransactionIdAllocatorStub allocator{200U};
    bored::txn::TransactionManager transaction_manager{allocator};

    CheckpointCoordinator coordinator{checkpoint_manager, transaction_manager, retention_manager};

    CheckpointScheduler::Config scheduler_config{};
    scheduler_config.min_interval = std::chrono::milliseconds{0};
    scheduler_config.flush_after_emit = false;
    scheduler_config.coordinator = &coordinator;

    CheckpointScheduler scheduler{checkpoint_manager, scheduler_config};

    std::atomic<int> provider_invocations{0};
    auto provider = [&](CheckpointSnapshot& snapshot) -> std::error_code {
        snapshot.redo_lsn = wal_writer->next_lsn();
        snapshot.undo_lsn = snapshot.redo_lsn;
        snapshot.dirty_pages = {
            WalCheckpointDirtyPageEntry{.page_id = 64U, .reserved = 0U, .page_lsn = snapshot.redo_lsn},
            WalCheckpointDirtyPageEntry{.page_id = 65U, .reserved = 0U, .page_lsn = snapshot.redo_lsn}
        };
        snapshot.active_transactions.clear();
        snapshot.index_metadata.clear();

        const auto invocation = provider_invocations.fetch_add(1, std::memory_order_relaxed);
        if ((invocation % 2) == 1) {
            const auto attempt = invocation / 2;
            if (attempt == 0) {
                std::this_thread::sleep_for(5ms);
            } else if (attempt == 1) {
                std::this_thread::sleep_for(2ms);
            } else {
                std::this_thread::sleep_for(1ms);
            }
        }

        return {};
    };

    std::optional<WalAppendResult> first_result;
    REQUIRE_FALSE(scheduler.maybe_run(std::chrono::steady_clock::now(), provider, false, first_result));
    REQUIRE(first_result.has_value());

    std::this_thread::sleep_for(1ms);

    std::optional<WalAppendResult> second_result;
    REQUIRE_FALSE(scheduler.maybe_run(std::chrono::steady_clock::now(), provider, true, second_result));
    REQUIRE(second_result.has_value());

    std::this_thread::sleep_for(1ms);

    std::optional<WalAppendResult> third_result;
    REQUIRE_FALSE(scheduler.maybe_run(std::chrono::steady_clock::now(), provider, true, third_result));
    REQUIRE(third_result.has_value());

    const auto telemetry = scheduler.telemetry_snapshot();
    CHECK(telemetry.emitted_checkpoints == 3U);
    CHECK(telemetry.total_checkpoint_duration_ns >= telemetry.max_checkpoint_duration_ns);
    CHECK(telemetry.max_checkpoint_duration_ns >= telemetry.last_checkpoint_duration_ns);
    CHECK(telemetry.last_checkpoint_duration_ns > 0U);
    CHECK(telemetry.total_fence_duration_ns >= telemetry.max_fence_duration_ns);
    CHECK(telemetry.max_fence_duration_ns >= telemetry.last_fence_duration_ns);

    const auto last_checkpoint_ns = std::chrono::nanoseconds{telemetry.last_checkpoint_duration_ns};
    const auto max_checkpoint_ns = std::chrono::nanoseconds{telemetry.max_checkpoint_duration_ns};
    const auto kMaxToleratedDuration = 25ms;  // Windows timer resolution can stretch 1ms sleeps to ~16ms.
    CHECK(last_checkpoint_ns <= kMaxToleratedDuration);
    CHECK(max_checkpoint_ns >= 5ms);

    const auto last_fence_ns = std::chrono::nanoseconds{telemetry.last_fence_duration_ns};
    const auto max_fence_ns = std::chrono::nanoseconds{telemetry.max_fence_duration_ns};
    CHECK(last_fence_ns <= kMaxToleratedDuration);
    CHECK(max_fence_ns >= 5ms);

    REQUIRE_FALSE(wal_writer->close());
    io->shutdown();
    (void)std::filesystem::remove_all(wal_dir);
}

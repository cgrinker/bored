#include "bored/storage/checkpoint_scheduler.hpp"

#include "bored/storage/async_io.hpp"
#include "bored/storage/checkpoint_manager.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_writer.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
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

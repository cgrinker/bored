#include "bored/storage/storage_metrics.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>

using namespace std::chrono_literals;
using Catch::Approx;

TEST_CASE("collect_storage_metrics computes lag and latency")
{
    using bored::storage::CheckpointTelemetrySnapshot;
    using bored::executor::ExecutorTelemetrySnapshot;
    using bored::storage::RecoveryTelemetrySnapshot;
    using bored::storage::StorageMetricsSnapshot;
    using bored::storage::StorageTelemetryRegistry;

    StorageTelemetryRegistry registry;

    const auto now = std::chrono::steady_clock::now();
    const auto last_checkpoint = now - 5s;
    const auto last_checkpoint_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(last_checkpoint.time_since_epoch()).count();

    registry.register_checkpoint_scheduler("test_checkpoint", [last_checkpoint_ns] {
        CheckpointTelemetrySnapshot snapshot{};
        snapshot.emitted_checkpoints = 7U;
        snapshot.last_checkpoint_timestamp_ns = static_cast<std::uint64_t>(last_checkpoint_ns);
        return snapshot;
    });

    registry.register_recovery("test_recovery", [] {
        RecoveryTelemetrySnapshot snapshot{};
        snapshot.last_replay_backlog_bytes = 42'000U;
        snapshot.max_replay_backlog_bytes = 84'000U;
        return snapshot;
    });

    registry.register_executor("test_executor", [] {
        ExecutorTelemetrySnapshot snapshot{};
        snapshot.projection_latency.invocations = 4U;
        snapshot.projection_latency.total_duration_ns = 8'000'000U;
        snapshot.projection_latency.last_duration_ns = 3'000'000U;
        return snapshot;
    });

    const StorageMetricsSnapshot metrics = bored::storage::collect_storage_metrics(registry, now);

    CHECK(metrics.checkpoint_emitted_total == 7U);
    CHECK(metrics.wal_replay_backlog_bytes == 42'000U);
    CHECK(metrics.wal_replay_backlog_max_bytes == 84'000U);
    CHECK(metrics.query_latency_count == 4U);
    CHECK(metrics.query_latency_total_seconds == Approx(0.008).margin(0.0000001));
    CHECK(metrics.query_latency_last_seconds == Approx(0.003).margin(0.0000001));
    CHECK(metrics.checkpoint_lag_seconds == Approx(5.0).margin(0.05));
}

TEST_CASE("storage_metrics_to_openmetrics emits gauges")
{
    bored::storage::StorageMetricsSnapshot snapshot{};
    snapshot.checkpoint_lag_seconds = 12.5;
    snapshot.checkpoint_last_timestamp_ns = 1'700'000'000'000'000ULL;
    snapshot.checkpoint_emitted_total = 9U;
    snapshot.wal_replay_backlog_bytes = 1024U;
    snapshot.wal_replay_backlog_max_bytes = 4096U;
    snapshot.query_latency_count = 3U;
    snapshot.query_latency_total_seconds = 0.012;
    snapshot.query_latency_last_seconds = 0.004;

    const auto wall_now = std::chrono::system_clock::time_point{std::chrono::seconds{1'700'000'100}};
    const auto text = bored::storage::storage_metrics_to_openmetrics(snapshot, wall_now);

    CHECK(text.find("bored_checkpoint_lag_seconds 12.5") != std::string::npos);
    CHECK(text.find("bored_checkpoint_emitted_total 9") != std::string::npos);
    CHECK(text.find("bored_wal_replay_backlog_bytes 1024") != std::string::npos);
    CHECK(text.find("bored_query_latency_seconds_count 3") != std::string::npos);
    CHECK(text.find("bored_query_latency_last_seconds 0.004") != std::string::npos);
}

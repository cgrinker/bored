#pragma once

#include "bored/storage/storage_telemetry_registry.hpp"

#include <chrono>
#include <cstdint>
#include <string>

namespace bored::storage {

struct StorageMetricsSnapshot final {
    double checkpoint_lag_seconds = 0.0;
    std::uint64_t checkpoint_last_timestamp_ns = 0U;
    std::uint64_t checkpoint_emitted_total = 0U;
    std::uint64_t wal_replay_backlog_bytes = 0U;
    std::uint64_t wal_replay_backlog_max_bytes = 0U;
    std::uint64_t query_latency_count = 0U;
    double query_latency_total_seconds = 0.0;
    double query_latency_last_seconds = 0.0;
};

StorageMetricsSnapshot collect_storage_metrics(StorageTelemetryRegistry& registry,
                                               std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

std::string storage_metrics_to_openmetrics(const StorageMetricsSnapshot& snapshot,
                                           std::chrono::system_clock::time_point wall_now = std::chrono::system_clock::now());

}  // namespace bored::storage

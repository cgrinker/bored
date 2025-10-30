#include "bored/storage/storage_metrics.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <sstream>

namespace bored::storage {
namespace {

constexpr double kNsPerSecond = 1'000'000'000.0;

[[nodiscard]] double to_seconds(std::uint64_t ns) noexcept
{
    return static_cast<double>(ns) / kNsPerSecond;
}

[[nodiscard]] std::string format_double(double value)
{
    if (!std::isfinite(value)) {
        return "0";
    }
    std::ostringstream stream;
    stream << std::setprecision(17) << value;
    return stream.str();
}

}  // namespace

StorageMetricsSnapshot collect_storage_metrics(StorageTelemetryRegistry& registry,
                                               std::chrono::steady_clock::time_point now)
{
    StorageMetricsSnapshot snapshot{};

    const auto checkpoint = registry.aggregate_checkpoint_schedulers();
    snapshot.checkpoint_emitted_total = checkpoint.emitted_checkpoints;
    snapshot.checkpoint_last_timestamp_ns = checkpoint.last_checkpoint_timestamp_ns;
    if (checkpoint.last_checkpoint_timestamp_ns != 0U) {
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
        const auto now_count = static_cast<double>(now_ns.count());
        const auto last_count = static_cast<double>(checkpoint.last_checkpoint_timestamp_ns);
        const auto lag_ns = std::max(0.0, now_count - last_count);
        snapshot.checkpoint_lag_seconds = lag_ns / kNsPerSecond;
    } else {
        snapshot.checkpoint_lag_seconds = 0.0;
    }

    const auto recovery = registry.aggregate_recovery();
    snapshot.wal_replay_backlog_bytes = recovery.last_replay_backlog_bytes;
    snapshot.wal_replay_backlog_max_bytes = recovery.max_replay_backlog_bytes;

    const auto executors = registry.aggregate_executors();
    snapshot.query_latency_count = executors.projection_latency.invocations;
    snapshot.query_latency_total_seconds = to_seconds(executors.projection_latency.total_duration_ns);
    snapshot.query_latency_last_seconds = to_seconds(executors.projection_latency.last_duration_ns);

    return snapshot;
}

std::string storage_metrics_to_openmetrics(const StorageMetricsSnapshot& snapshot,
                                           std::chrono::system_clock::time_point wall_now)
{
    const auto wall_seconds = std::chrono::duration_cast<std::chrono::nanoseconds>(wall_now.time_since_epoch()).count()
        / kNsPerSecond;

    std::ostringstream out;
    out << "# HELP bored_checkpoint_lag_seconds Time since the last emitted checkpoint.\n";
    out << "# TYPE bored_checkpoint_lag_seconds gauge\n";
    out << "bored_checkpoint_lag_seconds " << format_double(snapshot.checkpoint_lag_seconds) << '\n';

    out << "# HELP bored_checkpoint_last_timestamp_seconds Wall clock timestamp of the last checkpoint.\n";
    out << "# TYPE bored_checkpoint_last_timestamp_seconds gauge\n";
    double last_checkpoint_seconds = snapshot.checkpoint_last_timestamp_ns == 0U
        ? 0.0
        : static_cast<double>(snapshot.checkpoint_last_timestamp_ns) / kNsPerSecond;
    out << "bored_checkpoint_last_timestamp_seconds " << format_double(last_checkpoint_seconds) << '\n';

    out << "# HELP bored_checkpoint_emitted_total Total checkpoints emitted by schedulers.\n";
    out << "# TYPE bored_checkpoint_emitted_total counter\n";
    out << "bored_checkpoint_emitted_total " << snapshot.checkpoint_emitted_total << '\n';

    out << "# HELP bored_wal_replay_backlog_bytes Outstanding WAL bytes awaiting replay.\n";
    out << "# TYPE bored_wal_replay_backlog_bytes gauge\n";
    out << "bored_wal_replay_backlog_bytes " << snapshot.wal_replay_backlog_bytes << '\n';

    out << "# HELP bored_wal_replay_backlog_max_bytes Historical maximum WAL replay backlog.\n";
    out << "# TYPE bored_wal_replay_backlog_max_bytes gauge\n";
    out << "bored_wal_replay_backlog_max_bytes " << snapshot.wal_replay_backlog_max_bytes << '\n';

    out << "# HELP bored_query_latency_seconds Summary of projection operator latency (proxy for query latency).\n";
    out << "# TYPE bored_query_latency_seconds summary\n";
    out << "bored_query_latency_seconds_sum " << format_double(snapshot.query_latency_total_seconds) << '\n';
    out << "bored_query_latency_seconds_count " << snapshot.query_latency_count << '\n';

    out << "# HELP bored_query_latency_last_seconds Latest projection latency sample.\n";
    out << "# TYPE bored_query_latency_last_seconds gauge\n";
    out << "bored_query_latency_last_seconds " << format_double(snapshot.query_latency_last_seconds) << '\n';

    out << "# HELP bored_metrics_scrape_timestamp_seconds Wall clock time when the metrics snapshot was generated.\n";
    out << "# TYPE bored_metrics_scrape_timestamp_seconds gauge\n";
    out << "bored_metrics_scrape_timestamp_seconds " << format_double(wall_seconds) << '\n';

    return out.str();
}

}  // namespace bored::storage

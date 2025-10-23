#include "bored/storage/wal_telemetry_registry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

using bored::storage::WalTelemetryRegistry;
using bored::storage::WalWriterTelemetrySnapshot;

namespace {

WalWriterTelemetrySnapshot make_snapshot(std::uint64_t append_calls, std::uint64_t flush_calls)
{
    WalWriterTelemetrySnapshot snapshot{};
    snapshot.append_calls = append_calls;
    snapshot.appended_bytes = append_calls * 128U;
    snapshot.total_append_duration_ns = append_calls * 1000U;
    snapshot.last_append_duration_ns = append_calls * 100U;
    snapshot.flush_calls = flush_calls;
    snapshot.flushed_bytes = flush_calls * 256U;
    snapshot.max_flush_bytes = flush_calls * 256U;
    snapshot.total_flush_duration_ns = flush_calls * 2000U;
    snapshot.last_flush_duration_ns = flush_calls * 200U;
    snapshot.retention_invocations = flush_calls;
    snapshot.retention_failures = 0U;
    snapshot.retention_scanned_segments = flush_calls * 2U;
    snapshot.retention_candidate_segments = flush_calls;
    snapshot.retention_pruned_segments = flush_calls;
    snapshot.retention_archived_segments = flush_calls / 2U;
    snapshot.retention_total_duration_ns = flush_calls * 500U;
    snapshot.retention_last_duration_ns = flush_calls * 50U;
    return snapshot;
}

}  // namespace

TEST_CASE("WalTelemetryRegistry aggregates registered samplers")
{
    WalTelemetryRegistry registry{};

    registry.register_sampler("writer_a", [] {
        return make_snapshot(3U, 2U);
    });
    registry.register_sampler("writer_b", [] {
        return make_snapshot(5U, 4U);
    });

    auto total = registry.aggregate();
    REQUIRE(total.append_calls == 8U);
    REQUIRE(total.flush_calls == 6U);
    REQUIRE(total.appended_bytes == (3U + 5U) * 128U);
    REQUIRE(total.flushed_bytes == (2U + 4U) * 256U);
    REQUIRE(total.max_flush_bytes == 4U * 256U);
    REQUIRE(total.total_append_duration_ns == (3U + 5U) * 1000U);
    REQUIRE(total.total_flush_duration_ns == (2U + 4U) * 2000U);
    REQUIRE(total.last_append_duration_ns == 5U * 100U);
    REQUIRE(total.last_flush_duration_ns == 4U * 200U);
    REQUIRE(total.retention_invocations == 6U);
    REQUIRE(total.retention_failures == 0U);
    REQUIRE(total.retention_scanned_segments == (2U + 4U) * 2U);
    REQUIRE(total.retention_candidate_segments == 6U);
    REQUIRE(total.retention_pruned_segments == 6U);
    REQUIRE(total.retention_archived_segments == (2U + 4U) / 2U);
    REQUIRE(total.retention_total_duration_ns == (2U + 4U) * 500U);
    REQUIRE(total.retention_last_duration_ns == 4U * 50U);
}

TEST_CASE("WalTelemetryRegistry visit enumerates snapshots")
{
    WalTelemetryRegistry registry{};

    registry.register_sampler("writer_a", [] {
        return make_snapshot(1U, 1U);
    });

    registry.register_sampler("writer_b", [] {
        return make_snapshot(2U, 3U);
    });

    std::vector<std::string> identifiers{};
    std::vector<WalWriterTelemetrySnapshot> snapshots{};

    registry.visit([&](const std::string& identifier, const WalWriterTelemetrySnapshot& snapshot) {
        identifiers.push_back(identifier);
        snapshots.push_back(snapshot);
    });

    REQUIRE(identifiers.size() == 2U);
    REQUIRE(snapshots.size() == 2U);
    REQUIRE((identifiers == std::vector<std::string>{"writer_a", "writer_b"}
        || identifiers == std::vector<std::string>{"writer_b", "writer_a"}));

    WalWriterTelemetrySnapshot total{};
    for (const auto& snapshot : snapshots) {
        total.append_calls += snapshot.append_calls;
        total.flush_calls += snapshot.flush_calls;
        total.retention_invocations += snapshot.retention_invocations;
    }
    REQUIRE(total.append_calls == 3U);
    REQUIRE(total.flush_calls == 4U);
    REQUIRE(total.retention_invocations == 4U);

    registry.unregister_sampler("writer_b");
    auto aggregated = registry.aggregate();
    REQUIRE(aggregated.append_calls == 1U);
    REQUIRE(aggregated.flush_calls == 1U);
}

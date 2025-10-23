#include "bored/storage/storage_telemetry_registry.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <string>
#include <vector>

using namespace bored::storage;

namespace {

PageManagerTelemetrySnapshot make_page_snapshot(std::uint64_t base)
{
    PageManagerTelemetrySnapshot snapshot{};
    snapshot.initialize.attempts = base + 1U;
    snapshot.initialize.failures = base;
    snapshot.initialize.total_duration_ns = (base + 1U) * 10U;
    snapshot.initialize.last_duration_ns = (base + 1U) * 5U;

    snapshot.insert.attempts = base + 2U;
    snapshot.insert.failures = base / 2U;
    snapshot.insert.total_duration_ns = (base + 2U) * 20U;
    snapshot.insert.last_duration_ns = (base + 2U) * 6U;

    snapshot.remove.attempts = base + 3U;
    snapshot.remove.failures = base / 3U;
    snapshot.remove.total_duration_ns = (base + 3U) * 30U;
    snapshot.remove.last_duration_ns = (base + 3U) * 7U;

    snapshot.update.attempts = base + 4U;
    snapshot.update.failures = base / 4U;
    snapshot.update.total_duration_ns = (base + 4U) * 40U;
    snapshot.update.last_duration_ns = (base + 4U) * 8U;

    snapshot.compact.attempts = base + 5U;
    snapshot.compact.failures = base / 5U;
    snapshot.compact.total_duration_ns = (base + 5U) * 50U;
    snapshot.compact.last_duration_ns = (base + 5U) * 9U;

    snapshot.shared_latch.attempts = base + 6U;
    snapshot.shared_latch.failures = base / 6U;
    snapshot.shared_latch.total_wait_ns = (base + 6U) * 11U;
    snapshot.shared_latch.last_wait_ns = (base + 6U) * 10U;

    snapshot.exclusive_latch.attempts = base + 7U;
    snapshot.exclusive_latch.failures = base / 7U;
    snapshot.exclusive_latch.total_wait_ns = (base + 7U) * 12U;
    snapshot.exclusive_latch.last_wait_ns = (base + 7U) * 11U;

    return snapshot;
}

CheckpointTelemetrySnapshot make_checkpoint_snapshot(std::uint64_t base)
{
    CheckpointTelemetrySnapshot snapshot{};
    snapshot.invocations = base + 1U;
    snapshot.forced_requests = base + 2U;
    snapshot.skipped_runs = base + 3U;
    snapshot.emitted_checkpoints = base + 4U;
    snapshot.emit_failures = base + 5U;
    snapshot.flush_failures = base + 6U;
    snapshot.retention_invocations = base + 7U;
    snapshot.retention_failures = base + 8U;
    snapshot.trigger_force = base + 9U;
    snapshot.trigger_first = base + 10U;
    snapshot.trigger_dirty = base + 11U;
    snapshot.trigger_active = base + 12U;
    snapshot.trigger_interval = base + 13U;
    snapshot.trigger_lsn_gap = base + 14U;
    snapshot.total_emit_duration_ns = (base + 15U) * 100U;
    snapshot.last_emit_duration_ns = (base + 16U) * 10U;
    snapshot.total_flush_duration_ns = (base + 17U) * 200U;
    snapshot.last_flush_duration_ns = (base + 18U) * 20U;
    snapshot.total_retention_duration_ns = (base + 19U) * 300U;
    snapshot.last_retention_duration_ns = (base + 20U) * 30U;
    snapshot.last_checkpoint_id = base + 21U;
    snapshot.last_checkpoint_lsn = base + 22U;
    snapshot.last_checkpoint_timestamp_ns = (base + 23U) * 40U;
    return snapshot;
}

WalRetentionTelemetrySnapshot make_retention_snapshot(std::uint64_t base)
{
    WalRetentionTelemetrySnapshot snapshot{};
    snapshot.invocations = base + 1U;
    snapshot.failures = base;
    snapshot.scanned_segments = (base + 2U) * 2U;
    snapshot.candidate_segments = base + 3U;
    snapshot.pruned_segments = base + 4U;
    snapshot.archived_segments = base + 5U;
    snapshot.total_duration_ns = (base + 6U) * 50U;
    snapshot.last_duration_ns = (base + 7U) * 5U;
    return snapshot;
}

}  // namespace

TEST_CASE("StorageTelemetryRegistry aggregates page managers")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm_a", [] { return make_page_snapshot(1U); });
    registry.register_page_manager("pm_b", [] { return make_page_snapshot(3U); });

    const auto total = registry.aggregate_page_managers();

    REQUIRE(total.initialize.attempts == ((1U + 1U) + (3U + 1U)));
    REQUIRE(total.initialize.failures == (1U + 3U));
    REQUIRE(total.initialize.total_duration_ns == ((1U + 1U) * 10U + (3U + 1U) * 10U));
    REQUIRE(total.initialize.last_duration_ns == ((3U + 1U) * 5U));

    REQUIRE(total.insert.attempts == ((1U + 2U) + (3U + 2U)));
    REQUIRE(total.remove.total_duration_ns == ((1U + 3U) * 30U + (3U + 3U) * 30U));
    REQUIRE(total.update.failures == (1U / 4U + 3U / 4U));
    REQUIRE(total.compact.last_duration_ns == ((3U + 5U) * 9U));

    REQUIRE(total.shared_latch.attempts == ((1U + 6U) + (3U + 6U)));
    REQUIRE(total.shared_latch.last_wait_ns == ((3U + 6U) * 10U));
    REQUIRE(total.exclusive_latch.total_wait_ns == ((1U + 7U) * 12U + (3U + 7U) * 12U));
}

TEST_CASE("StorageTelemetryRegistry visits page manager samplers")
{
    StorageTelemetryRegistry registry;

    registry.register_page_manager("pm_a", [] { return make_page_snapshot(2U); });
    registry.register_page_manager("pm_b", [] { return make_page_snapshot(5U); });

    std::vector<std::string> ids;
    std::vector<PageManagerTelemetrySnapshot> snapshots;

    registry.visit_page_managers([&](const std::string& id, const PageManagerTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        snapshots.push_back(snapshot);
    });

    REQUIRE(ids.size() == 2U);
    REQUIRE(snapshots.size() == 2U);
    REQUIRE(std::find(ids.begin(), ids.end(), "pm_a") != ids.end());
    REQUIRE(std::find(ids.begin(), ids.end(), "pm_b") != ids.end());
}

TEST_CASE("StorageTelemetryRegistry aggregates checkpoint schedulers")
{
    StorageTelemetryRegistry registry;
    registry.register_checkpoint_scheduler("ckpt_a", [] { return make_checkpoint_snapshot(10U); });
    registry.register_checkpoint_scheduler("ckpt_b", [] { return make_checkpoint_snapshot(20U); });

    const auto total = registry.aggregate_checkpoint_schedulers();

    REQUIRE(total.invocations == ((10U + 1U) + (20U + 1U)));
    REQUIRE(total.emit_failures == ((10U + 5U) + (20U + 5U)));
    REQUIRE(total.total_emit_duration_ns == ((10U + 15U) * 100U + (20U + 15U) * 100U));
    REQUIRE(total.last_emit_duration_ns == ((20U + 16U) * 10U));
    REQUIRE(total.last_checkpoint_id == (20U + 21U));
    REQUIRE(total.last_checkpoint_timestamp_ns == ((20U + 23U) * 40U));
}

TEST_CASE("StorageTelemetryRegistry aggregates WAL retention samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_wal_retention("ret_a", [] { return make_retention_snapshot(3U); });
    registry.register_wal_retention("ret_b", [] { return make_retention_snapshot(7U); });

    const auto total = registry.aggregate_wal_retention();

    REQUIRE(total.invocations == ((3U + 1U) + (7U + 1U)));
    REQUIRE(total.failures == (3U + 7U));
    REQUIRE(total.scanned_segments == ((3U + 2U) * 2U + (7U + 2U) * 2U));
    REQUIRE(total.archived_segments == ((3U + 5U) + (7U + 5U)));
    REQUIRE(total.total_duration_ns == ((3U + 6U) * 50U + (7U + 6U) * 50U));
    REQUIRE(total.last_duration_ns == ((7U + 7U) * 5U));
}

TEST_CASE("StorageTelemetryRegistry unregisters samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_snapshot(1U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint_snapshot(1U); });
    registry.register_wal_retention("ret", [] { return make_retention_snapshot(1U); });

    registry.unregister_page_manager("pm");
    registry.unregister_checkpoint_scheduler("ckpt");
    registry.unregister_wal_retention("ret");

    auto pm_total = registry.aggregate_page_managers();
    auto ckpt_total = registry.aggregate_checkpoint_schedulers();
    auto ret_total = registry.aggregate_wal_retention();

    REQUIRE(pm_total.initialize.attempts == 0U);
    REQUIRE(ckpt_total.invocations == 0U);
    REQUIRE(ret_total.invocations == 0U);
}

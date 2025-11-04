#include "bored/executor/executor_telemetry_sampler.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

using namespace bored::storage;
using Catch::Approx;

namespace ddl = bored::ddl;
namespace parser = bored::parser;
namespace planner = bored::planner;
namespace executor = bored::executor;

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
    snapshot.total_checkpoint_duration_ns = (base + 17U) * 110U;
    snapshot.last_checkpoint_duration_ns = (base + 18U) * 11U;
    snapshot.max_checkpoint_duration_ns = (base + 19U) * 12U;
    snapshot.total_flush_duration_ns = (base + 20U) * 200U;
    snapshot.last_flush_duration_ns = (base + 21U) * 20U;
    snapshot.total_retention_duration_ns = (base + 22U) * 300U;
    snapshot.last_retention_duration_ns = (base + 23U) * 30U;
    snapshot.total_fence_duration_ns = (base + 24U) * 120U;
    snapshot.last_fence_duration_ns = (base + 25U) * 12U;
    snapshot.max_fence_duration_ns = (base + 26U) * 13U;
    snapshot.coordinator_begin_calls = base + 27U;
    snapshot.coordinator_begin_failures = base + 28U;
    snapshot.coordinator_prepare_calls = base + 29U;
    snapshot.coordinator_prepare_failures = base + 30U;
    snapshot.coordinator_commit_calls = base + 31U;
    snapshot.coordinator_commit_failures = base + 32U;
    snapshot.coordinator_abort_calls = base + 33U;
    snapshot.coordinator_dry_runs = base + 34U;
    snapshot.coordinator_total_begin_duration_ns = (base + 35U) * 400U;
    snapshot.coordinator_last_begin_duration_ns = (base + 36U) * 40U;
    snapshot.coordinator_total_prepare_duration_ns = (base + 37U) * 500U;
    snapshot.coordinator_last_prepare_duration_ns = (base + 38U) * 50U;
    snapshot.coordinator_total_commit_duration_ns = (base + 39U) * 600U;
    snapshot.coordinator_last_commit_duration_ns = (base + 40U) * 60U;
    snapshot.last_checkpoint_id = base + 41U;
    snapshot.last_checkpoint_lsn = base + 42U;
    snapshot.last_checkpoint_timestamp_ns = (base + 43U) * 40U;
    snapshot.io_throttle_deferrals = base + 44U;
    snapshot.io_throttle_bytes_consumed = (base + 45U) * 70U;
    snapshot.io_throttle_budget = base + 46U;
    snapshot.queue_waits = base + 47U;
    snapshot.last_queue_depth = base + 48U;
    snapshot.max_queue_depth = base + 49U;
    snapshot.blocked_transactions = base + 50U;
    snapshot.total_blocked_duration_ns = (base + 51U) * 80U;
    snapshot.last_blocked_duration_ns = (base + 52U) * 8U;
    snapshot.max_blocked_duration_ns = (base + 53U) * 9U;
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

RecoveryTelemetrySnapshot make_recovery_snapshot(std::uint64_t base)
{
    RecoveryTelemetrySnapshot snapshot{};
    snapshot.plan_invocations = base + 1U;
    snapshot.plan_failures = base % 2U;
    snapshot.total_enumerate_duration_ns = (base + 2U) * 3U;
    snapshot.last_enumerate_duration_ns = (base + 3U) * 2U;
    snapshot.max_enumerate_duration_ns = (base + 4U) * 4U;
    snapshot.total_plan_duration_ns = (base + 5U) * 7U;
    snapshot.last_plan_duration_ns = (base + 6U) * 5U;
    snapshot.max_plan_duration_ns = (base + 7U) * 6U;
    snapshot.redo_invocations = base + 8U;
    snapshot.redo_failures = base % 3U;
    snapshot.total_redo_duration_ns = (base + 9U) * 11U;
    snapshot.last_redo_duration_ns = (base + 10U) * 9U;
    snapshot.max_redo_duration_ns = (base + 11U) * 12U;
    snapshot.undo_invocations = base + 12U;
    snapshot.undo_failures = base % 5U;
    snapshot.total_undo_duration_ns = (base + 13U) * 13U;
    snapshot.last_undo_duration_ns = (base + 14U) * 7U;
    snapshot.max_undo_duration_ns = (base + 15U) * 14U;
    snapshot.cleanup_invocations = base + 16U;
    snapshot.cleanup_failures = base % 7U;
    snapshot.total_cleanup_duration_ns = (base + 17U) * 17U;
    snapshot.last_cleanup_duration_ns = (base + 18U) * 8U;
    snapshot.max_cleanup_duration_ns = (base + 19U) * 18U;
    snapshot.last_replay_backlog_bytes = (base + 20U) * 100U;
    snapshot.max_replay_backlog_bytes = (base + 21U) * 120U;
    return snapshot;
}

TempCleanupTelemetrySnapshot make_temp_cleanup_snapshot(std::uint64_t base)
{
    TempCleanupTelemetrySnapshot snapshot{};
    snapshot.invocations = base + 1U;
    snapshot.failures = base;
    snapshot.removed_entries = (base + 2U) * 3U;
    snapshot.total_duration_ns = (base + 3U) * 25U;
    snapshot.last_duration_ns = (base + 4U) * 5U;
    return snapshot;
}

CatalogTelemetrySnapshot make_catalog_snapshot(std::uint64_t base)
{
    CatalogTelemetrySnapshot snapshot{};
    snapshot.cache_hits = (base + 1U) * 10U;
    snapshot.cache_misses = (base + 2U) * 5U;
    snapshot.cache_relations = static_cast<std::size_t>((base + 3U));
    snapshot.cache_total_bytes = static_cast<std::size_t>((base + 4U) * 1024U);
    snapshot.published_batches = base + 5U;
    snapshot.published_mutations = base + 6U;
    snapshot.published_wal_records = base + 7U;
    snapshot.publish_failures = base + 8U;
    snapshot.aborted_batches = base + 9U;
    snapshot.aborted_mutations = base + 10U;
    return snapshot;
}

ddl::DdlTelemetrySnapshot make_ddl_snapshot(std::uint64_t base)
{
    ddl::DdlTelemetrySnapshot snapshot{};

    const auto set_verb = [&](ddl::DdlVerb verb, std::uint64_t offset) {
        const auto index = static_cast<std::size_t>(verb);
        snapshot.verbs[index].attempts = base + offset;
        snapshot.verbs[index].successes = base + offset + 1U;
        snapshot.verbs[index].failures = base + offset - 1U;
        snapshot.verbs[index].total_duration_ns = (base + offset) * 10U;
        snapshot.verbs[index].last_duration_ns = (base + offset) * 5U;
    };

    set_verb(ddl::DdlVerb::CreateDatabase, 1U);
    set_verb(ddl::DdlVerb::CreateTable, 3U);
    set_verb(ddl::DdlVerb::DropTable, 4U);

    snapshot.failures.handler_missing = base;
    snapshot.failures.validation_failures = base + 1U;
    snapshot.failures.execution_failures = base + 2U;
    snapshot.failures.other_failures = base + 3U;
    return snapshot;
}

parser::ParserTelemetrySnapshot make_parser_snapshot(std::uint64_t base)
{
    parser::ParserTelemetrySnapshot snapshot{};
    snapshot.scripts_attempted = base + 1U;
    snapshot.scripts_succeeded = base;
    snapshot.statements_attempted = base + 2U;
    snapshot.statements_succeeded = base + 1U;
    snapshot.diagnostics_info = base + 3U;
    snapshot.diagnostics_warning = base + 4U;
    snapshot.diagnostics_error = base + 5U;
    snapshot.total_parse_duration_ns = (base + 6U) * 20U;
    snapshot.last_parse_duration_ns = (base + 7U) * 10U;
    return snapshot;
}

planner::PlannerTelemetrySnapshot make_planner_snapshot(std::uint64_t base)
{
    planner::PlannerTelemetrySnapshot snapshot{};
    snapshot.plans_attempted = base + 1U;
    snapshot.plans_succeeded = base;
    snapshot.plans_failed = base / 2U;
    snapshot.rules_attempted = base + 2U;
    snapshot.rules_applied = base + 1U;
    snapshot.cost_evaluations = (base + 3U) * 2U;
    snapshot.alternatives_considered = base + 4U;
    snapshot.total_chosen_cost = static_cast<double>((base + 5U) * 15U);
    snapshot.last_chosen_cost = static_cast<double>((base + 6U) * 7U);
    snapshot.min_chosen_cost = static_cast<double>(base + 1U);
    snapshot.max_chosen_cost = static_cast<double>((base + 7U) * 5U);
    return snapshot;
}

executor::ExecutorTelemetrySnapshot make_executor_snapshot(std::uint64_t seed)
{
    executor::ExecutorTelemetrySnapshot snapshot{};
    snapshot.seq_scan_rows_read = seed + 1U;
    snapshot.seq_scan_rows_visible = seed + 2U;
    snapshot.filter_rows_evaluated = seed + 3U;
    snapshot.filter_rows_passed = seed + 1U;
    snapshot.projection_rows_emitted = seed + 4U;
    snapshot.nested_loop_rows_compared = seed + 5U;
    snapshot.nested_loop_rows_matched = seed + 2U;
    snapshot.nested_loop_rows_emitted = seed + 2U;
    snapshot.hash_join_build_rows = seed + 6U;
    snapshot.hash_join_probe_rows = seed + 7U;
    snapshot.hash_join_rows_matched = seed + 3U;
    snapshot.aggregation_input_rows = seed + 8U;
    snapshot.aggregation_groups_emitted = seed + 4U;
    snapshot.insert_rows_attempted = seed + 5U;
    snapshot.insert_rows_succeeded = seed + 5U;
    snapshot.insert_payload_bytes = (seed + 1U) * 10U;
    snapshot.insert_wal_bytes = (seed + 1U) * 12U;
    snapshot.update_rows_attempted = seed + 6U;
    snapshot.update_rows_succeeded = seed + 6U;
    snapshot.update_new_payload_bytes = (seed + 2U) * 9U;
    snapshot.update_old_payload_bytes = (seed + 3U) * 9U;
    snapshot.update_wal_bytes = (seed + 4U) * 11U;
    snapshot.delete_rows_attempted = seed + 7U;
    snapshot.delete_rows_succeeded = seed + 7U;
    snapshot.delete_reclaimed_bytes = (seed + 2U) * 7U;
    snapshot.delete_wal_bytes = (seed + 3U) * 8U;

    auto make_latency = [&](std::uint64_t base) {
        executor::ExecutorTelemetrySnapshot::OperatorLatencySnapshot latency{};
        latency.invocations = base + 1U;
        latency.total_duration_ns = (base + 2U) * 10U;
        latency.last_duration_ns = (base + 3U) * 5U;
        return latency;
    };

    snapshot.seq_scan_latency = make_latency(seed);
    snapshot.filter_latency = make_latency(seed + 10U);
    snapshot.projection_latency = make_latency(seed + 20U);
    snapshot.spool_latency = make_latency(seed + 30U);
    snapshot.nested_loop_latency = make_latency(seed + 40U);
    snapshot.hash_join_latency = make_latency(seed + 50U);
    snapshot.aggregation_latency = make_latency(seed + 60U);
    snapshot.insert_latency = make_latency(seed + 70U);
    snapshot.update_latency = make_latency(seed + 80U);
    snapshot.delete_latency = make_latency(seed + 90U);
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
    REQUIRE(total.total_checkpoint_duration_ns == ((10U + 17U) * 110U + (20U + 17U) * 110U));
    REQUIRE(total.last_checkpoint_duration_ns == ((20U + 18U) * 11U));
    REQUIRE(total.max_checkpoint_duration_ns == ((20U + 19U) * 12U));
    REQUIRE(total.total_flush_duration_ns == ((10U + 20U) * 200U + (20U + 20U) * 200U));
    REQUIRE(total.last_flush_duration_ns == ((20U + 21U) * 20U));
    REQUIRE(total.total_retention_duration_ns == ((10U + 22U) * 300U + (20U + 22U) * 300U));
    REQUIRE(total.last_retention_duration_ns == ((20U + 23U) * 30U));
    REQUIRE(total.total_fence_duration_ns == ((10U + 24U) * 120U + (20U + 24U) * 120U));
    REQUIRE(total.last_fence_duration_ns == ((20U + 25U) * 12U));
    REQUIRE(total.max_fence_duration_ns == ((20U + 26U) * 13U));
    REQUIRE(total.coordinator_begin_calls == ((10U + 27U) + (20U + 27U)));
    REQUIRE(total.coordinator_begin_failures == ((10U + 28U) + (20U + 28U)));
    REQUIRE(total.coordinator_prepare_calls == ((10U + 29U) + (20U + 29U)));
    REQUIRE(total.coordinator_prepare_failures == ((10U + 30U) + (20U + 30U)));
    REQUIRE(total.coordinator_commit_calls == ((10U + 31U) + (20U + 31U)));
    REQUIRE(total.coordinator_commit_failures == ((10U + 32U) + (20U + 32U)));
    REQUIRE(total.coordinator_abort_calls == ((10U + 33U) + (20U + 33U)));
    REQUIRE(total.coordinator_dry_runs == ((10U + 34U) + (20U + 34U)));
    REQUIRE(total.coordinator_total_begin_duration_ns == ((10U + 35U) * 400U + (20U + 35U) * 400U));
    REQUIRE(total.coordinator_last_begin_duration_ns == ((20U + 36U) * 40U));
    REQUIRE(total.coordinator_total_prepare_duration_ns == ((10U + 37U) * 500U + (20U + 37U) * 500U));
    REQUIRE(total.coordinator_last_prepare_duration_ns == ((20U + 38U) * 50U));
    REQUIRE(total.coordinator_total_commit_duration_ns == ((10U + 39U) * 600U + (20U + 39U) * 600U));
    REQUIRE(total.coordinator_last_commit_duration_ns == ((20U + 40U) * 60U));
    REQUIRE(total.last_checkpoint_id == (20U + 41U));
    REQUIRE(total.last_checkpoint_lsn == (20U + 42U));
    REQUIRE(total.last_checkpoint_timestamp_ns == ((20U + 43U) * 40U));
    REQUIRE(total.io_throttle_deferrals == ((10U + 44U) + (20U + 44U)));
    REQUIRE(total.io_throttle_bytes_consumed == ((10U + 45U) * 70U + (20U + 45U) * 70U));
    REQUIRE(total.io_throttle_budget == (20U + 46U));
    REQUIRE(total.queue_waits == ((10U + 47U) + (20U + 47U)));
    REQUIRE(total.last_queue_depth == (20U + 48U));
    REQUIRE(total.max_queue_depth == (20U + 49U));
    REQUIRE(total.blocked_transactions == ((10U + 50U) + (20U + 50U)));
    REQUIRE(total.total_blocked_duration_ns == ((10U + 51U) * 80U + (20U + 51U) * 80U));
    REQUIRE(total.last_blocked_duration_ns == ((20U + 52U) * 8U));
    REQUIRE(total.max_blocked_duration_ns == ((20U + 53U) * 9U));
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

TEST_CASE("StorageTelemetryRegistry aggregates recovery samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_recovery("rec_a", [] { return make_recovery_snapshot(2U); });
    registry.register_recovery("rec_b", [] { return make_recovery_snapshot(5U); });

    const auto total = registry.aggregate_recovery();

    REQUIRE(total.plan_invocations == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.plan_failures == (2U % 2U + 5U % 2U));
    REQUIRE(total.total_plan_duration_ns == ((2U + 5U) * 7U + (5U + 5U) * 7U));
    REQUIRE(total.last_plan_duration_ns == ((5U + 6U) * 5U));
    REQUIRE(total.max_plan_duration_ns == ((5U + 7U) * 6U));
    REQUIRE(total.redo_invocations == ((2U + 8U) + (5U + 8U)));
    REQUIRE(total.total_redo_duration_ns == ((2U + 9U) * 11U + (5U + 9U) * 11U));
    REQUIRE(total.last_redo_duration_ns == ((5U + 10U) * 9U));
    REQUIRE(total.max_redo_duration_ns == ((5U + 11U) * 12U));
    REQUIRE(total.last_replay_backlog_bytes == std::max((2U + 20U) * 100U, (5U + 20U) * 100U));
    REQUIRE(total.max_replay_backlog_bytes == std::max((2U + 21U) * 120U, (5U + 21U) * 120U));
}

TEST_CASE("StorageTelemetryRegistry aggregates temp cleanup samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_temp_cleanup("cleanup_a", [] { return make_temp_cleanup_snapshot(2U); });
    registry.register_temp_cleanup("cleanup_b", [] { return make_temp_cleanup_snapshot(5U); });

    const auto total = registry.aggregate_temp_cleanup();

    REQUIRE(total.invocations == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.failures == (2U + 5U));
    REQUIRE(total.removed_entries == (((2U + 2U) * 3U) + ((5U + 2U) * 3U)));
    REQUIRE(total.total_duration_ns == ((2U + 3U) * 25U + (5U + 3U) * 25U));
    REQUIRE(total.last_duration_ns == std::max((2U + 4U) * 5U, (5U + 4U) * 5U));
}

TEST_CASE("StorageTelemetryRegistry aggregates catalog telemetry")
{
    StorageTelemetryRegistry registry;
    registry.register_catalog("catalog_a", [] { return make_catalog_snapshot(1U); });
    registry.register_catalog("catalog_b", [] { return make_catalog_snapshot(4U); });

    const auto total = registry.aggregate_catalog();

    REQUIRE(total.cache_hits == ((1U + 1U) * 10U + (4U + 1U) * 10U));
    REQUIRE(total.cache_misses == ((1U + 2U) * 5U + (4U + 2U) * 5U));
    REQUIRE(total.cache_relations == static_cast<std::size_t>((1U + 3U) + (4U + 3U)));
    REQUIRE(total.cache_total_bytes == static_cast<std::size_t>(((1U + 4U) + (4U + 4U)) * 1024U));
    REQUIRE(total.published_batches == ((1U + 5U) + (4U + 5U)));
    REQUIRE(total.published_mutations == ((1U + 6U) + (4U + 6U)));
    REQUIRE(total.published_wal_records == ((1U + 7U) + (4U + 7U)));
    REQUIRE(total.publish_failures == ((1U + 8U) + (4U + 8U)));
    REQUIRE(total.aborted_batches == ((1U + 9U) + (4U + 9U)));
    REQUIRE(total.aborted_mutations == ((1U + 10U) + (4U + 10U)));
}

TEST_CASE("StorageTelemetryRegistry aggregates DDL telemetry")
{
    StorageTelemetryRegistry registry;
    registry.register_ddl("ddl_a", [] { return make_ddl_snapshot(1U); });
    registry.register_ddl("ddl_b", [] { return make_ddl_snapshot(4U); });

    const auto total = registry.aggregate_ddl();

    const auto create_database = static_cast<std::size_t>(ddl::DdlVerb::CreateDatabase);
    const auto create_table = static_cast<std::size_t>(ddl::DdlVerb::CreateTable);

    REQUIRE(total.verbs[create_database].attempts == ((1U + 1U) + (4U + 1U)));
    REQUIRE(total.verbs[create_database].successes == ((1U + 2U) + (4U + 2U)));
    REQUIRE(total.verbs[create_database].failures == ((1U + 0U) + (4U + 0U)));
    REQUIRE(total.verbs[create_database].total_duration_ns == ((1U + 1U) * 10U + (4U + 1U) * 10U));
    REQUIRE(total.verbs[create_database].last_duration_ns == ((4U + 1U) * 5U));

    REQUIRE(total.verbs[create_table].attempts == ((1U + 3U) + (4U + 3U)));
    REQUIRE(total.verbs[create_table].successes == ((1U + 4U) + (4U + 4U)));
    REQUIRE(total.failures.handler_missing == (1U + 4U));
    REQUIRE(total.failures.validation_failures == ((1U + 1U) + (4U + 1U)));
    REQUIRE(total.failures.execution_failures == ((1U + 2U) + (4U + 2U)));
}

TEST_CASE("StorageTelemetryRegistry aggregates parser telemetry")
{
    StorageTelemetryRegistry registry;
    registry.register_parser("parser_a", [] { return make_parser_snapshot(2U); });
    registry.register_parser("parser_b", [] { return make_parser_snapshot(5U); });

    const auto total = registry.aggregate_parser();

    REQUIRE(total.scripts_attempted == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.scripts_succeeded == (2U + 5U));
    REQUIRE(total.statements_attempted == ((2U + 2U) + (5U + 2U)));
    REQUIRE(total.diagnostics_warning == ((2U + 4U) + (5U + 4U)));
    REQUIRE(total.diagnostics_error == ((2U + 5U) + (5U + 5U)));
    REQUIRE(total.total_parse_duration_ns == ((2U + 6U) * 20U + (5U + 6U) * 20U));
    REQUIRE(total.last_parse_duration_ns == ((5U + 7U) * 10U));
}

TEST_CASE("StorageTelemetryRegistry visits parser samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_parser("parser_a", [] { return make_parser_snapshot(3U); });
    registry.register_parser("parser_b", [] { return make_parser_snapshot(6U); });

    std::vector<std::string> ids;
    std::vector<parser::ParserTelemetrySnapshot> snapshots;

    registry.visit_parser([&](const std::string& id, const parser::ParserTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        snapshots.push_back(snapshot);
    });

    REQUIRE(ids.size() == 2U);
    REQUIRE(std::find(ids.begin(), ids.end(), "parser_a") != ids.end());
    REQUIRE(std::find(ids.begin(), ids.end(), "parser_b") != ids.end());
    REQUIRE(snapshots.size() == 2U);
}

TEST_CASE("StorageTelemetryRegistry aggregates planner telemetry")
{
    StorageTelemetryRegistry registry;
    registry.register_planner("planner_a", [] { return make_planner_snapshot(2U); });
    registry.register_planner("planner_b", [] { return make_planner_snapshot(5U); });

    const auto total = registry.aggregate_planner();

    REQUIRE(total.plans_attempted == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.plans_succeeded == (2U + 5U));
    REQUIRE(total.plans_failed == (2U / 2U + 5U / 2U));
    REQUIRE(total.rules_attempted == ((2U + 2U) + (5U + 2U)));
    REQUIRE(total.rules_applied == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.cost_evaluations == (((2U + 3U) * 2U) + ((5U + 3U) * 2U)));
    REQUIRE(total.alternatives_considered == ((2U + 4U) + (5U + 4U)));
    REQUIRE(total.total_chosen_cost == Approx(static_cast<double>((2U + 5U) * 15U + (5U + 5U) * 15U)));
    CHECK(std::isfinite(total.last_chosen_cost));
    REQUIRE(total.min_chosen_cost == Approx(static_cast<double>(2U + 1U)));
    REQUIRE(total.max_chosen_cost == Approx(static_cast<double>((5U + 7U) * 5U)));
}

TEST_CASE("StorageTelemetryRegistry visits planner samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_planner("planner_a", [] { return make_planner_snapshot(3U); });
    registry.register_planner("planner_b", [] { return make_planner_snapshot(6U); });

    std::vector<std::string> ids;
    std::vector<planner::PlannerTelemetrySnapshot> snapshots;

    registry.visit_planner([&](const std::string& id, const planner::PlannerTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        snapshots.push_back(snapshot);
    });

    REQUIRE(ids.size() == 2U);
    REQUIRE(std::find(ids.begin(), ids.end(), "planner_a") != ids.end());
    REQUIRE(std::find(ids.begin(), ids.end(), "planner_b") != ids.end());
    REQUIRE(snapshots.size() == 2U);
}

TEST_CASE("StorageTelemetryRegistry aggregates executor telemetry")
{
    StorageTelemetryRegistry registry;
    registry.register_executor("exec_a", [] { return make_executor_snapshot(2U); });
    registry.register_executor("exec_b", [] { return make_executor_snapshot(5U); });

    const auto total = registry.aggregate_executors();

    REQUIRE(total.seq_scan_rows_read == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.seq_scan_rows_visible == ((2U + 2U) + (5U + 2U)));
    REQUIRE(total.hash_join_rows_matched == ((2U + 3U) + (5U + 3U)));
    REQUIRE(total.seq_scan_latency.invocations == ((2U + 1U) + (5U + 1U)));
    REQUIRE(total.seq_scan_latency.total_duration_ns == ((2U + 2U) * 10U + (5U + 2U) * 10U));
    REQUIRE(total.seq_scan_latency.last_duration_ns == std::max((2U + 3U) * 5U, (5U + 3U) * 5U));
    REQUIRE(total.spool_latency.invocations == ((2U + 30U + 1U) + (5U + 30U + 1U)));
    REQUIRE(total.spool_latency.total_duration_ns == ((2U + 30U + 2U) * 10U + (5U + 30U + 2U) * 10U));
    REQUIRE(total.spool_latency.last_duration_ns ==
        std::max((2U + 30U + 3U) * 5U, (5U + 30U + 3U) * 5U));
}

TEST_CASE("StorageTelemetryRegistry visits executor samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_executor("exec_a", [] { return make_executor_snapshot(3U); });
    registry.register_executor("exec_b", [] { return make_executor_snapshot(6U); });

    std::vector<std::string> ids;
    std::vector<executor::ExecutorTelemetrySnapshot> snapshots;

    registry.visit_executors([&](const std::string& id, const executor::ExecutorTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        snapshots.push_back(snapshot);
    });

    REQUIRE(ids.size() == 2U);
    REQUIRE(std::find(ids.begin(), ids.end(), "exec_a") != ids.end());
    REQUIRE(std::find(ids.begin(), ids.end(), "exec_b") != ids.end());
    REQUIRE(!snapshots.empty());
}

TEST_CASE("ExecutorTelemetrySampler manages registry registration")
{
    StorageTelemetryRegistry registry;
    executor::ExecutorTelemetry telemetry;

    {
        executor::ExecutorTelemetrySampler sampler{&registry, "exec_sampler", &telemetry};
        telemetry.record_projection_row();
        telemetry.record_seq_scan_row(true);

        const auto snapshot = registry.aggregate_executors();
        REQUIRE(snapshot.projection_rows_emitted == 1U);
        REQUIRE(snapshot.seq_scan_rows_visible == 1U);
        REQUIRE(snapshot.seq_scan_latency.invocations == 0U);  // latency recorded separately via scope
        REQUIRE(sampler.registered());
    }

    const auto after = registry.aggregate_executors();
    REQUIRE(after.projection_rows_emitted == 0U);
    REQUIRE(after.seq_scan_rows_visible == 0U);
}

TEST_CASE("StorageTelemetryRegistry visits DDL samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_ddl("ddl_a", [] { return make_ddl_snapshot(2U); });
    registry.register_ddl("ddl_b", [] { return make_ddl_snapshot(5U); });

    std::vector<std::string> ids;
    std::vector<ddl::DdlTelemetrySnapshot> snapshots;

    registry.visit_ddl([&](const std::string& id, const ddl::DdlTelemetrySnapshot& snapshot) {
        ids.push_back(id);
        snapshots.push_back(snapshot);
    });

    REQUIRE(ids.size() == 2U);
    REQUIRE(snapshots.size() == 2U);
    REQUIRE(std::find(ids.begin(), ids.end(), "ddl_a") != ids.end());
    REQUIRE(std::find(ids.begin(), ids.end(), "ddl_b") != ids.end());
}

TEST_CASE("StorageTelemetryRegistry unregisters samplers")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_snapshot(1U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint_snapshot(1U); });
    registry.register_wal_retention("ret", [] { return make_retention_snapshot(1U); });
    registry.register_temp_cleanup("cleanup", [] { return make_temp_cleanup_snapshot(2U); });
    registry.register_catalog("cat", [] { return make_catalog_snapshot(2U); });
    registry.register_ddl("ddl", [] { return make_ddl_snapshot(3U); });
    registry.register_parser("parser", [] { return make_parser_snapshot(4U); });
    registry.register_planner("planner", [] { return make_planner_snapshot(4U); });

    registry.unregister_page_manager("pm");
    registry.unregister_checkpoint_scheduler("ckpt");
    registry.unregister_wal_retention("ret");
    registry.unregister_temp_cleanup("cleanup");
    registry.unregister_catalog("cat");
    registry.unregister_ddl("ddl");
    registry.unregister_parser("parser");
    registry.unregister_planner("planner");

    auto pm_total = registry.aggregate_page_managers();
    auto ckpt_total = registry.aggregate_checkpoint_schedulers();
    auto ret_total = registry.aggregate_wal_retention();
    auto cleanup_total = registry.aggregate_temp_cleanup();
    auto cat_total = registry.aggregate_catalog();
    auto ddl_total = registry.aggregate_ddl();
    auto parser_total = registry.aggregate_parser();
    auto planner_total = registry.aggregate_planner();

    REQUIRE(pm_total.initialize.attempts == 0U);
    REQUIRE(ckpt_total.invocations == 0U);
    REQUIRE(ret_total.invocations == 0U);
    REQUIRE(cleanup_total.invocations == 0U);
    REQUIRE(cat_total.cache_hits == 0U);
    REQUIRE(std::all_of(ddl_total.verbs.begin(), ddl_total.verbs.end(), [](const auto& verb) {
        return verb.attempts == 0U && verb.successes == 0U && verb.failures == 0U && verb.total_duration_ns == 0U && verb.last_duration_ns == 0U;
    }));
    REQUIRE(ddl_total.failures.handler_missing == 0U);
    REQUIRE(ddl_total.failures.validation_failures == 0U);
    REQUIRE(ddl_total.failures.execution_failures == 0U);
    REQUIRE(ddl_total.failures.other_failures == 0U);
    REQUIRE(parser_total.scripts_attempted == 0U);
    REQUIRE(parser_total.diagnostics_error == 0U);
    REQUIRE(planner_total.plans_attempted == 0U);
    REQUIRE(planner_total.rules_attempted == 0U);
}

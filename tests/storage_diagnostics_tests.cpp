#include "bored/storage/storage_diagnostics.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <string>

using namespace bored::storage;
using Catch::Approx;
namespace ddl = bored::ddl;
namespace parser = bored::parser;
namespace txn = bored::txn;
namespace planner = bored::planner;
namespace executor = bored::executor;

namespace {

PageManagerTelemetrySnapshot make_page_manager(std::uint64_t seed)
{
    PageManagerTelemetrySnapshot snapshot{};
    snapshot.initialize.attempts = seed + 1U;
    snapshot.insert.attempts = seed + 2U;
    snapshot.update.attempts = seed + 3U;
    snapshot.remove.attempts = seed + 4U;
    snapshot.compact.attempts = seed + 5U;
    snapshot.shared_latch.attempts = seed + 6U;
    snapshot.exclusive_latch.attempts = seed + 7U;
    return snapshot;
}

CheckpointTelemetrySnapshot make_checkpoint(std::uint64_t seed)
{
    CheckpointTelemetrySnapshot snapshot{};
    snapshot.invocations = seed + 1U;
    snapshot.emitted_checkpoints = seed + 2U;
    snapshot.trigger_dirty = seed + 3U;
    snapshot.total_emit_duration_ns = (seed + 4U) * 10U;
    snapshot.total_checkpoint_duration_ns = (seed + 5U) * 100U;
    snapshot.last_checkpoint_duration_ns = (seed + 6U) * 50U;
    snapshot.max_checkpoint_duration_ns = (seed + 7U) * 75U;
    snapshot.total_fence_duration_ns = (seed + 8U) * 60U;
    snapshot.last_fence_duration_ns = (seed + 9U) * 30U;
    snapshot.max_fence_duration_ns = (seed + 10U) * 45U;
    snapshot.last_checkpoint_id = seed + 5U;
    return snapshot;
}

WalRetentionTelemetrySnapshot make_retention(std::uint64_t seed)
{
    WalRetentionTelemetrySnapshot snapshot{};
    snapshot.invocations = seed + 1U;
    snapshot.pruned_segments = seed + 2U;
    snapshot.total_duration_ns = (seed + 3U) * 5U;
    return snapshot;
}

RecoveryTelemetrySnapshot make_recovery(std::uint64_t seed)
{
    RecoveryTelemetrySnapshot snapshot{};
    snapshot.plan_invocations = seed + 1U;
    snapshot.plan_failures = seed % 2U;
    snapshot.total_enumerate_duration_ns = (seed + 2U) * 3U;
    snapshot.last_enumerate_duration_ns = (seed + 3U) * 2U;
    snapshot.max_enumerate_duration_ns = (seed + 4U) * 4U;
    snapshot.total_plan_duration_ns = (seed + 5U) * 7U;
    snapshot.last_plan_duration_ns = (seed + 6U) * 5U;
    snapshot.max_plan_duration_ns = (seed + 7U) * 6U;
    snapshot.redo_invocations = seed + 8U;
    snapshot.redo_failures = seed % 3U;
    snapshot.total_redo_duration_ns = (seed + 9U) * 11U;
    snapshot.last_redo_duration_ns = (seed + 10U) * 9U;
    snapshot.max_redo_duration_ns = (seed + 11U) * 12U;
    snapshot.undo_invocations = seed + 12U;
    snapshot.undo_failures = seed % 5U;
    snapshot.total_undo_duration_ns = (seed + 13U) * 13U;
    snapshot.last_undo_duration_ns = (seed + 14U) * 7U;
    snapshot.max_undo_duration_ns = (seed + 15U) * 14U;
    snapshot.cleanup_invocations = seed + 16U;
    snapshot.cleanup_failures = seed % 7U;
    snapshot.total_cleanup_duration_ns = (seed + 17U) * 17U;
    snapshot.last_cleanup_duration_ns = (seed + 18U) * 8U;
    snapshot.max_cleanup_duration_ns = (seed + 19U) * 18U;
    snapshot.last_replay_backlog_bytes = (seed + 20U) * 100U;
    snapshot.max_replay_backlog_bytes = (seed + 21U) * 120U;
    return snapshot;
}

IndexRetentionTelemetrySnapshot make_index_retention(std::uint64_t seed)
{
    IndexRetentionTelemetrySnapshot snapshot{};
    snapshot.scheduled_candidates = seed + 1U;
    snapshot.dropped_candidates = seed;
    snapshot.checkpoint_runs = seed + 2U;
    snapshot.checkpoint_failures = seed;
    snapshot.dispatch_batches = seed + 3U;
    snapshot.dispatch_failures = seed % 2U;
    snapshot.dispatched_candidates = seed + 4U;
    snapshot.pruned_candidates = seed + 5U;
    snapshot.skipped_candidates = seed + 1U;
    snapshot.total_checkpoint_duration_ns = (seed + 6U) * 7U;
    snapshot.last_checkpoint_duration_ns = (seed + 6U) * 5U;
    snapshot.total_dispatch_duration_ns = (seed + 7U) * 9U;
    snapshot.last_dispatch_duration_ns = (seed + 7U) * 6U;
    snapshot.pending_candidates = seed + 8U;
    return snapshot;
}

IndexTelemetrySnapshot make_index(std::uint64_t seed)
{
    IndexTelemetrySnapshot snapshot{};
    snapshot.build.attempts = seed + 1U;
    snapshot.build.failures = seed % 2U;
    snapshot.build.total_duration_ns = (seed + 2U) * 11U;
    snapshot.build.last_duration_ns = (seed + 3U) * 7U;
    snapshot.probe.attempts = seed + 4U;
    snapshot.probe.failures = seed % 3U;
    snapshot.probe.total_duration_ns = (seed + 5U) * 13U;
    snapshot.probe.last_duration_ns = (seed + 6U) * 9U;
    snapshot.mutation_attempts = seed + 7U;
    snapshot.split_events = seed + 2U;
    return snapshot;
}

TempCleanupTelemetrySnapshot make_temp_cleanup(std::uint64_t seed)
{
    TempCleanupTelemetrySnapshot snapshot{};
    snapshot.invocations = seed + 1U;
    snapshot.failures = seed;
    snapshot.removed_entries = (seed + 2U) * 3U;
    snapshot.total_duration_ns = (seed + 3U) * 7U;
    snapshot.last_duration_ns = (seed + 4U) * 5U;
    return snapshot;
}

DurabilityTelemetrySnapshot make_durability(std::uint64_t seed)
{
    DurabilityTelemetrySnapshot snapshot{};
    snapshot.last_commit_lsn = (seed + 1U) * 100U;
    snapshot.oldest_active_commit_lsn = (seed + 2U) * 80U;
    snapshot.last_commit_segment_id = seed + 3U;
    return snapshot;
}

VacuumTelemetrySnapshot make_vacuum(std::uint64_t seed)
{
    VacuumTelemetrySnapshot snapshot{};
    snapshot.scheduled_pages = seed + 1U;
    snapshot.dropped_pages = seed;
    snapshot.runs = seed + 2U;
    snapshot.batches_dispatched = seed + 3U;
    snapshot.pages_dispatched = seed + 4U;
    snapshot.pending_pages = seed + 5U;
    snapshot.total_dispatch_duration_ns = (seed + 6U) * 11U;
    snapshot.last_dispatch_duration_ns = (seed + 7U) * 13U;
    snapshot.last_safe_horizon = (seed + 8U) * 100U;
    return snapshot;
}

CatalogTelemetrySnapshot make_catalog(std::uint64_t seed)
{
    CatalogTelemetrySnapshot snapshot{};
    snapshot.cache_hits = (seed + 1U) * 10U;
    snapshot.cache_misses = (seed + 2U) * 4U;
    snapshot.cache_relations = static_cast<std::size_t>(seed + 3U);
    snapshot.cache_total_bytes = static_cast<std::size_t>((seed + 4U) * 512U);
    snapshot.published_batches = seed + 5U;
    snapshot.published_mutations = seed + 6U;
    snapshot.published_wal_records = seed + 7U;
    snapshot.publish_failures = seed + 8U;
    snapshot.aborted_batches = seed + 9U;
    snapshot.aborted_mutations = seed + 10U;
    return snapshot;
}

ddl::DdlTelemetrySnapshot make_ddl(std::uint64_t seed)
{
    ddl::DdlTelemetrySnapshot snapshot{};
    const auto create_table = static_cast<std::size_t>(ddl::DdlVerb::CreateTable);
    const auto drop_table = static_cast<std::size_t>(ddl::DdlVerb::DropTable);

    snapshot.verbs[create_table].attempts = seed + 1U;
    snapshot.verbs[create_table].successes = seed + 2U;
    snapshot.verbs[create_table].failures = seed;
    snapshot.verbs[create_table].total_duration_ns = (seed + 3U) * 7U;
    snapshot.verbs[create_table].last_duration_ns = (seed + 4U) * 3U;

    snapshot.verbs[drop_table].attempts = seed + 5U;
    snapshot.verbs[drop_table].successes = seed + 6U;
    snapshot.verbs[drop_table].failures = seed + 1U;
    snapshot.verbs[drop_table].total_duration_ns = (seed + 6U) * 11U;
    snapshot.verbs[drop_table].last_duration_ns = (seed + 7U) * 5U;

    snapshot.failures.handler_missing = seed;
    snapshot.failures.validation_failures = seed + 1U;
    snapshot.failures.execution_failures = seed + 2U;
    snapshot.failures.other_failures = seed + 3U;
    return snapshot;
}

parser::ParserTelemetrySnapshot make_parser(std::uint64_t seed)
{
    parser::ParserTelemetrySnapshot snapshot{};
    snapshot.scripts_attempted = seed + 1U;
    snapshot.scripts_succeeded = seed;
    snapshot.statements_attempted = seed + 2U;
    snapshot.statements_succeeded = seed + 1U;
    snapshot.diagnostics_info = seed;
    snapshot.diagnostics_warning = seed + 3U;
    snapshot.diagnostics_error = seed;
    snapshot.total_parse_duration_ns = (seed + 4U) * 10U;
    snapshot.last_parse_duration_ns = (seed + 5U) * 5U;
    return snapshot;
}

txn::TransactionTelemetrySnapshot make_transaction(std::uint64_t seed)
{
    txn::TransactionTelemetrySnapshot snapshot{};
    snapshot.active_transactions = seed + 1U;
    snapshot.committed_transactions = seed + 2U;
    snapshot.aborted_transactions = seed;
    snapshot.last_snapshot_xmin = seed + 3U;
    snapshot.last_snapshot_xmax = seed + 7U;
    snapshot.last_snapshot_age = (seed + 7U) - (seed + 3U);
    return snapshot;
}

planner::PlannerTelemetrySnapshot make_planner(std::uint64_t seed)
{
    planner::PlannerTelemetrySnapshot snapshot{};
    snapshot.plans_attempted = seed + 1U;
    snapshot.plans_succeeded = seed;
    snapshot.plans_failed = seed / 2U;
    snapshot.rules_attempted = seed + 2U;
    snapshot.rules_applied = seed + 1U;
    snapshot.cost_evaluations = (seed + 3U) * 2U;
    snapshot.alternatives_considered = seed + 4U;
    snapshot.total_chosen_cost = static_cast<double>((seed + 5U) * 10U);
    snapshot.last_chosen_cost = static_cast<double>((seed + 6U) * 5U);
    snapshot.min_chosen_cost = static_cast<double>(seed + 1U);
    snapshot.max_chosen_cost = static_cast<double>((seed + 7U) * 3U);
    return snapshot;
}

executor::ExecutorTelemetrySnapshot make_executor(std::uint64_t seed)
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
    snapshot.nested_loop_latency = make_latency(seed + 30U);
    snapshot.hash_join_latency = make_latency(seed + 40U);
    snapshot.aggregation_latency = make_latency(seed + 50U);
    snapshot.insert_latency = make_latency(seed + 60U);
    snapshot.update_latency = make_latency(seed + 70U);
    snapshot.delete_latency = make_latency(seed + 80U);
    return snapshot;
}

}  // namespace

TEST_CASE("collect_storage_diagnostics aggregates totals and details")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm_a", [] { return make_page_manager(1U); });
    registry.register_page_manager("pm_b", [] { return make_page_manager(5U); });
    registry.register_checkpoint_scheduler("ckpt_a", [] { return make_checkpoint(2U); });
    registry.register_checkpoint_scheduler("ckpt_b", [] { return make_checkpoint(4U); });
    registry.register_wal_retention("ret_a", [] { return make_retention(3U); });
    registry.register_recovery("rec_a", [] { return make_recovery(2U); });
    registry.register_recovery("rec_b", [] { return make_recovery(6U); });
    registry.register_index_retention("idx_ret_a", [] { return make_index_retention(2U); });
    registry.register_index("idx_a", [] { return make_index(2U); });
    registry.register_index("idx_b", [] { return make_index(6U); });
    registry.register_temp_cleanup("cleanup_a", [] { return make_temp_cleanup(2U); });
    registry.register_temp_cleanup("cleanup_b", [] { return make_temp_cleanup(4U); });
    registry.register_durability_horizon("dur_a", [] { return make_durability(2U); });
    registry.register_vacuum("vac_a", [] { return make_vacuum(2U); });
    registry.register_catalog("cat_a", [] { return make_catalog(1U); });
    registry.register_catalog("cat_b", [] { return make_catalog(4U); });
    registry.register_ddl("ddl_a", [] { return make_ddl(2U); });
    registry.register_ddl("ddl_b", [] { return make_ddl(6U); });
    registry.register_parser("parser_a", [] { return make_parser(2U); });
    registry.register_parser("parser_b", [] { return make_parser(5U); });
    registry.register_transaction("txn_a", [] { return make_transaction(1U); });
    registry.register_transaction("txn_b", [] { return make_transaction(4U); });
    registry.register_planner("planner_a", [] { return make_planner(2U); });
    registry.register_planner("planner_b", [] { return make_planner(5U); });
    registry.register_executor("exec_a", [] { return make_executor(2U); });
    registry.register_executor("exec_b", [] { return make_executor(5U); });

    const StorageDiagnosticsOptions options{};
    const auto doc = collect_storage_diagnostics(registry, options);

    REQUIRE(doc.page_managers.details.size() == 2U);
    REQUIRE(doc.page_managers.total.initialize.attempts == ((1U + 1U) + (5U + 1U)));
    REQUIRE(doc.checkpoints.details.size() == 2U);
    REQUIRE(doc.checkpoints.total.emitted_checkpoints == ((2U + 2U) + (4U + 2U)));
    REQUIRE(doc.retention.details.size() == 1U);
    REQUIRE(doc.retention.total.pruned_segments == (3U + 2U));
    REQUIRE(doc.recovery.details.size() == 2U);
    REQUIRE(doc.recovery.total.plan_invocations == ((2U + 1U) + (6U + 1U)));
    REQUIRE(doc.recovery.total.plan_failures == (2U % 2U + 6U % 2U));
    REQUIRE(doc.recovery.total.total_plan_duration_ns == ((2U + 5U) * 7U + (6U + 5U) * 7U));
    REQUIRE(doc.recovery.total.last_replay_backlog_bytes == std::max((2U + 20U) * 100U, (6U + 20U) * 100U));
    REQUIRE(doc.recovery.total.max_replay_backlog_bytes == std::max((2U + 21U) * 120U, (6U + 21U) * 120U));
    REQUIRE(doc.index_retention.details.size() == 1U);
    REQUIRE(doc.index_retention.total.scheduled_candidates == (2U + 1U));
    REQUIRE(doc.index_retention.total.dispatched_candidates == (2U + 4U));
    REQUIRE(doc.indexes.details.size() == 2U);
    REQUIRE(doc.indexes.total.build.attempts == ((2U + 1U) + (6U + 1U)));
    REQUIRE(doc.indexes.total.build.failures == ((2U % 2U) + (6U % 2U)));
    REQUIRE(doc.indexes.total.build.total_duration_ns == (((2U + 2U) * 11U) + ((6U + 2U) * 11U)));
    REQUIRE(doc.indexes.total.build.last_duration_ns == std::max((2U + 3U) * 7U, (6U + 3U) * 7U));
    REQUIRE(doc.indexes.total.probe.attempts == ((2U + 4U) + (6U + 4U)));
    REQUIRE(doc.indexes.total.probe.failures == ((2U % 3U) + (6U % 3U)));
    REQUIRE(doc.indexes.total.probe.total_duration_ns == (((2U + 5U) * 13U) + ((6U + 5U) * 13U)));
    REQUIRE(doc.indexes.total.probe.last_duration_ns == std::max((2U + 6U) * 9U, (6U + 6U) * 9U));
    REQUIRE(doc.indexes.total.mutation_attempts == ((2U + 7U) + (6U + 7U)));
    REQUIRE(doc.indexes.total.split_events == ((2U + 2U) + (6U + 2U)));
    REQUIRE(doc.temp_cleanup.details.size() == 2U);
    REQUIRE(doc.temp_cleanup.total.invocations == ((2U + 1U) + (4U + 1U)));
    REQUIRE(doc.temp_cleanup.total.failures == (2U + 4U));
    REQUIRE(doc.temp_cleanup.total.removed_entries == (((2U + 2U) * 3U) + ((4U + 2U) * 3U)));
    REQUIRE(doc.temp_cleanup.total.total_duration_ns == ((2U + 3U) * 7U + (4U + 3U) * 7U));
    REQUIRE(doc.temp_cleanup.total.last_duration_ns == std::max((2U + 4U) * 5U, (4U + 4U) * 5U));
    REQUIRE(doc.durability.details.size() == 1U);
    REQUIRE(doc.durability.total.last_commit_lsn == (2U + 1U) * 100U);
    REQUIRE(doc.durability.total.oldest_active_commit_lsn == (2U + 2U) * 80U);
    REQUIRE(doc.vacuum.details.size() == 1U);
    REQUIRE(doc.vacuum.total.scheduled_pages == (2U + 1U));
    REQUIRE(doc.catalog.details.size() == 2U);
    REQUIRE(doc.catalog.total.cache_hits == ((1U + 1U) * 10U + (4U + 1U) * 10U));
    REQUIRE(doc.ddl.details.size() == 2U);
    const auto create_table = static_cast<std::size_t>(ddl::DdlVerb::CreateTable);
    REQUIRE(doc.ddl.total.verbs[create_table].attempts == ((2U + 1U) + (6U + 1U)));
    REQUIRE(doc.ddl.total.failures.execution_failures == ((2U + 2U) + (6U + 2U)));
    REQUIRE(doc.parser.details.size() == 2U);
    REQUIRE(doc.parser.total.scripts_attempted == ((2U + 1U) + (5U + 1U)));
    REQUIRE(doc.parser.total.diagnostics_warning == ((2U + 3U) + (5U + 3U)));
    REQUIRE(doc.parser.total.last_parse_duration_ns == std::max((2U + 5U) * 5U, (5U + 5U) * 5U));
    REQUIRE(doc.transactions.details.size() == 2U);
    REQUIRE(doc.transactions.total.active_transactions == ((1U + 1U) + (4U + 1U)));
    REQUIRE(doc.transactions.total.committed_transactions == ((1U + 2U) + (4U + 2U)));
    REQUIRE(doc.transactions.total.last_snapshot_xmin == std::min(1U + 3U, 4U + 3U));
    REQUIRE(doc.transactions.total.last_snapshot_xmax == std::max(1U + 7U, 4U + 7U));
    REQUIRE(doc.planner.details.size() == 2U);
    REQUIRE(doc.planner.total.plans_attempted == ((2U + 1U) + (5U + 1U)));
    REQUIRE(doc.planner.total.plans_succeeded == (2U + 5U));
    REQUIRE(doc.planner.total.rules_attempted == ((2U + 2U) + (5U + 2U)));
    REQUIRE(doc.planner.total.min_chosen_cost == Approx(static_cast<double>(2U + 1U)));
    REQUIRE(doc.planner.total.max_chosen_cost == Approx(static_cast<double>((5U + 7U) * 3U)));
    REQUIRE(doc.executors.details.size() == 2U);
    REQUIRE(doc.executors.total.seq_scan_rows_read == ((2U + 1U) + (5U + 1U)));
    REQUIRE(doc.executors.total.seq_scan_rows_visible == ((2U + 2U) + (5U + 2U)));
    REQUIRE(doc.executors.total.filter_rows_evaluated == ((2U + 3U) + (5U + 3U)));
    REQUIRE(doc.executors.total.filter_rows_passed == ((2U + 1U) + (5U + 1U)));
    REQUIRE(doc.executors.total.projection_rows_emitted == ((2U + 4U) + (5U + 4U)));
    REQUIRE(doc.executors.total.nested_loop_rows_compared == ((2U + 5U) + (5U + 5U)));
    REQUIRE(doc.executors.total.nested_loop_rows_matched == ((2U + 2U) + (5U + 2U)));
    REQUIRE(doc.executors.total.nested_loop_rows_emitted == ((2U + 2U) + (5U + 2U)));
    REQUIRE(doc.executors.total.hash_join_build_rows == ((2U + 6U) + (5U + 6U)));
    REQUIRE(doc.executors.total.hash_join_probe_rows == ((2U + 7U) + (5U + 7U)));
    REQUIRE(doc.executors.total.hash_join_rows_matched == ((2U + 3U) + (5U + 3U)));
    REQUIRE(doc.executors.total.aggregation_input_rows == ((2U + 8U) + (5U + 8U)));
    REQUIRE(doc.executors.total.aggregation_groups_emitted == ((2U + 4U) + (5U + 4U)));
    REQUIRE(doc.executors.total.seq_scan_latency.invocations == ((2U + 1U) + (5U + 1U)));
    REQUIRE(doc.executors.total.seq_scan_latency.total_duration_ns == ((2U + 2U) * 10U + (5U + 2U) * 10U));
    REQUIRE(doc.executors.total.seq_scan_latency.last_duration_ns == std::max((2U + 3U) * 5U, (5U + 3U) * 5U));
    REQUIRE(doc.collected_at.time_since_epoch().count() != 0);

    REQUIRE(doc.page_managers.details.front().identifier == "pm_a");
    REQUIRE(doc.page_managers.details.back().identifier == "pm_b");
    REQUIRE(doc.indexes.details.front().identifier == "idx_a");
    REQUIRE(doc.indexes.details.back().identifier == "idx_b");
    REQUIRE(doc.indexes.details.front().snapshot.build.attempts == (2U + 1U));
    REQUIRE(doc.ddl.details.front().identifier == "ddl_a");
    REQUIRE(doc.ddl.details.back().identifier == "ddl_b");
    REQUIRE(doc.parser.details.front().identifier == "parser_a");
    REQUIRE(doc.parser.details.back().identifier == "parser_b");
    REQUIRE(doc.transactions.details.front().identifier == "txn_a");
    REQUIRE(doc.transactions.details.back().identifier == "txn_b");
    REQUIRE(doc.durability.details.front().identifier == "dur_a");
    REQUIRE(doc.temp_cleanup.details.front().identifier == "cleanup_a");
    REQUIRE(doc.temp_cleanup.details.back().identifier == "cleanup_b");
    REQUIRE(doc.planner.details.front().identifier == "planner_a");
    REQUIRE(doc.planner.details.back().identifier == "planner_b");
    REQUIRE(doc.executors.details.front().identifier == "exec_a");
    REQUIRE(doc.executors.details.back().identifier == "exec_b");
    REQUIRE(doc.executors.details.front().snapshot.seq_scan_latency.invocations == (2U + 1U));
    REQUIRE(doc.last_checkpoint_lsn == doc.checkpoints.total.last_checkpoint_lsn);
    REQUIRE(doc.outstanding_replay_backlog_bytes == doc.recovery.total.last_replay_backlog_bytes);
}

TEST_CASE("collect_storage_diagnostics honors detail options")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_manager(1U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint(1U); });
    registry.register_wal_retention("ret", [] { return make_retention(1U); });
    registry.register_recovery("rec", [] { return make_recovery(1U); });
    registry.register_index_retention("idx_ret", [] { return make_index_retention(1U); });
    registry.register_index("idx", [] { return make_index(1U); });
    registry.register_temp_cleanup("cleanup", [] { return make_temp_cleanup(2U); });
    registry.register_durability_horizon("dur", [] { return make_durability(3U); });
    registry.register_vacuum("vac", [] { return make_vacuum(2U); });
    registry.register_catalog("cat", [] { return make_catalog(2U); });
    registry.register_ddl("ddl", [] { return make_ddl(3U); });
    registry.register_parser("parser", [] { return make_parser(4U); });
    registry.register_transaction("txn", [] { return make_transaction(5U); });
    registry.register_planner("planner", [] { return make_planner(3U); });
    registry.register_executor("exec", [] { return make_executor(4U); });

    StorageDiagnosticsOptions options{};
    options.include_page_manager_details = false;
    options.include_checkpoint_details = false;
    options.include_retention_details = false;
    options.include_recovery_details = false;
    options.include_index_retention_details = false;
    options.include_index_details = false;
    options.include_temp_cleanup_details = false;
    options.include_durability_details = false;
    options.include_vacuum_details = false;
    options.include_catalog_details = false;
    options.include_ddl_details = false;
    options.include_parser_details = false;
    options.include_transaction_details = false;
    options.include_planner_details = false;
    options.include_executor_details = false;

    const auto doc = collect_storage_diagnostics(registry, options);

    REQUIRE(doc.page_managers.details.empty());
    REQUIRE(doc.checkpoints.details.empty());
    REQUIRE(doc.retention.details.empty());
    REQUIRE(doc.recovery.details.empty());
    REQUIRE(doc.index_retention.details.empty());
    REQUIRE(doc.indexes.details.empty());
    REQUIRE(doc.temp_cleanup.details.empty());
    REQUIRE(doc.durability.details.empty());
    REQUIRE(doc.vacuum.details.empty());
    REQUIRE(doc.catalog.details.empty());
    REQUIRE(doc.ddl.details.empty());
    REQUIRE(doc.parser.details.empty());
    REQUIRE(doc.transactions.details.empty());
    REQUIRE(doc.planner.details.empty());
    REQUIRE(doc.executors.details.empty());
}

TEST_CASE("storage_diagnostics_to_json serialises expected fields")
{
    StorageTelemetryRegistry registry;
    registry.register_page_manager("pm", [] { return make_page_manager(3U); });
    registry.register_checkpoint_scheduler("ckpt", [] { return make_checkpoint(5U); });
    registry.register_wal_retention("ret", [] { return make_retention(7U); });
    registry.register_recovery("rec", [] { return make_recovery(4U); });
    registry.register_index_retention("idx_ret", [] { return make_index_retention(5U); });
    registry.register_index("idx", [] { return make_index(4U); });
    registry.register_temp_cleanup("cleanup", [] { return make_temp_cleanup(5U); });
    registry.register_durability_horizon("dur", [] { return make_durability(5U); });
    registry.register_vacuum("vac", [] { return make_vacuum(3U); });
    registry.register_catalog("cat", [] { return make_catalog(4U); });
    registry.register_ddl("ddl", [] { return make_ddl(5U); });
    registry.register_parser("parser", [] { return make_parser(6U); });
    registry.register_transaction("txn", [] { return make_transaction(7U); });
    registry.register_planner("planner", [] { return make_planner(4U); });
    registry.register_executor("exec", [] { return make_executor(6U); });

    const auto doc = collect_storage_diagnostics(registry);
    const auto json = storage_diagnostics_to_json(doc);

    REQUIRE(json.find("\"page_managers\"") != std::string::npos);
    REQUIRE(json.find("\"details\"") != std::string::npos);
    REQUIRE(json.find("\"pm\"") != std::string::npos);
    REQUIRE(json.find("\"checkpoints\"") != std::string::npos);
    REQUIRE(json.find("\"retention\"") != std::string::npos);
    REQUIRE(json.find("\"recovery\"") != std::string::npos);
    REQUIRE(json.find("\"index_retention\"") != std::string::npos);
    REQUIRE(json.find("\"indexes\"") != std::string::npos);
    REQUIRE(json.find("\"temp_cleanup\"") != std::string::npos);
    REQUIRE(json.find("\"durability\"") != std::string::npos);
    REQUIRE(json.find("\"catalog\"") != std::string::npos);
    REQUIRE(json.find("\"vacuum\"") != std::string::npos);
    REQUIRE(json.find("\"transactions\"") != std::string::npos);
    REQUIRE(json.find("\"parser\"") != std::string::npos);
    REQUIRE(json.find("\"ddl\"") != std::string::npos);
    REQUIRE(json.find("\"planner\"") != std::string::npos);
    REQUIRE(json.find("\"executors\"") != std::string::npos);
    REQUIRE(json.find("\"pruned_segments\":9") != std::string::npos);
    REQUIRE(json.find("\"create_table\"") != std::string::npos);
    REQUIRE(json.find("\"nested_loop_rows_compared\"") != std::string::npos);
    REQUIRE(json.find("\"hash_join_build_rows\"") != std::string::npos);
    REQUIRE(json.find("\"aggregation_groups_emitted\"") != std::string::npos);
    REQUIRE(json.find("\"mutation_attempts\"") != std::string::npos);
    REQUIRE(json.find("\"outstanding_replay_backlog_bytes\"") != std::string::npos);
    REQUIRE(json.find("\"last_replay_backlog_bytes\"") != std::string::npos);
}

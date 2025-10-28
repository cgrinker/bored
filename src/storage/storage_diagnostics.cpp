#include "bored/storage/storage_diagnostics.hpp"

#include <algorithm>
#include <array>
#include <cmath>

namespace bored::storage {
namespace {

namespace ddl = bored::ddl;

constexpr std::array<const char*, static_cast<std::size_t>(ddl::DdlVerb::Count)> kDdlVerbNames = {
    "create_database", "drop_database", "create_schema", "drop_schema", "create_table",
    "drop_table",     "alter_table",   "create_index",  "drop_index"};

void append_field(std::string& out, const char* name, std::uint64_t value, bool& first)
{
    if (!first) {
        out.push_back(',');
    }
    first = false;
    out.push_back('"');
    out.append(name);
    out.append("\":");
    out.append(std::to_string(value));
}

void append_double_field(std::string& out, const char* name, double value, bool& first)
{
    if (!first) {
        out.push_back(',');
    }
    first = false;
    out.push_back('"');
    out.append(name);
    out.append("\":");
    if (std::isfinite(value)) {
        out.append(std::to_string(value));
    } else {
        out.append("0.0");
    }
}

void append_operation_snapshot(std::string& out, const OperationTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "attempts", snapshot.attempts, first);
    append_field(out, "failures", snapshot.failures, first);
    append_field(out, "total_duration_ns", snapshot.total_duration_ns, first);
    append_field(out, "last_duration_ns", snapshot.last_duration_ns, first);
    out.push_back('}');
}

void append_latch_snapshot(std::string& out, const LatchTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "attempts", snapshot.attempts, first);
    append_field(out, "failures", snapshot.failures, first);
    append_field(out, "total_wait_ns", snapshot.total_wait_ns, first);
    append_field(out, "last_wait_ns", snapshot.last_wait_ns, first);
    out.push_back('}');
}

void append_operator_latency_snapshot(std::string& out,
                                      const bored::executor::ExecutorTelemetrySnapshot::OperatorLatencySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "invocations", snapshot.invocations, first);
    append_field(out, "total_duration_ns", snapshot.total_duration_ns, first);
    append_field(out, "last_duration_ns", snapshot.last_duration_ns, first);
    out.push_back('}');
}

void append_json_string(std::string& out, const std::string& value)
{
    out.push_back('"');
    for (unsigned char ch : value) {
        switch (ch) {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if (ch < 0x20U) {
                    constexpr char kHex[] = "0123456789ABCDEF";
                    out.append("\\u00");
                    out.push_back(kHex[(ch >> 4U) & 0x0F]);
                    out.push_back(kHex[ch & 0x0F]);
                } else {
                    out.push_back(static_cast<char>(ch));
                }
                break;
        }
    }
    out.push_back('"');
}

void append_page_manager_snapshot(std::string& out, const PageManagerTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;

    auto append_object = [&](const char* name, auto&& fn) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('"');
        out.append(name);
        out.append("\":");
        fn();
    };

    append_object("initialize", [&] { append_operation_snapshot(out, snapshot.initialize); });
    append_object("insert", [&] { append_operation_snapshot(out, snapshot.insert); });
    append_object("remove", [&] { append_operation_snapshot(out, snapshot.remove); });
    append_object("update", [&] { append_operation_snapshot(out, snapshot.update); });
    append_object("compact", [&] { append_operation_snapshot(out, snapshot.compact); });
    append_object("shared_latch", [&] { append_latch_snapshot(out, snapshot.shared_latch); });
    append_object("exclusive_latch", [&] { append_latch_snapshot(out, snapshot.exclusive_latch); });

    out.push_back('}');
}

void append_checkpoint_snapshot(std::string& out, const CheckpointTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "invocations", snapshot.invocations, first);
    append_field(out, "forced_requests", snapshot.forced_requests, first);
    append_field(out, "skipped_runs", snapshot.skipped_runs, first);
    append_field(out, "emitted_checkpoints", snapshot.emitted_checkpoints, first);
    append_field(out, "emit_failures", snapshot.emit_failures, first);
    append_field(out, "flush_failures", snapshot.flush_failures, first);
    append_field(out, "retention_invocations", snapshot.retention_invocations, first);
    append_field(out, "retention_failures", snapshot.retention_failures, first);
    append_field(out, "trigger_force", snapshot.trigger_force, first);
    append_field(out, "trigger_first", snapshot.trigger_first, first);
    append_field(out, "trigger_dirty", snapshot.trigger_dirty, first);
    append_field(out, "trigger_active", snapshot.trigger_active, first);
    append_field(out, "trigger_interval", snapshot.trigger_interval, first);
    append_field(out, "trigger_lsn_gap", snapshot.trigger_lsn_gap, first);
    append_field(out, "total_emit_duration_ns", snapshot.total_emit_duration_ns, first);
    append_field(out, "last_emit_duration_ns", snapshot.last_emit_duration_ns, first);
    append_field(out, "total_flush_duration_ns", snapshot.total_flush_duration_ns, first);
    append_field(out, "last_flush_duration_ns", snapshot.last_flush_duration_ns, first);
    append_field(out, "total_retention_duration_ns", snapshot.total_retention_duration_ns, first);
    append_field(out, "last_retention_duration_ns", snapshot.last_retention_duration_ns, first);
    append_field(out, "last_checkpoint_id", snapshot.last_checkpoint_id, first);
    append_field(out, "last_checkpoint_lsn", snapshot.last_checkpoint_lsn, first);
    append_field(out, "last_checkpoint_timestamp_ns", snapshot.last_checkpoint_timestamp_ns, first);
    out.push_back('}');
}

void append_retention_snapshot(std::string& out, const WalRetentionTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "invocations", snapshot.invocations, first);
    append_field(out, "failures", snapshot.failures, first);
    append_field(out, "scanned_segments", snapshot.scanned_segments, first);
    append_field(out, "candidate_segments", snapshot.candidate_segments, first);
    append_field(out, "pruned_segments", snapshot.pruned_segments, first);
    append_field(out, "archived_segments", snapshot.archived_segments, first);
    append_field(out, "total_duration_ns", snapshot.total_duration_ns, first);
    append_field(out, "last_duration_ns", snapshot.last_duration_ns, first);
    out.push_back('}');
}

void append_durability_snapshot(std::string& out, const DurabilityTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "last_commit_lsn", snapshot.last_commit_lsn, first);
    append_field(out, "oldest_active_commit_lsn", snapshot.oldest_active_commit_lsn, first);
    append_field(out, "last_commit_segment_id", snapshot.last_commit_segment_id, first);
    out.push_back('}');
}

void append_vacuum_snapshot(std::string& out, const VacuumTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "scheduled_pages", snapshot.scheduled_pages, first);
    append_field(out, "dropped_pages", snapshot.dropped_pages, first);
    append_field(out, "runs", snapshot.runs, first);
    append_field(out, "forced_runs", snapshot.forced_runs, first);
    append_field(out, "skipped_runs", snapshot.skipped_runs, first);
    append_field(out, "batches_dispatched", snapshot.batches_dispatched, first);
    append_field(out, "dispatch_failures", snapshot.dispatch_failures, first);
    append_field(out, "pages_dispatched", snapshot.pages_dispatched, first);
    append_field(out, "pending_pages", snapshot.pending_pages, first);
    append_field(out, "total_dispatch_duration_ns", snapshot.total_dispatch_duration_ns, first);
    append_field(out, "last_dispatch_duration_ns", snapshot.last_dispatch_duration_ns, first);
    append_field(out, "last_safe_horizon", snapshot.last_safe_horizon, first);
    out.push_back('}');
}

void append_catalog_snapshot(std::string& out, const CatalogTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "cache_hits", snapshot.cache_hits, first);
    append_field(out, "cache_misses", snapshot.cache_misses, first);
    append_field(out, "cache_relations", static_cast<std::uint64_t>(snapshot.cache_relations), first);
    append_field(out, "cache_total_bytes", static_cast<std::uint64_t>(snapshot.cache_total_bytes), first);
    append_field(out, "published_batches", snapshot.published_batches, first);
    append_field(out, "published_mutations", snapshot.published_mutations, first);
    append_field(out, "published_wal_records", snapshot.published_wal_records, first);
    append_field(out, "publish_failures", snapshot.publish_failures, first);
    append_field(out, "aborted_batches", snapshot.aborted_batches, first);
    append_field(out, "aborted_mutations", snapshot.aborted_mutations, first);
    out.push_back('}');
}

void append_ddl_verb_snapshot(std::string& out, const ddl::DdlVerbTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "attempts", snapshot.attempts, first);
    append_field(out, "successes", snapshot.successes, first);
    append_field(out, "failures", snapshot.failures, first);
    append_field(out, "total_duration_ns", snapshot.total_duration_ns, first);
    append_field(out, "last_duration_ns", snapshot.last_duration_ns, first);
    out.push_back('}');
}

void append_ddl_failures_snapshot(std::string& out, const ddl::DdlFailureTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "handler_missing", snapshot.handler_missing, first);
    append_field(out, "validation_failures", snapshot.validation_failures, first);
    append_field(out, "execution_failures", snapshot.execution_failures, first);
    append_field(out, "other_failures", snapshot.other_failures, first);
    out.push_back('}');
}

void append_ddl_snapshot(std::string& out, const ddl::DdlTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    out.append("\"verbs\":{");
    bool first = true;
    for (std::size_t i = 0; i < snapshot.verbs.size(); ++i) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('"');
        out.append(kDdlVerbNames[i]);
        out.append("\":");
        append_ddl_verb_snapshot(out, snapshot.verbs[i]);
    }
    out.push_back('}');
    out.append(",\"failures\":");
    append_ddl_failures_snapshot(out, snapshot.failures);
    out.push_back('}');
}

void append_parser_snapshot(std::string& out, const bored::parser::ParserTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "scripts_attempted", snapshot.scripts_attempted, first);
    append_field(out, "scripts_succeeded", snapshot.scripts_succeeded, first);
    append_field(out, "statements_attempted", snapshot.statements_attempted, first);
    append_field(out, "statements_succeeded", snapshot.statements_succeeded, first);
    append_field(out, "diagnostics_info", snapshot.diagnostics_info, first);
    append_field(out, "diagnostics_warning", snapshot.diagnostics_warning, first);
    append_field(out, "diagnostics_error", snapshot.diagnostics_error, first);
    append_field(out, "total_parse_duration_ns", snapshot.total_parse_duration_ns, first);
    append_field(out, "last_parse_duration_ns", snapshot.last_parse_duration_ns, first);
    out.push_back('}');
}

void append_transaction_snapshot(std::string& out, const bored::txn::TransactionTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "active_transactions", snapshot.active_transactions, first);
    append_field(out, "committed_transactions", snapshot.committed_transactions, first);
    append_field(out, "aborted_transactions", snapshot.aborted_transactions, first);
    append_field(out, "last_snapshot_xmin", snapshot.last_snapshot_xmin, first);
    append_field(out, "last_snapshot_xmax", snapshot.last_snapshot_xmax, first);
    append_field(out, "last_snapshot_age", snapshot.last_snapshot_age, first);
    out.push_back('}');
}

void append_planner_snapshot(std::string& out, const bored::planner::PlannerTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "plans_attempted", snapshot.plans_attempted, first);
    append_field(out, "plans_succeeded", snapshot.plans_succeeded, first);
    append_field(out, "plans_failed", snapshot.plans_failed, first);
    append_field(out, "rules_attempted", snapshot.rules_attempted, first);
    append_field(out, "rules_applied", snapshot.rules_applied, first);
    append_field(out, "cost_evaluations", snapshot.cost_evaluations, first);
    append_field(out, "alternatives_considered", snapshot.alternatives_considered, first);
    append_double_field(out, "total_chosen_cost", snapshot.total_chosen_cost, first);
    append_double_field(out, "last_chosen_cost", snapshot.last_chosen_cost, first);
    append_double_field(out, "min_chosen_cost", snapshot.min_chosen_cost, first);
    append_double_field(out, "max_chosen_cost", snapshot.max_chosen_cost, first);
    out.push_back('}');
}

void append_executor_snapshot(std::string& out, const bored::executor::ExecutorTelemetrySnapshot& snapshot)
{
    out.push_back('{');
    bool first = true;
    append_field(out, "seq_scan_rows_read", snapshot.seq_scan_rows_read, first);
    append_field(out, "seq_scan_rows_visible", snapshot.seq_scan_rows_visible, first);
    append_field(out, "filter_rows_evaluated", snapshot.filter_rows_evaluated, first);
    append_field(out, "filter_rows_passed", snapshot.filter_rows_passed, first);
    append_field(out, "projection_rows_emitted", snapshot.projection_rows_emitted, first);
    append_field(out, "nested_loop_rows_compared", snapshot.nested_loop_rows_compared, first);
    append_field(out, "nested_loop_rows_matched", snapshot.nested_loop_rows_matched, first);
    append_field(out, "nested_loop_rows_emitted", snapshot.nested_loop_rows_emitted, first);
    append_field(out, "hash_join_build_rows", snapshot.hash_join_build_rows, first);
    append_field(out, "hash_join_probe_rows", snapshot.hash_join_probe_rows, first);
    append_field(out, "hash_join_rows_matched", snapshot.hash_join_rows_matched, first);
    append_field(out, "aggregation_input_rows", snapshot.aggregation_input_rows, first);
    append_field(out, "aggregation_groups_emitted", snapshot.aggregation_groups_emitted, first);
    append_field(out, "insert_rows_attempted", snapshot.insert_rows_attempted, first);
    append_field(out, "insert_rows_succeeded", snapshot.insert_rows_succeeded, first);
    append_field(out, "insert_payload_bytes", snapshot.insert_payload_bytes, first);
    append_field(out, "insert_wal_bytes", snapshot.insert_wal_bytes, first);
    append_field(out, "update_rows_attempted", snapshot.update_rows_attempted, first);
    append_field(out, "update_rows_succeeded", snapshot.update_rows_succeeded, first);
    append_field(out, "update_new_payload_bytes", snapshot.update_new_payload_bytes, first);
    append_field(out, "update_old_payload_bytes", snapshot.update_old_payload_bytes, first);
    append_field(out, "update_wal_bytes", snapshot.update_wal_bytes, first);
    append_field(out, "delete_rows_attempted", snapshot.delete_rows_attempted, first);
    append_field(out, "delete_rows_succeeded", snapshot.delete_rows_succeeded, first);
    append_field(out, "delete_reclaimed_bytes", snapshot.delete_reclaimed_bytes, first);
    append_field(out, "delete_wal_bytes", snapshot.delete_wal_bytes, first);

    auto append_latency = [&](const char* name,
                              const bored::executor::ExecutorTelemetrySnapshot::OperatorLatencySnapshot& latency) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('"');
        out.append(name);
        out.append("\":");
        append_operator_latency_snapshot(out, latency);
    };

    append_latency("seq_scan_latency", snapshot.seq_scan_latency);
    append_latency("filter_latency", snapshot.filter_latency);
    append_latency("projection_latency", snapshot.projection_latency);
    append_latency("nested_loop_latency", snapshot.nested_loop_latency);
    append_latency("hash_join_latency", snapshot.hash_join_latency);
    append_latency("aggregation_latency", snapshot.aggregation_latency);
    append_latency("insert_latency", snapshot.insert_latency);
    append_latency("update_latency", snapshot.update_latency);
    append_latency("delete_latency", snapshot.delete_latency);
    out.push_back('}');
}

void append_page_manager_section(std::string& out, const StorageDiagnosticsPageManagerSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_page_manager_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_page_manager_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_checkpoint_section(std::string& out, const StorageDiagnosticsCheckpointSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_checkpoint_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_checkpoint_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_retention_section(std::string& out, const StorageDiagnosticsRetentionSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_retention_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_retention_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_durability_section(std::string& out, const StorageDiagnosticsDurabilitySection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_durability_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_durability_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_vacuum_section(std::string& out, const StorageDiagnosticsVacuumSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_vacuum_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_vacuum_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_catalog_section(std::string& out, const StorageDiagnosticsCatalogSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_catalog_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_catalog_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_ddl_section(std::string& out, const StorageDiagnosticsDdlSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_ddl_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_ddl_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_parser_section(std::string& out, const StorageDiagnosticsParserSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_parser_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_parser_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_transaction_section(std::string& out, const StorageDiagnosticsTransactionSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_transaction_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_transaction_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_planner_section(std::string& out, const StorageDiagnosticsPlannerSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_planner_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_planner_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

void append_executor_section(std::string& out, const StorageDiagnosticsExecutorSection& section)
{
    out.push_back('{');
    out.append("\"total\":");
    append_executor_snapshot(out, section.total);
    out.append(",\"details\":[");
    bool first = true;
    for (const auto& entry : section.details) {
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.push_back('{');
        out.append("\"id\":");
        append_json_string(out, entry.identifier);
        out.append(",\"telemetry\":");
        append_executor_snapshot(out, entry.snapshot);
        out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
}

}  // namespace

StorageDiagnosticsDocument collect_storage_diagnostics(const StorageTelemetryRegistry& registry,
                                                       const StorageDiagnosticsOptions& options)
{
    StorageDiagnosticsDocument document{};
    document.collected_at = std::chrono::system_clock::now();

    document.page_managers.total = registry.aggregate_page_managers();
    if (options.include_page_manager_details) {
        registry.visit_page_managers([&](const std::string& identifier, const PageManagerTelemetrySnapshot& snapshot) {
            document.page_managers.details.push_back(StorageDiagnosticsPageManagerEntry{identifier, snapshot});
        });
        std::sort(document.page_managers.details.begin(), document.page_managers.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.checkpoints.total = registry.aggregate_checkpoint_schedulers();
    if (options.include_checkpoint_details) {
        registry.visit_checkpoint_schedulers([&](const std::string& identifier, const CheckpointTelemetrySnapshot& snapshot) {
            document.checkpoints.details.push_back(StorageDiagnosticsCheckpointEntry{identifier, snapshot});
        });
        std::sort(document.checkpoints.details.begin(), document.checkpoints.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.retention.total = registry.aggregate_wal_retention();
    if (options.include_retention_details) {
        registry.visit_wal_retention([&](const std::string& identifier, const WalRetentionTelemetrySnapshot& snapshot) {
            document.retention.details.push_back(StorageDiagnosticsRetentionEntry{identifier, snapshot});
        });
        std::sort(document.retention.details.begin(), document.retention.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.durability.total = registry.aggregate_durability_horizons();
    if (options.include_durability_details) {
        registry.visit_durability_horizons([&](const std::string& identifier, const DurabilityTelemetrySnapshot& snapshot) {
            document.durability.details.push_back(StorageDiagnosticsDurabilityEntry{identifier, snapshot});
        });
        std::sort(document.durability.details.begin(), document.durability.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.vacuum.total = registry.aggregate_vacuums();
    if (options.include_vacuum_details) {
        registry.visit_vacuums([&](const std::string& identifier, const VacuumTelemetrySnapshot& snapshot) {
            document.vacuum.details.push_back(StorageDiagnosticsVacuumEntry{identifier, snapshot});
        });
        std::sort(document.vacuum.details.begin(), document.vacuum.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.catalog.total = registry.aggregate_catalog();
    if (options.include_catalog_details) {
        registry.visit_catalog([&](const std::string& identifier, const CatalogTelemetrySnapshot& snapshot) {
            document.catalog.details.push_back(StorageDiagnosticsCatalogEntry{identifier, snapshot});
        });
        std::sort(document.catalog.details.begin(), document.catalog.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.parser.total = registry.aggregate_parser();
    if (options.include_parser_details) {
        registry.visit_parser([&](const std::string& identifier, const bored::parser::ParserTelemetrySnapshot& snapshot) {
            document.parser.details.push_back(StorageDiagnosticsParserEntry{identifier, snapshot});
        });
        std::sort(document.parser.details.begin(), document.parser.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.transactions.total = registry.aggregate_transactions();
    if (options.include_transaction_details) {
        registry.visit_transactions([&](const std::string& identifier, const bored::txn::TransactionTelemetrySnapshot& snapshot) {
            document.transactions.details.push_back(StorageDiagnosticsTransactionEntry{identifier, snapshot});
        });
        std::sort(document.transactions.details.begin(), document.transactions.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.planner.total = registry.aggregate_planner();
    if (options.include_planner_details) {
        registry.visit_planner([&](const std::string& identifier, const bored::planner::PlannerTelemetrySnapshot& snapshot) {
            document.planner.details.push_back(StorageDiagnosticsPlannerEntry{identifier, snapshot});
        });
        std::sort(document.planner.details.begin(), document.planner.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.executors.total = registry.aggregate_executors();
    if (options.include_executor_details) {
        registry.visit_executors([&](const std::string& identifier, const bored::executor::ExecutorTelemetrySnapshot& snapshot) {
            document.executors.details.push_back(StorageDiagnosticsExecutorEntry{identifier, snapshot});
        });
        std::sort(document.executors.details.begin(), document.executors.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    document.ddl.total = registry.aggregate_ddl();
    if (options.include_ddl_details) {
        registry.visit_ddl([&](const std::string& identifier, const ddl::DdlTelemetrySnapshot& snapshot) {
            document.ddl.details.push_back(StorageDiagnosticsDdlEntry{identifier, snapshot});
        });
        std::sort(document.ddl.details.begin(), document.ddl.details.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.identifier < rhs.identifier; });
    }

    return document;
}

std::string storage_diagnostics_to_json(const StorageDiagnosticsDocument& document)
{
    std::string out;
    out.reserve(2048U);
    out.push_back('{');
    out.append("\"collected_at_ns\":");
    const auto collected_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(document.collected_at.time_since_epoch()).count();
    out.append(std::to_string(static_cast<long long>(collected_ns)));
    out.append(",\"page_managers\":");
    append_page_manager_section(out, document.page_managers);
    out.append(",\"checkpoints\":");
    append_checkpoint_section(out, document.checkpoints);
    out.append(",\"retention\":");
    append_retention_section(out, document.retention);
    out.append(",\"durability\":");
    append_durability_section(out, document.durability);
    out.append(",\"vacuum\":");
    append_vacuum_section(out, document.vacuum);
    out.append(",\"catalog\":");
    append_catalog_section(out, document.catalog);
    out.append(",\"transactions\":");
    append_transaction_section(out, document.transactions);
    out.append(",\"parser\":");
    append_parser_section(out, document.parser);
    out.append(",\"ddl\":");
    append_ddl_section(out, document.ddl);
    out.append(",\"planner\":");
    append_planner_section(out, document.planner);
    out.append(",\"executors\":");
    append_executor_section(out, document.executors);
    out.push_back('}');
    return out;
}

}  // namespace bored::storage

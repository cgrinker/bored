#include "bored/storage/storage_diagnostics.hpp"

#include <algorithm>

namespace bored::storage {
namespace {

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
    out.push_back('}');
    return out;
}

}  // namespace bored::storage

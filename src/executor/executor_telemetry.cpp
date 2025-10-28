#include "bored/executor/executor_telemetry.hpp"

namespace bored::executor {

void ExecutorTelemetry::record_seq_scan_row(bool visible) noexcept
{
    seq_scan_rows_read_.fetch_add(1U, std::memory_order_relaxed);
    if (visible) {
        seq_scan_rows_visible_.fetch_add(1U, std::memory_order_relaxed);
    }
}

void ExecutorTelemetry::record_filter_row(bool passed) noexcept
{
    filter_rows_evaluated_.fetch_add(1U, std::memory_order_relaxed);
    if (passed) {
        filter_rows_passed_.fetch_add(1U, std::memory_order_relaxed);
    }
}

void ExecutorTelemetry::record_projection_row() noexcept
{
    projection_rows_emitted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_nested_loop_compare(bool matched) noexcept
{
    nested_loop_rows_compared_.fetch_add(1U, std::memory_order_relaxed);
    if (matched) {
        nested_loop_rows_matched_.fetch_add(1U, std::memory_order_relaxed);
    }
}

void ExecutorTelemetry::record_nested_loop_emit() noexcept
{
    nested_loop_rows_emitted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_hash_join_build_row() noexcept
{
    hash_join_build_rows_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_hash_join_probe(std::size_t match_count) noexcept
{
    hash_join_probe_rows_.fetch_add(1U, std::memory_order_relaxed);
    if (match_count != 0U) {
        hash_join_rows_matched_.fetch_add(match_count, std::memory_order_relaxed);
    }
}

void ExecutorTelemetry::record_aggregation_input_row() noexcept
{
    aggregation_input_rows_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_aggregation_group_emitted() noexcept
{
    aggregation_groups_emitted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_insert_attempt() noexcept
{
    insert_rows_attempted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_insert_success(std::size_t payload_bytes, std::size_t wal_bytes) noexcept
{
    insert_rows_succeeded_.fetch_add(1U, std::memory_order_relaxed);
    insert_payload_bytes_.fetch_add(static_cast<std::uint64_t>(payload_bytes), std::memory_order_relaxed);
    insert_wal_bytes_.fetch_add(static_cast<std::uint64_t>(wal_bytes), std::memory_order_relaxed);
}

ExecutorTelemetrySnapshot ExecutorTelemetry::snapshot() const noexcept
{
    ExecutorTelemetrySnapshot snapshot{};
    snapshot.seq_scan_rows_read = seq_scan_rows_read_.load(std::memory_order_relaxed);
    snapshot.seq_scan_rows_visible = seq_scan_rows_visible_.load(std::memory_order_relaxed);
    snapshot.filter_rows_evaluated = filter_rows_evaluated_.load(std::memory_order_relaxed);
    snapshot.filter_rows_passed = filter_rows_passed_.load(std::memory_order_relaxed);
    snapshot.projection_rows_emitted = projection_rows_emitted_.load(std::memory_order_relaxed);
    snapshot.nested_loop_rows_compared = nested_loop_rows_compared_.load(std::memory_order_relaxed);
    snapshot.nested_loop_rows_matched = nested_loop_rows_matched_.load(std::memory_order_relaxed);
    snapshot.nested_loop_rows_emitted = nested_loop_rows_emitted_.load(std::memory_order_relaxed);
    snapshot.hash_join_build_rows = hash_join_build_rows_.load(std::memory_order_relaxed);
    snapshot.hash_join_probe_rows = hash_join_probe_rows_.load(std::memory_order_relaxed);
    snapshot.hash_join_rows_matched = hash_join_rows_matched_.load(std::memory_order_relaxed);
    snapshot.aggregation_input_rows = aggregation_input_rows_.load(std::memory_order_relaxed);
    snapshot.aggregation_groups_emitted = aggregation_groups_emitted_.load(std::memory_order_relaxed);
    snapshot.insert_rows_attempted = insert_rows_attempted_.load(std::memory_order_relaxed);
    snapshot.insert_rows_succeeded = insert_rows_succeeded_.load(std::memory_order_relaxed);
    snapshot.insert_payload_bytes = insert_payload_bytes_.load(std::memory_order_relaxed);
    snapshot.insert_wal_bytes = insert_wal_bytes_.load(std::memory_order_relaxed);
    return snapshot;
}

void ExecutorTelemetry::reset() noexcept
{
    seq_scan_rows_read_.store(0U, std::memory_order_relaxed);
    seq_scan_rows_visible_.store(0U, std::memory_order_relaxed);
    filter_rows_evaluated_.store(0U, std::memory_order_relaxed);
    filter_rows_passed_.store(0U, std::memory_order_relaxed);
    projection_rows_emitted_.store(0U, std::memory_order_relaxed);
    nested_loop_rows_compared_.store(0U, std::memory_order_relaxed);
    nested_loop_rows_matched_.store(0U, std::memory_order_relaxed);
    nested_loop_rows_emitted_.store(0U, std::memory_order_relaxed);
    hash_join_build_rows_.store(0U, std::memory_order_relaxed);
    hash_join_probe_rows_.store(0U, std::memory_order_relaxed);
    hash_join_rows_matched_.store(0U, std::memory_order_relaxed);
    aggregation_input_rows_.store(0U, std::memory_order_relaxed);
    aggregation_groups_emitted_.store(0U, std::memory_order_relaxed);
    insert_rows_attempted_.store(0U, std::memory_order_relaxed);
    insert_rows_succeeded_.store(0U, std::memory_order_relaxed);
    insert_payload_bytes_.store(0U, std::memory_order_relaxed);
    insert_wal_bytes_.store(0U, std::memory_order_relaxed);
}

}  // namespace bored::executor

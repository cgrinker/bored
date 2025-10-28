#include "bored/executor/executor_telemetry.hpp"

namespace bored::executor {

ExecutorTelemetry::LatencyScope::LatencyScope(ExecutorTelemetry* telemetry, Operator op) noexcept
    : telemetry_{telemetry}
    , operator_{op}
{
    if (telemetry_ != nullptr) {
        start_ = std::chrono::steady_clock::now();
    }
}

ExecutorTelemetry::LatencyScope::~LatencyScope()
{
    if (telemetry_ == nullptr) {
        return;
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_);
    const auto duration_ns = elapsed.count() > 0 ? static_cast<std::uint64_t>(elapsed.count()) : 0ULL;
    telemetry_->record_latency(operator_, duration_ns);
}

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

void ExecutorTelemetry::record_update_attempt() noexcept
{
    update_rows_attempted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_update_success(std::size_t new_payload_bytes,
                                              std::size_t old_payload_bytes,
                                              std::size_t wal_bytes) noexcept
{
    update_rows_succeeded_.fetch_add(1U, std::memory_order_relaxed);
    update_new_payload_bytes_.fetch_add(static_cast<std::uint64_t>(new_payload_bytes), std::memory_order_relaxed);
    update_old_payload_bytes_.fetch_add(static_cast<std::uint64_t>(old_payload_bytes), std::memory_order_relaxed);
    update_wal_bytes_.fetch_add(static_cast<std::uint64_t>(wal_bytes), std::memory_order_relaxed);
}

void ExecutorTelemetry::record_delete_attempt() noexcept
{
    delete_rows_attempted_.fetch_add(1U, std::memory_order_relaxed);
}

void ExecutorTelemetry::record_delete_success(std::size_t reclaimed_bytes, std::size_t wal_bytes) noexcept
{
    delete_rows_succeeded_.fetch_add(1U, std::memory_order_relaxed);
    delete_reclaimed_bytes_.fetch_add(static_cast<std::uint64_t>(reclaimed_bytes), std::memory_order_relaxed);
    delete_wal_bytes_.fetch_add(static_cast<std::uint64_t>(wal_bytes), std::memory_order_relaxed);
}

void ExecutorTelemetry::record_latency(Operator op, std::uint64_t duration_ns) noexcept
{
    const auto index = static_cast<std::size_t>(op);
    if (index >= latencies_.size()) {
        return;
    }
    auto& counters = latencies_[index];
    counters.invocations.fetch_add(1U, std::memory_order_relaxed);
    counters.total_duration_ns.fetch_add(duration_ns, std::memory_order_relaxed);
    counters.last_duration_ns.store(duration_ns, std::memory_order_relaxed);
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
    snapshot.update_rows_attempted = update_rows_attempted_.load(std::memory_order_relaxed);
    snapshot.update_rows_succeeded = update_rows_succeeded_.load(std::memory_order_relaxed);
    snapshot.update_new_payload_bytes = update_new_payload_bytes_.load(std::memory_order_relaxed);
    snapshot.update_old_payload_bytes = update_old_payload_bytes_.load(std::memory_order_relaxed);
    snapshot.update_wal_bytes = update_wal_bytes_.load(std::memory_order_relaxed);
    snapshot.delete_rows_attempted = delete_rows_attempted_.load(std::memory_order_relaxed);
    snapshot.delete_rows_succeeded = delete_rows_succeeded_.load(std::memory_order_relaxed);
    snapshot.delete_reclaimed_bytes = delete_reclaimed_bytes_.load(std::memory_order_relaxed);
    snapshot.delete_wal_bytes = delete_wal_bytes_.load(std::memory_order_relaxed);

    const auto make_latency_snapshot = [&](Operator operator_kind) {
        ExecutorTelemetrySnapshot::OperatorLatencySnapshot latency{};
        const auto index = static_cast<std::size_t>(operator_kind);
        if (index < latencies_.size()) {
            const auto& counters = latencies_[index];
            latency.invocations = counters.invocations.load(std::memory_order_relaxed);
            latency.total_duration_ns = counters.total_duration_ns.load(std::memory_order_relaxed);
            latency.last_duration_ns = counters.last_duration_ns.load(std::memory_order_relaxed);
        }
        return latency;
    };

    snapshot.seq_scan_latency = make_latency_snapshot(Operator::SeqScan);
    snapshot.filter_latency = make_latency_snapshot(Operator::Filter);
    snapshot.projection_latency = make_latency_snapshot(Operator::Projection);
    snapshot.nested_loop_latency = make_latency_snapshot(Operator::NestedLoopJoin);
    snapshot.hash_join_latency = make_latency_snapshot(Operator::HashJoin);
    snapshot.aggregation_latency = make_latency_snapshot(Operator::Aggregation);
    snapshot.insert_latency = make_latency_snapshot(Operator::Insert);
    snapshot.update_latency = make_latency_snapshot(Operator::Update);
    snapshot.delete_latency = make_latency_snapshot(Operator::Delete);

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
    update_rows_attempted_.store(0U, std::memory_order_relaxed);
    update_rows_succeeded_.store(0U, std::memory_order_relaxed);
    update_new_payload_bytes_.store(0U, std::memory_order_relaxed);
    update_old_payload_bytes_.store(0U, std::memory_order_relaxed);
    update_wal_bytes_.store(0U, std::memory_order_relaxed);
    delete_rows_attempted_.store(0U, std::memory_order_relaxed);
    delete_rows_succeeded_.store(0U, std::memory_order_relaxed);
    delete_reclaimed_bytes_.store(0U, std::memory_order_relaxed);
    delete_wal_bytes_.store(0U, std::memory_order_relaxed);

    for (auto& latency : latencies_) {
        latency.invocations.store(0U, std::memory_order_relaxed);
        latency.total_duration_ns.store(0U, std::memory_order_relaxed);
        latency.last_duration_ns.store(0U, std::memory_order_relaxed);
    }
}

}  // namespace bored::executor

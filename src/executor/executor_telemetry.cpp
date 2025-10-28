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

ExecutorTelemetrySnapshot ExecutorTelemetry::snapshot() const noexcept
{
    ExecutorTelemetrySnapshot snapshot{};
    snapshot.seq_scan_rows_read = seq_scan_rows_read_.load(std::memory_order_relaxed);
    snapshot.seq_scan_rows_visible = seq_scan_rows_visible_.load(std::memory_order_relaxed);
    snapshot.filter_rows_evaluated = filter_rows_evaluated_.load(std::memory_order_relaxed);
    snapshot.filter_rows_passed = filter_rows_passed_.load(std::memory_order_relaxed);
    snapshot.projection_rows_emitted = projection_rows_emitted_.load(std::memory_order_relaxed);
    return snapshot;
}

void ExecutorTelemetry::reset() noexcept
{
    seq_scan_rows_read_.store(0U, std::memory_order_relaxed);
    seq_scan_rows_visible_.store(0U, std::memory_order_relaxed);
    filter_rows_evaluated_.store(0U, std::memory_order_relaxed);
    filter_rows_passed_.store(0U, std::memory_order_relaxed);
    projection_rows_emitted_.store(0U, std::memory_order_relaxed);
}

}  // namespace bored::executor

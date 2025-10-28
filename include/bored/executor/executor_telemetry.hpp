#pragma once

#include <atomic>
#include <cstdint>

namespace bored::executor {

struct ExecutorTelemetrySnapshot final {
    std::uint64_t seq_scan_rows_read = 0U;
    std::uint64_t seq_scan_rows_visible = 0U;
    std::uint64_t filter_rows_evaluated = 0U;
    std::uint64_t filter_rows_passed = 0U;
    std::uint64_t projection_rows_emitted = 0U;
};

class ExecutorTelemetry final {
public:
    void record_seq_scan_row(bool visible) noexcept;
    void record_filter_row(bool passed) noexcept;
    void record_projection_row() noexcept;

    [[nodiscard]] ExecutorTelemetrySnapshot snapshot() const noexcept;
    void reset() noexcept;

private:
    std::atomic<std::uint64_t> seq_scan_rows_read_{0U};
    std::atomic<std::uint64_t> seq_scan_rows_visible_{0U};
    std::atomic<std::uint64_t> filter_rows_evaluated_{0U};
    std::atomic<std::uint64_t> filter_rows_passed_{0U};
    std::atomic<std::uint64_t> projection_rows_emitted_{0U};
};

}  // namespace bored::executor

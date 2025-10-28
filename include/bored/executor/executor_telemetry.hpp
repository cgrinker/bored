#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace bored::executor {

struct ExecutorTelemetrySnapshot final {
    std::uint64_t seq_scan_rows_read = 0U;
    std::uint64_t seq_scan_rows_visible = 0U;
    std::uint64_t filter_rows_evaluated = 0U;
    std::uint64_t filter_rows_passed = 0U;
    std::uint64_t projection_rows_emitted = 0U;
    std::uint64_t nested_loop_rows_compared = 0U;
    std::uint64_t nested_loop_rows_matched = 0U;
    std::uint64_t nested_loop_rows_emitted = 0U;
    std::uint64_t hash_join_build_rows = 0U;
    std::uint64_t hash_join_probe_rows = 0U;
    std::uint64_t hash_join_rows_matched = 0U;
    std::uint64_t aggregation_input_rows = 0U;
    std::uint64_t aggregation_groups_emitted = 0U;
    std::uint64_t insert_rows_attempted = 0U;
    std::uint64_t insert_rows_succeeded = 0U;
    std::uint64_t insert_payload_bytes = 0U;
    std::uint64_t insert_wal_bytes = 0U;
    std::uint64_t update_rows_attempted = 0U;
    std::uint64_t update_rows_succeeded = 0U;
    std::uint64_t update_new_payload_bytes = 0U;
    std::uint64_t update_old_payload_bytes = 0U;
    std::uint64_t update_wal_bytes = 0U;
    std::uint64_t delete_rows_attempted = 0U;
    std::uint64_t delete_rows_succeeded = 0U;
    std::uint64_t delete_reclaimed_bytes = 0U;
    std::uint64_t delete_wal_bytes = 0U;
};

class ExecutorTelemetry final {
public:
    void record_seq_scan_row(bool visible) noexcept;
    void record_filter_row(bool passed) noexcept;
    void record_projection_row() noexcept;
    void record_nested_loop_compare(bool matched) noexcept;
    void record_nested_loop_emit() noexcept;
    void record_hash_join_build_row() noexcept;
    void record_hash_join_probe(std::size_t match_count) noexcept;
    void record_aggregation_input_row() noexcept;
    void record_aggregation_group_emitted() noexcept;
    void record_insert_attempt() noexcept;
    void record_insert_success(std::size_t payload_bytes, std::size_t wal_bytes) noexcept;
    void record_update_attempt() noexcept;
    void record_update_success(std::size_t new_payload_bytes,
                               std::size_t old_payload_bytes,
                               std::size_t wal_bytes) noexcept;
    void record_delete_attempt() noexcept;
    void record_delete_success(std::size_t reclaimed_bytes, std::size_t wal_bytes) noexcept;

    [[nodiscard]] ExecutorTelemetrySnapshot snapshot() const noexcept;
    void reset() noexcept;

private:
    std::atomic<std::uint64_t> seq_scan_rows_read_{0U};
    std::atomic<std::uint64_t> seq_scan_rows_visible_{0U};
    std::atomic<std::uint64_t> filter_rows_evaluated_{0U};
    std::atomic<std::uint64_t> filter_rows_passed_{0U};
    std::atomic<std::uint64_t> projection_rows_emitted_{0U};
    std::atomic<std::uint64_t> nested_loop_rows_compared_{0U};
    std::atomic<std::uint64_t> nested_loop_rows_matched_{0U};
    std::atomic<std::uint64_t> nested_loop_rows_emitted_{0U};
    std::atomic<std::uint64_t> hash_join_build_rows_{0U};
    std::atomic<std::uint64_t> hash_join_probe_rows_{0U};
    std::atomic<std::uint64_t> hash_join_rows_matched_{0U};
    std::atomic<std::uint64_t> aggregation_input_rows_{0U};
    std::atomic<std::uint64_t> aggregation_groups_emitted_{0U};
    std::atomic<std::uint64_t> insert_rows_attempted_{0U};
    std::atomic<std::uint64_t> insert_rows_succeeded_{0U};
    std::atomic<std::uint64_t> insert_payload_bytes_{0U};
    std::atomic<std::uint64_t> insert_wal_bytes_{0U};
    std::atomic<std::uint64_t> update_rows_attempted_{0U};
    std::atomic<std::uint64_t> update_rows_succeeded_{0U};
    std::atomic<std::uint64_t> update_new_payload_bytes_{0U};
    std::atomic<std::uint64_t> update_old_payload_bytes_{0U};
    std::atomic<std::uint64_t> update_wal_bytes_{0U};
    std::atomic<std::uint64_t> delete_rows_attempted_{0U};
    std::atomic<std::uint64_t> delete_rows_succeeded_{0U};
    std::atomic<std::uint64_t> delete_reclaimed_bytes_{0U};
    std::atomic<std::uint64_t> delete_wal_bytes_{0U};
};

}  // namespace bored::executor

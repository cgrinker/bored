#pragma once

#include "bored/storage/async_io.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_retention.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <vector>
#include <mutex>

namespace bored::storage {

class WalTelemetryRegistry;

struct WalWriterTelemetrySnapshot final {
    std::uint64_t append_calls = 0U;
    std::uint64_t appended_bytes = 0U;
    std::uint64_t total_append_duration_ns = 0U;
    std::uint64_t last_append_duration_ns = 0U;
    std::uint64_t flush_calls = 0U;
    std::uint64_t flushed_bytes = 0U;
    std::uint64_t max_flush_bytes = 0U;
    std::uint64_t total_flush_duration_ns = 0U;
    std::uint64_t last_flush_duration_ns = 0U;
    std::uint64_t retention_invocations = 0U;
    std::uint64_t retention_failures = 0U;
    std::uint64_t retention_scanned_segments = 0U;
    std::uint64_t retention_candidate_segments = 0U;
    std::uint64_t retention_pruned_segments = 0U;
    std::uint64_t retention_archived_segments = 0U;
    std::uint64_t retention_total_duration_ns = 0U;
    std::uint64_t retention_last_duration_ns = 0U;
};

struct WalWriterConfig final {
    std::filesystem::path directory{};
    std::string file_prefix = "wal";
    std::string file_extension = ".seg";
    std::uint64_t start_segment_id = 0U;
    std::uint64_t start_lsn = 0U;
    std::size_t buffer_size = 4U * kWalBlockSize;
    std::size_t segment_size = kWalSegmentSize;
    std::size_t size_flush_threshold = 0U;
    std::chrono::milliseconds time_flush_interval{0};
    bool flush_on_commit = true;
    WalTelemetryRegistry* telemetry_registry = nullptr;
    std::string telemetry_identifier{};
    WalRetentionConfig retention{};
};

struct WalRecordDescriptor final {
    WalRecordType type = WalRecordType::PageImage;
    std::uint32_t page_id = 0U;
    WalRecordFlag flags = WalRecordFlag::None;
    std::span<const std::byte> payload{};
};

struct WalAppendResult final {
    std::uint64_t lsn = 0U;
    std::uint64_t prev_lsn = 0U;
    std::uint64_t segment_id = 0U;
    std::uint32_t total_length = 0U;
    std::size_t written_bytes = 0U;
};

class WalWriter final {
public:
    WalWriter(std::shared_ptr<AsyncIo> io, WalWriterConfig config);
    ~WalWriter();

    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;
    WalWriter(WalWriter&&) = delete;
    WalWriter& operator=(WalWriter&&) = delete;

    [[nodiscard]] std::error_code append_record(const WalRecordDescriptor& descriptor, WalAppendResult& out_result);
    [[nodiscard]] std::error_code flush();
    [[nodiscard]] std::error_code close();
    [[nodiscard]] std::error_code notify_commit();

    [[nodiscard]] bool is_closed() const noexcept;
    [[nodiscard]] std::uint64_t next_lsn() const noexcept;
    [[nodiscard]] std::filesystem::path segment_path(std::uint64_t segment_id) const;
    [[nodiscard]] WalWriterTelemetrySnapshot telemetry_snapshot() const;

private:
    [[nodiscard]] std::error_code ensure_directory();
    [[nodiscard]] std::error_code open_segment();
    [[nodiscard]] std::error_code ensure_capacity(std::size_t aligned_length);
    [[nodiscard]] std::error_code flush_buffer();
    [[nodiscard]] std::error_code write_segment_header(bool dsync);
    [[nodiscard]] std::error_code maybe_flush_after_append();
    [[nodiscard]] std::error_code apply_retention();

    [[nodiscard]] std::filesystem::path make_segment_path(std::uint64_t segment_id) const;

    std::shared_ptr<AsyncIo> io_{};
    WalWriterConfig config_{};

    std::vector<std::byte> buffer_{};
    std::size_t buffer_offset_ = 0U;

    std::vector<std::byte> segment_header_block_{};

    WalSegmentHeader segment_header_{};
    bool segment_open_ = false;
    bool segment_header_dirty_ = false;
    std::filesystem::path current_segment_path_{};
    std::uint64_t current_segment_id_ = 0U;
    std::size_t segment_offset_ = 0U;

    std::uint64_t next_lsn_ = 0U;
    std::uint64_t last_lsn_ = 0U;
    bool have_last_lsn_ = false;
    bool closed_ = false;

    std::size_t bytes_since_last_flush_ = 0U;
    std::chrono::steady_clock::time_point last_flush_time_{};

    WalWriterTelemetrySnapshot telemetry_{};
    mutable std::mutex telemetry_mutex_{};
    WalTelemetryRegistry* telemetry_registry_ = nullptr;
    std::string telemetry_identifier_{};

    std::unique_ptr<WalRetentionManager> retention_manager_{};
};

}  // namespace bored::storage

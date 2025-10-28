#include "bored/storage/wal_writer.hpp"

#include "bored/storage/checksum.hpp"
#include "bored/storage/temp_resource_registry.hpp"
#include "bored/storage/wal_retention.hpp"
#include "bored/storage/wal_telemetry_registry.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <mutex>
#include <limits>
#include <span>
#include <sstream>
#include <stdexcept>

namespace bored::storage {

namespace {

constexpr std::size_t align_buffer_size(std::size_t size)
{
    return std::max<std::size_t>(kWalBlockSize, align_up_to_block(size));
}

bool is_multiple_of_block(std::size_t size)
{
    return (size % kWalBlockSize) == 0U;
}

}  // namespace

WalWriter::WalWriter(std::shared_ptr<AsyncIo> io, WalWriterConfig config)
    : io_{std::move(io)}
    , config_{std::move(config)}
    , buffer_(align_buffer_size(config_.buffer_size), std::byte{0})
    , segment_header_block_(kWalBlockSize, std::byte{0})
    , current_segment_id_{config_.start_segment_id}
    , next_lsn_{config_.start_lsn}
{
    if (!io_) {
        throw std::invalid_argument{"WalWriter requires a valid AsyncIo instance"};
    }
    if (config_.directory.empty()) {
        throw std::invalid_argument{"WalWriter requires a WAL directory"};
    }
    if (config_.segment_size < kWalBlockSize * 2U) {
        throw std::invalid_argument{"WalWriter segment size must allow at least one record block"};
    }
    if (!is_multiple_of_block(config_.segment_size)) {
        throw std::invalid_argument{"WalWriter segment size must be block aligned"};
    }
    config_.buffer_size = buffer_.size();
    last_flush_time_ = std::chrono::steady_clock::now();

    telemetry_registry_ = config_.telemetry_registry;
    telemetry_identifier_ = config_.telemetry_identifier;
    if (telemetry_registry_ && !telemetry_identifier_.empty()) {
        telemetry_registry_->register_sampler(telemetry_identifier_, [this] {
            return this->telemetry_snapshot();
        });
    }

    storage_telemetry_registry_ = config_.storage_telemetry_registry;
    temp_cleanup_telemetry_identifier_ = config_.temp_cleanup_telemetry_identifier;
    if (storage_telemetry_registry_ && !temp_cleanup_telemetry_identifier_.empty()) {
        storage_telemetry_registry_->register_temp_cleanup(temp_cleanup_telemetry_identifier_, [this] {
            return this->temp_cleanup_snapshot();
        });
    }

    durability_horizon_ = config_.durability_horizon;
    temp_resource_registry_ = config_.temp_resource_registry;

    const bool retention_enabled = config_.retention.retention_segments > 0U
        || config_.retention.retention_hours.count() > 0
        || !config_.retention.archive_path.empty();
    if (retention_enabled) {
        retention_manager_ = std::make_unique<WalRetentionManager>(config_.directory,
                                                                   config_.file_prefix,
                                                                   config_.file_extension,
                                                                   durability_horizon_);
    }
}

WalWriter::~WalWriter()
{
    (void)close();
    if (telemetry_registry_ && !telemetry_identifier_.empty()) {
        telemetry_registry_->unregister_sampler(telemetry_identifier_);
    }
    if (storage_telemetry_registry_ && !temp_cleanup_telemetry_identifier_.empty()) {
        storage_telemetry_registry_->unregister_temp_cleanup(temp_cleanup_telemetry_identifier_);
    }
}

std::error_code WalWriter::ensure_directory()
{
    std::error_code ec;
    std::filesystem::create_directories(config_.directory, ec);
    return ec;
}

std::filesystem::path WalWriter::make_segment_path(std::uint64_t segment_id) const
{
    std::ostringstream name;
    name << config_.file_prefix << '_' << std::setw(16) << std::setfill('0') << std::uppercase << std::hex << segment_id << config_.file_extension;
    return config_.directory / name.str();
}

std::error_code WalWriter::open_segment()
{
    if (segment_open_) {
        return {};
    }

    if (auto ec = ensure_directory(); ec) {
        return ec;
    }

    current_segment_path_ = make_segment_path(current_segment_id_);

    segment_header_ = WalSegmentHeader{};
    segment_header_.segment_id = current_segment_id_;
    segment_header_.start_lsn = next_lsn_;
    segment_header_.end_lsn = next_lsn_;

    std::fill(segment_header_block_.begin(), segment_header_block_.end(), std::byte{0});
    std::memcpy(segment_header_block_.data(), &segment_header_, sizeof(WalSegmentHeader));

    WriteRequest request{};
    request.path = current_segment_path_;
    request.offset = 0U;
    request.file_class = FileClass::WriteAheadLog;
    request.data = segment_header_block_.data();
    request.size = segment_header_block_.size();
    request.flags = IoFlag::Dsync;

    auto result = io_->submit_write(request).get();
    if (result.status) {
        return result.status;
    }
    if (result.bytes_transferred != request.size) {
        return std::make_error_code(std::errc::io_error);
    }

    segment_open_ = true;
    segment_header_dirty_ = false;
    segment_offset_ = kWalBlockSize;
    buffer_offset_ = 0U;
    return {};
}

std::error_code WalWriter::flush_buffer()
{
    if (buffer_offset_ == 0U) {
        return {};
    }

    if (auto ec = open_segment(); ec) {
        return ec;
    }

    WriteRequest request{};
    request.path = current_segment_path_;
    request.offset = segment_offset_;
    request.file_class = FileClass::WriteAheadLog;
    request.data = buffer_.data();
    request.size = buffer_offset_;

    auto result = io_->submit_write(request).get();
    if (result.status) {
        return result.status;
    }
    if (result.bytes_transferred != request.size) {
        return std::make_error_code(std::errc::io_error);
    }

    segment_offset_ += buffer_offset_;
    buffer_offset_ = 0U;
    std::fill(buffer_.begin(), buffer_.end(), std::byte{0});
    return {};
}

std::error_code WalWriter::write_segment_header(bool dsync)
{
    if (!segment_open_) {
        return {};
    }
    if (!segment_header_dirty_) {
        return {};
    }

    segment_header_.end_lsn = next_lsn_;
    std::fill(segment_header_block_.begin(), segment_header_block_.end(), std::byte{0});
    std::memcpy(segment_header_block_.data(), &segment_header_, sizeof(WalSegmentHeader));

    WriteRequest request{};
    request.path = current_segment_path_;
    request.offset = 0U;
    request.file_class = FileClass::WriteAheadLog;
    request.data = segment_header_block_.data();
    request.size = segment_header_block_.size();
    request.flags = dsync ? IoFlag::Dsync : IoFlag::None;

    auto result = io_->submit_write(request).get();
    if (result.status) {
        return result.status;
    }
    if (result.bytes_transferred != request.size) {
        return std::make_error_code(std::errc::io_error);
    }

    segment_header_dirty_ = false;
    return {};
}

std::error_code WalWriter::ensure_capacity(std::size_t aligned_length)
{
    if (auto ec = open_segment(); ec) {
        return ec;
    }

    if (segment_offset_ + buffer_offset_ + aligned_length <= config_.segment_size) {
        return {};
    }

    if (auto ec = flush_buffer(); ec) {
        return ec;
    }

    if (segment_offset_ + aligned_length <= config_.segment_size) {
        return {};
    }

    if (auto ec = write_segment_header(true); ec) {
        return ec;
    }

    segment_open_ = false;
    current_segment_path_.clear();
    segment_offset_ = 0U;
    ++current_segment_id_;

    return open_segment();
}

std::error_code WalWriter::append_record_internal(const WalRecordDescriptor& descriptor,
                                                  WalAppendResult& out_result,
                                                  WalStagedAppend* stage)
{
    if (closed_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    const auto append_start = std::chrono::steady_clock::now();

    const auto payload_size = descriptor.payload.size();
    if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) - sizeof(WalRecordHeader)) {
        return std::make_error_code(std::errc::value_too_large);
    }

    const auto total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) + payload_size);
    const auto aligned_length = align_up_to_block(static_cast<std::size_t>(total_length));

    if (aligned_length > config_.segment_size - kWalBlockSize) {
        return std::make_error_code(std::errc::no_buffer_space);
    }

    if (aligned_length > buffer_.size()) {
        if (auto ec = flush_buffer(); ec) {
            return ec;
        }
        buffer_.assign(align_buffer_size(aligned_length), std::byte{0});
        config_.buffer_size = buffer_.size();
    } else if (buffer_offset_ + aligned_length > buffer_.size()) {
        if (auto ec = flush_buffer(); ec) {
            return ec;
        }
    }

    if (auto ec = ensure_capacity(aligned_length); ec) {
        return ec;
    }

    const std::size_t initial_buffer_offset = buffer_offset_;
    const std::uint64_t initial_next_lsn = next_lsn_;
    const std::uint64_t initial_last_lsn = last_lsn_;
    const bool initial_have_last_lsn = have_last_lsn_;
    const std::uint64_t initial_segment_end_lsn = segment_header_.end_lsn;
    const bool initial_segment_header_dirty = segment_header_dirty_;
    const std::size_t initial_bytes_since_last_flush = bytes_since_last_flush_;

    WalRecordFlag flags = descriptor.flags;
    if (!descriptor.payload.empty()) {
        flags = flags | WalRecordFlag::HasPayload;
    }

    WalRecordHeader header{};
    header.total_length = total_length;
    header.type = static_cast<std::uint16_t>(descriptor.type);
    header.flags = static_cast<std::uint16_t>(flags);
    header.lsn = next_lsn_;
    header.prev_lsn = have_last_lsn_ ? last_lsn_ : 0U;
    header.page_id = descriptor.page_id;

    apply_wal_checksum(header, descriptor.payload);

    auto target = std::span<std::byte>(buffer_.data() + buffer_offset_, aligned_length);
    std::fill(target.begin(), target.end(), std::byte{0});
    std::memcpy(target.data(), &header, sizeof(WalRecordHeader));
    if (!descriptor.payload.empty()) {
        std::memcpy(target.data() + sizeof(WalRecordHeader), descriptor.payload.data(), descriptor.payload.size());
    }

    buffer_offset_ += aligned_length;
    last_lsn_ = header.lsn;
    have_last_lsn_ = true;
    next_lsn_ += aligned_length;
    segment_header_.end_lsn = next_lsn_;
    segment_header_dirty_ = true;
    bytes_since_last_flush_ += aligned_length;

    out_result.lsn = header.lsn;
    out_result.prev_lsn = header.prev_lsn;
    out_result.segment_id = current_segment_id_;
    out_result.total_length = total_length;
    out_result.written_bytes = aligned_length;

    if (!stage) {
        if (auto ec = maybe_flush_after_append(); ec) {
            return ec;
        }
    } else {
        stage->buffer_offset = initial_buffer_offset;
        stage->aligned_length = aligned_length;
        stage->next_lsn_before = initial_next_lsn;
        stage->last_lsn_before = initial_last_lsn;
        stage->had_last_lsn_before = initial_have_last_lsn;
        stage->segment_end_lsn_before = initial_segment_end_lsn;
        stage->segment_header_dirty_before = initial_segment_header_dirty;
        stage->bytes_since_last_flush_before = initial_bytes_since_last_flush;
        stage->segment_id = current_segment_id_;
        stage->valid = true;
    }

    const auto append_end = std::chrono::steady_clock::now();
    const auto append_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(append_end - append_start).count();
    {
        std::lock_guard guard(telemetry_mutex_);
        telemetry_.append_calls += 1U;
        telemetry_.appended_bytes += aligned_length;
        telemetry_.last_append_duration_ns = static_cast<std::uint64_t>(append_ns >= 0 ? append_ns : 0);
        telemetry_.total_append_duration_ns += telemetry_.last_append_duration_ns;
    }

    return {};
}

std::error_code WalWriter::append_record(const WalRecordDescriptor& descriptor, WalAppendResult& out_result)
{
    return append_record_internal(descriptor, out_result, nullptr);
}

std::error_code WalWriter::append_commit_record(const WalCommitHeader& header, WalAppendResult& out_result)
{
    std::array<std::byte, wal_commit_payload_size()> buffer{};
    auto payload = std::span<std::byte>(buffer.data(), buffer.size());
    if (!encode_wal_commit(payload, header)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Commit;
    descriptor.page_id = header.transaction_id <= std::numeric_limits<std::uint32_t>::max()
        ? static_cast<std::uint32_t>(header.transaction_id)
        : 0U;
    descriptor.flags = WalRecordFlag::HasPayload;
    descriptor.payload = std::span<const std::byte>(buffer.data(), buffer.size());

    return append_record_internal(descriptor, out_result, nullptr);
}

std::error_code WalWriter::stage_commit_record(const WalCommitHeader& header,
                                               WalAppendResult& out_result,
                                               WalStagedAppend& out_stage)
{
    out_stage = WalStagedAppend{};

    std::array<std::byte, wal_commit_payload_size()> buffer{};
    auto payload = std::span<std::byte>(buffer.data(), buffer.size());
    if (!encode_wal_commit(payload, header)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Commit;
    descriptor.page_id = header.transaction_id <= std::numeric_limits<std::uint32_t>::max()
        ? static_cast<std::uint32_t>(header.transaction_id)
        : 0U;
    descriptor.flags = WalRecordFlag::HasPayload;
    descriptor.payload = std::span<const std::byte>(buffer.data(), buffer.size());

    auto ec = append_record_internal(descriptor, out_result, &out_stage);
    if (ec) {
        out_stage.valid = false;
    }
    return ec;
}

void WalWriter::rollback_staged_append(const WalStagedAppend& stage) noexcept
{
    if (!stage.valid) {
        return;
    }

    buffer_offset_ = stage.buffer_offset;
    next_lsn_ = stage.next_lsn_before;
    last_lsn_ = stage.last_lsn_before;
    have_last_lsn_ = stage.had_last_lsn_before;
    segment_header_.end_lsn = stage.segment_end_lsn_before;
    segment_header_dirty_ = stage.segment_header_dirty_before;
    bytes_since_last_flush_ = stage.bytes_since_last_flush_before;

    const auto begin = buffer_.begin() + static_cast<std::ptrdiff_t>(stage.buffer_offset);
    const auto end = begin + static_cast<std::ptrdiff_t>(stage.aligned_length);
    if (begin >= buffer_.begin() && end <= buffer_.end()) {
        std::fill(begin, end, std::byte{0});
    }
}

std::error_code WalWriter::flush()
{
    if (closed_) {
        return {};
    }

    const auto pending_bytes = bytes_since_last_flush_;
    const auto flush_start = std::chrono::steady_clock::now();

    if (auto ec = flush_buffer(); ec) {
        return ec;
    }

    if (auto ec = write_segment_header(true); ec) {
        return ec;
    }

    auto result = io_->flush(FileClass::WriteAheadLog).get();
    if (!result.status) {
        last_flush_time_ = std::chrono::steady_clock::now();
        bytes_since_last_flush_ = 0U;

        const auto flush_end = std::chrono::steady_clock::now();
        const auto flush_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(flush_end - flush_start).count();
        const auto duration_ns = static_cast<std::uint64_t>(flush_ns >= 0 ? flush_ns : 0);

        std::lock_guard guard(telemetry_mutex_);
        telemetry_.flush_calls += 1U;
        telemetry_.flushed_bytes += pending_bytes;
        telemetry_.max_flush_bytes = std::max(telemetry_.max_flush_bytes, static_cast<std::uint64_t>(pending_bytes));
        telemetry_.last_flush_duration_ns = duration_ns;
        telemetry_.total_flush_duration_ns += duration_ns;
    }
    if (result.status) {
        return result.status;
    }

    if (auto retention_ec = apply_retention_internal(config_.retention, current_segment_id_, nullptr); retention_ec) {
        return retention_ec;
    }

    return {};
}

std::error_code WalWriter::close()
{
    if (closed_) {
        return {};
    }

    if (auto ec = flush(); ec) {
        return ec;
    }

    segment_open_ = false;
    closed_ = true;
    return {};
}

bool WalWriter::is_closed() const noexcept
{
    return closed_;
}

std::uint64_t WalWriter::next_lsn() const noexcept
{
    return next_lsn_;
}

std::filesystem::path WalWriter::segment_path(std::uint64_t segment_id) const
{
    return make_segment_path(segment_id);
}

WalWriterTelemetrySnapshot WalWriter::telemetry_snapshot() const
{
    std::lock_guard guard(telemetry_mutex_);
    return telemetry_;
}

TempCleanupTelemetrySnapshot WalWriter::temp_cleanup_snapshot() const
{
    std::lock_guard guard(telemetry_mutex_);
    TempCleanupTelemetrySnapshot snapshot{};
    snapshot.invocations = telemetry_.temp_cleanup_invocations;
    snapshot.failures = telemetry_.temp_cleanup_failures;
    snapshot.removed_entries = telemetry_.temp_cleanup_removed_entries;
    snapshot.total_duration_ns = telemetry_.temp_cleanup_total_duration_ns;
    snapshot.last_duration_ns = telemetry_.temp_cleanup_last_duration_ns;
    return snapshot;
}

std::shared_ptr<WalDurabilityHorizon> WalWriter::durability_horizon() const noexcept
{
    return durability_horizon_;
}

std::error_code WalWriter::notify_commit()
{
    if (!config_.flush_on_commit) {
        return {};
    }
    return flush();
}

std::error_code WalWriter::apply_retention(const WalRetentionConfig& config,
                                           std::uint64_t last_segment_id,
                                           WalRetentionStats* stats)
{
    return apply_retention_internal(config, last_segment_id, stats);
}

std::error_code WalWriter::maybe_flush_after_append()
{
    const bool size_trigger = config_.size_flush_threshold > 0U
        && bytes_since_last_flush_ >= config_.size_flush_threshold;

    const bool time_trigger = config_.time_flush_interval.count() > 0
        && (std::chrono::steady_clock::now() - last_flush_time_) >= config_.time_flush_interval;

    if (!size_trigger && !time_trigger) {
        return {};
    }

    return flush();
}

std::error_code WalWriter::apply_retention_internal(const WalRetentionConfig& config,
                                                    std::uint64_t last_segment_id,
                                                    WalRetentionStats* stats)
{
    WalRetentionStats local_stats{};
    WalRetentionStats* stats_ptr = stats != nullptr ? stats : &local_stats;

    const bool retention_enabled = config.retention_segments > 0U
        || config.retention_hours.count() > 0
        || !config.archive_path.empty();

    if (retention_enabled && !retention_manager_) {
        retention_manager_ = std::make_unique<WalRetentionManager>(config_.directory,
                                                                   config_.file_prefix,
                                                                   config_.file_extension,
                                                                   durability_horizon_);
    }

    config_.retention = config;

    std::error_code retention_error{};
    if (retention_manager_) {
        const auto retention_start = std::chrono::steady_clock::now();
        retention_error = retention_manager_->apply(config, last_segment_id, stats_ptr);
        const auto retention_end = std::chrono::steady_clock::now();
        const auto retention_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(retention_end - retention_start).count();
        const auto duration_ns = static_cast<std::uint64_t>(retention_ns >= 0 ? retention_ns : 0ULL);

        {
            std::lock_guard guard(telemetry_mutex_);
            telemetry_.retention_invocations += 1U;
            telemetry_.retention_scanned_segments += stats_ptr->scanned_segments;
            telemetry_.retention_candidate_segments += stats_ptr->candidate_segments;
            telemetry_.retention_pruned_segments += stats_ptr->pruned_segments;
            telemetry_.retention_archived_segments += stats_ptr->archived_segments;
            telemetry_.retention_total_duration_ns += duration_ns;
            telemetry_.retention_last_duration_ns = duration_ns;
            if (retention_error) {
                telemetry_.retention_failures += 1U;
            }
        }
    } else if (stats != nullptr) {
        *stats = WalRetentionStats{};
    }

    std::error_code cleanup_error = run_temp_cleanup(TempResourcePurgeReason::Checkpoint);

    if (!retention_error) {
        return cleanup_error;
    }
    return retention_error;
}

std::error_code WalWriter::run_temp_cleanup(TempResourcePurgeReason reason)
{
    if (!temp_resource_registry_) {
        return {};
    }

    const auto cleanup_start = std::chrono::steady_clock::now();
    TempResourcePurgeStats stats{};
    auto cleanup_error = temp_resource_registry_->purge(reason, &stats);
    const auto cleanup_end = std::chrono::steady_clock::now();
    const auto cleanup_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(cleanup_end - cleanup_start).count();
    const auto duration_ns = static_cast<std::uint64_t>(cleanup_ns >= 0 ? cleanup_ns : 0ULL);

    {
        std::lock_guard guard(telemetry_mutex_);
        telemetry_.temp_cleanup_invocations += 1U;
        telemetry_.temp_cleanup_removed_entries += stats.purged_entries;
        telemetry_.temp_cleanup_total_duration_ns += duration_ns;
        telemetry_.temp_cleanup_last_duration_ns = duration_ns;
        if (cleanup_error) {
            telemetry_.temp_cleanup_failures += 1U;
        }
    }

    return cleanup_error;
}

}  // namespace bored::storage

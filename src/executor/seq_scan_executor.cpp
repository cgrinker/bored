#include "bored/executor/seq_scan_executor.hpp"

#include "bored/executor/executor_context.hpp"
#include "bored/executor/mvcc_visibility.hpp"

#include <span>
#include <stdexcept>
#include <utility>

namespace {

std::uint64_t encode_row_id(const bored::storage::TableTuple& tuple) noexcept
{
    return (static_cast<std::uint64_t>(tuple.page_id) << 16U) | static_cast<std::uint64_t>(tuple.slot_id);
}

std::span<const std::byte> header_bytes(const bored::storage::TableTuple& tuple) noexcept
{
    return std::as_bytes(std::span<const bored::storage::TupleHeader>(&tuple.header, 1U));
}

}  // namespace

namespace bored::executor {

SequentialScanExecutor::SequentialScanExecutor(Config config)
    : config_{std::move(config)}
{
    if (config_.reader == nullptr) {
        throw std::invalid_argument{"SequentialScanExecutor requires a storage reader"};
    }
    if (!config_.relation_id.is_valid()) {
        throw std::invalid_argument{"SequentialScanExecutor requires a valid relation identifier"};
    }
}

void SequentialScanExecutor::open(ExecutorContext& context)
{
    (void)context;
    bored::storage::TableScanConfig scan_config{};
    scan_config.relation_id = config_.relation_id;
    scan_config.root_page_id = config_.root_page_id;
    cursor_ = config_.reader->create_table_scan(scan_config);
    index_hits_.clear();
    index_position_ = 0U;
    yielded_row_ids_.clear();

    if (config_.index_probe && config_.reader != nullptr) {
        const auto& probe = *config_.index_probe;
        bored::storage::IndexProbeConfig probe_config{};
        probe_config.relation_id = config_.relation_id;
        probe_config.index_id = probe.index_id;
        probe_config.key = std::span<const std::byte>(probe.key.data(), probe.key.size());
        index_hits_ = config_.reader->probe_index(probe_config);
    }
}

bool SequentialScanExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::SeqScan};
    while (index_position_ < index_hits_.size()) {
        const auto& hit = index_hits_[index_position_++];
        const bool visible = is_tuple_visible(hit.tuple.header, context.transaction_id(), context.snapshot());
        if (config_.telemetry != nullptr) {
            config_.telemetry->record_seq_scan_row(visible);
        }
        if (!visible) {
            continue;
        }

        const auto row_id = encode_row_id(hit.tuple);
        if (row_id != 0U) {
            auto [_, inserted] = yielded_row_ids_.insert(row_id);
            if (!inserted) {
                continue;
            }
        }

        TupleWriter writer{buffer};
        writer.reset();
        writer.append_column(header_bytes(hit.tuple));
        writer.append_column(hit.tuple.payload);
        writer.finalize();
        return true;
    }

    if (!cursor_ || !config_.enable_heap_fallback) {
        return false;
    }

    bored::storage::TableTuple tuple{};
    while (cursor_->next(tuple)) {
        const bool visible = is_tuple_visible(tuple.header, context.transaction_id(), context.snapshot());
        if (config_.telemetry != nullptr) {
            config_.telemetry->record_seq_scan_row(visible);
        }
        if (!visible) {
            continue;
        }

        const auto row_id = encode_row_id(tuple);
        if (row_id != 0U && yielded_row_ids_.contains(row_id)) {
            continue;
        }
        if (row_id != 0U) {
            yielded_row_ids_.insert(row_id);
        }

        TupleWriter writer{buffer};
        writer.reset();
        writer.append_column(header_bytes(tuple));
        writer.append_column(tuple.payload);
        writer.finalize();
        return true;
    }

    return false;
}

void SequentialScanExecutor::close(ExecutorContext& context)
{
    (void)context;
    cursor_.reset();
    index_hits_.clear();
    index_position_ = 0U;
    yielded_row_ids_.clear();
}

}  // namespace bored::executor

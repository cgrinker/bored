#include "bored/executor/seq_scan_executor.hpp"

#include "bored/executor/executor_context.hpp"
#include "bored/executor/mvcc_visibility.hpp"

#include <span>
#include <stdexcept>
#include <utility>

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
}

bool SequentialScanExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    if (!cursor_) {
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

        TupleWriter writer{buffer};
        writer.reset();

        const auto header_bytes = std::as_bytes(std::span<const bored::storage::TupleHeader>(&tuple.header, 1U));
        writer.append_column(header_bytes);
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
}

}  // namespace bored::executor

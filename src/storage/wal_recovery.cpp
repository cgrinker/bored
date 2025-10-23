#include "bored/storage/wal_recovery.hpp"

#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

namespace bored::storage {

namespace {

struct TransactionState final {
    std::vector<WalRecoveryRecord> records{};
    std::size_t sequence = 0U;
};

std::optional<std::uint32_t> owner_page_id(const WalRecordView& view)
{
    const auto type = static_cast<WalRecordType>(view.header.type);
    auto payload = std::span<const std::byte>(view.payload.data(), view.payload.size());

    switch (type) {
    case WalRecordType::TupleInsert:
    case WalRecordType::TupleDelete: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta || meta->page_id == 0U || meta->page_id != view.header.page_id) {
            return view.header.page_id;
        }
        return meta->page_id;
    }
    case WalRecordType::TupleUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta || meta->base.page_id == 0U || meta->base.page_id != view.header.page_id) {
            return view.header.page_id;
        }
        return meta->base.page_id;
    }
    case WalRecordType::TupleBeforeImage: {
        auto before_view = decode_wal_tuple_before_image(payload);
        if (!before_view || before_view->meta.page_id == 0U || before_view->meta.page_id != view.header.page_id) {
            return view.header.page_id;
        }
        return before_view->meta.page_id;
    }
    case WalRecordType::TupleOverflowChunk: {
        auto meta = decode_wal_overflow_chunk_meta(payload);
        if (!meta || meta->owner.page_id == 0U) {
            return view.header.page_id;
        }
        return meta->owner.page_id;
    }
    case WalRecordType::TupleOverflowTruncate: {
        auto meta = decode_wal_overflow_truncate_meta(payload);
        if (!meta || meta->owner.page_id == 0U) {
            return view.header.page_id;
        }
        return meta->owner.page_id;
    }
    case WalRecordType::PageCompaction:
    case WalRecordType::Commit:
    case WalRecordType::Abort:
    case WalRecordType::Checkpoint:
        return view.header.page_id;
    default:
        return view.header.page_id;
    }
}

WalRecoveryRecord make_recovery_record(const WalRecordView& view)
{
    WalRecoveryRecord record{};
    record.header = view.header;
    record.payload = view.payload;
    return record;
}

std::uint64_t last_valid_lsn(std::uint64_t start_lsn, const std::vector<WalRecordView>& records)
{
    if (records.empty()) {
        return start_lsn;
    }
    const auto& tail = records.back();
    return tail.header.lsn + align_up_to_block(tail.header.total_length);
}

}  // namespace

WalRecoveryDriver::WalRecoveryDriver(std::filesystem::path directory,
                                     std::string file_prefix,
                                     std::string file_extension)
    : reader_{std::move(directory), std::move(file_prefix), std::move(file_extension)}
{
}

std::error_code WalRecoveryDriver::build_plan(WalRecoveryPlan& plan) const
{
    plan.redo.clear();
    plan.undo.clear();
    plan.undo_spans.clear();
    plan.truncated_tail = false;
    plan.truncated_segment_id = 0U;
    plan.truncated_lsn = 0U;

    std::vector<WalSegmentView> segments;
    if (auto ec = reader_.enumerate_segments(segments); ec) {
        return ec;
    }

    std::unordered_map<std::uint32_t, TransactionState> transactions;
    std::size_t transaction_sequence = 0U;

    auto ensure_transaction = [&](std::uint32_t txn_id) -> TransactionState& {
        auto [it, inserted] = transactions.try_emplace(txn_id);
        if (inserted) {
            it->second.sequence = transaction_sequence++;
        }
        return it->second;
    };

    auto append_undo_span = [&](std::uint32_t txn_id, const std::vector<WalRecoveryRecord>& records) {
        if (records.empty()) {
            return;
        }
        const auto start = plan.undo.size();
        for (auto rit = records.rbegin(); rit != records.rend(); ++rit) {
            plan.undo.push_back(*rit);
        }
        const auto count = plan.undo.size() - start;
        if (count != 0U) {
            plan.undo_spans.push_back(WalUndoSpan{txn_id, start, count});
        }
    };

    for (const auto& segment : segments) {
        std::vector<WalRecordView> records;
        auto ec = reader_.read_records(segment, records);
        if (ec) {
            if (ec == std::make_error_code(std::errc::io_error)) {
                plan.truncated_tail = true;
                plan.truncated_segment_id = segment.header.segment_id;
                plan.truncated_lsn = last_valid_lsn(segment.header.start_lsn, records);
            } else {
                return ec;
            }
        }

        for (const auto& record_view : records) {
            const auto type = static_cast<WalRecordType>(record_view.header.type);
            auto owner = owner_page_id(record_view);
            if (!owner) {
                return std::make_error_code(std::errc::invalid_argument);
            }
            const auto txn_id = *owner;

            switch (type) {
            case WalRecordType::Commit: {
                auto it = transactions.find(txn_id);
                if (it != transactions.end()) {
                    auto& prepared = it->second.records;
                    for (const auto& record : prepared) {
                        const auto record_type = static_cast<WalRecordType>(record.header.type);
                        if (record_type == WalRecordType::TupleBeforeImage) {
                            continue;
                        }
                        plan.redo.push_back(record);
                    }
                    transactions.erase(it);
                }
                break;
            }
            case WalRecordType::Abort: {
                auto it = transactions.find(txn_id);
                if (it != transactions.end()) {
                    append_undo_span(txn_id, it->second.records);
                    transactions.erase(it);
                }
                break;
            }
            case WalRecordType::Checkpoint: {
                plan.redo.push_back(make_recovery_record(record_view));
                break;
            }
            default: {
                auto& txn = ensure_transaction(txn_id);
                txn.records.push_back(make_recovery_record(record_view));
                break;
            }
            }
        }

        if (plan.truncated_tail) {
            break;
        }
    }

    std::vector<std::pair<std::uint32_t, TransactionState*>> survivors;
    survivors.reserve(transactions.size());
    for (auto& [txn_id, state] : transactions) {
        survivors.emplace_back(txn_id, &state);
    }

    std::sort(survivors.begin(), survivors.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.second->sequence < rhs.second->sequence;
    });

    for (const auto& [txn_id, state] : survivors) {
        append_undo_span(txn_id, state->records);
    }

    return {};
}

}  // namespace bored::storage

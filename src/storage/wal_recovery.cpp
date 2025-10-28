#include "bored/storage/wal_recovery.hpp"

#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"

#include <algorithm>
#include <limits>
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

struct RecoveredTransactionState final {
    WalRecoveredTransaction transaction{};
    std::size_t sequence = 0U;
    bool has_first_lsn = false;
};

std::optional<std::uint64_t> owner_identifier(const WalRecordView& view)
{
    const auto type = static_cast<WalRecordType>(view.header.type);
    auto payload = std::span<const std::byte>(view.payload.data(), view.payload.size());

    switch (type) {
    case WalRecordType::TupleInsert:
    case WalRecordType::CatalogInsert:
    case WalRecordType::TupleDelete:
    case WalRecordType::CatalogDelete: {
        auto meta = decode_wal_tuple_meta(payload);
        if (!meta || meta->page_id == 0U || meta->page_id != view.header.page_id) {
            return static_cast<std::uint64_t>(view.header.page_id);
        }
        return static_cast<std::uint64_t>(meta->page_id);
    }
    case WalRecordType::TupleUpdate:
    case WalRecordType::CatalogUpdate: {
        auto meta = decode_wal_tuple_update_meta(payload);
        if (!meta || meta->base.page_id == 0U || meta->base.page_id != view.header.page_id) {
            return static_cast<std::uint64_t>(view.header.page_id);
        }
        return static_cast<std::uint64_t>(meta->base.page_id);
    }
    case WalRecordType::TupleBeforeImage: {
        auto before_view = decode_wal_tuple_before_image(payload);
        if (!before_view || before_view->meta.page_id == 0U || before_view->meta.page_id != view.header.page_id) {
            return static_cast<std::uint64_t>(view.header.page_id);
        }
        return static_cast<std::uint64_t>(before_view->meta.page_id);
    }
    case WalRecordType::TupleOverflowChunk: {
        auto meta = decode_wal_overflow_chunk_meta(payload);
        if (!meta || meta->owner.page_id == 0U) {
            return static_cast<std::uint64_t>(view.header.page_id);
        }
        return static_cast<std::uint64_t>(meta->owner.page_id);
    }
    case WalRecordType::TupleOverflowTruncate: {
        auto meta = decode_wal_overflow_truncate_meta(payload);
        if (!meta || meta->owner.page_id == 0U) {
            return static_cast<std::uint64_t>(view.header.page_id);
        }
        return static_cast<std::uint64_t>(meta->owner.page_id);
    }
    case WalRecordType::Commit: {
        auto commit = decode_wal_commit(payload);
        if (commit && commit->transaction_id != 0U && commit->transaction_id <= std::numeric_limits<std::uint32_t>::max()) {
            return commit->transaction_id;
        }
        return static_cast<std::uint64_t>(view.header.page_id);
    }
    case WalRecordType::PageCompaction:
    case WalRecordType::Abort:
    case WalRecordType::Checkpoint:
        return static_cast<std::uint64_t>(view.header.page_id);
    default:
        return static_cast<std::uint64_t>(view.header.page_id);
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
    plan.transactions.clear();
    plan.truncated_tail = false;
    plan.truncated_segment_id = 0U;
    plan.truncated_lsn = 0U;
    plan.next_transaction_id_high_water = 0U;
    plan.oldest_active_transaction_id = 0U;
    plan.oldest_active_commit_lsn = 0U;

    std::vector<WalSegmentView> segments;
    if (auto ec = reader_.enumerate_segments(segments); ec) {
        return ec;
    }

    std::unordered_map<std::uint32_t, TransactionState> transactions;
    std::size_t transaction_sequence = 0U;

    std::unordered_map<std::uint64_t, RecoveredTransactionState> recovered_transactions;
    std::size_t recovered_sequence = 0U;

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

    auto ensure_recovered_transaction = [&](std::uint64_t txn_id) -> RecoveredTransactionState& {
        auto [it, inserted] = recovered_transactions.try_emplace(txn_id);
        if (inserted) {
            it->second.transaction.transaction_id = txn_id;
            it->second.transaction.state = WalRecoveredTransactionState::InFlight;
            it->second.sequence = recovered_sequence++;
        }
        return it->second;
    };

    auto record_transaction_progress = [&](std::uint64_t txn_id, const WalRecordHeader& header) {
        if (txn_id == 0U) {
            return;
        }
        auto& entry = ensure_recovered_transaction(txn_id);
        if (!entry.has_first_lsn) {
            entry.transaction.first_lsn = header.lsn;
            entry.has_first_lsn = true;
        }
        entry.transaction.last_lsn = header.lsn;
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

            if (type == WalRecordType::CatalogInsert || type == WalRecordType::CatalogDelete || type == WalRecordType::CatalogUpdate) {
                plan.redo.push_back(make_recovery_record(record_view));
                if (auto owner = owner_identifier(record_view); owner) {
                    record_transaction_progress(*owner, record_view.header);
                }
                continue;
            }

            switch (type) {
            case WalRecordType::Commit: {
                auto payload = std::span<const std::byte>(record_view.payload.data(), record_view.payload.size());
                auto commit = decode_wal_commit(payload);
                if (!commit) {
                    return std::make_error_code(std::errc::invalid_argument);
                }

                if (commit->transaction_id != 0U) {
                    auto& recovered = ensure_recovered_transaction(commit->transaction_id);
                    if (!recovered.has_first_lsn) {
                        recovered.transaction.first_lsn = record_view.header.lsn;
                        recovered.has_first_lsn = true;
                    }
                    recovered.transaction.last_lsn = record_view.header.lsn;
                    recovered.transaction.commit_lsn = commit->commit_lsn != 0U ? commit->commit_lsn : record_view.header.lsn;
                    recovered.transaction.state = WalRecoveredTransactionState::Committed;
                    recovered.transaction.commit_record = make_recovery_record(record_view);
                }

                auto safe_increment = [](std::uint64_t value) {
                    return value < std::numeric_limits<std::uint64_t>::max() ? value + 1U : value;
                };

                const auto next_txn_id = commit->next_transaction_id != 0U
                    ? commit->next_transaction_id
                    : safe_increment(commit->transaction_id);
                plan.next_transaction_id_high_water = std::max(plan.next_transaction_id_high_water, next_txn_id);

                if (commit->transaction_id != 0U) {
                    plan.next_transaction_id_high_water = std::max(plan.next_transaction_id_high_water, safe_increment(commit->transaction_id));
                }

                if (commit->oldest_active_transaction_id != 0U) {
                    if (plan.oldest_active_transaction_id == 0U || commit->oldest_active_transaction_id < plan.oldest_active_transaction_id) {
                        plan.oldest_active_transaction_id = commit->oldest_active_transaction_id;
                    }
                }
                if (commit->oldest_active_commit_lsn != 0U) {
                    if (plan.oldest_active_commit_lsn == 0U || commit->oldest_active_commit_lsn < plan.oldest_active_commit_lsn) {
                        plan.oldest_active_commit_lsn = commit->oldest_active_commit_lsn;
                    }
                }

                if (commit->transaction_id != 0U && commit->transaction_id <= std::numeric_limits<std::uint32_t>::max()) {
                    const auto owner_id = static_cast<std::uint32_t>(commit->transaction_id);
                    auto it = transactions.find(owner_id);
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
                }
                break;
            }
            case WalRecordType::Abort: {
                auto owner = owner_identifier(record_view);
                if (!owner) {
                    return std::make_error_code(std::errc::invalid_argument);
                }
                if (*owner != 0U) {
                    record_transaction_progress(*owner, record_view.header);
                    auto& recovered = ensure_recovered_transaction(*owner);
                    recovered.transaction.state = WalRecoveredTransactionState::Aborted;
                }

                if (*owner <= std::numeric_limits<std::uint32_t>::max()) {
                    const auto owner_id = static_cast<std::uint32_t>(*owner);
                    auto it = transactions.find(owner_id);
                    if (it != transactions.end()) {
                        append_undo_span(owner_id, it->second.records);
                        transactions.erase(it);
                    }
                }
                break;
            }
            case WalRecordType::Checkpoint: {
                plan.redo.push_back(make_recovery_record(record_view));
                auto payload = std::span<const std::byte>(record_view.payload.data(), record_view.payload.size());
                auto checkpoint = decode_wal_checkpoint(payload);
                if (!checkpoint) {
                    return std::make_error_code(std::errc::invalid_argument);
                }
                for (const auto& entry : checkpoint->active_transactions) {
                    if (entry.transaction_id == 0U) {
                        continue;
                    }
                    auto& recovered = ensure_recovered_transaction(entry.transaction_id);
                    if (!recovered.has_first_lsn) {
                        recovered.transaction.first_lsn = entry.last_lsn;
                        recovered.has_first_lsn = true;
                    }
                    if (entry.last_lsn != 0U && entry.last_lsn > recovered.transaction.last_lsn) {
                        recovered.transaction.last_lsn = entry.last_lsn;
                    }
                }
                break;
            }
            default: {
                auto owner = owner_identifier(record_view);
                if (!owner) {
                    return std::make_error_code(std::errc::invalid_argument);
                }
                if (*owner == 0U || *owner > std::numeric_limits<std::uint32_t>::max()) {
                    return std::make_error_code(std::errc::invalid_argument);
                }
                const auto owner_id = static_cast<std::uint32_t>(*owner);
                auto& txn = ensure_transaction(owner_id);
                txn.records.push_back(make_recovery_record(record_view));
                record_transaction_progress(*owner, record_view.header);
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
        if (!state->records.empty()) {
            record_transaction_progress(txn_id, state->records.back().header);
        }
    }

    std::vector<std::pair<std::uint64_t, RecoveredTransactionState*>> recovered;
    recovered.reserve(recovered_transactions.size());
    for (auto& [txn_id, state] : recovered_transactions) {
        if (txn_id == 0U) {
            continue;
        }
        recovered.emplace_back(txn_id, &state);
    }

    std::sort(recovered.begin(), recovered.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.second->sequence < rhs.second->sequence;
    });

    for (const auto& entry : recovered) {
        plan.transactions.push_back(entry.second->transaction);
    }

    return {};
}

}  // namespace bored::storage

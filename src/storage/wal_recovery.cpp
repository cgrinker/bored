#include "bored/storage/wal_recovery.hpp"

#include "bored/storage/wal_format.hpp"

#include <algorithm>
#include <unordered_map>

namespace bored::storage {

namespace {

struct TransactionState final {
    std::vector<WalRecoveryRecord> records{};
};

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
    plan.truncated_tail = false;
    plan.truncated_segment_id = 0U;
    plan.truncated_lsn = 0U;

    std::vector<WalSegmentView> segments;
    if (auto ec = reader_.enumerate_segments(segments); ec) {
        return ec;
    }

    std::unordered_map<std::uint32_t, TransactionState> transactions;

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
            const auto txn_id = record_view.header.page_id;

            switch (type) {
            case WalRecordType::Commit: {
                auto it = transactions.find(txn_id);
                if (it != transactions.end()) {
                    auto& prepared = it->second.records;
                    plan.redo.insert(plan.redo.end(), prepared.begin(), prepared.end());
                    transactions.erase(it);
                }
                break;
            }
            case WalRecordType::Abort: {
                auto it = transactions.find(txn_id);
                if (it != transactions.end()) {
                    auto& prepared = it->second.records;
                    for (auto rit = prepared.rbegin(); rit != prepared.rend(); ++rit) {
                        plan.undo.push_back(*rit);
                    }
                    transactions.erase(it);
                }
                break;
            }
            case WalRecordType::Checkpoint: {
                plan.redo.push_back(make_recovery_record(record_view));
                break;
            }
            default: {
                auto& txn = transactions[txn_id];  // Treat page_id as provisional transaction identifier.
                txn.records.push_back(make_recovery_record(record_view));
                break;
            }
            }
        }

        if (plan.truncated_tail) {
            break;
        }
    }

    for (auto& [txn_id, state] : transactions) {
        (void)txn_id;
        for (auto rit = state.records.rbegin(); rit != state.records.rend(); ++rit) {
            plan.undo.push_back(*rit);
        }
    }

    return {};
}

}  // namespace bored::storage

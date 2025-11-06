#pragma once

#include <cstdint>

namespace bored::txn {

struct TransactionTelemetrySnapshot final {
    std::uint64_t active_transactions = 0U;
    std::uint64_t committed_transactions = 0U;
    std::uint64_t aborted_transactions = 0U;
    std::uint64_t last_snapshot_xmin = 0U;
    std::uint64_t last_snapshot_xmax = 0U;
    std::uint64_t last_snapshot_age = 0U;
    std::uint64_t snapshot_isolation_active = 0U;
    std::uint64_t read_committed_active = 0U;
    std::uint64_t lock_conflicts = 0U;
    std::uint64_t snapshot_conflicts = 0U;
    std::uint64_t serialization_failures = 0U;
};

}  // namespace bored::txn

#pragma once

#include "bored/txn/transaction_types.hpp"

namespace bored::txn {

inline bool snapshots_equal(const Snapshot& lhs, const Snapshot& rhs) noexcept
{
    if (lhs.read_lsn != rhs.read_lsn || lhs.xmin != rhs.xmin || lhs.xmax != rhs.xmax) {
        return false;
    }
    return lhs.in_progress == rhs.in_progress;
}

}  // namespace bored::txn

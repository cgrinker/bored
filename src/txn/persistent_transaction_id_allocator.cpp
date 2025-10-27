#include "bored/txn/persistent_transaction_id_allocator.hpp"

#include <algorithm>

namespace bored::txn {

PersistentTransactionIdAllocator::PersistentTransactionIdAllocator(TransactionId start) noexcept
    : next_{start}
{
}

TransactionId PersistentTransactionIdAllocator::allocate()
{
    std::lock_guard guard{mutex_};
    return next_++;
}

TransactionId PersistentTransactionIdAllocator::peek_next() const noexcept
{
    std::lock_guard guard{mutex_};
    return next_;
}

void PersistentTransactionIdAllocator::advance_to(TransactionId high_water_mark)
{
    std::lock_guard guard{mutex_};
    if (high_water_mark >= next_) {
        next_ = high_water_mark + 1U;
    }
}

}  // namespace bored::txn

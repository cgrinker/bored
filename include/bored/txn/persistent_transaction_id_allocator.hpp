#pragma once

#include <mutex>

#include "bored/txn/transaction_types.hpp"

namespace bored::txn {

class PersistentTransactionIdAllocator final : public TransactionIdAllocator {
public:
    explicit PersistentTransactionIdAllocator(TransactionId start = 1U) noexcept;

    TransactionId allocate() override;
    TransactionId peek_next() const noexcept override;
    void advance_to(TransactionId high_water_mark) override;

private:
    mutable std::mutex mutex_{};
    TransactionId next_ = 1U;
};

}  // namespace bored::txn

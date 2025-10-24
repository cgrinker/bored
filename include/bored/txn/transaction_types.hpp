#pragma once

#include <cstdint>
#include <vector>

namespace bored::txn {

using TransactionId = std::uint64_t;

struct Snapshot final {
    TransactionId xmin = 0U;
    TransactionId xmax = 0U;
    std::vector<TransactionId> in_progress{};
};

class SnapshotManager {
public:
    virtual ~SnapshotManager() = default;
    virtual Snapshot capture_snapshot(TransactionId current_transaction_id) = 0;
};

class TransactionIdAllocator {
public:
    virtual ~TransactionIdAllocator() = default;
    virtual TransactionId allocate() = 0;
};

class TransactionIdAllocatorStub final : public TransactionIdAllocator {
public:
    explicit TransactionIdAllocatorStub(TransactionId start = 1U)
        : next_{start}
    {
    }

    TransactionId allocate() override
    {
        return next_++;
    }

private:
    TransactionId next_ = 1U;
};

class SnapshotManagerStub final : public SnapshotManager {
public:
    SnapshotManagerStub() = default;

    explicit SnapshotManagerStub(Snapshot snapshot)
        : snapshot_{std::move(snapshot)}
    {
    }

    void set_snapshot(Snapshot snapshot)
    {
        snapshot_ = std::move(snapshot);
    }

    Snapshot capture_snapshot(TransactionId) override
    {
        return snapshot_;
    }

private:
    Snapshot snapshot_{};
};

}  // namespace bored::txn

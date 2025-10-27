#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace bored::txn {

using TransactionId = std::uint64_t;
using CommitSequence = std::uint64_t;

struct TransactionOptions final {
    bool read_only = false;
    bool deferrable = false;
};

enum class TransactionState {
    Idle,
    Active,
    Preparing,
    FlushingWal,
    Durable,
    Publishing,
    AbortCleanup,
    Committed,
    Aborted
};

struct Snapshot final {
    CommitSequence read_lsn = 0U;
    TransactionId xmin = 0U;
    TransactionId xmax = 0U;
    std::vector<TransactionId> in_progress{};
};

class TransactionContext;

class SnapshotManager {
public:
    virtual ~SnapshotManager() = default;
    virtual Snapshot capture_snapshot(TransactionId current_transaction_id) = 0;
};

class TransactionIdAllocator {
public:
    virtual ~TransactionIdAllocator() = default;
    virtual TransactionId allocate() = 0;
    virtual TransactionId peek_next() const noexcept = 0;
    virtual void advance_to(TransactionId high_water_mark) = 0;
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

    TransactionId peek_next() const noexcept override
    {
        return next_;
    }

    void advance_to(TransactionId high_water_mark) override
    {
        if (high_water_mark >= next_) {
            next_ = high_water_mark + 1U;
        }
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

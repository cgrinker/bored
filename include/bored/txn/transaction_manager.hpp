#pragma once

#include "bored/txn/transaction_types.hpp"
#include "bored/txn/transaction_telemetry.hpp"

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace bored::txn {

class TransactionContext final {
public:
    TransactionContext() = default;

    TransactionId id() const noexcept;
    const Snapshot& snapshot() const noexcept;
    TransactionState state() const noexcept;
    const TransactionOptions& options() const noexcept;

    void on_commit(std::function<void()> callback);
    void on_abort(std::function<void()> callback);

    explicit operator bool() const noexcept;

private:
    struct State {
        TransactionId id = 0U;
        Snapshot snapshot{};
        TransactionOptions options{};
        TransactionState state = TransactionState::Idle;
        std::vector<std::function<void()>> commit_callbacks{};
        std::vector<std::function<void()>> abort_callbacks{};
    };

    explicit TransactionContext(std::shared_ptr<State> state);

    std::shared_ptr<State> state_{};

    friend class TransactionManager;
};

class TransactionManager final : public SnapshotManager {
public:
    explicit TransactionManager(TransactionIdAllocator& id_allocator);

    TransactionContext begin(const TransactionOptions& options = {});
    void commit(TransactionContext& ctx);
    void abort(TransactionContext& ctx);
    void refresh_snapshot(TransactionContext& ctx);

    Snapshot current_snapshot() const;
    Snapshot capture_snapshot(TransactionId current_transaction_id) override;
    TransactionTelemetrySnapshot telemetry_snapshot() const;
    TransactionId oldest_active_transaction() const;
    TransactionId next_transaction_id() const noexcept;
    std::size_t active_transaction_count() const;

    void advance_low_water_mark(TransactionId txn_id);

private:
    using StatePtr = std::shared_ptr<TransactionContext::State>;

    void record_state_locked(const StatePtr& state);
    void erase_state_locked(TransactionId id);
    void recompute_oldest_locked();
    std::size_t count_active_locked() const;
    Snapshot build_snapshot_locked(TransactionId self_id) const;

    TransactionIdAllocator* id_allocator_ = nullptr;

    struct Telemetry final {
        std::uint64_t committed_transactions = 0U;
        std::uint64_t aborted_transactions = 0U;
        std::uint64_t last_snapshot_xmin = 0U;
        std::uint64_t last_snapshot_xmax = 0U;
        std::uint64_t last_snapshot_age = 0U;
    };

    mutable std::mutex mutex_{};
    mutable std::map<TransactionId, std::weak_ptr<TransactionContext::State>> active_{};
    mutable TransactionId oldest_active_ = 0U;
    mutable TransactionId external_low_water_mark_ = 0U;
    mutable TransactionId last_allocated_id_ = 0U;
    mutable Telemetry telemetry_{};
};

}  // namespace bored::txn

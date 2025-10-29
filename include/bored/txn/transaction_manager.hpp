#pragma once

#include "bored/txn/commit_pipeline.hpp"
#include "bored/txn/transaction_telemetry.hpp"
#include "bored/txn/transaction_types.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <vector>

namespace bored::txn {

class TransactionContext final {
public:
    TransactionContext() = default;

    TransactionId id() const noexcept;
    const Snapshot& snapshot() const noexcept;
    TransactionState state() const noexcept;
    const TransactionOptions& options() const noexcept;
    std::error_code last_error() const noexcept;

    void on_commit(std::function<void()> callback);
    void on_abort(std::function<void()> callback);
    void register_undo(std::function<std::error_code()> callback);

    explicit operator bool() const noexcept;

private:
    struct State {
        TransactionId id = 0U;
        Snapshot snapshot{};
        TransactionOptions options{};
        TransactionState state = TransactionState::Idle;
        std::vector<std::function<void()>> commit_callbacks{};
        std::vector<std::function<void()>> abort_callbacks{};
        std::vector<std::function<std::error_code()>> undo_callbacks{};
        std::optional<CommitTicket> commit_ticket{};
        std::error_code last_error{};
    };

    explicit TransactionContext(std::shared_ptr<State> state);

    std::shared_ptr<State> state_{};

    friend class TransactionManager;
};

class TransactionManager final : public SnapshotManager {
public:
    class CheckpointFence final {
    public:
        CheckpointFence() = default;
        CheckpointFence(TransactionManager* manager,
                        Snapshot snapshot,
                        TransactionId oldest_active,
                        TransactionId next_transaction_id) noexcept;
        CheckpointFence(const CheckpointFence&) = delete;
        CheckpointFence& operator=(const CheckpointFence&) = delete;
        CheckpointFence(CheckpointFence&& other) noexcept;
        CheckpointFence& operator=(CheckpointFence&& other) noexcept;
        ~CheckpointFence();

        [[nodiscard]] bool active() const noexcept;
        [[nodiscard]] const Snapshot& snapshot() const noexcept;
        [[nodiscard]] TransactionId oldest_active_transaction() const noexcept;
        [[nodiscard]] TransactionId next_transaction_id() const noexcept;

        void release() noexcept;

    private:
        void move_from(CheckpointFence&& other) noexcept;

        TransactionManager* manager_ = nullptr;
        Snapshot snapshot_{};
        TransactionId oldest_active_transaction_ = 0U;
        TransactionId next_transaction_id_ = 0U;
        bool active_ = false;
    };

    explicit TransactionManager(TransactionIdAllocator& id_allocator, CommitPipeline* pipeline = nullptr);

    void set_commit_pipeline(CommitPipeline* pipeline);
    CommitPipeline* commit_pipeline() const noexcept;

    CommitSequence durable_commit_lsn() const noexcept;

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
    CheckpointFence acquire_checkpoint_fence();

private:
    using StatePtr = std::shared_ptr<TransactionContext::State>;

    void record_state_locked(const StatePtr& state);
    void erase_state_locked(TransactionId id);
    void recompute_oldest_locked();
    std::size_t count_active_locked() const;
    Snapshot build_snapshot_locked(TransactionId self_id) const;
    void complete_abort(TransactionContext& ctx);
    [[noreturn]] void fail_commit(TransactionContext& ctx, const char* stage, std::error_code ec);
    void update_durable_commit_lsn(CommitSequence commit_lsn) noexcept;
    void release_checkpoint_fence() noexcept;

    TransactionIdAllocator* id_allocator_ = nullptr;
    std::atomic<CommitPipeline*> commit_pipeline_{nullptr};
    std::atomic<CommitSequence> durable_commit_lsn_{0U};

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
    bool checkpoint_guard_active_ = false;
    mutable std::condition_variable checkpoint_condition_{};
};

}  // namespace bored::txn

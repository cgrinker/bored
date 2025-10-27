#include "bored/txn/transaction_manager.hpp"

#include <algorithm>
#include <stdexcept>

namespace bored::txn {

TransactionContext::TransactionContext(std::shared_ptr<State> state)
    : state_{std::move(state)}
{
}

TransactionId TransactionContext::id() const noexcept
{
    return state_ ? state_->id : 0U;
}

const Snapshot& TransactionContext::snapshot() const noexcept
{
    static const Snapshot kEmptySnapshot{};
    return state_ ? state_->snapshot : kEmptySnapshot;
}

TransactionState TransactionContext::state() const noexcept
{
    return state_ ? state_->state : TransactionState::Idle;
}

const TransactionOptions& TransactionContext::options() const noexcept
{
    static const TransactionOptions kDefaultOptions{};
    return state_ ? state_->options : kDefaultOptions;
}

void TransactionContext::on_commit(std::function<void()> callback)
{
    if (!state_) {
        throw std::logic_error{"TransactionContext::on_commit requires active context"};
    }

    state_->commit_callbacks.emplace_back(std::move(callback));
}

void TransactionContext::on_abort(std::function<void()> callback)
{
    if (!state_) {
        throw std::logic_error{"TransactionContext::on_abort requires active context"};
    }

    state_->abort_callbacks.emplace_back(std::move(callback));
}

TransactionContext::operator bool() const noexcept
{
    return static_cast<bool>(state_);
}

TransactionManager::TransactionManager(TransactionIdAllocator& id_allocator,
                                       SnapshotManager& snapshot_manager)
    : id_allocator_{&id_allocator}
    , snapshot_manager_{&snapshot_manager}
{
}

TransactionContext TransactionManager::begin(const TransactionOptions& options)
{
    const auto txn_id = id_allocator_->allocate();
    auto snapshot = snapshot_manager_->capture_snapshot(txn_id);
    auto state = std::make_shared<TransactionContext::State>();
    state->id = txn_id;
    state->snapshot = std::move(snapshot);
    state->options = options;
    state->state = TransactionState::Active;

    {
        std::scoped_lock lock{mutex_};
        last_allocated_id_ = txn_id;
        record_state_locked(state);
    }

    return TransactionContext{std::move(state)};
}

void TransactionManager::commit(TransactionContext& ctx)
{
    auto state = ctx.state_;
    if (!state) {
        throw std::logic_error{"TransactionManager::commit requires active context"};
    }

    {
        std::scoped_lock lock{mutex_};
        if (state->state != TransactionState::Active && state->state != TransactionState::Preparing) {
            throw std::logic_error{"TransactionManager::commit requires active transaction"};
        }
        state->state = TransactionState::Preparing;
    }

    // WAL flush and durability hooks will slot between Preparing and Committed states.

    {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::Committed;
        erase_state_locked(state->id);
    }

    for (auto& callback : state->commit_callbacks) {
        if (callback) {
            callback();
        }
    }
    state->commit_callbacks.clear();
    state->abort_callbacks.clear();
}

void TransactionManager::abort(TransactionContext& ctx)
{
    auto state = ctx.state_;
    if (!state) {
        throw std::logic_error{"TransactionManager::abort requires active context"};
    }

    {
        std::scoped_lock lock{mutex_};
        if (state->state != TransactionState::Active && state->state != TransactionState::Preparing) {
            throw std::logic_error{"TransactionManager::abort requires active transaction"};
        }
        state->state = TransactionState::Aborted;
        erase_state_locked(state->id);
    }

    for (auto& callback : state->abort_callbacks) {
        if (callback) {
            callback();
        }
    }
    state->commit_callbacks.clear();
    state->abort_callbacks.clear();
}

Snapshot TransactionManager::current_snapshot() const
{
    TransactionId current_id = 0U;
    {
        std::scoped_lock lock{mutex_};
        current_id = last_allocated_id_;
    }

    return snapshot_manager_->capture_snapshot(current_id);
}

TransactionId TransactionManager::oldest_active_transaction() const
{
    std::scoped_lock lock{mutex_};
    return oldest_active_ != 0U ? oldest_active_ : external_low_water_mark_;
}

void TransactionManager::advance_low_water_mark(TransactionId txn_id)
{
    std::scoped_lock lock{mutex_};
    external_low_water_mark_ = std::max(external_low_water_mark_, txn_id);
    recompute_oldest_locked();
}

void TransactionManager::record_state_locked(const StatePtr& state)
{
    // Clean out expired entry if one already exists for this id.
    active_.insert_or_assign(state->id, state);
    if (oldest_active_ == 0U || state->id < oldest_active_) {
        oldest_active_ = state->id;
    }
}

void TransactionManager::erase_state_locked(TransactionId id)
{
    active_.erase(id);
    recompute_oldest_locked();
}

void TransactionManager::recompute_oldest_locked()
{
    TransactionId candidate = 0U;
    for (auto it = active_.begin(); it != active_.end();) {
        if (it->second.expired()) {
            it = active_.erase(it);
            continue;
        }
        if (candidate == 0U || it->first < candidate) {
            candidate = it->first;
        }
        ++it;
    }

    oldest_active_ = candidate;
    if (oldest_active_ == 0U && external_low_water_mark_ != 0U) {
        oldest_active_ = external_low_water_mark_;
    }
}

}  // namespace bored::txn

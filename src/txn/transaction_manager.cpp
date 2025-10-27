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

TransactionManager::TransactionManager(TransactionIdAllocator& id_allocator)
    : id_allocator_{&id_allocator}
{
}

TransactionContext TransactionManager::begin(const TransactionOptions& options)
{
    auto state = std::make_shared<TransactionContext::State>();
    state->options = options;

    {
        std::scoped_lock lock{mutex_};
        state->id = id_allocator_->allocate();
        state->state = TransactionState::Active;
        last_allocated_id_ = state->id;
        record_state_locked(state);
        state->snapshot = build_snapshot_locked(state->id);
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

void TransactionManager::refresh_snapshot(TransactionContext& ctx)
{
    auto state = ctx.state_;
    if (!state) {
        throw std::logic_error{"TransactionManager::refresh_snapshot requires active context"};
    }

    std::scoped_lock lock{mutex_};
    state->snapshot = build_snapshot_locked(state->id);
}

Snapshot TransactionManager::current_snapshot() const
{
    std::scoped_lock lock{mutex_};
    return build_snapshot_locked(0U);
}

TransactionId TransactionManager::oldest_active_transaction() const
{
    std::scoped_lock lock{mutex_};
    return oldest_active_ != 0U ? oldest_active_ : external_low_water_mark_;
}

TransactionId TransactionManager::next_transaction_id() const noexcept
{
    return id_allocator_ != nullptr ? id_allocator_->peek_next() : 0U;
}

std::size_t TransactionManager::active_transaction_count() const
{
    std::scoped_lock lock{mutex_};
    std::size_t count = 0U;
    for (auto it = active_.begin(); it != active_.end();) {
        if (it->second.expired()) {
            it = active_.erase(it);
            continue;
        }
        ++count;
        ++it;
    }
    return count;
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

    if (candidate == 0U || (external_low_water_mark_ != 0U && external_low_water_mark_ < candidate)) {
        candidate = external_low_water_mark_;
    }

    oldest_active_ = candidate;
}

Snapshot TransactionManager::build_snapshot_locked(TransactionId self_id) const
{
    Snapshot snapshot{};
    snapshot.read_lsn = 0U;  // Commit sequencing will populate this once integrated with WAL.
    snapshot.xmax = id_allocator_ != nullptr ? id_allocator_->peek_next() : 0U;

    std::vector<TransactionId> in_progress;
    in_progress.reserve(active_.size());

    TransactionId xmin_candidate = snapshot.xmax;

    for (auto it = active_.begin(); it != active_.end();) {
        auto shared = it->second.lock();
        if (!shared) {
            it = active_.erase(it);
            continue;
        }

        const auto active_id = it->first;
        if (active_id != self_id) {
            in_progress.push_back(active_id);
        }
        if (active_id < xmin_candidate) {
            xmin_candidate = active_id;
        }

        ++it;
    }

    if (self_id != 0U && (xmin_candidate == 0U || self_id < xmin_candidate)) {
        xmin_candidate = self_id;
    }

    if (xmin_candidate == 0U) {
        xmin_candidate = snapshot.xmax;
    }

    std::sort(in_progress.begin(), in_progress.end());

    snapshot.xmin = xmin_candidate;
    snapshot.in_progress = std::move(in_progress);
    return snapshot;
}

}  // namespace bored::txn

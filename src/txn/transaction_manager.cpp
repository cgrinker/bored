#include "bored/txn/transaction_manager.hpp"

#include <algorithm>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>

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

std::error_code TransactionContext::last_error() const noexcept
{
    return state_ ? state_->last_error : std::error_code{};
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

void TransactionContext::register_undo(std::function<std::error_code()> callback)
{
    if (!state_) {
        throw std::logic_error{"TransactionContext::register_undo requires active context"};
    }

    state_->undo_callbacks.emplace_back(std::move(callback));
}

TransactionContext::operator bool() const noexcept
{
    return static_cast<bool>(state_);
}

TransactionManager::TransactionManager(TransactionIdAllocator& id_allocator, CommitPipeline* pipeline)
    : id_allocator_{&id_allocator}
{
    commit_pipeline_.store(pipeline, std::memory_order_relaxed);
}

void TransactionManager::set_commit_pipeline(CommitPipeline* pipeline)
{
    commit_pipeline_.store(pipeline, std::memory_order_release);
}

CommitPipeline* TransactionManager::commit_pipeline() const noexcept
{
    return commit_pipeline_.load(std::memory_order_acquire);
}

CommitSequence TransactionManager::durable_commit_lsn() const noexcept
{
    return durable_commit_lsn_.load(std::memory_order_acquire);
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

    CommitPipeline* pipeline = commit_pipeline_.load(std::memory_order_acquire);
    CommitRequest request{};
    if (pipeline) {
        request.transaction_id = state->id;
        request.snapshot = state->snapshot;
    }

    {
        std::scoped_lock lock{mutex_};
        if (state->state != TransactionState::Active && state->state != TransactionState::Preparing) {
            throw std::logic_error{"TransactionManager::commit requires active transaction"};
        }
        state->state = TransactionState::Preparing;
        if (pipeline) {
            state->snapshot = build_snapshot_locked(state->id);
            request.snapshot = state->snapshot;
            request.next_transaction_id = id_allocator_ != nullptr ? id_allocator_->peek_next() : 0U;
            request.oldest_active_transaction_id = oldest_active_ != 0U ? oldest_active_ : external_low_water_mark_;
        }
    }

    CommitTicket ticket{};
    bool prepared_commit = false;

    if (pipeline) {
        auto ec = pipeline->prepare_commit(request, ticket);
        if (ec) {
            state->last_error = ec;
            state->commit_ticket.reset();
            fail_commit(ctx, "prepare", ec);
        }

        prepared_commit = true;
        state->commit_ticket = ticket;

        {
            std::scoped_lock lock{mutex_};
            state->state = TransactionState::FlushingWal;
        }

        ec = pipeline->flush_commit(ticket);
        if (ec) {
            pipeline->rollback_commit(ticket);
            state->last_error = ec;
            state->commit_ticket.reset();
            fail_commit(ctx, "flush", ec);
        }

        {
            std::scoped_lock lock{mutex_};
            state->state = TransactionState::Durable;
        }
    } else {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::Durable;
    }

    {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::Publishing;
        erase_state_locked(state->id);
        ++telemetry_.committed_transactions;
        state->commit_ticket.reset();
    }

    if (pipeline && prepared_commit) {
        pipeline->confirm_commit(ticket);
    }

    update_durable_commit_lsn(ticket.commit_sequence);

    {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::Committed;
        state->last_error = {};
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

    CommitPipeline* pipeline = commit_pipeline();
    std::optional<CommitTicket> ticket{};

    {
        std::scoped_lock lock{mutex_};
        switch (state->state) {
        case TransactionState::Active:
        case TransactionState::Preparing:
            break;
        case TransactionState::FlushingWal:
            if (state->commit_ticket.has_value()) {
                ticket = state->commit_ticket;
            }
            break;
        default:
            throw std::logic_error{"TransactionManager::abort requires active transaction"};
        }
    }

    if (ticket && pipeline) {
        pipeline->rollback_commit(*ticket);
    }

    state->last_error = std::make_error_code(std::errc::operation_canceled);
    complete_abort(ctx);
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

Snapshot TransactionManager::capture_snapshot(TransactionId current_transaction_id)
{
    std::scoped_lock lock{mutex_};
    return build_snapshot_locked(current_transaction_id);
}

TransactionTelemetrySnapshot TransactionManager::telemetry_snapshot() const
{
    std::scoped_lock lock{mutex_};
    TransactionTelemetrySnapshot snapshot{};
    snapshot.active_transactions = count_active_locked();
    snapshot.committed_transactions = telemetry_.committed_transactions;
    snapshot.aborted_transactions = telemetry_.aborted_transactions;
    snapshot.last_snapshot_xmin = telemetry_.last_snapshot_xmin;
    snapshot.last_snapshot_xmax = telemetry_.last_snapshot_xmax;
    snapshot.last_snapshot_age = telemetry_.last_snapshot_age;
    return snapshot;
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
    return count_active_locked();
}

void TransactionManager::advance_low_water_mark(TransactionId txn_id)
{
    std::scoped_lock lock{mutex_};
    external_low_water_mark_ = std::max(external_low_water_mark_, txn_id);
    recompute_oldest_locked();
}

void TransactionManager::complete_abort(TransactionContext& ctx)
{
    auto state = ctx.state_;
    if (!state) {
        return;
    }

    std::error_code undo_error{};
    for (auto it = state->undo_callbacks.rbegin(); it != state->undo_callbacks.rend(); ++it) {
        if (*it) {
            try {
                if (auto ec = (*it)()) {
                    if (!undo_error) {
                        undo_error = ec;
                    }
                }
            } catch (...) {
                if (!undo_error) {
                    undo_error = std::make_error_code(std::errc::io_error);
                }
            }
        }
    }
    state->undo_callbacks.clear();

    {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::AbortCleanup;
        erase_state_locked(state->id);
        ++telemetry_.aborted_transactions;
        state->commit_ticket.reset();
    }

    for (auto& callback : state->abort_callbacks) {
        if (callback) {
            callback();
        }
    }

    state->commit_callbacks.clear();
    state->abort_callbacks.clear();

    if (undo_error) {
        state->last_error = undo_error;
    }

    {
        std::scoped_lock lock{mutex_};
        state->state = TransactionState::Aborted;
    }
}

[[noreturn]] void TransactionManager::fail_commit(TransactionContext& ctx, const char* stage, std::error_code ec)
{
    if (!ec) {
        ec = std::make_error_code(std::errc::io_error);
    }

    complete_abort(ctx);

    std::string message{"TransactionManager::commit "};
    message += stage;
    message += " failed";
    throw std::system_error{ec, message};
}

void TransactionManager::update_durable_commit_lsn(CommitSequence commit_lsn) noexcept
{
    if (commit_lsn == 0U) {
        return;
    }

    auto current = durable_commit_lsn_.load(std::memory_order_relaxed);
    while (commit_lsn > current
           && !durable_commit_lsn_.compare_exchange_weak(current,
                                                         commit_lsn,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed)) {
        // retry until stored value is up to date or already ahead
    }
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

std::size_t TransactionManager::count_active_locked() const
{
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

Snapshot TransactionManager::build_snapshot_locked(TransactionId self_id) const
{
    Snapshot snapshot{};
    snapshot.read_lsn = durable_commit_lsn_.load(std::memory_order_acquire);
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
    const auto age = snapshot.xmax >= snapshot.xmin ? snapshot.xmax - snapshot.xmin : 0U;
    telemetry_.last_snapshot_xmin = snapshot.xmin;
    telemetry_.last_snapshot_xmax = snapshot.xmax;
    telemetry_.last_snapshot_age = age;
    return snapshot;
}

}  // namespace bored::txn

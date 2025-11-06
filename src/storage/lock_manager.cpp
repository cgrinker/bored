#include "bored/storage/lock_manager.hpp"

#include "bored/txn/transaction_manager.hpp"

#include <algorithm>
#include <sstream>
#include <utility>

namespace bored::storage {

namespace {

[[nodiscard]] std::error_code make_busy_error()
{
    return std::make_error_code(std::errc::resource_unavailable_try_again);
}

}  // namespace

LockManager::LockManager()
    : LockManager(Config{})
{
}

LockManager::LockManager(Config config)
    : config_{config}
{
}

std::error_code LockManager::acquire(std::uint32_t page_id, PageLatchMode mode)
{
    const auto self = std::this_thread::get_id();
    std::scoped_lock lock{mutex_};
    auto& state = pages_[page_id];
    auto holder_it = state.holders.find(self);
    if (holder_it == state.holders.end()) {
        holder_it = state.holders.emplace(self, HolderState{}).first;
    }

    switch (mode) {
    case PageLatchMode::Shared:
        return acquire_shared(page_id, state, holder_it->second);
    case PageLatchMode::Exclusive:
        return acquire_exclusive(page_id, state, holder_it->second);
    default:
        return std::make_error_code(std::errc::invalid_argument);
    }
}

std::error_code LockManager::acquire(std::uint32_t page_id, PageLatchMode mode, txn::TransactionContext* txn)
{
    auto ec = acquire(page_id, mode);
    if (ec == make_busy_error() && txn != nullptr) {
        txn->record_conflict(txn::TransactionConflictKind::Lock);
    }
    if (ec || txn == nullptr) {
        return ec;
    }

    auto state = txn->state();
    if (state != txn::TransactionState::Active && state != txn::TransactionState::Preparing) {
        release(page_id, mode);
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    try {
        txn->on_abort([this, page_id, mode]() {
            this->release(page_id, mode);
        });
    } catch (...) {
        release(page_id, mode);
        throw;
    }

    return {};
}

void LockManager::release(std::uint32_t page_id, PageLatchMode mode)
{
    const auto self = std::this_thread::get_id();
    std::scoped_lock lock{mutex_};
    auto state_it = pages_.find(page_id);
    if (state_it == pages_.end()) {
        return;
    }

    auto& state = state_it->second;
    auto holder_it = state.holders.find(self);
    if (holder_it == state.holders.end()) {
        return;
    }

    switch (mode) {
    case PageLatchMode::Shared:
        release_shared(page_id, state, holder_it->second);
        break;
    case PageLatchMode::Exclusive:
        release_exclusive(page_id, state, holder_it->second);
        break;
    default:
        return;
    }

    if (holder_it->second.shared == 0U && holder_it->second.exclusive == 0U) {
        state.holders.erase(holder_it);
    }
    cleanup_if_unused(page_id, state);
}

PageLatchCallbacks LockManager::page_latch_callbacks()
{
    PageLatchCallbacks callbacks{};
    callbacks.acquire = [this](std::uint32_t page_id, PageLatchMode mode) -> std::error_code {
        return acquire(page_id, mode);
    };
    callbacks.release = [this](std::uint32_t page_id, PageLatchMode mode) {
        release(page_id, mode);
    };
    return callbacks;
}

std::error_code LockManager::acquire_shared(std::uint32_t /*page_id*/, PageState& state, HolderState& holder)
{
    const auto self = std::this_thread::get_id();
    if (state.exclusive_depth > 0U && (!state.exclusive_owner || *state.exclusive_owner != self)) {
        return make_busy_error();
    }

    ++holder.shared;
    ++state.shared_total;
    return {};
}

std::error_code LockManager::acquire_exclusive(std::uint32_t /*page_id*/, PageState& state, HolderState& holder)
{
    const auto self = std::this_thread::get_id();

    if (state.exclusive_depth > 0U && (!state.exclusive_owner || *state.exclusive_owner != self)) {
        return make_busy_error();
    }

    const auto other_shared = (state.shared_total > holder.shared) ? (state.shared_total - holder.shared) : 0U;
    if (other_shared > 0U) {
        return make_busy_error();
    }

    if (!config_.enable_reentrancy && holder.exclusive > 0U) {
        return make_busy_error();
    }

    if (state.exclusive_depth == 0U) {
        state.exclusive_owner = self;
    }

    ++state.exclusive_depth;
    ++holder.exclusive;
    return {};
}

void LockManager::release_shared(std::uint32_t /*page_id*/, PageState& state, HolderState& holder)
{
    if (holder.shared == 0U) {
        return;
    }
    --holder.shared;
    if (state.shared_total > 0U) {
        --state.shared_total;
    }
}

void LockManager::release_exclusive(std::uint32_t /*page_id*/, PageState& state, HolderState& holder)
{
    if (holder.exclusive == 0U) {
        return;
    }

    --holder.exclusive;
    if (state.exclusive_depth > 0U) {
        --state.exclusive_depth;
    }

    if (state.exclusive_depth == 0U) {
        state.exclusive_owner.reset();
    }
}

void LockManager::cleanup_if_unused(std::uint32_t page_id, PageState& state)
{
    if (state.shared_total == 0U && state.exclusive_depth == 0U && state.holders.empty()) {
        pages_.erase(page_id);
    }
}

std::vector<LockManager::LockSnapshot> LockManager::snapshot() const
{
    std::scoped_lock lock{mutex_};
    std::vector<LockSnapshot> result;
    result.reserve(pages_.size());

    for (const auto& [page_id, state] : pages_) {
        LockSnapshot snapshot{};
        snapshot.page_id = page_id;
        snapshot.total_shared = state.shared_total;
        snapshot.exclusive_depth = state.exclusive_depth;
        if (state.exclusive_owner) {
            std::ostringstream stream;
            stream << *state.exclusive_owner;
            snapshot.exclusive_owner = stream.str();
        }

        snapshot.holders.reserve(state.holders.size());
        for (const auto& [thread_id, holder] : state.holders) {
            LockHolderSnapshot holder_snapshot{};
            std::ostringstream stream;
            stream << thread_id;
            holder_snapshot.thread_id = stream.str();
            holder_snapshot.shared = holder.shared;
            holder_snapshot.exclusive = holder.exclusive;
            snapshot.holders.push_back(std::move(holder_snapshot));
        }

        std::sort(snapshot.holders.begin(), snapshot.holders.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.thread_id < rhs.thread_id;
        });

        result.push_back(std::move(snapshot));
    }

    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.page_id < rhs.page_id;
    });

    return result;
}

}  // namespace bored::storage

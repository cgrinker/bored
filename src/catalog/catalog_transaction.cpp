#include "bored/catalog/catalog_transaction.hpp"

#include <stdexcept>

namespace bored::catalog {

CatalogTransaction::CatalogTransaction(const CatalogTransactionConfig& config)
    : id_allocator_{config.id_allocator}
    , snapshot_manager_{config.snapshot_manager}
{
    if (id_allocator_ == nullptr || snapshot_manager_ == nullptr) {
        throw std::invalid_argument{"CatalogTransaction requires allocator and snapshot manager"};
    }

    transaction_id_ = id_allocator_->allocate();
    snapshot_ = CatalogSnapshot{snapshot_manager_->capture_snapshot(transaction_id_)};
}

std::uint64_t CatalogTransaction::transaction_id() const noexcept
{
    return transaction_id_;
}

const CatalogSnapshot& CatalogTransaction::snapshot() const noexcept
{
    return snapshot_;
}

void CatalogTransaction::refresh_snapshot()
{
    snapshot_ = CatalogSnapshot{snapshot_manager_->capture_snapshot(transaction_id_)};
}

bool CatalogTransaction::is_visible(const CatalogTupleDescriptor& tuple) const noexcept
{
    return is_tuple_visible(tuple, snapshot_, transaction_id_);
}

bool CatalogTransaction::is_active() const noexcept
{
    return state_ == State::Active;
}

bool CatalogTransaction::is_committed() const noexcept
{
    return state_ == State::Committed;
}

bool CatalogTransaction::is_aborted() const noexcept
{
    return state_ == State::Aborted;
}

void CatalogTransaction::register_commit_hook(CommitHook hook)
{
    if (!hook) {
        throw std::invalid_argument{"CatalogTransaction::register_commit_hook requires valid hook"};
    }
    if (!can_register_hook()) {
        throw std::logic_error{"CatalogTransaction::register_commit_hook requires active transaction"};
    }
    commit_hooks_.push_back(std::move(hook));
}

void CatalogTransaction::register_abort_hook(AbortHook hook)
{
    if (!hook) {
        throw std::invalid_argument{"CatalogTransaction::register_abort_hook requires valid hook"};
    }
    if (!can_register_hook()) {
        throw std::logic_error{"CatalogTransaction::register_abort_hook requires active transaction"};
    }
    abort_hooks_.push_back(std::move(hook));
}

std::error_code CatalogTransaction::commit()
{
    if (state_ != State::Active) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    for (auto& hook : commit_hooks_) {
        if (auto ec = hook()) {
            run_abort_hooks();
            state_ = State::Aborted;
            commit_hooks_.clear();
            abort_hooks_.clear();
            return ec;
        }
    }

    state_ = State::Committed;
    commit_hooks_.clear();
    abort_hooks_.clear();
    return {};
}

std::error_code CatalogTransaction::abort()
{
    if (state_ == State::Committed) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    if (state_ == State::Aborted) {
        return {};
    }

    run_abort_hooks();
    state_ = State::Aborted;
    commit_hooks_.clear();
    abort_hooks_.clear();
    return {};
}

void CatalogTransaction::run_abort_hooks() noexcept
{
    for (auto it = abort_hooks_.rbegin(); it != abort_hooks_.rend(); ++it) {
        if (*it) {
            try {
                (*it)();
            } catch (...) {
            }
        }
    }
}

bool CatalogTransaction::can_register_hook() const noexcept
{
    return state_ == State::Active;
}

}  // namespace bored::catalog

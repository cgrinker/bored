#include "bored/catalog/catalog_transaction.hpp"

#include "bored/txn/transaction_manager.hpp"

#include <stdexcept>
#include <system_error>

namespace bored::catalog {

CatalogTransaction::CatalogTransaction(const CatalogTransactionConfig& config)
    : id_allocator_{config.id_allocator}
    , snapshot_manager_{config.snapshot_manager}
    , txn_manager_{config.transaction_manager}
    , txn_context_{config.transaction_context}
{
    if (txn_context_ != nullptr) {
        if (txn_manager_ != nullptr) {
            txn_manager_->refresh_snapshot(*txn_context_);
        }
        transaction_id_ = txn_context_->id();
        snapshot_ = CatalogSnapshot{txn_context_->snapshot()};
    } else {
        if (id_allocator_ == nullptr || snapshot_manager_ == nullptr) {
            throw std::invalid_argument{"CatalogTransaction requires allocator and snapshot manager"};
        }

        transaction_id_ = id_allocator_->allocate();
        snapshot_ = CatalogSnapshot{snapshot_manager_->capture_snapshot(transaction_id_)};
    }

    if (txn_context_ != nullptr) {
        txn_context_->register_undo([this]() -> std::error_code {
            this->perform_abort_cleanup();
            state_ = State::Aborted;
            return {};
        });

        txn_context_->on_abort([this]() {
            this->perform_abort_cleanup();
            state_ = State::Aborted;
        });

        context_hooks_registered_ = true;
    }
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
    if (txn_context_ != nullptr) {
        if (txn_manager_ != nullptr) {
            txn_manager_->refresh_snapshot(*txn_context_);
        }
        snapshot_ = CatalogSnapshot{txn_context_->snapshot()};
        return;
    }

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

txn::TransactionContext* CatalogTransaction::transaction_context() const noexcept
{
    return txn_context_;
}

void CatalogTransaction::bind_transaction_context(txn::TransactionManager* manager,
                                                   txn::TransactionContext* context)
{
    if (context == nullptr) {
        return;
    }

    if (txn_context_ != nullptr) {
        if (txn_context_ != context) {
            throw std::logic_error{"CatalogTransaction already bound to a different TransactionContext"};
        }

        if (manager != nullptr && txn_manager_ != manager) {
            txn_manager_ = manager;
            txn_manager_->refresh_snapshot(*txn_context_);
        }

        snapshot_ = CatalogSnapshot{txn_context_->snapshot()};
        return;
    }

    txn_context_ = context;
    txn_manager_ = manager;
    transaction_id_ = txn_context_->id();

    if (txn_manager_ != nullptr) {
        txn_manager_->refresh_snapshot(*txn_context_);
    }

    snapshot_ = CatalogSnapshot{txn_context_->snapshot()};

    if (!context_hooks_registered_) {
        txn_context_->register_undo([this]() -> std::error_code {
            this->perform_abort_cleanup();
            state_ = State::Aborted;
            return {};
        });

        txn_context_->on_abort([this]() {
            this->perform_abort_cleanup();
            state_ = State::Aborted;
        });

        context_hooks_registered_ = true;
    }
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
    abort_cleaned_ = true;
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

    perform_abort_cleanup();
    state_ = State::Aborted;
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

void CatalogTransaction::perform_abort_cleanup() noexcept
{
    if (abort_cleaned_) {
        return;
    }

    run_abort_hooks();
    commit_hooks_.clear();
    abort_hooks_.clear();
    abort_cleaned_ = true;
}

}  // namespace bored::catalog

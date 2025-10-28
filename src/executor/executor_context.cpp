#include "bored/executor/executor_context.hpp"

#include <utility>

namespace bored::executor {

ExecutorContext::ExecutorContext()
{
    config_.scratch = std::pmr::get_default_resource();
}

ExecutorContext::ExecutorContext(ExecutorContextConfig config)
    : config_{std::move(config)}
{
    if (config_.scratch == nullptr) {
        config_.scratch = std::pmr::get_default_resource();
    }
}

const bored::catalog::CatalogAccessor* ExecutorContext::catalog() const noexcept
{
    return config_.catalog;
}

const bored::planner::StatisticsCatalog* ExecutorContext::statistics() const noexcept
{
    return config_.statistics;
}

txn::TransactionId ExecutorContext::transaction_id() const noexcept
{
    return config_.transaction_id;
}

const txn::Snapshot& ExecutorContext::snapshot() const noexcept
{
    return config_.snapshot;
}

std::pmr::memory_resource* ExecutorContext::scratch_resource() const noexcept
{
    return config_.scratch;
}

txn::TransactionContext* ExecutorContext::transaction_context() const noexcept
{
    return config_.transaction;
}

void ExecutorContext::set_transaction_id(txn::TransactionId transaction_id) noexcept
{
    config_.transaction_id = transaction_id;
}

void ExecutorContext::set_snapshot(txn::Snapshot snapshot) noexcept
{
    config_.snapshot = std::move(snapshot);
}

void ExecutorContext::set_transaction_context(txn::TransactionContext* transaction) noexcept
{
    config_.transaction = transaction;
}

}  // namespace bored::executor

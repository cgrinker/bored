#pragma once

#include "bored/txn/transaction_types.hpp"

#include <memory_resource>

namespace bored::catalog {
class CatalogAccessor;
}

namespace bored::planner {
class StatisticsCatalog;
}

namespace bored::executor {

struct ExecutorContextConfig final {
    const bored::catalog::CatalogAccessor* catalog = nullptr;
    const bored::planner::StatisticsCatalog* statistics = nullptr;
    txn::TransactionId transaction_id = 0U;
    txn::Snapshot snapshot{};
    std::pmr::memory_resource* scratch = nullptr;
};

class ExecutorContext final {
public:
    ExecutorContext();
    explicit ExecutorContext(ExecutorContextConfig config);

    [[nodiscard]] const bored::catalog::CatalogAccessor* catalog() const noexcept;
    [[nodiscard]] const bored::planner::StatisticsCatalog* statistics() const noexcept;
    [[nodiscard]] txn::TransactionId transaction_id() const noexcept;
    [[nodiscard]] const txn::Snapshot& snapshot() const noexcept;
    [[nodiscard]] std::pmr::memory_resource* scratch_resource() const noexcept;

    void set_transaction_id(txn::TransactionId transaction_id) noexcept;
    void set_snapshot(txn::Snapshot snapshot) noexcept;

private:
    ExecutorContextConfig config_{};
};

}  // namespace bored::executor

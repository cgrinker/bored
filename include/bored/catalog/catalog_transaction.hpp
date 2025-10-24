#pragma once

#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <vector>

namespace bored::catalog {

struct CatalogTransactionConfig final {
    txn::TransactionIdAllocator* id_allocator = nullptr;
    txn::SnapshotManager* snapshot_manager = nullptr;
};

class CatalogTransaction final {
public:
    using CommitHook = std::function<std::error_code()>;
    using AbortHook = std::function<void()>;

    explicit CatalogTransaction(const CatalogTransactionConfig& config);

    CatalogTransaction(const CatalogTransaction&) = delete;
    CatalogTransaction& operator=(const CatalogTransaction&) = delete;
    CatalogTransaction(CatalogTransaction&&) = delete;
    CatalogTransaction& operator=(CatalogTransaction&&) = delete;

    [[nodiscard]] std::uint64_t transaction_id() const noexcept;
    [[nodiscard]] const CatalogSnapshot& snapshot() const noexcept;
    [[nodiscard]] bool is_active() const noexcept;
    [[nodiscard]] bool is_committed() const noexcept;
    [[nodiscard]] bool is_aborted() const noexcept;

    void refresh_snapshot();
    [[nodiscard]] bool is_visible(const CatalogTupleDescriptor& tuple) const noexcept;

    void register_commit_hook(CommitHook hook);
    void register_abort_hook(AbortHook hook);
    std::error_code commit();
    std::error_code abort();

private:
    enum class State {
        Active,
        Committed,
        Aborted
    };

    void run_abort_hooks() noexcept;
    [[nodiscard]] bool can_register_hook() const noexcept;

    txn::TransactionIdAllocator* id_allocator_ = nullptr;
    txn::SnapshotManager* snapshot_manager_ = nullptr;
    std::uint64_t transaction_id_ = 0U;
    CatalogSnapshot snapshot_{};
    std::vector<CommitHook> commit_hooks_{};
    std::vector<AbortHook> abort_hooks_{};
    State state_ = State::Active;
};

}  // namespace bored::catalog

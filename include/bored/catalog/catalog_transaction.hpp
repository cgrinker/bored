#pragma once

#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstdint>
#include <memory>

namespace bored::catalog {

struct CatalogTransactionConfig final {
    txn::TransactionIdAllocator* id_allocator = nullptr;
    txn::SnapshotManager* snapshot_manager = nullptr;
};

class CatalogTransaction final {
public:
    explicit CatalogTransaction(const CatalogTransactionConfig& config);

    CatalogTransaction(const CatalogTransaction&) = delete;
    CatalogTransaction& operator=(const CatalogTransaction&) = delete;
    CatalogTransaction(CatalogTransaction&&) = delete;
    CatalogTransaction& operator=(CatalogTransaction&&) = delete;

    [[nodiscard]] std::uint64_t transaction_id() const noexcept;
    [[nodiscard]] const CatalogSnapshot& snapshot() const noexcept;

    void refresh_snapshot();
    [[nodiscard]] bool is_visible(const CatalogTupleDescriptor& tuple) const noexcept;

private:
    txn::TransactionIdAllocator* id_allocator_ = nullptr;
    txn::SnapshotManager* snapshot_manager_ = nullptr;
    std::uint64_t transaction_id_ = 0U;
    CatalogSnapshot snapshot_{};
};

}  // namespace bored::catalog

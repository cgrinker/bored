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

}  // namespace bored::catalog

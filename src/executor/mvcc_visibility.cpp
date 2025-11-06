#include "bored/executor/mvcc_visibility.hpp"

#include <algorithm>

namespace bored::executor {
namespace {

[[nodiscard]] bool is_in_progress(const txn::Snapshot& snapshot, txn::TransactionId transaction_id) noexcept
{
    if (transaction_id == 0U || snapshot.in_progress.empty()) {
        return false;
    }
    return std::find(snapshot.in_progress.begin(), snapshot.in_progress.end(), transaction_id) != snapshot.in_progress.end();
}

[[nodiscard]] bool is_visible_insert(const txn::Snapshot& snapshot,
                                     txn::TransactionId inserting_transaction,
                                     txn::TransactionId reader_transaction) noexcept
{
    if (inserting_transaction == 0U) {
        return false;
    }
    if (inserting_transaction == reader_transaction) {
        return true;
    }
    if (inserting_transaction < snapshot.xmin) {
        return true;
    }
    if (inserting_transaction >= snapshot.xmax) {
        return false;
    }
    return !is_in_progress(snapshot, inserting_transaction);
}

[[nodiscard]] bool is_visible_delete(const txn::Snapshot& snapshot,
                                     txn::TransactionId deleting_transaction,
                                     txn::TransactionId reader_transaction) noexcept
{
    if (deleting_transaction == 0U) {
        return false;
    }
    if (deleting_transaction == reader_transaction) {
        return true;
    }
    if (deleting_transaction < snapshot.xmin) {
        return true;
    }
    if (deleting_transaction >= snapshot.xmax) {
        return false;
    }
    return !is_in_progress(snapshot, deleting_transaction);
}

}  // namespace

bool is_tuple_visible(const storage::TupleHeader& header,
                      txn::TransactionId reader_transaction_id,
                      const txn::Snapshot& snapshot) noexcept
{
    if (!is_visible_insert(snapshot, header.created_transaction_id, reader_transaction_id)) {
        return false;
    }

    if (header.deleted_transaction_id == 0U) {
        return true;
    }

    return !is_visible_delete(snapshot, header.deleted_transaction_id, reader_transaction_id);
}

}  // namespace bored::executor

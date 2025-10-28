#pragma once

#include "bored/storage/page_operations.hpp"
#include "bored/txn/transaction_types.hpp"

namespace bored::executor {

[[nodiscard]] bool is_tuple_visible(const storage::TupleHeader& header,
                                    txn::TransactionId reader_transaction_id,
                                    const txn::Snapshot& snapshot) noexcept;

}  // namespace bored::executor

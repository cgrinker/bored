#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/txn/transaction_types.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::txn {
class TransactionContext;
}

namespace bored::storage {

class KeyRangeLockManager final {
public:
    KeyRangeLockManager() = default;

    [[nodiscard]] std::error_code acquire(catalog::IndexId index_id,
                                          std::span<const std::byte> key,
                                          txn::TransactionContext* txn);

private:
    class Handle;
    friend class Handle;

    struct LockState final {
        txn::TransactionId owner = 0U;
        std::uint32_t depth = 0U;
    };

    [[nodiscard]] std::string make_storage_key(catalog::IndexId index_id, std::span<const std::byte> key) const;
    void release(catalog::IndexId index_id, std::span<const std::byte> key, txn::TransactionId owner) noexcept;

    mutable std::mutex mutex_{};
    std::unordered_map<std::string, LockState> locks_{};
};

}  // namespace bored::storage

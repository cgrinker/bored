#include "bored/storage/key_range_lock_manager.hpp"

#include "bored/txn/transaction_manager.hpp"

#include <cstring>
#include <memory>
#include <system_error>

namespace bored::storage {

class KeyRangeLockManager::Handle final {
public:
    Handle(KeyRangeLockManager* manager,
           catalog::IndexId index_id,
           std::vector<std::byte> key,
           txn::TransactionId owner) noexcept
        : manager_{manager}
        , index_id_{index_id}
        , key_{std::move(key)}
        , owner_{owner}
    {
    }

    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;

    ~Handle()
    {
        release();
    }

    void release() noexcept
    {
        if (!manager_ || !held_) {
            return;
        }
        manager_->release(index_id_, key_span(), owner_);
        held_ = false;
    }

private:
    [[nodiscard]] std::span<const std::byte> key_span() const noexcept
    {
        return std::span<const std::byte>(key_.data(), key_.size());
    }

    KeyRangeLockManager* manager_ = nullptr;
    catalog::IndexId index_id_{};
    std::vector<std::byte> key_{};
    txn::TransactionId owner_ = 0U;
    bool held_ = true;
};

std::error_code KeyRangeLockManager::acquire(catalog::IndexId index_id,
                                             std::span<const std::byte> key,
                                             txn::TransactionContext* txn)
{
    if (txn == nullptr) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    const auto owner = txn->id();
    if (owner == 0U) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    const auto storage_key = make_storage_key(index_id, key);

    {
        std::scoped_lock lock{mutex_};
        auto [it, inserted] = locks_.try_emplace(storage_key);
        auto& state = it->second;
        if (inserted || state.depth == 0U) {
            state.owner = owner;
            state.depth = 1U;
        } else if (state.owner == owner) {
            ++state.depth;
        } else {
            return std::make_error_code(std::errc::resource_unavailable_try_again);
        }
    }

    try {
    auto handle = std::make_shared<Handle>(this,
                           index_id,
                           std::vector<std::byte>(key.begin(), key.end()),
                           owner);
        txn->on_commit([handle]() noexcept {
            handle->release();
        });
        txn->on_abort([handle]() noexcept {
            handle->release();
        });
    } catch (...) {
        release(index_id, key, owner);
        throw;
    }

    return {};
}

std::string KeyRangeLockManager::make_storage_key(catalog::IndexId index_id,
                                                  std::span<const std::byte> key) const
{
    std::string storage_key;
    storage_key.resize(sizeof(index_id.value) + key.size());
    std::memcpy(storage_key.data(), &index_id.value, sizeof(index_id.value));
    if (!key.empty()) {
        std::memcpy(storage_key.data() + sizeof(index_id.value), key.data(), key.size());
    }
    return storage_key;
}

void KeyRangeLockManager::release(catalog::IndexId index_id,
                                  std::span<const std::byte> key,
                                  txn::TransactionId owner) noexcept
{
    const auto storage_key = make_storage_key(index_id, key);
    std::scoped_lock lock{mutex_};
    auto it = locks_.find(storage_key);
    if (it == locks_.end()) {
        return;
    }
    auto& state = it->second;
    if (state.owner != owner || state.depth == 0U) {
        return;
    }
    --state.depth;
    if (state.depth == 0U) {
        locks_.erase(it);
    }
}

}  // namespace bored::storage

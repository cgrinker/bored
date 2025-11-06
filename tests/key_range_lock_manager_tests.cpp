#include "bored/storage/key_range_lock_manager.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstring>
#include <system_error>
#include <span>
#include <string_view>
#include <vector>

using bored::catalog::IndexId;
using bored::storage::KeyRangeLockManager;
using bored::txn::TransactionContext;
using bored::txn::TransactionIdAllocatorStub;
using bored::txn::TransactionManager;

namespace {

std::vector<std::byte> make_key(std::string_view text)
{
    std::vector<std::byte> key(text.size());
    std::memcpy(key.data(), text.data(), text.size());
    return key;
}

}  // namespace

TEST_CASE("KeyRangeLockManager enforces exclusive ownership")
{
    TransactionIdAllocatorStub allocator{1000U};
    TransactionManager txn_manager{allocator};
    KeyRangeLockManager lock_manager{};

    auto key = make_key("alpha");

    auto txn1 = txn_manager.begin();
    REQUIRE_FALSE(lock_manager.acquire(IndexId{1U}, std::span<const std::byte>(key.data(), key.size()), &txn1));
    REQUIRE_FALSE(lock_manager.acquire(IndexId{1U}, std::span<const std::byte>(key.data(), key.size()), &txn1));

    auto txn2 = txn_manager.begin();
    auto ec = lock_manager.acquire(IndexId{1U}, std::span<const std::byte>(key.data(), key.size()), &txn2);
    CHECK(ec == std::make_error_code(std::errc::resource_unavailable_try_again));
    txn_manager.abort(txn2);

    txn_manager.commit(txn1);

    auto txn3 = txn_manager.begin();
    REQUIRE_FALSE(lock_manager.acquire(IndexId{1U}, std::span<const std::byte>(key.data(), key.size()), &txn3));
    txn_manager.abort(txn3);
}

TEST_CASE("KeyRangeLockManager releases locks on abort")
{
    TransactionIdAllocatorStub allocator{2000U};
    TransactionManager txn_manager{allocator};
    KeyRangeLockManager lock_manager{};

    auto key = make_key("beta");

    auto txn1 = txn_manager.begin();
    REQUIRE_FALSE(lock_manager.acquire(IndexId{2U}, std::span<const std::byte>(key.data(), key.size()), &txn1));

    txn_manager.abort(txn1);

    auto txn2 = txn_manager.begin();
    auto ec = lock_manager.acquire(IndexId{2U}, std::span<const std::byte>(key.data(), key.size()), &txn2);
    CHECK_FALSE(ec);
    txn_manager.abort(txn2);
}

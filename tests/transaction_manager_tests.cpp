#include <catch2/catch_test_macros.hpp>

#include "bored/txn/transaction_manager.hpp"

namespace {

bored::txn::Snapshot make_snapshot(std::uint64_t xmin, std::uint64_t xmax)
{
    bored::txn::Snapshot snapshot{};
    snapshot.read_lsn = xmax;
    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    return snapshot;
}

}  // namespace

TEST_CASE("TransactionManager assigns monotonically increasing transaction ids", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{41U};
    bored::txn::SnapshotManagerStub snapshot_manager{make_snapshot(10U, 100U)};
    bored::txn::TransactionManager manager{allocator, snapshot_manager};

    auto first = manager.begin();
    REQUIRE(first);
    CHECK(first.id() == 41U);
    CHECK(first.snapshot().xmin == 10U);
    CHECK(first.snapshot().xmax == 100U);
    CHECK(manager.oldest_active_transaction() == 41U);

    auto second = manager.begin();
    REQUIRE(second);
    CHECK(second.id() == 42U);
    CHECK(manager.oldest_active_transaction() == 41U);

    second.on_commit([&]() {
        CHECK(true);  // callback invoked
    });

    manager.commit(first);
    CHECK(first.state() == bored::txn::TransactionState::Committed);
    CHECK(manager.oldest_active_transaction() == 42U);

    manager.commit(second);
    CHECK(second.state() == bored::txn::TransactionState::Committed);
    CHECK(manager.oldest_active_transaction() == 0U);
}

TEST_CASE("TransactionManager abort triggers callbacks and clears active set", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{7U};
    bored::txn::SnapshotManagerStub snapshot_manager{make_snapshot(1U, 5U)};
    bored::txn::TransactionManager manager{allocator, snapshot_manager};

    auto ctx = manager.begin();
    bool abort_called = false;
    ctx.on_abort([&]() { abort_called = true; });

    manager.abort(ctx);

    CHECK(abort_called);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);
    CHECK(manager.oldest_active_transaction() == 0U);
}

TEST_CASE("TransactionManager low water mark advances once active set drains", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{80U};
    bored::txn::SnapshotManagerStub snapshot_manager{make_snapshot(2U, 10U)};
    bored::txn::TransactionManager manager{allocator, snapshot_manager};

    auto ctx = manager.begin();
    manager.commit(ctx);

    manager.advance_low_water_mark(70U);
    CHECK(manager.oldest_active_transaction() == 70U);

    auto snapshot = manager.current_snapshot();
    CHECK(snapshot.read_lsn == 10U);
}

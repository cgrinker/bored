#include <catch2/catch_test_macros.hpp>

#include "bored/txn/transaction_manager.hpp"

TEST_CASE("TransactionManager assigns monotonically increasing transaction ids", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{41U};
    bored::txn::TransactionManager manager{allocator};

    auto first = manager.begin();
    REQUIRE(first);
    CHECK(first.id() == 41U);
    CHECK(first.snapshot().xmin == 41U);
    CHECK(first.snapshot().xmax == 42U);
    CHECK(first.snapshot().in_progress.empty());
    CHECK(manager.oldest_active_transaction() == 41U);

    auto second = manager.begin();
    REQUIRE(second);
    CHECK(second.id() == 42U);
    CHECK(second.snapshot().xmin == 41U);
    CHECK(second.snapshot().xmax == 43U);
    REQUIRE(second.snapshot().in_progress.size() == 1U);
    CHECK(second.snapshot().in_progress.front() == 41U);
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
    bored::txn::TransactionManager manager{allocator};

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
    bored::txn::TransactionManager manager{allocator};

    auto ctx = manager.begin();
    manager.commit(ctx);

    manager.advance_low_water_mark(70U);
    CHECK(manager.oldest_active_transaction() == 70U);

    auto snapshot = manager.current_snapshot();
    CHECK(snapshot.xmin == snapshot.xmax);
    CHECK(snapshot.xmax == manager.next_transaction_id());
    CHECK(snapshot.in_progress.empty());
}

TEST_CASE("TransactionManager surfaces snapshot manager interface", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{100U};
    bored::txn::TransactionManager manager{allocator};

    auto first = manager.begin();
    auto second = manager.begin();

    bored::txn::SnapshotManager* snapshot_manager = &manager;
    auto snapshot = snapshot_manager->capture_snapshot(102U);

    CHECK(snapshot.xmax == manager.next_transaction_id());
    CHECK(snapshot.xmin == first.id());
    REQUIRE(snapshot.in_progress.size() == 2U);
    CHECK(snapshot.in_progress.front() == first.id());
    CHECK(snapshot.in_progress.back() == second.id());

    manager.commit(first);

    auto refreshed = snapshot_manager->capture_snapshot(102U);
    REQUIRE(refreshed.in_progress.size() == 1U);
    CHECK(refreshed.in_progress.front() == second.id());

    manager.commit(second);
}

TEST_CASE("TransactionManager telemetry tracks lifecycle", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{12U};
    bored::txn::TransactionManager manager{allocator};

    auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.active_transactions == 0U);
    CHECK(telemetry.committed_transactions == 0U);
    CHECK(telemetry.aborted_transactions == 0U);

    auto first = manager.begin();
    auto second = manager.begin();

    auto active = manager.telemetry_snapshot();
    CHECK(active.active_transactions == 2U);
    CHECK(active.committed_transactions == 0U);
    CHECK(active.aborted_transactions == 0U);
    CHECK(active.last_snapshot_xmin == first.id());
    CHECK(active.last_snapshot_xmax == manager.next_transaction_id());
    CHECK(active.last_snapshot_age == active.last_snapshot_xmax - active.last_snapshot_xmin);

    manager.commit(first);
    manager.abort(second);

    (void)manager.current_snapshot();
    auto after = manager.telemetry_snapshot();
    CHECK(after.active_transactions == 0U);
    CHECK(after.committed_transactions == 1U);
    CHECK(after.aborted_transactions == 1U);
    CHECK(after.last_snapshot_xmax >= after.last_snapshot_xmin);
}

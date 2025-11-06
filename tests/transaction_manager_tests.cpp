#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <future>
#include <string>
#include <system_error>
#include <vector>

#include "bored/txn/transaction_manager.hpp"

namespace {

class RecordingCommitPipeline final : public bored::txn::CommitPipeline {
public:
    std::vector<std::string> events;
    std::vector<bored::txn::CommitRequest> prepared_requests;
    bored::txn::CommitTicket confirmed_ticket{};

    std::error_code prepare_commit(const bored::txn::CommitRequest& request, bored::txn::CommitTicket& out_ticket) override
    {
        events.emplace_back("prepare");
        prepared_requests.push_back(request);
        out_ticket.transaction_id = request.transaction_id;
        out_ticket.commit_sequence = 101U;
        return {};
    }

    std::error_code flush_commit(const bored::txn::CommitTicket& ticket) override
    {
        events.emplace_back("flush");
        last_ticket_ = ticket;
        return {};
    }

    void confirm_commit(const bored::txn::CommitTicket& ticket) override
    {
        events.emplace_back("confirm");
        confirmed_ticket = ticket;
    }

    void rollback_commit(const bored::txn::CommitTicket& ticket) noexcept override
    {
        events.emplace_back("rollback");
        rolled_back_ticket_ = ticket;
    }

    const bored::txn::CommitTicket& last_ticket() const noexcept { return last_ticket_; }
    const bored::txn::CommitTicket& rolled_back_ticket() const noexcept { return rolled_back_ticket_; }

private:
    bored::txn::CommitTicket last_ticket_{};
    bored::txn::CommitTicket rolled_back_ticket_{};
};

class FailingFlushPipeline final : public bored::txn::CommitPipeline {
public:
    std::vector<std::string> events;

    std::error_code prepare_commit(const bored::txn::CommitRequest& request, bored::txn::CommitTicket& out_ticket) override
    {
        ++prepare_calls;
        events.emplace_back("prepare");
        out_ticket.transaction_id = request.transaction_id;
        out_ticket.commit_sequence = 202U;
        prepared_ticket_ = out_ticket;
        return {};
    }

    std::error_code flush_commit(const bored::txn::CommitTicket&) override
    {
        ++flush_calls;
        events.emplace_back("flush");
        return std::make_error_code(std::errc::io_error);
    }

    void confirm_commit(const bored::txn::CommitTicket&) override
    {
        ++confirm_calls;
        events.emplace_back("confirm");
    }

    void rollback_commit(const bored::txn::CommitTicket& ticket) noexcept override
    {
        ++rollback_calls;
        events.emplace_back("rollback");
        rolled_back_ticket_ = ticket;
    }

    int prepare_calls = 0;
    int flush_calls = 0;
    int confirm_calls = 0;
    int rollback_calls = 0;

    const bored::txn::CommitTicket& prepared_ticket() const noexcept { return prepared_ticket_; }
    const bored::txn::CommitTicket& rolled_back_ticket() const noexcept { return rolled_back_ticket_; }

private:
    bored::txn::CommitTicket prepared_ticket_{};
    bored::txn::CommitTicket rolled_back_ticket_{};
};

}  // namespace

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
    CHECK_FALSE(first.last_error());
    CHECK(manager.oldest_active_transaction() == 42U);

    manager.commit(second);
    CHECK(second.state() == bored::txn::TransactionState::Committed);
    CHECK_FALSE(second.last_error());
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
    CHECK(ctx.last_error() == std::make_error_code(std::errc::operation_canceled));
    CHECK(manager.oldest_active_transaction() == 0U);
}

TEST_CASE("TransactionManager abort executes undo callbacks", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{33U};
    bored::txn::TransactionManager manager{allocator};

    auto ctx = manager.begin();
    std::vector<int> order;
    ctx.register_undo([&]() {
        order.push_back(1);
        return std::error_code{};
    });
    ctx.register_undo([&]() {
        order.push_back(2);
        return std::error_code{};
    });

    manager.abort(ctx);

    const std::vector<int> expected{2, 1};
    CHECK(order == expected);
    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);
    CHECK(ctx.last_error() == std::make_error_code(std::errc::operation_canceled));
}

TEST_CASE("TransactionManager abort surfaces undo error", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{90U};
    bored::txn::TransactionManager manager{allocator};

    auto ctx = manager.begin();
    ctx.register_undo([]() {
        return std::make_error_code(std::errc::not_enough_memory);
    });

    manager.abort(ctx);

    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);
    CHECK(ctx.last_error() == std::make_error_code(std::errc::not_enough_memory));
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
    CHECK(telemetry.snapshot_isolation_active == 0U);
    CHECK(telemetry.read_committed_active == 0U);
    CHECK(telemetry.lock_conflicts == 0U);
    CHECK(telemetry.snapshot_conflicts == 0U);
    CHECK(telemetry.serialization_failures == 0U);

    auto first = manager.begin();
    auto second = manager.begin();

    auto active = manager.telemetry_snapshot();
    CHECK(active.active_transactions == 2U);
    CHECK(active.committed_transactions == 0U);
    CHECK(active.aborted_transactions == 0U);
    CHECK(active.last_snapshot_xmin == first.id());
    CHECK(active.last_snapshot_xmax == manager.next_transaction_id());
    CHECK(active.last_snapshot_age == active.last_snapshot_xmax - active.last_snapshot_xmin);
    CHECK(active.snapshot_isolation_active == 2U);
    CHECK(active.read_committed_active == 0U);

    manager.commit(first);
    manager.abort(second);

    (void)manager.current_snapshot();
    auto after = manager.telemetry_snapshot();
    CHECK(after.active_transactions == 0U);
    CHECK(after.committed_transactions == 1U);
    CHECK(after.aborted_transactions == 1U);
    CHECK(after.last_snapshot_xmax >= after.last_snapshot_xmin);
    CHECK(after.snapshot_isolation_active == 0U);
    CHECK(after.lock_conflicts == 0U);
}

TEST_CASE("TransactionManager tracks isolation distribution and conflicts", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{55U};
    bored::txn::TransactionManager manager{allocator};

    bored::txn::TransactionOptions read_committed{};
    read_committed.isolation_level = bored::txn::IsolationLevel::ReadCommitted;
    auto read_committed_ctx = manager.begin(read_committed);
    auto snapshot_ctx = manager.begin();

    snapshot_ctx.record_conflict(bored::txn::TransactionConflictKind::Lock);
    read_committed_ctx.record_conflict(bored::txn::TransactionConflictKind::Snapshot);

    auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.active_transactions == 2U);
    CHECK(telemetry.snapshot_isolation_active == 1U);
    CHECK(telemetry.read_committed_active == 1U);
    CHECK(telemetry.lock_conflicts == 1U);
    CHECK(telemetry.snapshot_conflicts == 1U);
    CHECK(snapshot_ctx.last_conflict().has_value());
    CHECK(*snapshot_ctx.last_conflict() == bored::txn::TransactionConflictKind::Lock);
    CHECK(read_committed_ctx.last_conflict().has_value());
    CHECK(*read_committed_ctx.last_conflict() == bored::txn::TransactionConflictKind::Snapshot);

    manager.abort(snapshot_ctx);
    manager.abort(read_committed_ctx);
}

TEST_CASE("TransactionManager commit drives commit pipeline", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{10U};
    RecordingCommitPipeline pipeline{};
    bored::txn::TransactionManager manager{allocator, &pipeline};

    auto ctx = manager.begin();
    manager.commit(ctx);

    CHECK(ctx.state() == bored::txn::TransactionState::Committed);
    CHECK_FALSE(ctx.last_error());

    const std::vector<std::string> expected{"prepare", "flush", "confirm"};
    CHECK(pipeline.events == expected);
    CHECK(pipeline.confirmed_ticket.transaction_id == ctx.id());
    CHECK(pipeline.confirmed_ticket.commit_sequence == 101U);

    CHECK(manager.durable_commit_lsn() == pipeline.confirmed_ticket.commit_sequence);

    auto snapshot_after = manager.current_snapshot();
    CHECK(snapshot_after.read_lsn == manager.durable_commit_lsn());

    auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.committed_transactions == 1U);
    CHECK(telemetry.aborted_transactions == 0U);
}

TEST_CASE("TransactionManager provides oldest snapshot read lsn to commit pipeline", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{300U};
    RecordingCommitPipeline pipeline{};
    bored::txn::TransactionManager manager{allocator, &pipeline};

    auto guard = manager.begin();
    auto first = manager.begin();

    manager.commit(first);
    REQUIRE_FALSE(pipeline.prepared_requests.empty());
    const auto first_request = pipeline.prepared_requests.back();
    CHECK(first_request.oldest_snapshot_read_lsn == guard.snapshot().read_lsn);

    auto second = manager.begin();
    manager.commit(second);
    REQUIRE(pipeline.prepared_requests.size() >= 2U);
    const auto second_request = pipeline.prepared_requests.back();
    CHECK(second_request.oldest_snapshot_read_lsn == guard.snapshot().read_lsn);
    CHECK(second_request.snapshot.read_lsn == manager.durable_commit_lsn());

    manager.commit(guard);
}

TEST_CASE("TransactionManager commit failure rolls back pipeline", "[txn]")
{
    bored::txn::TransactionIdAllocatorStub allocator{20U};
    FailingFlushPipeline pipeline{};
    bored::txn::TransactionManager manager{allocator, &pipeline};

    auto ctx = manager.begin();
    REQUIRE_THROWS_AS(manager.commit(ctx), std::system_error);

    CHECK(ctx.state() == bored::txn::TransactionState::Aborted);
    CHECK(ctx.last_error() == std::make_error_code(std::errc::io_error));

    CHECK(manager.durable_commit_lsn() == 0U);

    const std::vector<std::string> expected{"prepare", "flush", "rollback"};
    CHECK(pipeline.events == expected);
    CHECK(pipeline.rollback_calls == 1);
    CHECK(pipeline.confirm_calls == 0);
    CHECK(pipeline.rolled_back_ticket().transaction_id == ctx.id());

    auto telemetry = manager.telemetry_snapshot();
    CHECK(telemetry.committed_transactions == 0U);
    CHECK(telemetry.aborted_transactions == 1U);
}

TEST_CASE("TransactionManager checkpoint fence blocks new writers until release", "[txn]")
{
    using namespace std::chrono_literals;

    bored::txn::TransactionIdAllocatorStub allocator{60U};
    bored::txn::TransactionManager manager{allocator};

    auto active = manager.begin();
    CHECK(active.id() == 60U);

    auto fence = manager.acquire_checkpoint_fence();
    CHECK(fence.active());
    CHECK(fence.oldest_active_transaction() == active.id());
    CHECK(fence.next_transaction_id() == manager.next_transaction_id());

    std::promise<void> ready;
    auto ready_future = ready.get_future();
    auto begin_future = std::async(std::launch::async, [&manager, ready = std::move(ready)]() mutable {
        ready.set_value();
        auto ctx = manager.begin();
        const auto id = ctx.id();
        manager.commit(ctx);
        return id;
    });

    ready_future.wait();
    auto status = begin_future.wait_for(25ms);
    CHECK(status == std::future_status::timeout);

    fence.release();

    const auto resumed_status = begin_future.wait_for(250ms);
    REQUIRE(resumed_status == std::future_status::ready);
    const auto resumed_id = begin_future.get();
    CHECK(resumed_id == manager.next_transaction_id() - 1U);

    manager.commit(active);
}

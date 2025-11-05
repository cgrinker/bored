#include "bored/executor/executor_context.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

using bored::executor::ExecutorContext;
using bored::executor::ExecutorNode;
using bored::executor::ExecutorNodePtr;
using bored::executor::SpoolExecutor;
using bored::executor::TupleBuffer;
using bored::txn::Snapshot;

namespace {

std::vector<std::byte> encode_u32(std::uint32_t value)
{
    std::vector<std::byte> bytes(sizeof(value));
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
}

std::uint32_t decode_u32(const std::span<const std::byte>& bytes)
{
    std::uint32_t value = 0U;
    REQUIRE(bytes.size() == sizeof(value));
    std::memcpy(&value, bytes.data(), sizeof(value));
    return value;
}

class MockChildExecutor final : public ExecutorNode {
public:
    explicit MockChildExecutor(std::vector<std::vector<std::byte>> rows)
        : rows_{std::move(rows)}
    {
    }

    void open(ExecutorContext&) override
    {
        ++open_count_;
        index_ = 0U;
    }

    bool next(ExecutorContext&, TupleBuffer& buffer) override
    {
        if (index_ >= rows_.size()) {
            return false;
        }

        bored::executor::TupleWriter writer{buffer};
        writer.reset();
        writer.append_column(std::span<const std::byte>(rows_[index_].data(), rows_[index_].size()), false);
        writer.finalize();
        ++index_;
        ++produced_count_;
        return true;
    }

    void close(ExecutorContext&) override
    {
        ++close_count_;
    }

    void set_rows(std::vector<std::vector<std::byte>> rows)
    {
        rows_ = std::move(rows);
        index_ = 0U;
    }

    [[nodiscard]] std::size_t open_count() const noexcept { return open_count_; }
    [[nodiscard]] std::size_t close_count() const noexcept { return close_count_; }
    [[nodiscard]] std::size_t produced_count() const noexcept { return produced_count_; }

private:
    std::vector<std::vector<std::byte>> rows_{};
    std::size_t index_ = 0U;
    std::size_t open_count_ = 0U;
    std::size_t close_count_ = 0U;
    std::size_t produced_count_ = 0U;
};

}  // namespace

TEST_CASE("SpoolExecutor materialises child rows once")
{
    std::vector<std::vector<std::byte>> rows;
    rows.push_back(encode_u32(11U));
    rows.push_back(encode_u32(22U));
    rows.push_back(encode_u32(33U));

    auto child = std::make_unique<MockChildExecutor>(rows);
    auto* child_ptr = child.get();

    bored::executor::ExecutorTelemetry telemetry;
    SpoolExecutor::Config config{};
    config.telemetry = &telemetry;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    TupleBuffer buffer{};

    spool.open(context);

    std::vector<std::uint32_t> emitted;
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        REQUIRE(tuple_view.column_count() == 1U);
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        emitted.push_back(decode_u32(column.data));
        buffer.reset();
    }

    spool.close(context);

    REQUIRE(emitted == std::vector<std::uint32_t>{11U, 22U, 33U});
    CHECK(child_ptr->open_count() == 1U);
    CHECK(child_ptr->close_count() == 1U);
    CHECK(child_ptr->produced_count() == rows.size());

    const auto snapshot = telemetry.snapshot();
    CHECK(snapshot.spool_latency.invocations == rows.size() + 1U);
    CHECK(snapshot.spool_latency.total_duration_ns >= snapshot.spool_latency.last_duration_ns);

    // Re-open should replay cached rows without re-reading the child.
    emitted.clear();
    spool.open(context);
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        emitted.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(emitted == std::vector<std::uint32_t>{11U, 22U, 33U});
    CHECK(child_ptr->open_count() == 1U);
    CHECK(child_ptr->close_count() == 1U);
}

TEST_CASE("SpoolExecutor reset clears materialised data")
{
    std::vector<std::vector<std::byte>> initial_rows;
    initial_rows.push_back(encode_u32(5U));
    initial_rows.push_back(encode_u32(10U));

    auto child = std::make_unique<MockChildExecutor>(initial_rows);
    auto* child_ptr = child.get();

    SpoolExecutor spool{std::move(child), {}};

    ExecutorContext context{};
    TupleBuffer buffer{};

    spool.open(context);
    std::vector<std::uint32_t> observed;
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(observed == std::vector<std::uint32_t>{5U, 10U});
    CHECK(child_ptr->open_count() == 1U);
    CHECK(child_ptr->close_count() == 1U);

    // Update child rows and force re-materialisation.
    std::vector<std::vector<std::byte>> refreshed_rows;
    refreshed_rows.push_back(encode_u32(42U));
    refreshed_rows.push_back(encode_u32(84U));
    refreshed_rows.push_back(encode_u32(168U));
    child_ptr->set_rows(std::move(refreshed_rows));

    spool.reset();

    spool.open(context);
    observed.clear();
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(observed == std::vector<std::uint32_t>{42U, 84U, 168U});
    CHECK(child_ptr->open_count() == 2U);
    CHECK(child_ptr->close_count() == 2U);
    CHECK(child_ptr->produced_count() == 5U);
}

TEST_CASE("SpoolExecutor rematerialises when snapshot changes")
{
    std::vector<std::vector<std::byte>> initial_rows;
    initial_rows.push_back(encode_u32(7U));
    initial_rows.push_back(encode_u32(14U));

    auto child = std::make_unique<MockChildExecutor>(initial_rows);
    auto* child_ptr = child.get();

    SpoolExecutor spool{std::move(child), {}};

    ExecutorContext context{};
    TupleBuffer buffer{};

    Snapshot first_snapshot{};
    first_snapshot.read_lsn = 10U;
    first_snapshot.xmin = 1U;
    first_snapshot.xmax = 20U;
    context.set_snapshot(first_snapshot);

    spool.open(context);
    std::vector<std::uint32_t> observed;
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(observed == std::vector<std::uint32_t>{7U, 14U});
    CHECK(child_ptr->open_count() == 1U);
    CHECK(child_ptr->close_count() == 1U);

    std::vector<std::vector<std::byte>> refreshed_rows;
    refreshed_rows.push_back(encode_u32(21U));
    refreshed_rows.push_back(encode_u32(28U));
    child_ptr->set_rows(std::move(refreshed_rows));

    Snapshot second_snapshot = first_snapshot;
    second_snapshot.xmin = 2U;
    context.set_snapshot(second_snapshot);

    observed.clear();
    spool.open(context);
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(observed == std::vector<std::uint32_t>{21U, 28U});
    CHECK(child_ptr->open_count() == 2U);
    CHECK(child_ptr->close_count() == 2U);
}

TEST_CASE("SpoolExecutor uses worktable cache when snapshot matches")
{
    std::vector<std::vector<std::byte>> rows;
    rows.push_back(encode_u32(101U));
    rows.push_back(encode_u32(202U));

    bored::executor::WorkTableRegistry registry;

    auto child = std::make_unique<MockChildExecutor>(rows);
    auto* child_ptr = child.get();

    bored::executor::ExecutorTelemetry telemetry;
    SpoolExecutor::Config config{};
    config.telemetry = &telemetry;
    config.worktable_registry = &registry;
    config.worktable_id = 42U;
    config.enable_recursive_cursor = true;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    Snapshot snapshot{};
    snapshot.read_lsn = 99U;
    snapshot.xmin = 10U;
    snapshot.xmax = 200U;
    context.set_snapshot(snapshot);

    TupleBuffer buffer{};
    std::vector<std::uint32_t> observed;

    spool.open(context);
    while (spool.next(context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    REQUIRE(observed == std::vector<std::uint32_t>{101U, 202U});
    CHECK(child_ptr->open_count() == 1U);
    CHECK(child_ptr->close_count() == 1U);
    CHECK(child_ptr->produced_count() == rows.size());

    // Second spool should reuse the cached worktable and avoid touching its child.
    std::vector<std::vector<std::byte>> unused_rows;
    unused_rows.push_back(encode_u32(303U));
    auto second_child = std::make_unique<MockChildExecutor>(unused_rows);
    auto* second_child_ptr = second_child.get();

    SpoolExecutor::Config cached_config{};
    cached_config.telemetry = &telemetry;
    cached_config.worktable_registry = &registry;
    cached_config.worktable_id = 42U;
    cached_config.enable_recursive_cursor = true;

    SpoolExecutor cached_spool{std::move(second_child), cached_config};
    ExecutorContext cached_context{};
    cached_context.set_snapshot(snapshot);

    observed.clear();
    cached_spool.open(cached_context);
    while (cached_spool.next(cached_context, buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(buffer);
        REQUIRE(tuple_view.valid());
        const auto column = tuple_view.column(0U);
        REQUIRE_FALSE(column.is_null);
        observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    cached_spool.close(cached_context);

    REQUIRE(observed == std::vector<std::uint32_t>{101U, 202U});
    CHECK(second_child_ptr->open_count() == 0U);
    CHECK(second_child_ptr->produced_count() == 0U);
}

TEST_CASE("WorkTableRegistry snapshot iterator preserves cached rows")
{
    std::vector<std::vector<std::byte>> rows;
    rows.push_back(encode_u32(1U));
    rows.push_back(encode_u32(2U));
    rows.push_back(encode_u32(3U));

    bored::executor::WorkTableRegistry registry;

    auto child = std::make_unique<MockChildExecutor>(rows);

    SpoolExecutor::Config config{};
    config.worktable_registry = &registry;
    config.worktable_id = 0xABCDU;
    config.enable_recursive_cursor = true;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    Snapshot snapshot{};
    snapshot.read_lsn = 777U;
    snapshot.xmin = 12U;
    snapshot.xmax = 1024U;
    context.set_snapshot(snapshot);

    TupleBuffer buffer{};
    spool.open(context);
    while (spool.next(context, buffer)) {
        buffer.reset();
    }
    spool.close(context);

    auto iterator_opt = registry.snapshot_iterator(*config.worktable_id, snapshot);
    REQUIRE(iterator_opt.has_value());
    auto iterator = std::move(*iterator_opt);
    CHECK(iterator.valid());
    CHECK(iterator.matches(snapshot));
    CHECK(iterator.size() == rows.size());

    const TupleBuffer* tuple_ptr = nullptr;
    std::vector<std::uint32_t> decoded;
    while (iterator.next(tuple_ptr)) {
        REQUIRE(tuple_ptr != nullptr);
        auto view = bored::executor::TupleView::from_buffer(*tuple_ptr);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 1U);
        const auto column = view.column(0U);
        REQUIRE_FALSE(column.is_null);
        decoded.push_back(decode_u32(column.data));
    }
    REQUIRE(decoded == std::vector<std::uint32_t>{1U, 2U, 3U});

    iterator.reset();
    std::size_t replay_count = 0U;
    while (iterator.next(tuple_ptr)) {
        ++replay_count;
    }
    CHECK(replay_count == rows.size());

    Snapshot mismatched = snapshot;
    mismatched.read_lsn += 1U;
    CHECK_FALSE(iterator.matches(mismatched));
}

TEST_CASE("SpoolExecutor snapshot iterator exposes cached registry view")
{
    std::vector<std::vector<std::byte>> rows;
    rows.push_back(encode_u32(11U));
    rows.push_back(encode_u32(22U));

    bored::executor::WorkTableRegistry registry;

    auto child = std::make_unique<MockChildExecutor>(rows);

    SpoolExecutor::Config config{};
    config.worktable_registry = &registry;
    config.worktable_id = 0x4242U;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    Snapshot snapshot{};
    snapshot.read_lsn = 1234U;
    snapshot.xmin = 8U;
    snapshot.xmax = 2048U;
    context.set_snapshot(snapshot);

    TupleBuffer buffer{};
    spool.open(context);
    while (spool.next(context, buffer)) {
        buffer.reset();
    }
    spool.close(context);

    auto iterator_opt = spool.snapshot_iterator();
    REQUIRE(iterator_opt.has_value());
    auto iterator = std::move(*iterator_opt);
    CHECK(iterator.matches(snapshot));
    CHECK(iterator.size() == rows.size());

    const TupleBuffer* tuple_ptr = nullptr;
    std::vector<std::uint32_t> decoded;
    while (iterator.next(tuple_ptr)) {
        REQUIRE(tuple_ptr != nullptr);
        auto view = bored::executor::TupleView::from_buffer(*tuple_ptr);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 1U);
        const auto column = view.column(0U);
        REQUIRE_FALSE(column.is_null);
        decoded.push_back(decode_u32(column.data));
    }
    REQUIRE(decoded == std::vector<std::uint32_t>{11U, 22U});

    Snapshot mismatch = snapshot;
    mismatch.xmax += 1U;
    auto mismatch_iterator = spool.snapshot_iterator();
    REQUIRE(mismatch_iterator.has_value());
    CHECK_FALSE(mismatch_iterator->matches(mismatch));
}

TEST_CASE("WorkTableRegistry recursive cursor manages seeds and deltas")
{
    std::vector<std::vector<std::byte>> seed_rows;
    seed_rows.push_back(encode_u32(11U));
    seed_rows.push_back(encode_u32(22U));

    bored::executor::WorkTableRegistry registry;

    auto child = std::make_unique<MockChildExecutor>(seed_rows);

    SpoolExecutor::Config config{};
    config.worktable_registry = &registry;
    config.worktable_id = 0xFEEDU;
    config.enable_recursive_cursor = true;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    Snapshot snapshot{};
    snapshot.read_lsn = 4096U;
    snapshot.xmin = 5U;
    snapshot.xmax = 9001U;
    context.set_snapshot(snapshot);

    TupleBuffer buffer{};
    spool.open(context);
    while (spool.next(context, buffer)) {
        buffer.reset();
    }
    spool.close(context);

    auto cursor_opt = spool.recursive_cursor();
    REQUIRE(cursor_opt.has_value());
    auto cursor = std::move(*cursor_opt);
    CHECK(cursor.valid());
    CHECK(cursor.snapshot().read_lsn == snapshot.read_lsn);
    CHECK(cursor.seed_count() == seed_rows.size());

    const TupleBuffer* tuple_ptr = nullptr;
    std::vector<std::uint32_t> decoded_seeds;
    cursor.reset_seed();
    while (cursor.next_seed(tuple_ptr)) {
        REQUIRE(tuple_ptr != nullptr);
        auto view = bored::executor::TupleView::from_buffer(*tuple_ptr);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 1U);
        const auto column = view.column(0U);
        REQUIRE_FALSE(column.is_null);
        decoded_seeds.push_back(decode_u32(column.data));
    }
    REQUIRE(decoded_seeds == std::vector<std::uint32_t>{11U, 22U});

    const TupleBuffer* delta_ptr = nullptr;
    CHECK_FALSE(cursor.next_delta(delta_ptr));

    TupleBuffer delta_buffer{};
    bored::executor::TupleWriter delta_writer{delta_buffer};
    delta_writer.reset();
    auto delta_bytes = encode_u32(33U);
    delta_writer.append_column(std::span<const std::byte>(delta_bytes.data(), delta_bytes.size()), false);
    delta_writer.finalize();
    cursor.append_delta(std::move(delta_buffer));

    cursor.reset_delta();
    REQUIRE(cursor.delta_count() == 1U);
    REQUIRE(cursor.next_delta(delta_ptr));
    REQUIRE(delta_ptr != nullptr);
    auto delta_view = bored::executor::TupleView::from_buffer(*delta_ptr);
    REQUIRE(delta_view.valid());
    REQUIRE(delta_view.column_count() == 1U);
    auto delta_column = delta_view.column(0U);
    REQUIRE_FALSE(delta_column.is_null);
    CHECK(decode_u32(delta_column.data) == 33U);
    CHECK_FALSE(cursor.next_delta(delta_ptr));
    cursor.mark_delta_processed();
    CHECK(cursor.delta_count() == 0U);

    TupleBuffer second_delta{};
    bored::executor::TupleWriter second_writer{second_delta};
    second_writer.reset();
    auto second_bytes = encode_u32(44U);
    second_writer.append_column(std::span<const std::byte>(second_bytes.data(), second_bytes.size()), false);
    second_writer.finalize();
    cursor.append_delta(std::move(second_delta));

    CHECK(cursor.delta_count() == 1U);
    cursor.reset_delta();
    REQUIRE(cursor.next_delta(delta_ptr));
    REQUIRE(delta_ptr != nullptr);
    auto second_view = bored::executor::TupleView::from_buffer(*delta_ptr);
    REQUIRE(second_view.valid());
    REQUIRE(second_view.column_count() == 1U);
    auto second_column = second_view.column(0U);
    REQUIRE_FALSE(second_column.is_null);
    CHECK(decode_u32(second_column.data) == 44U);
    CHECK_FALSE(cursor.next_delta(delta_ptr));
    cursor.mark_delta_processed();
    CHECK(cursor.delta_count() == 0U);
}

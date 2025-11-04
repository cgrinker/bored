#include "bored/executor/executor_context.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"

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
    CHECK(snapshot.spool_latency.invocations == rows.size());
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

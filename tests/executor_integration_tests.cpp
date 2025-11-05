#include "bored/executor/executor_context.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <optional>
#include <vector>

using bored::executor::ExecutorContext;
using bored::executor::ExecutorNode;
using bored::executor::SpoolExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleView;
using bored::executor::WorkTableRegistry;
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

class VectorChildExecutor final : public ExecutorNode {
public:
    explicit VectorChildExecutor(std::vector<std::vector<std::byte>> rows)
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

struct StatementResult final {
    std::vector<std::uint32_t> observed;
    std::size_t child_open_count = 0U;
    std::size_t child_produced_count = 0U;
};

TupleBuffer make_tuple(std::uint32_t value)
{
    TupleBuffer buffer{};
    bored::executor::TupleWriter writer{buffer};
    writer.reset();
    auto bytes = encode_u32(value);
    writer.append_column(std::span<const std::byte>(bytes.data(), bytes.size()), false);
    writer.finalize();
    return buffer;
}

StatementResult run_statement(WorkTableRegistry& registry,
                              Snapshot snapshot,
                              std::uint64_t worktable_id,
                              std::initializer_list<std::uint32_t> child_values,
                              std::optional<std::uint32_t> appended_delta)
{
    std::vector<std::vector<std::byte>> encoded_rows;
    encoded_rows.reserve(child_values.size());
    for (auto value : child_values) {
        encoded_rows.push_back(encode_u32(value));
    }

    auto child = std::make_unique<VectorChildExecutor>(std::move(encoded_rows));
    auto* raw_child = child.get();

    SpoolExecutor::Config config{};
    config.worktable_registry = &registry;
    config.worktable_id = worktable_id;
    config.enable_recursive_cursor = true;

    SpoolExecutor spool{std::move(child), config};

    ExecutorContext context{};
    context.set_snapshot(snapshot);

    TupleBuffer buffer{};
    StatementResult result{};

    spool.open(context);
    while (spool.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 1U);
        const auto column = view.column(0U);
        REQUIRE_FALSE(column.is_null);
        result.observed.push_back(decode_u32(column.data));
        buffer.reset();
    }
    spool.close(context);

    result.child_open_count = raw_child->open_count();
    result.child_produced_count = raw_child->produced_count();

    if (appended_delta.has_value()) {
        auto cursor_opt = spool.recursive_cursor();
        REQUIRE(cursor_opt.has_value());
        auto cursor = std::move(*cursor_opt);
        TupleBuffer delta = make_tuple(*appended_delta);
        cursor.append_delta(std::move(delta));
        cursor.mark_delta_processed();
    }

    return result;
}

}  // namespace

TEST_CASE("executor integration replays recursive spool deltas across statements")
{
    WorkTableRegistry registry;
    const std::uint64_t worktable_id = 0xDEADBEEFU;

    Snapshot snapshot{};
    snapshot.read_lsn = 1234U;
    snapshot.xmin = 5U;
    snapshot.xmax = 2048U;

    auto first = run_statement(registry, snapshot, worktable_id, {1U, 2U}, 3U);
    CHECK(first.observed == std::vector<std::uint32_t>{1U, 2U});
    CHECK(first.child_open_count == 1U);
    CHECK(first.child_produced_count == 2U);

    auto second = run_statement(registry, snapshot, worktable_id, {}, 4U);
    CHECK(second.observed == std::vector<std::uint32_t>{1U, 2U, 3U});
    CHECK(second.child_open_count == 0U);
    CHECK(second.child_produced_count == 0U);

    auto third = run_statement(registry, snapshot, worktable_id, {}, std::nullopt);
    CHECK(third.observed == std::vector<std::uint32_t>{1U, 2U, 3U, 4U});
    CHECK(third.child_open_count == 0U);
    CHECK(third.child_produced_count == 0U);
}

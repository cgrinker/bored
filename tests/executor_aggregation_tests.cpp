#include "bored/executor/aggregation_executor.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include "bored/catalog/catalog_ids.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

using bored::catalog::RelationId;
using bored::executor::AggregationExecutor;
using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorTelemetry;
using bored::executor::SequentialScanExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleColumnView;
using bored::executor::TupleView;
using bored::executor::TupleWriter;
using bored::storage::StorageReader;
using bored::storage::TableScanConfig;
using bored::storage::TableScanCursor;
using bored::storage::TableTuple;
using bored::storage::TupleHeader;
using bored::txn::Snapshot;
using bored::txn::TransactionId;

namespace {

struct FrozenTableTuple final {
    TupleHeader header{};
    std::vector<std::byte> payload{};
};

class FrozenTableCursor final : public TableScanCursor {
public:
    explicit FrozenTableCursor(const std::vector<FrozenTableTuple>* tuples)
        : tuples_{tuples}
    {}

    bool next(TableTuple& out_tuple) override
    {
        if (tuples_ == nullptr || index_ >= tuples_->size()) {
            return false;
        }
        const auto& entry = (*tuples_)[index_++];
        out_tuple.header = entry.header;
        out_tuple.payload = std::span<const std::byte>(entry.payload.data(), entry.payload.size());
        return true;
    }

    void reset() override { index_ = 0U; }

private:
    const std::vector<FrozenTableTuple>* tuples_ = nullptr;
    std::size_t index_ = 0U;
};

class FrozenStorageReader final : public StorageReader {
public:
    void set_table(RelationId relation, std::vector<FrozenTableTuple> tuples)
    {
        tables_[relation.value] = std::move(tuples);
    }

    [[nodiscard]] std::unique_ptr<TableScanCursor> create_table_scan(const TableScanConfig& config) override
    {
        auto it = tables_.find(config.relation_id.value);
        if (it == tables_.end()) {
            return std::make_unique<FrozenTableCursor>(nullptr);
        }
        return std::make_unique<FrozenTableCursor>(&it->second);
    }

private:
    std::unordered_map<std::uint64_t, std::vector<FrozenTableTuple>> tables_{};
};

FrozenTableTuple make_tuple(TransactionId created, TransactionId deleted, std::string_view payload)
{
    FrozenTableTuple tuple{};
    tuple.header.created_transaction_id = created;
    tuple.header.deleted_transaction_id = deleted;
    tuple.payload.resize(payload.size());
    std::memcpy(tuple.payload.data(), payload.data(), payload.size());
    return tuple;
}

std::string column_to_string(const TupleColumnView& column)
{
    if (column.is_null) {
        return {};
    }
    return std::string(reinterpret_cast<const char*>(column.data.data()), column.data.size());
}

ExecutorContext make_context(TransactionId transaction_id, Snapshot snapshot)
{
    ExecutorContextConfig config{};
    config.transaction_id = transaction_id;
    config.snapshot = std::move(snapshot);
    return ExecutorContext{config};
}

std::uint64_t column_to_u64(const TupleColumnView& column)
{
    REQUIRE(column.data.size() == sizeof(std::uint64_t));
    std::uint64_t value = 0U;
    std::memcpy(&value, column.data.data(), sizeof(std::uint64_t));
    return value;
}

}  // namespace

TEST_CASE("AggregationExecutor groups rows and computes counts")
{
    FrozenStorageReader reader;
    RelationId relation{501U};

    reader.set_table(relation, {
        make_tuple(10U, 0U, "alpha"),
        make_tuple(11U, 0U, "alpha"),
        make_tuple(12U, 0U, "bravo"),
        make_tuple(13U, 0U, "charlie"),
        make_tuple(14U, 0U, "charlie"),
        make_tuple(15U, 0U, "charlie"),
    });

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = relation;

    auto scan = std::make_unique<SequentialScanExecutor>(scan_config);

    ExecutorTelemetry telemetry;

    AggregationExecutor::AggregateDefinition count_def{};
    count_def.state_size = sizeof(std::uint64_t);
    count_def.state_alignment = alignof(std::uint64_t);
    count_def.initialize = [](std::span<std::byte> state) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        *value = 0U;
    };
    count_def.accumulate = [](std::span<std::byte> state, const TupleView&, ExecutorContext&) {
        auto* value = reinterpret_cast<std::uint64_t*>(state.data());
        *value += 1U;
    };
    count_def.project = [](std::span<const std::byte> state, TupleWriter& writer, ExecutorContext&) {
        writer.append_column(state, false);
    };

    AggregationExecutor::Config agg_config{};
    agg_config.telemetry = &telemetry;
    agg_config.group_key = [](const TupleView& view, ExecutorContext&) {
        return column_to_string(view.column(1U));
    };
    agg_config.group_projection = [](const TupleView& first_row, TupleWriter& writer, ExecutorContext&) {
        const auto column = first_row.column(1U);
        writer.append_column(column.data, column.is_null);
    };
    agg_config.aggregates.push_back(count_def);

    AggregationExecutor aggregator{std::move(scan), std::move(agg_config)};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(902U, snapshot);

    aggregator.open(context);

    TupleBuffer buffer{};
    std::vector<std::pair<std::string, std::uint64_t>> groups;
    while (aggregator.next(context, buffer)) {
        const auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 2U);
        const auto key = column_to_string(view.column(0U));
        const auto count = column_to_u64(view.column(1U));
        groups.emplace_back(key, count);
    }

    aggregator.close(context);

    std::vector<std::pair<std::string, std::uint64_t>> expected{{"alpha", 2U}, {"bravo", 1U}, {"charlie", 3U}};
    REQUIRE(groups == expected);

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.aggregation_input_rows == 6U);
    REQUIRE(snapshot_telemetry.aggregation_groups_emitted == 3U);
}

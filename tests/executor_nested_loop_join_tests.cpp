#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/nested_loop_join_executor.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include "bored/catalog/catalog_ids.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

using bored::catalog::RelationId;
using bored::executor::ExecutorContext;
using bored::executor::ExecutorContextConfig;
using bored::executor::ExecutorTelemetry;
using bored::executor::NestedLoopJoinExecutor;
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

}  // namespace

TEST_CASE("NestedLoopJoinExecutor matches rows across child scans")
{
    FrozenStorageReader reader;
    RelationId left_relation{301U};
    RelationId right_relation{302U};

    reader.set_table(left_relation, {
        make_tuple(10U, 0U, "alpha"),
        make_tuple(11U, 0U, "bravo"),
        make_tuple(12U, 0U, "charlie"),
    });

    reader.set_table(right_relation, {
        make_tuple(20U, 0U, "alpha"),
        make_tuple(21U, 0U, "charlie"),
        make_tuple(22U, 0U, "delta"),
    });

    SequentialScanExecutor::Config left_scan_config{};
    left_scan_config.reader = &reader;
    left_scan_config.relation_id = left_relation;

    SequentialScanExecutor::Config right_scan_config{};
    right_scan_config.reader = &reader;
    right_scan_config.relation_id = right_relation;

    auto outer = std::make_unique<SequentialScanExecutor>(left_scan_config);
    auto inner = std::make_unique<SequentialScanExecutor>(right_scan_config);

    ExecutorTelemetry telemetry;
    std::vector<std::string> rebinding_log;

    NestedLoopJoinExecutor::Config join_config{};
    join_config.telemetry = &telemetry;
    join_config.predicate = [](const TupleView& outer_view, const TupleView& inner_view, ExecutorContext&) {
        const auto outer_payload = column_to_string(outer_view.column(1U));
        const auto inner_payload = column_to_string(inner_view.column(1U));
        return outer_payload == inner_payload;
    };
    join_config.projections.push_back([](const TupleView& outer_view,
                                         const TupleView& inner_view,
                                         TupleWriter& writer,
                                         ExecutorContext&) {
        const auto left_payload = outer_view.column(1U);
        const auto right_payload = inner_view.column(1U);
        writer.append_column(left_payload.data, left_payload.is_null);
        writer.append_column(right_payload.data, right_payload.is_null);
    });
    join_config.rebind_probe = [&rebinding_log](const TupleView& outer_view, ExecutorContext&) {
        rebinding_log.push_back(column_to_string(outer_view.column(1U)));
    };

    NestedLoopJoinExecutor join{std::move(outer), std::move(inner), std::move(join_config)};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(900U, snapshot);

    join.open(context);

    TupleBuffer buffer{};
    std::vector<std::pair<std::string, std::string>> matches;
    while (join.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 2U);
        matches.emplace_back(column_to_string(view.column(0U)), column_to_string(view.column(1U)));
    }

    join.close(context);

    std::vector<std::pair<std::string, std::string>> expected{{"alpha", "alpha"}, {"charlie", "charlie"}};
    REQUIRE(matches == expected);

    std::vector<std::string> expected_rebindings{"alpha", "bravo", "charlie"};
    REQUIRE(rebinding_log == expected_rebindings);

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.nested_loop_rows_compared == 9U);
    REQUIRE(snapshot_telemetry.nested_loop_rows_matched == 2U);
    REQUIRE(snapshot_telemetry.nested_loop_rows_emitted == 2U);
}

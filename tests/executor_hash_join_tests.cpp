#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/hash_join_executor.hpp"
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
using bored::executor::HashJoinExecutor;
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

TEST_CASE("HashJoinExecutor joins rows using build-side hash table")
{
    FrozenStorageReader reader;
    RelationId build_relation{401U};
    RelationId probe_relation{402U};

    reader.set_table(build_relation, {
        make_tuple(10U, 0U, "alpha"),
        make_tuple(11U, 0U, "bravo"),
        make_tuple(12U, 0U, "charlie"),
    });

    reader.set_table(probe_relation, {
        make_tuple(20U, 0U, "alpha"),
        make_tuple(21U, 0U, "charlie"),
        make_tuple(22U, 0U, "delta"),
    });

    SequentialScanExecutor::Config build_scan_config{};
    build_scan_config.reader = &reader;
    build_scan_config.relation_id = build_relation;

    SequentialScanExecutor::Config probe_scan_config{};
    probe_scan_config.reader = &reader;
    probe_scan_config.relation_id = probe_relation;

    auto build = std::make_unique<SequentialScanExecutor>(build_scan_config);
    auto probe = std::make_unique<SequentialScanExecutor>(probe_scan_config);

    ExecutorTelemetry telemetry;

    HashJoinExecutor::Config join_config{};
    join_config.telemetry = &telemetry;
    join_config.build_key = [](const TupleView& build_view, ExecutorContext&) {
        return column_to_string(build_view.column(1U));
    };
    join_config.probe_key = [](const TupleView& probe_view, ExecutorContext&) {
        return column_to_string(probe_view.column(1U));
    };
    join_config.projections.push_back([](const TupleView& build_view,
                                         const TupleView& probe_view,
                                         TupleWriter& writer,
                                         ExecutorContext&) {
        const auto left_payload = build_view.column(1U);
        const auto right_payload = probe_view.column(1U);
        writer.append_column(left_payload.data, left_payload.is_null);
        writer.append_column(right_payload.data, right_payload.is_null);
    });

    HashJoinExecutor join{std::move(build), std::move(probe), std::move(join_config)};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(901U, snapshot);

    join.open(context);

    TupleBuffer buffer{};
    std::vector<std::pair<std::string, std::string>> matches;
    while (join.next(context, buffer)) {
        const auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 2U);
        matches.emplace_back(column_to_string(view.column(0U)), column_to_string(view.column(1U)));
    }

    join.close(context);

    std::vector<std::pair<std::string, std::string>> expected{{"alpha", "alpha"}, {"charlie", "charlie"}};
    REQUIRE(matches == expected);

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.hash_join_build_rows == 3U);
    REQUIRE(snapshot_telemetry.hash_join_probe_rows == 3U);
    REQUIRE(snapshot_telemetry.hash_join_rows_matched == 2U);
}

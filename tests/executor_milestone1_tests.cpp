#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/filter_executor.hpp"
#include "bored/executor/projection_executor.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include "bored/catalog/catalog_ids.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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
using bored::executor::FilterExecutor;
using bored::executor::ProjectionExecutor;
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

TEST_CASE("SequentialScanExecutor respects snapshot visibility")
{
    FrozenStorageReader reader;
    RelationId relation{42U};

    std::vector<FrozenTableTuple> tuples;
    tuples.push_back(make_tuple(5U, 0U, "alpha"));              // committed insert
    tuples.push_back(make_tuple(15U, 0U, "bravo"));             // in-progress insert
    tuples.push_back(make_tuple(25U, 0U, "charlie"));           // committed insert within window
    tuples.push_back(make_tuple(12U, 22U, "delta"));            // deleted before snapshot
    tuples.push_back(make_tuple(8U, 21U, "echo"));              // delete in progress, should be visible
    reader.set_table(relation, std::move(tuples));

    ExecutorTelemetry telemetry;

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = relation;
    scan_config.root_page_id = 9000U;
    scan_config.telemetry = &telemetry;

    SequentialScanExecutor scan{scan_config};

    Snapshot snapshot{};
    snapshot.xmin = 10U;
    snapshot.xmax = 30U;
    snapshot.in_progress = {15U, 21U};
    auto context = make_context(40U, snapshot);

    scan.open(context);

    TupleBuffer buffer{};
    std::vector<std::string> visible;
    while (scan.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        REQUIRE(view.column_count() == 2U);

        const auto header_column = view.column(0U);
        REQUIRE_FALSE(header_column.is_null);
        REQUIRE(header_column.data.size() == sizeof(TupleHeader));

        const auto payload_column = view.column(1U);
        REQUIRE_FALSE(payload_column.is_null);
        visible.push_back(column_to_string(payload_column));
    }

    scan.close(context);

    std::vector<std::string> expected{"alpha", "charlie", "echo"};
    REQUIRE(visible == expected);

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.seq_scan_rows_read == 5U);
    REQUIRE(snapshot_telemetry.seq_scan_rows_visible == expected.size());
    REQUIRE(snapshot_telemetry.filter_rows_evaluated == 0U);
    REQUIRE(snapshot_telemetry.projection_rows_emitted == 0U);
}

TEST_CASE("FilterExecutor applies predicate to sequential scan output")
{
    FrozenStorageReader reader;
    RelationId relation{99U};

    reader.set_table(relation, {
        make_tuple(10U, 0U, "alpha"),
        make_tuple(11U, 0U, "bravo"),
        make_tuple(12U, 0U, "charlie"),
        make_tuple(13U, 0U, "delta"),
    });

    ExecutorTelemetry telemetry;

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = relation;
    scan_config.root_page_id = 777U;
    scan_config.telemetry = &telemetry;

    auto scan = std::make_unique<SequentialScanExecutor>(scan_config);

    FilterExecutor::Config filter_config{};
    filter_config.telemetry = &telemetry;
    filter_config.predicate = [](const TupleView& view, ExecutorContext&) {
        const auto payload = view.column(1U);
        if (payload.is_null) {
            return false;
        }
        const auto value = column_to_string(payload);
        return value.find('a') != std::string::npos;
    };

    auto filter = std::make_unique<FilterExecutor>(std::move(scan), filter_config);

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(500U, snapshot);

    filter->open(context);

    TupleBuffer buffer{};
    std::vector<std::string> rows;
    while (filter->next(context, buffer)) {
        const auto view = TupleView::from_buffer(buffer);
        rows.push_back(column_to_string(view.column(1U)));
    }

    filter->close(context);

    std::vector<std::string> expected{"alpha", "bravo", "charlie", "delta"};
    expected.erase(std::remove_if(expected.begin(), expected.end(), [](const std::string& value) {
        return value.find('a') == std::string::npos;
    }), expected.end());

    REQUIRE(rows == expected);

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.seq_scan_rows_visible == 4U);
    REQUIRE(snapshot_telemetry.filter_rows_evaluated == 4U);
    REQUIRE(snapshot_telemetry.filter_rows_passed == expected.size());
}

TEST_CASE("ProjectionExecutor materialises derived columns")
{
    FrozenStorageReader reader;
    RelationId relation{211U};
    reader.set_table(relation, {
        make_tuple(30U, 0U, "alpha"),
        make_tuple(31U, 0U, "beta"),
    });

    ExecutorTelemetry telemetry;

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = relation;
    scan_config.telemetry = &telemetry;

    auto scan = std::make_unique<SequentialScanExecutor>(scan_config);

    FilterExecutor::Config filter_config{};
    filter_config.telemetry = &telemetry;
    filter_config.predicate = [](const TupleView& view, ExecutorContext&) {
        const auto payload = view.column(1U);
        if (payload.is_null) {
            return false;
        }
        const auto value = column_to_string(payload);
        return value != "beta";
    };

    auto filter = std::make_unique<FilterExecutor>(std::move(scan), filter_config);

    ProjectionExecutor::Config projection_config{};
    projection_config.telemetry = &telemetry;
    projection_config.projections.push_back([](const TupleView& input, TupleWriter& writer, ExecutorContext&) {
        const auto payload = input.column(1U);
        writer.append_column(payload.data, payload.is_null);
    });
    projection_config.projections.push_back([](const TupleView& input, TupleWriter& writer, ExecutorContext&) {
        const auto header = input.column(0U);
        writer.append_column(header.data, header.is_null);
    });

    auto projection = std::make_unique<ProjectionExecutor>(std::move(filter), std::move(projection_config));

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(700U, snapshot);

    projection->open(context);

    TupleBuffer buffer{};
    std::vector<std::string> payloads;
    while (projection->next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.column_count() == 2U);
        const auto payload_column = view.column(0U);
        payloads.push_back(column_to_string(payload_column));
        const auto header_column = view.column(1U);
        REQUIRE_FALSE(header_column.is_null);
        REQUIRE(header_column.data.size() == sizeof(TupleHeader));
    }

    projection->close(context);

    REQUIRE(payloads == std::vector<std::string>{"alpha"});

    const auto snapshot_telemetry = telemetry.snapshot();
    REQUIRE(snapshot_telemetry.seq_scan_rows_read == 2U);
    REQUIRE(snapshot_telemetry.filter_rows_evaluated == 2U);
    REQUIRE(snapshot_telemetry.filter_rows_passed == 1U);
    REQUIRE(snapshot_telemetry.projection_rows_emitted == 1U);
}

}  // namespace

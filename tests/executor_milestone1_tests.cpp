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
#include <limits>

using bored::catalog::RelationId;
using bored::catalog::IndexId;
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
    std::uint32_t page_id = 0U;
    std::uint16_t slot_id = 0U;
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
        out_tuple.page_id = entry.page_id;
        out_tuple.slot_id = entry.slot_id;
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
        auto& stored = tables_[relation.value] = std::move(tuples);
        std::uint32_t next_page = 1U;
        std::uint16_t next_slot = 1U;
        for (auto& tuple : stored) {
            if (tuple.page_id == 0U) {
                tuple.page_id = next_page;
            }
            if (tuple.slot_id == 0U) {
                tuple.slot_id = next_slot++;
                if (next_slot == std::numeric_limits<std::uint16_t>::max()) {
                    ++next_page;
                    next_slot = 1U;
                }
            }
        }
    }

    void register_index(RelationId relation,
                        IndexId index,
                        std::unordered_map<std::string, std::vector<std::size_t>> entries)
    {
        indexes_[compose_index_key(relation.value, index.value)] = std::move(entries);
    }

    [[nodiscard]] std::unique_ptr<TableScanCursor> create_table_scan(const TableScanConfig& config) override
    {
        auto it = tables_.find(config.relation_id.value);
        if (it == tables_.end()) {
            return std::make_unique<FrozenTableCursor>(nullptr);
        }
        return std::make_unique<FrozenTableCursor>(&it->second);
    }

    [[nodiscard]] std::vector<bored::storage::IndexProbeResult> probe_index(
        const bored::storage::IndexProbeConfig& config) override
    {
        std::vector<bored::storage::IndexProbeResult> results;
        auto table_it = tables_.find(config.relation_id.value);
        if (table_it == tables_.end()) {
            return results;
        }

        const auto key_string = std::string(reinterpret_cast<const char*>(config.key.data()), config.key.size());
        auto index_it = indexes_.find(compose_index_key(config.relation_id.value, config.index_id.value));
        if (index_it == indexes_.end()) {
            return results;
        }

        auto row_it = index_it->second.find(key_string);
        if (row_it == index_it->second.end()) {
            return results;
        }

        results.reserve(row_it->second.size());
        for (auto tuple_index : row_it->second) {
            if (tuple_index >= table_it->second.size()) {
                continue;
            }
            const auto& frozen = table_it->second[tuple_index];
            bored::storage::IndexProbeResult result{};
            result.tuple.header = frozen.header;
            result.tuple.payload = std::span<const std::byte>(frozen.payload.data(), frozen.payload.size());
            result.tuple.page_id = frozen.page_id;
            result.tuple.slot_id = frozen.slot_id;
            results.push_back(result);
        }
        return results;
    }

private:
    static std::uint64_t compose_index_key(std::uint64_t relation_value, std::uint64_t index_value) noexcept
    {
        return (relation_value << 32U) ^ index_value;
    }

    std::unordered_map<std::uint64_t, std::vector<FrozenTableTuple>> tables_{};
    std::unordered_map<std::uint64_t, std::unordered_map<std::string, std::vector<std::size_t>>> indexes_{};
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

std::vector<std::byte> make_probe_key(std::string_view value)
{
    std::vector<std::byte> key(value.size());
    std::memcpy(key.data(), value.data(), value.size());
    return key;
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
    REQUIRE(snapshot_telemetry.seq_scan_latency.invocations >= expected.size());
}

TEST_CASE("SequentialScanExecutor uses index probe before heap fallback")
{
    FrozenStorageReader reader;
    RelationId relation{55U};
    IndexId index{909U};

    reader.set_table(relation, {
        make_tuple(5U, 0U, "alpha"),
        make_tuple(6U, 0U, "bravo"),
        make_tuple(7U, 0U, "charlie")
    });

    reader.register_index(relation, index, {
        {"bravo", {1U}}
    });

    ExecutorTelemetry telemetry;

    SequentialScanExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = relation;
    config.root_page_id = 6000U;
    config.telemetry = &telemetry;
    config.index_probe = SequentialScanExecutor::IndexProbe{
        .index_id = index,
        .key = make_probe_key("bravo")
    };

    SequentialScanExecutor scan{config};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(77U, snapshot);

    scan.open(context);
    TupleBuffer buffer{};
    std::vector<std::string> observed;
    while (scan.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        observed.push_back(column_to_string(view.column(1U)));
    }
    scan.close(context);

    REQUIRE(observed.size() == 3U);
    CHECK(observed.front() == "bravo");

    auto sorted = observed;
    std::sort(sorted.begin(), sorted.end());
    CHECK(sorted == std::vector<std::string>{"alpha", "bravo", "charlie"});
}

TEST_CASE("SequentialScanExecutor falls back to heap when index probe misses")
{
    FrozenStorageReader reader;
    RelationId relation{56U};
    IndexId index{910U};

    reader.set_table(relation, {
        make_tuple(3U, 0U, "alpha"),
        make_tuple(4U, 0U, "bravo"),
        make_tuple(5U, 0U, "charlie")
    });

    reader.register_index(relation, index, {
        {"delta", {0U}}
    });

    ExecutorTelemetry telemetry;
    SequentialScanExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = relation;
    config.root_page_id = 6100U;
    config.telemetry = &telemetry;
    config.index_probe = SequentialScanExecutor::IndexProbe{
        .index_id = index,
        .key = make_probe_key("omega")
    };

    SequentialScanExecutor scan{config};

    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 100U;
    auto context = make_context(88U, snapshot);

    scan.open(context);
    TupleBuffer buffer{};
    std::vector<std::string> observed;
    while (scan.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        observed.push_back(column_to_string(view.column(1U)));
    }
    scan.close(context);

    CHECK(observed == std::vector<std::string>{"alpha", "bravo", "charlie"});
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
    REQUIRE(snapshot_telemetry.filter_latency.invocations >= expected.size());
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
    REQUIRE(snapshot_telemetry.projection_latency.invocations >= payloads.size());
}

}  // namespace

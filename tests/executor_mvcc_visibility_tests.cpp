#include "bored/executor/executor_context.hpp"
#include "bored/executor/delete_executor.hpp"
#include "bored/executor/nested_loop_join_executor.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/spool_executor.hpp"
#include "bored/executor/update_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using bored::catalog::RelationId;
using bored::executor::ExecutorContext;
using bored::executor::DeleteExecutor;
using bored::executor::NestedLoopJoinExecutor;
using bored::executor::SequentialScanExecutor;
using bored::executor::SpoolExecutor;
using bored::executor::UpdateExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleView;
using bored::storage::IndexProbeConfig;
using bored::storage::IndexProbeResult;
using bored::storage::StorageReader;
using bored::storage::TableScanConfig;
using bored::storage::TableScanCursor;
using bored::storage::TableTuple;
using bored::storage::TupleHeader;
using bored::txn::Snapshot;
using bored::txn::TransactionId;

namespace {

struct StoredTuple final {
    TupleHeader header{};
    std::vector<std::byte> payload{};
    std::uint32_t page_id = 0U;
    std::uint16_t slot_id = 0U;
};

class StubTableScanCursor final : public TableScanCursor {
public:
    explicit StubTableScanCursor(const std::vector<StoredTuple>* tuples)
        : tuples_{tuples}
    {
    }

    bool next(TableTuple& out_tuple) override
    {
        if (tuples_ == nullptr || index_ >= tuples_->size()) {
            return false;
        }

        const auto& stored = tuples_->at(index_++);
        out_tuple.header = stored.header;
        out_tuple.payload = std::span<const std::byte>(stored.payload.data(), stored.payload.size());
        out_tuple.page_id = stored.page_id;
        out_tuple.slot_id = stored.slot_id;
        return true;
    }

    void reset() override
    {
        index_ = 0U;
    }

private:
    const std::vector<StoredTuple>* tuples_ = nullptr;
    std::size_t index_ = 0U;
};

class StubStorageReader final : public StorageReader {
public:
    std::unique_ptr<TableScanCursor> create_table_scan(const TableScanConfig&) override
    {
        return std::make_unique<StubTableScanCursor>(&heap_tuples_);
    }

    std::vector<IndexProbeResult> probe_index(const IndexProbeConfig& config) override
    {
        (void)config;
        std::vector<IndexProbeResult> results;
        results.reserve(index_hits_.size());
        for (const auto& stored : index_hits_) {
            IndexProbeResult result{};
            result.tuple.header = stored.header;
            result.tuple.payload = std::span<const std::byte>(stored.payload.data(), stored.payload.size());
            result.tuple.page_id = stored.page_id;
            result.tuple.slot_id = stored.slot_id;
            results.push_back(result);
        }
        return results;
    }

    StoredTuple& add_heap_tuple(TransactionId create_txn,
                                TransactionId delete_txn,
                                std::string_view payload_text,
                                std::uint32_t page_id,
                                std::uint16_t slot_id)
    {
        auto& stored = heap_tuples_.emplace_back();
        stored.header.created_transaction_id = create_txn;
        stored.header.deleted_transaction_id = delete_txn;
        stored.payload.resize(payload_text.size());
        std::memcpy(stored.payload.data(), payload_text.data(), payload_text.size());
        stored.page_id = page_id;
        stored.slot_id = slot_id;
        return stored;
    }

    void add_index_hit(const StoredTuple& tuple)
    {
        index_hits_.push_back(tuple);
    }

private:
    std::vector<StoredTuple> heap_tuples_{};
    std::vector<StoredTuple> index_hits_{};
};

std::string_view column_text(const TupleView& view, std::size_t column_index)
{
    const auto column = view.column(column_index);
    return std::string_view(reinterpret_cast<const char*>(column.data.data()), column.data.size());
}

TupleHeader decode_header(const TupleView& view)
{
    const auto header_column = view.column(0U);
    REQUIRE_FALSE(header_column.is_null);
    REQUIRE(header_column.data.size() == sizeof(TupleHeader));

    TupleHeader header{};
    std::memcpy(&header, header_column.data.data(), sizeof(header));
    return header;
}

class CollectingDeleteTarget final : public DeleteExecutor::Target {
public:
    std::error_code delete_tuple(const TupleView& tuple,
                                 ExecutorContext& context,
                                 DeleteExecutor::DeleteStats& stats) override
    {
        (void)context;
        stats = {};
        processed_headers.push_back(decode_header(tuple));
        return {};
    }

    std::vector<TupleHeader> processed_headers;
};

class CollectingUpdateTarget final : public UpdateExecutor::Target {
public:
    std::error_code update_tuple(const TupleView& tuple,
                                 ExecutorContext& context,
                                 UpdateExecutor::UpdateStats& stats) override
    {
        (void)context;
        stats = {};
        processed_headers.push_back(decode_header(tuple));
        payloads.emplace_back(column_text(tuple, 1U));
        return {};
    }

    std::vector<TupleHeader> processed_headers;
    std::vector<std::string> payloads;
};

ExecutorContext make_context(TransactionId txn_id, Snapshot snapshot)
{
    ExecutorContext context{};
    context.set_transaction_id(txn_id);
    context.set_snapshot(std::move(snapshot));
    return context;
}

}  // namespace

TEST_CASE("SequentialScanExecutor hides tuples from in-progress writers")
{
    StubStorageReader reader;
    reader.add_heap_tuple(/*create_txn=*/42U, /*delete_txn=*/0U, "visible", /*page_id=*/1U, /*slot_id=*/1U);
    reader.add_heap_tuple(/*create_txn=*/120U, /*delete_txn=*/0U, "in_progress", /*page_id=*/1U, /*slot_id=*/2U);

    SequentialScanExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{7U};
    config.enable_heap_fallback = true;

    SequentialScanExecutor executor{config};

    Snapshot snapshot{};
    snapshot.xmin = 40U;
    snapshot.xmax = 100U;
    snapshot.in_progress = {120U};

    auto context = make_context(/*txn_id=*/77U, snapshot);

    TupleBuffer buffer{};
    executor.open(context);

    std::vector<std::string> values;
    while (executor.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        values.emplace_back(column_text(view, 1U));
        buffer.reset();
    }

    executor.close(context);

    REQUIRE(values.size() == 1U);
    CHECK(values.front() == "visible");
}

TEST_CASE("SequentialScanExecutor sees self-inserts regardless of snapshot")
{
    constexpr TransactionId kTxn = 900U;

    StubStorageReader reader;
    reader.add_heap_tuple(/*create_txn=*/kTxn, /*delete_txn=*/0U, "self", /*page_id=*/2U, /*slot_id=*/1U);
    reader.add_heap_tuple(/*create_txn=*/10U, /*delete_txn=*/kTxn, "deleted", /*page_id=*/2U, /*slot_id=*/2U);

    SequentialScanExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{9U};
    config.enable_heap_fallback = true;

    SequentialScanExecutor executor{config};

    Snapshot snapshot{};
    snapshot.xmin = 50U;
    snapshot.xmax = 60U;
    snapshot.in_progress = {kTxn};

    auto context = make_context(kTxn, snapshot);

    TupleBuffer buffer{};
    executor.open(context);

    std::vector<std::string> values;
    while (executor.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        values.emplace_back(column_text(view, 1U));
        buffer.reset();
    }

    executor.close(context);

    REQUIRE(values.size() == 1U);
    CHECK(values.front() == "self");
}

TEST_CASE("NestedLoopJoinExecutor respects MVCC visibility from child scans")
{
    StubStorageReader outer_reader;
    outer_reader.add_heap_tuple(/*create_txn=*/42U, /*delete_txn=*/0U, "joined", /*page_id=*/3U, /*slot_id=*/1U);
    outer_reader.add_heap_tuple(/*create_txn=*/120U, /*delete_txn=*/0U, "joined", /*page_id=*/3U, /*slot_id=*/2U);

    StubStorageReader inner_reader;
    inner_reader.add_heap_tuple(/*create_txn=*/55U, /*delete_txn=*/0U, "joined", /*page_id=*/4U, /*slot_id=*/1U);
    inner_reader.add_heap_tuple(/*create_txn=*/220U, /*delete_txn=*/0U, "joined", /*page_id=*/4U, /*slot_id=*/2U);

    SequentialScanExecutor::Config outer_config{};
    outer_config.reader = &outer_reader;
    outer_config.relation_id = RelationId{11U};
    outer_config.enable_heap_fallback = true;

    SequentialScanExecutor::Config inner_config{};
    inner_config.reader = &inner_reader;
    inner_config.relation_id = RelationId{12U};
    inner_config.enable_heap_fallback = true;

    auto outer_scan = std::make_unique<SequentialScanExecutor>(outer_config);
    auto inner_scan = std::make_unique<SequentialScanExecutor>(inner_config);

    NestedLoopJoinExecutor::Config join_config{};
    join_config.predicate = [](const TupleView& outer_view, const TupleView& inner_view, ExecutorContext&) {
        return column_text(outer_view, 1U) == column_text(inner_view, 1U);
    };

    NestedLoopJoinExecutor join_executor{std::move(outer_scan), std::move(inner_scan), std::move(join_config)};

    Snapshot snapshot{};
    snapshot.xmin = 30U;
    snapshot.xmax = 130U;
    snapshot.in_progress = {120U};

    auto context = make_context(/*txn_id=*/77U, snapshot);

    TupleBuffer buffer{};
    join_executor.open(context);

    std::vector<std::pair<std::string, std::string>> results;
    while (join_executor.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        const auto outer_payload = std::string{column_text(view, 1U)};
        const auto inner_payload = std::string{column_text(view, 3U)};
        results.emplace_back(outer_payload, inner_payload);
        buffer.reset();
    }

    join_executor.close(context);

    REQUIRE(results.size() == 1U);
    CHECK(results.front().first == "joined");
    CHECK(results.front().second == "joined");
}

TEST_CASE("SpoolExecutor rematerializes rows per snapshot and preserves MVCC filters")
{
    StubStorageReader reader;
    reader.add_heap_tuple(/*create_txn=*/42U, /*delete_txn=*/0U, "visible", /*page_id=*/5U, /*slot_id=*/1U);
    reader.add_heap_tuple(/*create_txn=*/120U, /*delete_txn=*/0U, "in_progress", /*page_id=*/5U, /*slot_id=*/2U);

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = RelationId{15U};
    scan_config.enable_heap_fallback = true;

    auto scan_child = std::make_unique<SequentialScanExecutor>(scan_config);
    SpoolExecutor::Config spool_config{};
    SpoolExecutor spool{std::move(scan_child), spool_config};

    Snapshot first_snapshot{};
    first_snapshot.xmin = 40U;
    first_snapshot.xmax = 100U;
    first_snapshot.in_progress = {120U};

    auto context = make_context(/*txn_id=*/88U, first_snapshot);

    TupleBuffer buffer{};
    spool.open(context);

    std::vector<std::string> first_pass{};
    while (spool.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        first_pass.emplace_back(column_text(view, 1U));
        buffer.reset();
    }

    spool.close(context);

    REQUIRE(first_pass.size() == 1U);
    CHECK(first_pass.front() == "visible");

    Snapshot second_snapshot{};
    second_snapshot.xmin = 40U;
    second_snapshot.xmax = 200U;
    second_snapshot.in_progress = {};

    context.set_snapshot(second_snapshot);
    spool.open(context);

    std::vector<std::string> second_pass{};
    while (spool.next(context, buffer)) {
        auto view = TupleView::from_buffer(buffer);
        REQUIRE(view.valid());
        second_pass.emplace_back(column_text(view, 1U));
        buffer.reset();
    }

    spool.close(context);

    REQUIRE(second_pass.size() == 2U);
    CHECK(second_pass.at(0U) == "visible");
    CHECK(second_pass.at(1U) == "in_progress");
}

TEST_CASE("DeleteExecutor honours MVCC visibility when draining child results")
{
    StubStorageReader reader;
    reader.add_heap_tuple(/*create_txn=*/42U, /*delete_txn=*/0U, "visible", /*page_id=*/6U, /*slot_id=*/1U);
    reader.add_heap_tuple(/*create_txn=*/120U, /*delete_txn=*/0U, "hidden", /*page_id=*/6U, /*slot_id=*/2U);

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = RelationId{16U};
    scan_config.enable_heap_fallback = true;

    auto scan_child = std::make_unique<SequentialScanExecutor>(scan_config);

    CollectingDeleteTarget target;
    DeleteExecutor::Config delete_config{};
    delete_config.target = &target;

    DeleteExecutor delete_executor{std::move(scan_child), delete_config};

    Snapshot snapshot{};
    snapshot.xmin = 40U;
    snapshot.xmax = 100U;
    snapshot.in_progress = {120U};

    auto context = make_context(/*txn_id=*/88U, snapshot);

    TupleBuffer buffer{};
    delete_executor.open(context);

    CHECK_FALSE(delete_executor.next(context, buffer));
    delete_executor.close(context);

    REQUIRE(target.processed_headers.size() == 1U);
    CHECK(target.processed_headers.front().created_transaction_id == 42U);
}

TEST_CASE("UpdateExecutor honours MVCC visibility when draining child results")
{
    StubStorageReader reader;
    reader.add_heap_tuple(/*create_txn=*/42U, /*delete_txn=*/0U, "visible", /*page_id=*/7U, /*slot_id=*/1U);
    reader.add_heap_tuple(/*create_txn=*/120U, /*delete_txn=*/0U, "hidden", /*page_id=*/7U, /*slot_id=*/2U);

    SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &reader;
    scan_config.relation_id = RelationId{17U};
    scan_config.enable_heap_fallback = true;

    auto scan_child = std::make_unique<SequentialScanExecutor>(scan_config);

    CollectingUpdateTarget target;
    UpdateExecutor::Config update_config{};
    update_config.target = &target;

    UpdateExecutor update_executor{std::move(scan_child), update_config};

    Snapshot snapshot{};
    snapshot.xmin = 40U;
    snapshot.xmax = 100U;
    snapshot.in_progress = {120U};

    auto context = make_context(/*txn_id=*/88U, snapshot);

    TupleBuffer buffer{};
    update_executor.open(context);

    CHECK_FALSE(update_executor.next(context, buffer));
    update_executor.close(context);

    REQUIRE(target.processed_headers.size() == 1U);
    CHECK(target.processed_headers.front().created_transaction_id == 42U);
    REQUIRE(target.payloads.size() == 1U);
    CHECK(target.payloads.front() == "visible");
}

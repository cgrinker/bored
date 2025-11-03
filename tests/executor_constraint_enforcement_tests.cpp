#include "bored/executor/executor_context.hpp"
#include "bored/executor/foreign_key_check_executor.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/unique_enforce_executor.hpp"
#include "bored/storage/storage_reader.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using bored::catalog::ConstraintId;
using bored::catalog::IndexId;
using bored::catalog::RelationId;
using bored::executor::ExecutorContext;
using bored::executor::ForeignKeyCheckExecutor;
using bored::executor::TupleBuffer;
using bored::executor::TupleWriter;
using bored::executor::UniqueEnforceExecutor;
using bored::storage::IndexProbeConfig;
using bored::storage::IndexProbeResult;
using bored::storage::StorageReader;
using bored::storage::TableScanConfig;
using bored::storage::TableScanCursor;
using bored::storage::TableTuple;
using bored::storage::TupleHeader;
using bored::txn::Snapshot;
using bored::txn::TransactionId;

struct ColumnData final {
    std::vector<std::byte> bytes{};
    bool is_null = false;
};

using RowData = std::vector<ColumnData>;

class ValuesExecutor final : public bored::executor::ExecutorNode {
public:
    explicit ValuesExecutor(std::vector<RowData> rows)
        : rows_{std::move(rows)}
    {
    }

    void open(ExecutorContext&) override { index_ = 0U; }

    bool next(ExecutorContext&, TupleBuffer& buffer) override
    {
        if (index_ >= rows_.size()) {
            return false;
        }

        TupleWriter writer{buffer};
        writer.reset();
        const auto& row = rows_[index_++];
        for (const auto& column : row) {
            auto span = std::span<const std::byte>(column.bytes.data(), column.bytes.size());
            writer.append_column(span, column.is_null);
        }
        writer.finalize();
        return true;
    }

    void close(ExecutorContext&) override {}

private:
    std::vector<RowData> rows_{};
    std::size_t index_ = 0U;
};

class EmptyTableScanCursor final : public TableScanCursor {
public:
    bool next(TableTuple&) override { return false; }
    void reset() override {}
};

struct IndexEntry final {
    std::vector<std::byte> key{};
    TupleHeader header{};
    std::vector<std::byte> payload{};
    std::uint32_t page_id = 0U;
    std::uint16_t slot_id = 0U;
};

class StubStorageReader final : public StorageReader {
public:
    std::unique_ptr<TableScanCursor> create_table_scan(const TableScanConfig&) override
    {
        return std::make_unique<EmptyTableScanCursor>();
    }

    std::vector<IndexProbeResult> probe_index(const IndexProbeConfig& config) override
    {
        std::vector<IndexProbeResult> results;
        auto it = indexes_.find(config.index_id.value);
        if (it == indexes_.end()) {
            return results;
        }

        for (const auto& entry : it->second) {
            if (entry.key.size() != config.key.size()) {
                continue;
            }
            if (!std::equal(entry.key.begin(), entry.key.end(), config.key.begin())) {
                continue;
            }

            IndexProbeResult result{};
            result.tuple.header = entry.header;
            result.tuple.payload = std::span<const std::byte>(entry.payload.data(), entry.payload.size());
            result.tuple.page_id = entry.page_id;
            result.tuple.slot_id = entry.slot_id;
            results.push_back(result);
        }

        return results;
    }

    void add_index_entry(IndexId index_id,
                         std::vector<std::byte> key,
                         TupleHeader header,
                         std::vector<std::byte> payload = {},
                         std::uint32_t page_id = 0U,
                         std::uint16_t slot_id = 0U)
    {
        IndexEntry entry{};
        entry.key = std::move(key);
        entry.header = header;
        entry.payload = std::move(payload);
        entry.page_id = page_id;
        entry.slot_id = slot_id;
        indexes_[index_id.value].push_back(std::move(entry));
    }

private:
    std::unordered_map<std::uint64_t, std::vector<IndexEntry>> indexes_{};
};

ExecutorContext make_context(TransactionId txn_id)
{
    ExecutorContext context{};
    context.set_transaction_id(txn_id);
    Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 10'000U;
    context.set_snapshot(snapshot);
    return context;
}

std::vector<std::byte> encode_int64(std::int64_t value)
{
    std::vector<std::byte> buffer(sizeof(value));
    std::memcpy(buffer.data(), &value, sizeof(value));
    return buffer;
}

std::vector<std::byte> encode_text(std::string_view text)
{
    std::vector<std::byte> buffer(text.size());
    std::memcpy(buffer.data(), text.data(), text.size());
    return buffer;
}

UniqueEnforceExecutor::KeyExtractor make_single_column_key_extractor(std::size_t column_index)
{
    return [column_index](const bored::executor::TupleView& view,
                          ExecutorContext&,
                          std::vector<std::byte>& out_key,
                          bool& has_null) {
        if (column_index >= view.column_count()) {
            return false;
        }
        auto column = view.column(column_index);
        has_null = column.is_null;
        out_key.assign(column.data.begin(), column.data.end());
        return true;
    };
}

ForeignKeyCheckExecutor::KeyExtractor make_fk_key_extractor(std::size_t column_index)
{
    return [column_index](const bored::executor::TupleView& view,
                          ExecutorContext&,
                          std::vector<std::byte>& out_key,
                          bool& has_null) {
        if (column_index >= view.column_count()) {
            return false;
        }
        auto column = view.column(column_index);
        has_null = column.is_null;
        out_key.assign(column.data.begin(), column.data.end());
        return true;
    };
}

TupleHeader visible_tuple(TransactionId creating_txn, TransactionId deleting_txn = 0U)
{
    TupleHeader header{};
    header.created_transaction_id = creating_txn;
    header.deleted_transaction_id = deleting_txn;
    return header;
}

}  // namespace

TEST_CASE("UniqueEnforceExecutor permits distinct keys")
{
    StubStorageReader reader;
    std::vector<RowData> rows{
        {ColumnData{encode_int64(101), false}},
        {ColumnData{encode_int64(202), false}}
    };

    auto child = std::make_unique<ValuesExecutor>(rows);

    UniqueEnforceExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{11U};
    config.index_id = IndexId{12U};
    config.constraint_id = ConstraintId{13U};
    config.constraint_name = "users_pkey";
    config.key_extractor = make_single_column_key_extractor(0U);

    UniqueEnforceExecutor executor{std::move(child), config};

    auto context = make_context(900U);
    TupleBuffer buffer{};

    executor.open(context);
    std::size_t produced = 0U;
    while (executor.next(context, buffer)) {
        ++produced;
        buffer.reset();
    }
    executor.close(context);

    CHECK(produced == rows.size());
}

TEST_CASE("UniqueEnforceExecutor rejects duplicate keys in batch")
{
    StubStorageReader reader;
    std::vector<RowData> rows{
        {ColumnData{encode_int64(55), false}},
        {ColumnData{encode_int64(55), false}}
    };

    auto child = std::make_unique<ValuesExecutor>(rows);

    UniqueEnforceExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{21U};
    config.index_id = IndexId{22U};
    config.constraint_name = "email_unique";
    config.key_extractor = make_single_column_key_extractor(0U);

    UniqueEnforceExecutor executor{std::move(child), config};

    auto context = make_context(1001U);
    TupleBuffer buffer{};

    executor.open(context);
    REQUIRE(executor.next(context, buffer));
    buffer.reset();
    CHECK_THROWS_AS(executor.next(context, buffer), std::runtime_error);
    executor.close(context);
}

TEST_CASE("UniqueEnforceExecutor detects conflicts via storage reader")
{
    StubStorageReader reader;
    const auto key = encode_text("alpha");

    TupleHeader header = visible_tuple(500U, 0U);
    reader.add_index_entry(IndexId{33U}, key, header);

    std::vector<RowData> rows{{ColumnData{key, false}}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    UniqueEnforceExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{32U};
    config.index_id = IndexId{33U};
    config.constraint_name = "users_email_key";
    config.key_extractor = make_single_column_key_extractor(0U);

    UniqueEnforceExecutor executor{std::move(child), config};

    auto context = make_context(2000U);
    TupleBuffer buffer{};

    executor.open(context);
    CHECK_THROWS_AS(executor.next(context, buffer), std::runtime_error);
    executor.close(context);
}

TEST_CASE("UniqueEnforceExecutor ignores non-visible matches")
{
    StubStorageReader reader;
    const auto key = encode_int64(77);

    TupleHeader header = visible_tuple(10U, 11U);  // deleted before snapshot xmax
    reader.add_index_entry(IndexId{44U}, key, header);

    std::vector<RowData> rows{{ColumnData{key, false}}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    UniqueEnforceExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{43U};
    config.index_id = IndexId{44U};
    config.constraint_name = "orders_id_key";
    config.key_extractor = make_single_column_key_extractor(0U);

    UniqueEnforceExecutor executor{std::move(child), config};

    auto context = make_context(3000U);
    TupleBuffer buffer{};

    executor.open(context);
    REQUIRE(executor.next(context, buffer));
    buffer.reset();
    CHECK_FALSE(executor.next(context, buffer));
    executor.close(context);
}

TEST_CASE("UniqueEnforceExecutor skips NULL keys when allowed")
{
    StubStorageReader reader;
    ColumnData nullable_column{};
    nullable_column.is_null = true;

    std::vector<RowData> rows{{nullable_column}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    UniqueEnforceExecutor::Config config{};
    config.reader = &reader;
    config.relation_id = RelationId{51U};
    config.index_id = IndexId{52U};
    config.constraint_name = "optional_code_key";
    config.key_extractor = make_single_column_key_extractor(0U);
    config.allow_null_keys = true;

    UniqueEnforceExecutor executor{std::move(child), config};

    auto context = make_context(4000U);
    TupleBuffer buffer{};

    executor.open(context);
    REQUIRE(executor.next(context, buffer));
    buffer.reset();
    CHECK_FALSE(executor.next(context, buffer));
    executor.close(context);
}

TEST_CASE("ForeignKeyCheckExecutor validates referenced rows")
{
    StubStorageReader reader;
    const auto key = encode_int64(999);

    TupleHeader header = visible_tuple(100U);
    reader.add_index_entry(IndexId{61U}, key, header);

    std::vector<RowData> rows{{ColumnData{key, false}}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    ForeignKeyCheckExecutor::Config config{};
    config.reader = &reader;
    config.referencing_relation_id = RelationId{60U};
    config.referenced_relation_id = RelationId{61U};
    config.referenced_index_id = IndexId{61U};
    config.constraint_name = "fk_orders_customer";
    config.key_extractor = make_fk_key_extractor(0U);

    ForeignKeyCheckExecutor executor{std::move(child), config};

    auto context = make_context(6000U);
    TupleBuffer buffer{};

    executor.open(context);
    REQUIRE(executor.next(context, buffer));
    buffer.reset();
    CHECK_FALSE(executor.next(context, buffer));
    executor.close(context);
}

TEST_CASE("ForeignKeyCheckExecutor rejects missing parent row")
{
    StubStorageReader reader;
    const auto key = encode_int64(404);

    std::vector<RowData> rows{{ColumnData{key, false}}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    ForeignKeyCheckExecutor::Config config{};
    config.reader = &reader;
    config.referencing_relation_id = RelationId{70U};
    config.referenced_relation_id = RelationId{71U};
    config.referenced_index_id = IndexId{72U};
    config.constraint_name = "fk_orders_product";
    config.key_extractor = make_fk_key_extractor(0U);

    ForeignKeyCheckExecutor executor{std::move(child), config};

    auto context = make_context(7000U);
    TupleBuffer buffer{};

    executor.open(context);
    CHECK_THROWS_AS(executor.next(context, buffer), std::runtime_error);
    executor.close(context);
}

TEST_CASE("ForeignKeyCheckExecutor skips NULL referencing keys")
{
    StubStorageReader reader;
    ColumnData nullable_column{};
    nullable_column.is_null = true;

    std::vector<RowData> rows{{nullable_column}};
    auto child = std::make_unique<ValuesExecutor>(rows);

    ForeignKeyCheckExecutor::Config config{};
    config.reader = &reader;
    config.referencing_relation_id = RelationId{80U};
    config.referenced_relation_id = RelationId{81U};
    config.referenced_index_id = IndexId{82U};
    config.constraint_name = "fk_nullable_parent";
    config.key_extractor = make_fk_key_extractor(0U);
    config.skip_when_null = true;

    ForeignKeyCheckExecutor executor{std::move(child), config};

    auto context = make_context(8000U);
    TupleBuffer buffer{};

    executor.open(context);
    REQUIRE(executor.next(context, buffer));
    buffer.reset();
    CHECK_FALSE(executor.next(context, buffer));
    executor.close(context);
}

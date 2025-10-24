#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using namespace bored::catalog;

CatalogTupleDescriptor tuple_descriptor(std::uint64_t xmin,
                                        std::uint64_t xmax,
                                        CatalogVisibilityFlag flags = CatalogVisibilityFlag::None)
{
    CatalogTupleDescriptor descriptor{};
    descriptor.xmin = xmin;
    descriptor.xmax = xmax;
    descriptor.visibility_flags = to_value(flags);
    return descriptor;
}

struct RelationStorage final {
    std::unordered_map<std::uint64_t, std::vector<std::vector<std::byte>>> tuples{};

    void add(RelationId relation_id, std::vector<std::byte> tuple)
    {
        tuples[relation_id.value].push_back(std::move(tuple));
    }

    bored::catalog::CatalogAccessor::RelationScanner make_scanner() const
    {
        return [this](RelationId relation_id, const CatalogAccessor::TupleCallback& callback) {
            auto it = tuples.find(relation_id.value);
            if (it == tuples.end()) {
                return;
            }
            for (const auto& buffer : it->second) {
                callback(std::span<const std::byte>(buffer.data(), buffer.size()));
            }
        };
    }
};

}  // namespace

TEST_CASE("Catalog tuple visibility honors MVCC snapshot")
{
    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 10U;
    snapshot.xmax = 40U;
    snapshot.in_progress = {18U, 28U};

    bored::txn::TransactionIdAllocatorStub allocator{35U};
    bored::txn::SnapshotManagerStub manager{snapshot};

    CatalogTransaction transaction({&allocator, &manager});

    auto committed = tuple_descriptor(5U, 0U);
    auto in_progress = tuple_descriptor(18U, 0U);
    auto deleted_committed = tuple_descriptor(12U, 19U);
    auto self_insert = tuple_descriptor(transaction.transaction_id(), 0U);
    auto self_delete = tuple_descriptor(8U, transaction.transaction_id());
    auto frozen = tuple_descriptor(2U, 25U, CatalogVisibilityFlag::Frozen);
    auto invalid = tuple_descriptor(4U, 0U, CatalogVisibilityFlag::Invalid);

    CHECK(transaction.is_visible(committed));
    CHECK_FALSE(transaction.is_visible(in_progress));
    CHECK_FALSE(transaction.is_visible(deleted_committed));
    CHECK(transaction.is_visible(self_insert));
    CHECK_FALSE(transaction.is_visible(self_delete));
    CHECK(transaction.is_visible(frozen));
    CHECK_FALSE(transaction.is_visible(invalid));
}

TEST_CASE("Catalog accessor caches relation scans and filters visibility")
{
    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 5U;
    snapshot.xmax = 20U;
    snapshot.in_progress = {9U};

    bored::txn::TransactionIdAllocatorStub allocator{30U};
    bored::txn::SnapshotManagerStub manager{snapshot};

    CatalogTransaction transaction({&allocator, &manager});

    RelationStorage storage;
    std::unordered_map<std::uint64_t, std::size_t> scan_counts;

    auto scanner = [&storage, &scan_counts](RelationId relation_id, const CatalogAccessor::TupleCallback& callback) {
        ++scan_counts[relation_id.value];
        auto it = storage.tuples.find(relation_id.value);
        if (it == storage.tuples.end()) {
            return;
        }
        for (const auto& buffer : it->second) {
            callback(std::span<const std::byte>(buffer.data(), buffer.size()));
        }
    };

    CatalogDatabaseDescriptor database_visible{tuple_descriptor(4U, 0U), kSystemDatabaseId, kSystemSchemaId, "system"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database_visible));

    CatalogDatabaseDescriptor database_hidden{tuple_descriptor(9U, 0U), DatabaseId{777U}, SchemaId{888U}, "hidden"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database_hidden));

    CatalogSchemaDescriptor schema_visible{tuple_descriptor(4U, 0U), kSystemSchemaId, kSystemDatabaseId, "system"};
    storage.add(kCatalogSchemasRelationId, serialize_catalog_schema(schema_visible));

    CatalogTableDescriptor table_visible{tuple_descriptor(4U, 0U), kCatalogTablesRelationId, kSystemSchemaId, CatalogTableType::Catalog, 123U, "catalog_tables"};
    storage.add(kCatalogTablesRelationId, serialize_catalog_table(table_visible));

    CatalogColumnDescriptor column_hidden{tuple_descriptor(12U, 15U), ColumnId{4000U}, table_visible.relation_id, CatalogColumnType::Utf8, 1U, "name"};
    storage.add(kCatalogColumnsRelationId, serialize_catalog_column(column_hidden));

    CatalogAccessor accessor({&transaction, scanner});

    auto db = accessor.database(kSystemDatabaseId);
    REQUIRE(db);
    CHECK(db->name == "system");

    auto missing = accessor.database(DatabaseId{777U});
    CHECK_FALSE(missing);

    // Cached scan should not increment again.
    (void)accessor.database(kSystemDatabaseId);
    CHECK(scan_counts[kCatalogDatabasesRelationId.value] == 1U);

    auto schemas = accessor.schemas(kSystemDatabaseId);
    REQUIRE(schemas.size() == 1U);
    CHECK(schemas.front().name == "system");

    auto tables = accessor.tables(kSystemSchemaId);
    REQUIRE(tables.size() == 1U);
    CHECK(tables.front().name == "catalog_tables");

    auto columns = accessor.columns(table_visible.relation_id);
    CHECK(columns.empty());

    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 1U);
    CHECK(scan_counts[kCatalogTablesRelationId.value] == 1U);
    CHECK(scan_counts[kCatalogColumnsRelationId.value] == 1U);
}

TEST_CASE("Catalog accessor snapshot differences affect visibility")
{
    RelationStorage storage;

    CatalogDatabaseDescriptor database_base{tuple_descriptor(2U, 0U), kSystemDatabaseId, kSystemSchemaId, "system"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database_base));

    CatalogDatabaseDescriptor database_new{tuple_descriptor(12U, 0U), DatabaseId{999U}, SchemaId{1000U}, "analytics"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database_new));

    bored::txn::TransactionIdAllocatorStub allocator_a{40U};
    bored::txn::Snapshot snapshot_a{};
    snapshot_a.xmin = 5U;
    snapshot_a.xmax = 50U;
    bored::txn::SnapshotManagerStub manager_a{snapshot_a};
    CatalogTransaction tx_a({&allocator_a, &manager_a});

    bored::txn::TransactionIdAllocatorStub allocator_b{50U};
    bored::txn::Snapshot snapshot_b{};
    snapshot_b.xmin = 5U;
    snapshot_b.xmax = 15U;
    snapshot_b.in_progress = {12U};
    bored::txn::SnapshotManagerStub manager_b{snapshot_b};
    CatalogTransaction tx_b({&allocator_b, &manager_b});

    CatalogAccessor accessor_a({&tx_a, storage.make_scanner()});
    CatalogAccessor accessor_b({&tx_b, storage.make_scanner()});

    auto dbs_a = accessor_a.schemas(kSystemDatabaseId);
    CHECK(dbs_a.empty());

    auto analytics_visible = accessor_a.database("analytics");
    CHECK(analytics_visible);

    auto analytics_hidden = accessor_b.database("analytics");
    CHECK_FALSE(analytics_hidden);
}

TEST_CASE("Wal replayer preserves catalog tuple mvcc metadata")
{
    using namespace bored::storage;

    CatalogTableDescriptor descriptor{tuple_descriptor(42U, 0U), RelationId{1234U}, SchemaId{56U}, CatalogTableType::Catalog, 789U, "test_table"};
    auto tuple_payload = serialize_catalog_table(descriptor);

    WalTupleMeta meta{};
    meta.page_id = 4242U;
    meta.slot_index = 0U;
    meta.tuple_length = static_cast<std::uint16_t>(tuple_payload.size());
    meta.row_id = descriptor.relation_id.value;

    const auto wal_payload_size = wal_tuple_insert_payload_size(static_cast<std::uint16_t>(tuple_payload.size()));
    std::vector<std::byte> wal_payload(wal_payload_size);
    REQUIRE(encode_wal_tuple_insert(std::span<std::byte>(wal_payload.data(), wal_payload.size()), meta, std::span<const std::byte>(tuple_payload.data(), tuple_payload.size())));

    WalRecoveryRecord insert_record{};
    insert_record.header.type = static_cast<std::uint16_t>(WalRecordType::CatalogInsert);
    insert_record.header.lsn = 1000U;
    insert_record.header.page_id = meta.page_id;
    insert_record.header.flags = static_cast<std::uint16_t>(WalRecordFlag::HasPayload);
    insert_record.header.total_length = static_cast<std::uint32_t>(sizeof(WalRecordHeader) + wal_payload.size());
    insert_record.payload = wal_payload;

    WalReplayContext context(PageType::Meta, nullptr);
    WalReplayer replayer(context);

    WalRecoveryPlan redo_plan{};
    redo_plan.redo.push_back(insert_record);
    REQUIRE_FALSE(replayer.apply_redo(redo_plan));

    auto page = context.get_page(meta.page_id);
    auto tuple_span = read_tuple(std::span<const std::byte>(page.data(), page.size()), meta.slot_index);
    auto view = decode_catalog_table(tuple_span);
    REQUIRE(view);
    CHECK(view->tuple.xmin == descriptor.tuple.xmin);
    CHECK(view->tuple.visibility_flags == descriptor.tuple.visibility_flags);

    WalRecoveryPlan undo_plan{};
    undo_plan.undo.push_back(insert_record);
    REQUIRE_FALSE(replayer.apply_undo(undo_plan));

    auto page_after = context.get_page(meta.page_id);
    auto tuple_after = read_tuple(std::span<const std::byte>(page_after.data(), page_after.size()), meta.slot_index);
    CHECK(tuple_after.empty());
}

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_format.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_replayer.hpp"
#include "bored/storage/wal_recovery.hpp"
#include "bored/txn/persistent_transaction_id_allocator.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstring>
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
    CatalogCache::instance().reset();

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

TEST_CASE("Catalog transaction captures snapshot from transaction manager")
{
    CatalogCache::instance().reset();

    bored::txn::PersistentTransactionIdAllocator allocator{100U};
    bored::txn::TransactionManager manager{allocator};

    auto first = manager.begin();
    auto second = manager.begin();

    CatalogTransactionConfig config{};
    config.id_allocator = &allocator;
    config.snapshot_manager = &manager;
    CatalogTransaction transaction{config};

    CHECK(transaction.transaction_id() == 102U);

    const auto snapshot = transaction.snapshot();
    CHECK(snapshot.xmin == first.id());
    CHECK(snapshot.xmax == manager.next_transaction_id());
    REQUIRE(snapshot.in_progress.size() == 2U);
    CHECK(snapshot.in_progress.front() == first.id());
    CHECK(snapshot.in_progress.back() == second.id());

    auto committed_tuple = tuple_descriptor(first.id() - 2U, 0U);
    CHECK(transaction.is_visible(committed_tuple));

    auto concurrent_tuple = tuple_descriptor(first.id(), 0U);
    CHECK_FALSE(transaction.is_visible(concurrent_tuple));

    auto self_tuple = tuple_descriptor(transaction.transaction_id(), 0U);
    CHECK(transaction.is_visible(self_tuple));

    auto future_tuple = tuple_descriptor(manager.next_transaction_id(), 0U);
    CHECK_FALSE(transaction.is_visible(future_tuple));

    manager.commit(first);
    manager.commit(second);
}

TEST_CASE("Catalog transaction refreshes snapshot via transaction manager when context is shared")
{
    CatalogCache::instance().reset();

    bored::txn::TransactionIdAllocatorStub allocator{10U};
    bored::txn::TransactionManager manager{allocator};

    auto ctx = manager.begin();
    CatalogTransactionConfig config{};
    config.transaction_context = &ctx;
    config.transaction_manager = &manager;
    CatalogTransaction transaction{config};

    auto initial_snapshot = transaction.snapshot();
    CHECK(initial_snapshot.in_progress.empty());

    auto other = manager.begin();
    (void)other;

    transaction.refresh_snapshot();
    auto refreshed = transaction.snapshot();
    REQUIRE(refreshed.in_progress.size() == 1U);
    CHECK(refreshed.in_progress.front() == other.id());

    manager.commit(other);

    transaction.refresh_snapshot();
    auto after_commit = transaction.snapshot();
    CHECK(after_commit.in_progress.empty());
}

TEST_CASE("Catalog accessor caches relation scans and filters visibility")
{
    CatalogCache::instance().reset();

    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 5U;
    snapshot.xmax = 30U;
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

    CatalogSequenceDescriptor sequence_visible{tuple_descriptor(4U, 0U),
                                               SequenceId{6000U},
                                               kSystemSchemaId,
                                               table_visible.relation_id,
                                               ColumnId{4100U},
                                               1U,
                                               1U,
                                               1,
                                               1U,
                                               1000U,
                                               32U,
                                               false,
                                               "catalog_tables_id_seq"};
    storage.add(kCatalogSequencesRelationId, serialize_catalog_sequence(sequence_visible));

    CatalogSequenceDescriptor sequence_hidden{tuple_descriptor(9U, 0U),
                                              SequenceId{6001U},
                                              kSystemSchemaId,
                                              table_visible.relation_id,
                                              ColumnId{4101U},
                                              1U,
                                              1U,
                                              1,
                                              1U,
                                              1000U,
                                              32U,
                                              false,
                                              "catalog_tables_id_seq_shadow"};
    storage.add(kCatalogSequencesRelationId, serialize_catalog_sequence(sequence_hidden));

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

    auto sequences_for_schema = accessor.sequences(kSystemSchemaId);
    REQUIRE(sequences_for_schema.size() == 1U);
    CHECK(sequences_for_schema.front().name == "catalog_tables_id_seq");

    auto sequences_for_table = accessor.sequences_for_relation(table_visible.relation_id);
    REQUIRE(sequences_for_table.size() == 1U);
    CHECK(sequences_for_table.front().sequence_id == sequence_visible.sequence_id);

    auto visible_sequence = accessor.sequence(sequence_visible.sequence_id);
    REQUIRE(visible_sequence);
    CHECK(visible_sequence->owning_relation_id == table_visible.relation_id);
    CHECK(visible_sequence->cache_size == 32U);

    auto hidden_sequence = accessor.sequence(sequence_hidden.sequence_id);
    CHECK_FALSE(hidden_sequence);

    auto all_sequences = accessor.sequences();
    REQUIRE(all_sequences.size() == 1U);
    CHECK(all_sequences.front().sequence_id == sequence_visible.sequence_id);

    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 1U);
    CHECK(scan_counts[kCatalogTablesRelationId.value] == 1U);
    CHECK(scan_counts[kCatalogColumnsRelationId.value] == 1U);
    CHECK(scan_counts[kCatalogSequencesRelationId.value] == 1U);
}

TEST_CASE("Catalog accessor cache invalidation reloads relation data")
{
    CatalogCache::instance().reset();

    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 5U;
    snapshot.xmax = 20U;

    bored::txn::TransactionIdAllocatorStub allocator{200U};
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

    CatalogDatabaseDescriptor database{tuple_descriptor(4U, 0U), kSystemDatabaseId, kSystemSchemaId, "system"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database));

    CatalogSchemaDescriptor base_schema{tuple_descriptor(4U, 0U), SchemaId{50U}, kSystemDatabaseId, "base"};
    storage.add(kCatalogSchemasRelationId, serialize_catalog_schema(base_schema));

    CatalogAccessor accessor({&transaction, scanner});

    auto schemas_initial = accessor.schemas(kSystemDatabaseId);
    REQUIRE(schemas_initial.size() == 1U);
    CHECK(schemas_initial.front().name == "base");
    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 1U);

    CatalogSchemaDescriptor new_schema{tuple_descriptor(18U, 0U), SchemaId{60U}, kSystemDatabaseId, "analytics"};
    storage.add(kCatalogSchemasRelationId, serialize_catalog_schema(new_schema));

    bored::txn::Snapshot refreshed{};
    refreshed.xmin = 5U;
    refreshed.xmax = 50U;
    manager.set_snapshot(refreshed);
    transaction.refresh_snapshot();

    CatalogAccessor::invalidate_relation(kCatalogSchemasRelationId);

    auto schemas_after = accessor.schemas(kSystemDatabaseId);
    REQUIRE(schemas_after.size() == 2U);
    std::vector<std::string> schema_names;
    schema_names.reserve(schemas_after.size());
    for (const auto& schema_descriptor : schemas_after) {
        schema_names.emplace_back(schema_descriptor.name.begin(), schema_descriptor.name.end());
    }
    CHECK(std::find(schema_names.begin(), schema_names.end(), "base") != schema_names.end());
    CHECK(std::find(schema_names.begin(), schema_names.end(), "analytics") != schema_names.end());
    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 2U);
}

TEST_CASE("Catalog accessor refreshes caches when transaction snapshot changes")
{
    CatalogCache::instance().reset();

    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 5U;
    snapshot.xmax = 20U;

    bored::txn::TransactionIdAllocatorStub allocator{220U};
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

    CatalogDatabaseDescriptor database{tuple_descriptor(4U, 0U), kSystemDatabaseId, kSystemSchemaId, "system"};
    storage.add(kCatalogDatabasesRelationId, serialize_catalog_database(database));

    const std::uint64_t pending_txn_id = 22U;
    CatalogSchemaDescriptor base_schema{tuple_descriptor(4U, 0U), SchemaId{70U}, kSystemDatabaseId, "base"};
    storage.add(kCatalogSchemasRelationId, serialize_catalog_schema(base_schema));
    CatalogSchemaDescriptor pending_schema{tuple_descriptor(pending_txn_id, 0U), SchemaId{80U}, kSystemDatabaseId, "analytics"};
    storage.add(kCatalogSchemasRelationId, serialize_catalog_schema(pending_schema));

    snapshot.in_progress = {pending_txn_id};
    manager.set_snapshot(snapshot);
    transaction.refresh_snapshot();

    CatalogAccessor accessor({&transaction, scanner});

    auto first_pass = accessor.schemas(kSystemDatabaseId);
    REQUIRE(first_pass.size() == 1U);
    CHECK(first_pass.front().name == "base");
    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 1U);

    bored::txn::Snapshot refreshed{};
    refreshed.xmin = 5U;
    refreshed.xmax = 60U;
    refreshed.in_progress.clear();
    manager.set_snapshot(refreshed);
    transaction.refresh_snapshot();

    auto second_pass = accessor.schemas(kSystemDatabaseId);
    REQUIRE(second_pass.size() == 2U);
    std::vector<std::string> schema_names;
    schema_names.reserve(second_pass.size());
    for (const auto& schema_descriptor : second_pass) {
        schema_names.emplace_back(schema_descriptor.name.begin(), schema_descriptor.name.end());
    }
    CHECK(std::find(schema_names.begin(), schema_names.end(), "base") != schema_names.end());
    CHECK(std::find(schema_names.begin(), schema_names.end(), "analytics") != schema_names.end());
    CHECK(scan_counts[kCatalogSchemasRelationId.value] == 1U);
}

TEST_CASE("Catalog accessor snapshot differences affect visibility")
{
    CatalogCache::instance().reset();

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

TEST_CASE("Catalog accessor reconciles concurrent version visibility across snapshots")
{
    CatalogCache::instance().reset();

    RelationStorage storage;

    const std::uint64_t update_txn_id = 60U;
    const SchemaId test_schema{600U};

    CatalogTableDescriptor committed_base{
        tuple_descriptor(8U, 0U), RelationId{200U}, test_schema, CatalogTableType::Heap, 900U, "committed"};
    CatalogTableDescriptor updated_old{
        tuple_descriptor(12U, update_txn_id), RelationId{201U}, test_schema, CatalogTableType::Heap, 901U, "orders_old"};
    CatalogTableDescriptor updated_new{
        tuple_descriptor(update_txn_id, 0U), RelationId{201U}, test_schema, CatalogTableType::Heap, 901U, "orders_new"};
    CatalogTableDescriptor pending_delete{
        tuple_descriptor(14U, update_txn_id), RelationId{202U}, test_schema, CatalogTableType::Heap, 902U, "to_delete"};
    CatalogTableDescriptor pending_insert{
        tuple_descriptor(update_txn_id, 0U), RelationId{203U}, test_schema, CatalogTableType::Heap, 903U, "new_table"};

    storage.add(kCatalogTablesRelationId, serialize_catalog_table(committed_base));
    storage.add(kCatalogTablesRelationId, serialize_catalog_table(updated_old));
    storage.add(kCatalogTablesRelationId, serialize_catalog_table(updated_new));
    storage.add(kCatalogTablesRelationId, serialize_catalog_table(pending_delete));
    storage.add(kCatalogTablesRelationId, serialize_catalog_table(pending_insert));

    bored::txn::Snapshot snapshot_during{};
    snapshot_during.xmin = 10U;
    snapshot_during.xmax = 80U;
    snapshot_during.in_progress = {update_txn_id};
    bored::txn::TransactionIdAllocatorStub allocator_during{120U};
    bored::txn::SnapshotManagerStub manager_during{snapshot_during};
    CatalogTransaction tx_during({&allocator_during, &manager_during});

    bored::txn::Snapshot snapshot_after{};
    snapshot_after.xmin = 10U;
    snapshot_after.xmax = 120U;
    bored::txn::TransactionIdAllocatorStub allocator_after{130U};
    bored::txn::SnapshotManagerStub manager_after{snapshot_after};
    CatalogTransaction tx_after({&allocator_after, &manager_after});

    CatalogAccessor accessor_during({&tx_during, storage.make_scanner()});
    CatalogAccessor accessor_after({&tx_after, storage.make_scanner()});

    const auto collect_names = [](const std::vector<CatalogTableDescriptor>& tables) {
        std::vector<std::string> names;
        names.reserve(tables.size());
        for (const auto& table : tables) {
            names.emplace_back(table.name.begin(), table.name.end());
        }
        return names;
    };

    auto names_during = collect_names(accessor_during.tables(test_schema));
    CHECK(std::find(names_during.begin(), names_during.end(), "committed") != names_during.end());
    CHECK(std::find(names_during.begin(), names_during.end(), "orders_old") != names_during.end());
    CHECK(std::find(names_during.begin(), names_during.end(), "to_delete") != names_during.end());
    CHECK(std::find(names_during.begin(), names_during.end(), "orders_new") == names_during.end());
    CHECK(std::find(names_during.begin(), names_during.end(), "new_table") == names_during.end());

    auto names_after = collect_names(accessor_after.tables(test_schema));
    CHECK(std::find(names_after.begin(), names_after.end(), "committed") != names_after.end());
    CHECK(std::find(names_after.begin(), names_after.end(), "orders_new") != names_after.end());
    CHECK(std::find(names_after.begin(), names_after.end(), "new_table") != names_after.end());
    CHECK(std::find(names_after.begin(), names_after.end(), "orders_old") == names_after.end());
    CHECK(std::find(names_after.begin(), names_after.end(), "to_delete") == names_after.end());
}

TEST_CASE("Wal replayer preserves catalog tuple mvcc metadata")
{
    CatalogCache::instance().reset();

    using namespace bored::storage;

    CatalogTableDescriptor descriptor{tuple_descriptor(42U, 0U), RelationId{1234U}, SchemaId{56U}, CatalogTableType::Catalog, 789U, "test_table"};
    auto tuple_payload = serialize_catalog_table(descriptor);

    bored::storage::TupleHeader tuple_header{};
    std::vector<std::byte> tuple_storage(bored::storage::tuple_storage_length(tuple_payload.size()));
    std::memcpy(tuple_storage.data(), &tuple_header, bored::storage::tuple_header_size());
    if (!tuple_payload.empty()) {
        std::memcpy(tuple_storage.data() + bored::storage::tuple_header_size(),
                    tuple_payload.data(),
                    tuple_payload.size());
    }

    WalTupleMeta meta{};
    meta.page_id = 4242U;
    meta.slot_index = 0U;
    meta.tuple_length = static_cast<std::uint16_t>(tuple_storage.size());
    meta.row_id = descriptor.relation_id.value;

    const auto wal_payload_size = wal_tuple_insert_payload_size(meta.tuple_length);
    std::vector<std::byte> wal_payload(wal_payload_size);
    REQUIRE(encode_wal_tuple_insert(std::span<std::byte>(wal_payload.data(), wal_payload.size()),
                                    meta,
                                    std::span<const std::byte>(tuple_storage.data(), tuple_storage.size())));

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

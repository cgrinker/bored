#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using namespace bored::catalog;

struct StubAllocator final : CatalogIdentifierAllocator {
    SchemaId allocate_schema_id() override
    {
        ++schema_calls;
        return SchemaId{next_schema_id++};
    }

    RelationId allocate_table_id() override
    {
        ++table_calls;
        return RelationId{next_table_id++};
    }

    IndexId allocate_index_id() override
    {
        ++index_calls;
        return IndexId{next_index_id++};
    }

    ColumnId allocate_column_id() override
    {
        ++column_calls;
        return ColumnId{next_column_id++};
    }

    std::uint64_t next_schema_id = 10'000U;
    std::uint64_t next_table_id = 20'000U;
    std::uint64_t next_index_id = 30'000U;
    std::uint64_t next_column_id = 40'000U;
    std::uint32_t schema_calls = 0U;
    std::uint32_t table_calls = 0U;
    std::uint32_t index_calls = 0U;
    std::uint32_t column_calls = 0U;
};

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
    }

    void apply(const CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            auto& relation = relations_[mutation.relation_id.value];
            switch (mutation.kind) {
            case CatalogMutationKind::Insert:
                REQUIRE(mutation.after.has_value());
                relation[mutation.row_id] = mutation.after->payload;
                break;
            case CatalogMutationKind::Update:
                REQUIRE(mutation.after.has_value());
                relation[mutation.row_id] = mutation.after->payload;
                break;
            case CatalogMutationKind::Delete:
                relation.erase(mutation.row_id);
                break;
            }
        }
    }

    [[nodiscard]] CatalogAccessor::RelationScanner make_scanner() const
    {
        return [this](RelationId relation_id, const CatalogAccessor::TupleCallback& callback) {
            auto it = relations_.find(relation_id.value);
            if (it == relations_.end()) {
                return;
            }
            for (const auto& [row_id, payload] : it->second) {
                (void)row_id;
                callback(std::span<const std::byte>(payload.data(), payload.size()));
            }
        };
    }

    std::unordered_map<std::uint64_t, Relation> relations_{};
};

struct WalRetentionSpy final {
    void notify(const CatalogMutationBatch& batch)
    {
        commit_lsns.push_back(batch.commit_lsn);
        std::size_t total_records = 0U;
        for (const auto& entry : batch.wal_records) {
            if (entry) {
                REQUIRE(entry->commit_lsn == batch.commit_lsn);
                total_records += entry->records.size();
            }
        }
        record_counts.push_back(total_records);
    }

    std::vector<std::uint64_t> commit_lsns{};
    std::vector<std::size_t> record_counts{};
};

struct CatalogIntegrationHarness final {
    CatalogIntegrationHarness()
    {
        CatalogCache::instance().reset();
    }

    InMemoryCatalogStorage storage;
    WalRetentionSpy retention;

    void apply_batch(const CatalogMutationBatch& batch)
    {
        storage.apply(batch);
        retention.notify(batch);
    }
};

[[nodiscard]] bored::txn::Snapshot make_relaxed_snapshot() noexcept
{
    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = std::numeric_limits<std::uint64_t>::max();
    return snapshot;
}

void seed_schema(CatalogIntegrationHarness& harness, SchemaId schema_id, DatabaseId database_id, std::string_view name)
{
    CatalogSchemaDescriptor descriptor{};
    descriptor.tuple.xmin = 1U;
    descriptor.tuple.xmax = 0U;
    descriptor.schema_id = schema_id;
    descriptor.database_id = database_id;
    descriptor.name = name;
    harness.storage.seed(kCatalogSchemasRelationId, schema_id.value, serialize_catalog_schema(descriptor));
}

void seed_table(CatalogIntegrationHarness& harness,
                RelationId relation_id,
                SchemaId schema_id,
                std::string_view name,
                CatalogTableType type = CatalogTableType::Heap,
                std::uint32_t root_page_id = 100U)
{
    CatalogTableDescriptor descriptor{};
    descriptor.tuple.xmin = 1U;
    descriptor.tuple.xmax = 0U;
    descriptor.relation_id = relation_id;
    descriptor.schema_id = schema_id;
    descriptor.table_type = type;
    descriptor.root_page_id = root_page_id;
    descriptor.name = name;
    harness.storage.seed(kCatalogTablesRelationId, relation_id.value, serialize_catalog_table(descriptor));
}

}  // namespace

TEST_CASE("Catalog DDL integration applies create operations on commit")
{
    CatalogIntegrationHarness harness;
    StubAllocator id_allocator;

    bored::txn::TransactionIdAllocatorStub txn_allocator{1'000U};
    auto snapshot = make_relaxed_snapshot();
    bored::txn::SnapshotManagerStub snapshot_manager{snapshot};
    CatalogTransaction transaction({&txn_allocator, &snapshot_manager});

    const std::uint64_t expected_commit_lsn = 0xABCDEFULL;
    CatalogMutator mutator({&transaction, [expected_commit_lsn]() { return expected_commit_lsn; }});

    SchemaId schema_id{500U};
    CreateSchemaRequest schema_request{};
    schema_request.database_id = kSystemDatabaseId;
    schema_request.schema_id = schema_id;
    schema_request.name = "analytics";
    CreateSchemaResult schema_result{};
    REQUIRE_FALSE(stage_create_schema(mutator, id_allocator, schema_request, schema_result));

    CreateTableRequest table_request{};
    table_request.schema_id = schema_id;
    table_request.relation_id = RelationId{900U};
    table_request.table_type = CatalogTableType::Heap;
    table_request.root_page_id = 777U;
    table_request.name = "events";
    table_request.columns = {
        ColumnDefinition{"id", CatalogColumnType::Int64},
        ColumnDefinition{"payload", CatalogColumnType::Utf8}
    };
    CreateTableResult table_result{};
    REQUIRE_FALSE(stage_create_table(mutator, id_allocator, table_request, table_result));

    CreateIndexRequest index_request{};
    index_request.relation_id = table_result.relation_id;
    index_request.index_type = CatalogIndexType::BTree;
    index_request.index_id = IndexId{2'000U};
    index_request.name = "events_id_idx";
    index_request.root_page_id = 501U;
    index_request.max_fanout = 96U;
    index_request.comparator = "int64_ascending";
    CreateIndexResult index_result{};
    REQUIRE_FALSE(stage_create_index(mutator, id_allocator, index_request, index_result));

    auto commit_ec = transaction.commit();
    REQUIRE_FALSE(commit_ec);
    REQUIRE(mutator.has_published_batch());

    auto batch = mutator.consume_published_batch();
    harness.apply_batch(batch);

    REQUIRE(harness.retention.commit_lsns.size() == 1U);
    CHECK(harness.retention.commit_lsns.front() == expected_commit_lsn);
    CHECK(harness.retention.record_counts.front() >= 3U);

    bored::txn::TransactionIdAllocatorStub reader_allocator{2'000U};
    CatalogTransaction reader_tx({&reader_allocator, &snapshot_manager});
    CatalogAccessor accessor({&reader_tx, harness.storage.make_scanner()});

    auto fetched_schema = accessor.schema(schema_id);
    REQUIRE(fetched_schema);
    CHECK(fetched_schema->name == schema_request.name);

    auto tables = accessor.tables(schema_id);
    REQUIRE(tables.size() == 1U);
    CHECK(tables.front().relation_id == table_result.relation_id);
    CHECK(tables.front().root_page_id == table_request.root_page_id);

    auto columns = accessor.columns(table_result.relation_id);
    REQUIRE(columns.size() == table_request.columns.size());
    CHECK(columns[0].name == "id");
    CHECK(columns[1].name == "payload");

    auto indexes = accessor.indexes(table_result.relation_id);
    REQUIRE(indexes.size() == 1U);
    CHECK(indexes.front().max_fanout == index_request.max_fanout);
    CHECK(indexes.front().comparator == index_request.comparator);

    auto index_span = accessor.columns(table_result.relation_id);
    (void)index_span;  // sanity coverage for repeated column access
}

TEST_CASE("Catalog DDL integration supports alter and drop cycles")
{
    CatalogIntegrationHarness harness;
    const SchemaId schema_id{600U};
    const RelationId relation_id{700U};

    seed_schema(harness, schema_id, kSystemDatabaseId, "analytics");
    seed_table(harness, relation_id, schema_id, "metrics");

    bored::txn::TransactionIdAllocatorStub reader_allocator{3'000U};
    auto snapshot = make_relaxed_snapshot();
    bored::txn::SnapshotManagerStub reader_snapshot_manager{snapshot};
    CatalogTransaction reader_tx({&reader_allocator, &reader_snapshot_manager});
    CatalogAccessor accessor({&reader_tx, harness.storage.make_scanner()});

    auto baseline_tables = accessor.tables(schema_id);
    REQUIRE(baseline_tables.size() == 1U);
    CHECK(baseline_tables.front().name == "metrics");

    bored::txn::TransactionIdAllocatorStub rename_allocator{3'500U};
    bored::txn::SnapshotManagerStub rename_snapshot_manager{snapshot};
    CatalogTransaction rename_tx({&rename_allocator, &rename_snapshot_manager});
    CatalogMutator rename_mutator({&rename_tx});

    auto relation_it = harness.storage.relations_.find(kCatalogTablesRelationId.value);
    REQUIRE(relation_it != harness.storage.relations_.end());
    auto payload_it = relation_it->second.find(relation_id.value);
    REQUIRE(payload_it != relation_it->second.end());
    std::vector<std::byte> before_payload = payload_it->second;
    auto before_view = decode_catalog_table(std::span<const std::byte>(before_payload.data(), before_payload.size()));
    REQUIRE(before_view);

    CatalogTableDescriptor updated_descriptor{};
    updated_descriptor.tuple = CatalogTupleBuilder::for_update(rename_tx, before_view->tuple);
    updated_descriptor.relation_id = before_view->relation_id;
    updated_descriptor.schema_id = before_view->schema_id;
    updated_descriptor.table_type = before_view->table_type;
    updated_descriptor.root_page_id = before_view->root_page_id;
    updated_descriptor.name = "metrics_v2";
    auto after_payload = serialize_catalog_table(updated_descriptor);

    rename_mutator.stage_update(kCatalogTablesRelationId,
                                relation_id.value,
                                before_view->tuple,
                                std::move(before_payload),
                                updated_descriptor.tuple,
                                std::move(after_payload));

    REQUIRE_FALSE(rename_tx.commit());
    REQUIRE(rename_mutator.has_published_batch());
    auto rename_batch = rename_mutator.consume_published_batch();
    harness.apply_batch(rename_batch);

    reader_snapshot_manager.set_snapshot(snapshot);
    reader_tx.refresh_snapshot();
    auto renamed_tables = accessor.tables(schema_id);
    REQUIRE(renamed_tables.size() == 1U);
    CHECK(renamed_tables.front().name == "metrics_v2");

    bored::txn::TransactionIdAllocatorStub drop_allocator{3'800U};
    bored::txn::SnapshotManagerStub drop_snapshot_manager{snapshot};
    CatalogTransaction drop_tx({&drop_allocator, &drop_snapshot_manager});
    CatalogMutator drop_mutator({&drop_tx});

    auto updated_payload_it = relation_it->second.find(relation_id.value);
    REQUIRE(updated_payload_it != relation_it->second.end());
    std::vector<std::byte> drop_payload = updated_payload_it->second;
    auto drop_view = decode_catalog_table(std::span<const std::byte>(drop_payload.data(), drop_payload.size()));
    REQUIRE(drop_view);

    drop_mutator.stage_delete(kCatalogTablesRelationId, relation_id.value, drop_view->tuple, std::move(drop_payload));
    REQUIRE_FALSE(drop_tx.commit());
    REQUIRE(drop_mutator.has_published_batch());
    auto drop_batch = drop_mutator.consume_published_batch();
    harness.apply_batch(drop_batch);

    reader_tx.refresh_snapshot();
    auto final_tables = accessor.tables(schema_id);
    CHECK(final_tables.empty());

    REQUIRE(harness.retention.commit_lsns.size() == 2U);
    CHECK(harness.retention.record_counts.size() == 2U);
    CHECK(harness.retention.record_counts[0] >= 1U);
    CHECK(harness.retention.record_counts[1] >= 1U);
}

TEST_CASE("Catalog DDL integration abort discards staged mutations")
{
    CatalogIntegrationHarness harness;
    StubAllocator id_allocator;

    bored::txn::TransactionIdAllocatorStub txn_allocator{4'000U};
    auto snapshot = make_relaxed_snapshot();
    bored::txn::SnapshotManagerStub snapshot_manager{snapshot};
    CatalogTransaction transaction({&txn_allocator, &snapshot_manager});
    CatalogMutator mutator({&transaction});

    CreateSchemaRequest schema_request{};
    schema_request.database_id = kSystemDatabaseId;
    schema_request.name = "transient";
    CreateSchemaResult schema_result{};
    REQUIRE_FALSE(stage_create_schema(mutator, id_allocator, schema_request, schema_result));

    REQUIRE(mutator.staged_mutations().size() == 1U);

    auto abort_ec = transaction.abort();
    REQUIRE_FALSE(abort_ec);
    CHECK(mutator.staged_mutations().empty());
    CHECK_FALSE(mutator.has_published_batch());
    CHECK(harness.retention.commit_lsns.empty());
    CHECK(harness.storage.relations_.empty());
}
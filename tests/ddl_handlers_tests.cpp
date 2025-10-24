#include "bored/ddl/ddl_handlers.hpp"

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/ddl/ddl_command.hpp"
#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/ddl/ddl_errors.hpp"
#include "bored/ddl/ddl_validation.hpp"
#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

using namespace bored;
using namespace bored::ddl;

namespace {

using catalog::CatalogAccessor;
using catalog::CatalogCache;
using catalog::CatalogColumnDescriptor;
using catalog::CatalogMutationBatch;
using catalog::CatalogSchemaDescriptor;
using catalog::CatalogTableDescriptor;
using catalog::CatalogTupleDescriptor;

struct StubIdentifierAllocator final : catalog::CatalogIdentifierAllocator {
    catalog::SchemaId allocate_schema_id() override
    {
        return catalog::SchemaId{++schema_ids};
    }

    catalog::RelationId allocate_table_id() override
    {
        return catalog::RelationId{++table_ids};
    }

    catalog::IndexId allocate_index_id() override
    {
        return catalog::IndexId{++index_ids};
    }

    catalog::ColumnId allocate_column_id() override
    {
        return catalog::ColumnId{++column_ids};
    }

    std::uint64_t schema_ids = 8'000U;
    std::uint64_t table_ids = 9'000U;
    std::uint64_t index_ids = 10'000U;
    std::uint64_t column_ids = 11'000U;
};

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void apply(const CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            auto& relation = relations_[mutation.relation_id.value];
            switch (mutation.kind) {
            case catalog::CatalogMutationKind::Insert:
                if (mutation.after) {
                    relation[mutation.row_id] = mutation.after->payload;
                }
                break;
            case catalog::CatalogMutationKind::Update:
                if (mutation.after) {
                    relation[mutation.row_id] = mutation.after->payload;
                }
                break;
            case catalog::CatalogMutationKind::Delete:
                relation.erase(mutation.row_id);
                break;
            }
        }
    }

    void seed(catalog::RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
    }

    void scan(catalog::RelationId relation_id, const CatalogAccessor::TupleCallback& callback) const
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return;
        }
        for (const auto& [row_id, payload] : it->second) {
            (void)row_id;
            callback(std::span<const std::byte>(payload.data(), payload.size()));
        }
    }

    [[nodiscard]] const Relation& relation(catalog::RelationId relation_id) const
    {
        static const Relation kEmpty{};
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return kEmpty;
        }
        return it->second;
    }

private:
    std::unordered_map<std::uint64_t, Relation> relations_{};
};

[[nodiscard]] txn::Snapshot make_snapshot() noexcept
{
    txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = std::numeric_limits<std::uint64_t>::max();
    return snapshot;
}

CatalogAccessor::RelationScanner make_scanner(const InMemoryCatalogStorage* storage)
{
    return [storage](catalog::RelationId relation_id, const CatalogAccessor::TupleCallback& callback) {
        storage->scan(relation_id, callback);
    };
}

CatalogTupleDescriptor make_seed_descriptor()
{
    catalog::CatalogTupleDescriptor tuple{};
    tuple.xmin = 1U;
    tuple.xmax = 0U;
    return tuple;
}

std::vector<std::byte> make_database_payload(catalog::DatabaseId database_id,
                                             catalog::SchemaId default_schema,
                                             std::string_view name)
{
    catalog::CatalogDatabaseDescriptor descriptor{};
    descriptor.tuple = make_seed_descriptor();
    descriptor.database_id = database_id;
    descriptor.default_schema_id = default_schema;
    descriptor.name = name;
    return catalog::serialize_catalog_database(descriptor);
}

std::vector<std::byte> make_schema_payload(catalog::SchemaId schema_id,
                                           catalog::DatabaseId database_id,
                                           std::string_view name)
{
    catalog::CatalogSchemaDescriptor descriptor{};
    descriptor.tuple = make_seed_descriptor();
    descriptor.schema_id = schema_id;
    descriptor.database_id = database_id;
    descriptor.name = name;
    return catalog::serialize_catalog_schema(descriptor);
}

std::vector<std::byte> make_table_payload(catalog::RelationId relation_id,
                                          catalog::SchemaId schema_id,
                                          std::string_view name)
{
    catalog::CatalogTableDescriptor descriptor{};
    descriptor.tuple = make_seed_descriptor();
    descriptor.relation_id = relation_id;
    descriptor.schema_id = schema_id;
    descriptor.table_type = catalog::CatalogTableType::Heap;
    descriptor.root_page_id = 42U;
    descriptor.name = name;
    return catalog::serialize_catalog_table(descriptor);
}

std::vector<std::byte> make_column_payload(catalog::ColumnId column_id,
                                           catalog::RelationId relation_id,
                                           std::string_view name,
                                           std::uint16_t ordinal)
{
    catalog::CatalogColumnDescriptor descriptor{};
    descriptor.tuple = make_seed_descriptor();
    descriptor.column_id = column_id;
    descriptor.relation_id = relation_id;
    descriptor.column_type = catalog::CatalogColumnType::Int64;
    descriptor.ordinal_position = ordinal;
    descriptor.name = name;
    return catalog::serialize_catalog_column(descriptor);
}

struct DispatcherHarness final {
    DispatcherHarness()
        : dispatcher_(make_dispatcher_config())
    {
        CatalogCache::instance().reset();
        register_catalog_handlers(dispatcher_);
    }

    DdlCommandResponse dispatch(const DdlCommand& command)
    {
        return dispatcher_.dispatch(command);
    }

    void seed_database(std::string_view name)
    {
        storage_.seed(catalog::kCatalogDatabasesRelationId, catalog::kSystemDatabaseId.value, make_database_payload(catalog::kSystemDatabaseId, catalog::kSystemSchemaId, name));
    }

    void seed_schema(catalog::SchemaId schema_id, catalog::DatabaseId database_id, std::string_view name)
    {
        storage_.seed(catalog::kCatalogSchemasRelationId, schema_id.value, make_schema_payload(schema_id, database_id, name));
    }

    void seed_table(catalog::RelationId relation_id, catalog::SchemaId schema_id, std::string_view name)
    {
        storage_.seed(catalog::kCatalogTablesRelationId, relation_id.value, make_table_payload(relation_id, schema_id, name));
    }

    void seed_column(catalog::ColumnId column_id, catalog::RelationId relation_id, std::string_view name, std::uint16_t ordinal)
    {
        storage_.seed(catalog::kCatalogColumnsRelationId, column_id.value, make_column_payload(column_id, relation_id, name, ordinal));
    }

    [[nodiscard]] const InMemoryCatalogStorage::Relation& schemas() const
    {
        return storage_.relation(catalog::kCatalogSchemasRelationId);
    }

    [[nodiscard]] const InMemoryCatalogStorage::Relation& tables() const
    {
        return storage_.relation(catalog::kCatalogTablesRelationId);
    }

    [[nodiscard]] const InMemoryCatalogStorage::Relation& columns() const
    {
        return storage_.relation(catalog::kCatalogColumnsRelationId);
    }

    StubIdentifierAllocator allocator{};

private:
    DdlCommandDispatcher::Config make_dispatcher_config()
    {
        DdlCommandDispatcher::Config config{};
        config.transaction_factory = [this]() {
            catalog::CatalogTransactionConfig cfg{&txn_allocator_, &snapshot_manager_};
            return std::make_unique<catalog::CatalogTransaction>(cfg);
        };

        config.mutator_factory = [this](catalog::CatalogTransaction& tx) {
            catalog::CatalogMutatorConfig mutator_cfg{};
            mutator_cfg.transaction = &tx;
            mutator_cfg.commit_lsn_provider = [] { return 0ULL; };
            auto mutator = std::make_unique<catalog::CatalogMutator>(mutator_cfg);
            auto* raw = mutator.get();
            tx.register_commit_hook([this, raw]() -> std::error_code {
                if (!raw->has_published_batch()) {
                    return {};
                }
                auto batch = raw->consume_published_batch();
                storage_.apply(batch);
                return {};
            });
            return mutator;
        };

        config.accessor_factory = [this](catalog::CatalogTransaction& tx) {
            CatalogAccessor::Config accessor_cfg{};
            accessor_cfg.transaction = &tx;
            accessor_cfg.scanner = make_scanner(&storage_);
            return std::make_unique<CatalogAccessor>(accessor_cfg);
        };

        config.identifier_allocator = &allocator;
        config.commit_lsn_provider = [] { return 0ULL; };
        config.telemetry_registry = &telemetry_registry_;
        config.telemetry_identifier = "ddl.handlers";
        return config;
    }

    InMemoryCatalogStorage storage_{};
    txn::TransactionIdAllocatorStub txn_allocator_{5'000U};
    txn::SnapshotManagerStub snapshot_manager_{make_snapshot()};
    DdlTelemetryRegistry telemetry_registry_{};
    DdlCommandDispatcher dispatcher_;
};

CatalogSchemaDescriptor decode_schema(const std::vector<std::byte>& payload)
{
    auto view = catalog::decode_catalog_schema(std::span<const std::byte>(payload.data(), payload.size()));
    REQUIRE(view);
    CatalogSchemaDescriptor descriptor{};
    descriptor.tuple = view->tuple;
    descriptor.schema_id = view->schema_id;
    descriptor.database_id = view->database_id;
    descriptor.name = view->name;
    return descriptor;
}

CatalogTableDescriptor decode_table(const std::vector<std::byte>& payload)
{
    auto view = catalog::decode_catalog_table(std::span<const std::byte>(payload.data(), payload.size()));
    REQUIRE(view);
    CatalogTableDescriptor descriptor{};
    descriptor.tuple = view->tuple;
    descriptor.relation_id = view->relation_id;
    descriptor.schema_id = view->schema_id;
    descriptor.table_type = view->table_type;
    descriptor.root_page_id = view->root_page_id;
    descriptor.name = view->name;
    return descriptor;
}

CatalogColumnDescriptor decode_column(const std::vector<std::byte>& payload)
{
    auto view = catalog::decode_catalog_column(std::span<const std::byte>(payload.data(), payload.size()));
    REQUIRE(view);
    CatalogColumnDescriptor descriptor{};
    descriptor.tuple = view->tuple;
    descriptor.column_id = view->column_id;
    descriptor.relation_id = view->relation_id;
    descriptor.column_type = view->column_type;
    descriptor.ordinal_position = view->ordinal_position;
    descriptor.name = view->name;
    return descriptor;
}

}  // namespace

TEST_CASE("Create schema handler stages schema insert")
{
    DispatcherHarness harness;
    harness.seed_database("system");

    CreateSchemaRequest request{};
    request.database_id = catalog::kSystemDatabaseId;
    request.name = "analytics";

    DdlCommand command = request;
    const auto response = harness.dispatch(command);

    REQUIRE(response.success);
    REQUIRE(harness.schemas().size() == 1U);
    const auto& entry = *harness.schemas().begin();
    const auto descriptor = decode_schema(entry.second);
    CHECK(descriptor.database_id == catalog::kSystemDatabaseId);
    CHECK(descriptor.name == request.name);
    CHECK(harness.allocator.schema_ids > 8'000U);
}

TEST_CASE("Create schema respects IF NOT EXISTS")
{
    DispatcherHarness harness;
    harness.seed_database("system");

    CreateSchemaRequest request{};
    request.database_id = catalog::kSystemDatabaseId;
    request.name = "analytics";

    DdlCommand create = request;
    REQUIRE(harness.dispatch(create).success);

    request.if_not_exists = true;
    DdlCommand duplicate = request;
    const auto response = harness.dispatch(duplicate);

    CHECK(response.success);
    CHECK(harness.schemas().size() == 1U);
}

TEST_CASE("Create table handler stages table and columns")
{
    DispatcherHarness harness;
    harness.seed_database("system");
    catalog::SchemaId schema_id{3U};
    harness.seed_schema(schema_id, catalog::kSystemDatabaseId, "analytics");

    CreateTableRequest request{};
    request.schema_id = schema_id;
    request.name = "metrics";
    request.table_type = catalog::CatalogTableType::Heap;
    request.root_page_id = 77U;
    request.columns = {
        catalog::ColumnDefinition{"id", catalog::CatalogColumnType::Int64},
        catalog::ColumnDefinition{"name", catalog::CatalogColumnType::Utf8}
    };

    DdlCommand command = request;
    const auto response = harness.dispatch(command);

    REQUIRE(response.success);
    REQUIRE(harness.tables().size() == 1U);
    const auto table_descriptor = decode_table(harness.tables().begin()->second);
    CHECK(table_descriptor.schema_id == schema_id);
    CHECK(table_descriptor.name == request.name);

    REQUIRE(harness.columns().size() == request.columns.size());
    std::vector<std::string> column_names;
    column_names.reserve(harness.columns().size());
    for (const auto& [row_id, payload] : harness.columns()) {
        (void)row_id;
        auto column_descriptor = decode_column(payload);
        column_names.emplace_back(column_descriptor.name);
        CHECK(column_descriptor.relation_id == table_descriptor.relation_id);
    }
    CHECK(std::find(column_names.begin(), column_names.end(), "id") != column_names.end());
    CHECK(std::find(column_names.begin(), column_names.end(), "name") != column_names.end());
}

TEST_CASE("Drop schema fails when tables exist")
{
    DispatcherHarness harness;
    harness.seed_database("system");
    const catalog::SchemaId schema_id{4U};
    harness.seed_schema(schema_id, catalog::kSystemDatabaseId, "analytics");
    harness.seed_table(catalog::RelationId{12'000U}, schema_id, "metrics");

    DropSchemaRequest request{};
    request.database_id = catalog::kSystemDatabaseId;
    request.name = "analytics";

    DdlCommand command = request;
    const auto response = harness.dispatch(command);

    CHECK_FALSE(response.success);
    CHECK(response.error == make_error_code(DdlErrc::ValidationFailed));
    CHECK(harness.schemas().size() == 1U);
}

TEST_CASE("Drop table removes table and columns")
{
    DispatcherHarness harness;
    harness.seed_database("system");
    const catalog::SchemaId schema_id{5U};
    harness.seed_schema(schema_id, catalog::kSystemDatabaseId, "analytics");
    const catalog::RelationId table_id{13'000U};
    harness.seed_table(table_id, schema_id, "metrics");
    harness.seed_column(catalog::ColumnId{21'000U}, table_id, "id", 1U);
    harness.seed_column(catalog::ColumnId{21'001U}, table_id, "name", 2U);

    DropTableRequest request{};
    request.schema_id = schema_id;
    request.name = "metrics";

    DdlCommand command = request;
    const auto response = harness.dispatch(command);

    REQUIRE(response.success);
    CHECK(harness.tables().empty());
    CHECK(harness.columns().empty());
}

TEST_CASE("Drop table IF EXISTS suppresses missing errors")
{
    DispatcherHarness harness;
    harness.seed_database("system");
    const catalog::SchemaId schema_id{6U};
    harness.seed_schema(schema_id, catalog::kSystemDatabaseId, "analytics");

    DropTableRequest request{};
    request.schema_id = schema_id;
    request.name = "missing";
    request.if_exists = true;

    DdlCommand command = request;
    const auto response = harness.dispatch(command);

    CHECK(response.success);
}

}  // namespace

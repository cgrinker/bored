#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include "bored/txn/transaction_types.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using namespace bored::catalog;

struct InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
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

[[nodiscard]] bored::txn::Snapshot relaxed_snapshot() noexcept
{
    bored::txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = 1'000U;
    return snapshot;
}

}  // namespace

TEST_CASE("CatalogAccessor enumerates view metadata")
{
    CatalogCache::instance().reset();

    InMemoryCatalogStorage storage;

    CatalogDatabaseDescriptor database{};
    database.tuple.xmin = 1U;
    database.database_id = kSystemDatabaseId;
    database.default_schema_id = kSystemSchemaId;
    database.name = "system";

    CatalogSchemaDescriptor schema{};
    schema.tuple.xmin = 1U;
    schema.schema_id = SchemaId{200U};
    schema.database_id = database.database_id;
    schema.name = "analytics";

    CatalogTableDescriptor table{};
    table.tuple.xmin = 1U;
    table.relation_id = RelationId{400U};
    table.schema_id = schema.schema_id;
    table.table_type = CatalogTableType::View;
    table.root_page_id = 0U;
    table.name = "recent_metrics";

    CatalogViewDescriptor view{};
    view.tuple.xmin = 1U;
    view.relation_id = table.relation_id;
    view.definition = "SELECT * FROM metrics";

    storage.seed(kCatalogDatabasesRelationId, database.database_id.value, serialize_catalog_database(database));
    storage.seed(kCatalogSchemasRelationId, schema.schema_id.value, serialize_catalog_schema(schema));
    storage.seed(kCatalogTablesRelationId, table.relation_id.value, serialize_catalog_table(table));
    storage.seed(kCatalogViewsRelationId, view.relation_id.value, serialize_catalog_view(view));

    bored::txn::TransactionIdAllocatorStub allocator{100U};
    bored::txn::SnapshotManagerStub snapshot_manager{relaxed_snapshot()};
    CatalogTransaction transaction({&allocator, &snapshot_manager});

    CatalogAccessor accessor({&transaction, storage.make_scanner()});

    SECTION("Lookup by relation identifier")
    {
        auto found = accessor.view(table.relation_id);
        REQUIRE(found.has_value());
        CHECK(found->relation_id == table.relation_id);
        CHECK(found->schema_id == table.schema_id);
        CHECK(found->name == table.name);
        CHECK(found->definition == view.definition);
    }

    SECTION("Schema-scoped enumeration")
    {
        auto by_schema = accessor.views(schema.schema_id);
        REQUIRE(by_schema.size() == 1U);
        CHECK(by_schema.front().name == table.name);
    }

    SECTION("Global enumeration")
    {
        auto all_views = accessor.views();
        REQUIRE(all_views.size() == 1U);
        CHECK(all_views.front().definition == view.definition);
    }

    SECTION("Missing lookups return empty results")
    {
        CHECK_FALSE(accessor.view(RelationId{table.relation_id.value + 1U}).has_value());
        CHECK(accessor.views(SchemaId{schema.schema_id.value + 1U}).empty());
    }
}

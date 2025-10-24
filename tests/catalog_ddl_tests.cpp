#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
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

    std::uint64_t next_schema_id = 5000U;
    std::uint64_t next_table_id = 6000U;
    std::uint64_t next_index_id = 7000U;
    std::uint64_t next_column_id = 8000U;
    std::uint32_t schema_calls = 0U;
    std::uint32_t table_calls = 0U;
    std::uint32_t index_calls = 0U;
    std::uint32_t column_calls = 0U;
};

}  // namespace

TEST_CASE("Catalog DDL stages create schema")
{
    bored::txn::TransactionIdAllocatorStub allocator_stub{2000U};
    bored::txn::SnapshotManagerStub snapshot_stub{};
    CatalogTransaction transaction({&allocator_stub, &snapshot_stub});
    CatalogMutator mutator({&transaction});
    StubAllocator id_allocator{};

    CreateSchemaRequest request{};
    request.database_id = kSystemDatabaseId;
    request.name = "analytics";

    CreateSchemaResult result{};
    REQUIRE_FALSE(stage_create_schema(mutator, id_allocator, request, result));

    CHECK(result.schema_id.is_valid());
    CHECK(id_allocator.schema_calls == 1U);

    const auto& mutations = mutator.staged_mutations();
    REQUIRE(mutations.size() == 1U);
    const auto& mutation = mutations[0];
    CHECK(mutation.kind == CatalogMutationKind::Insert);
    CHECK(mutation.relation_id == kCatalogSchemasRelationId);
    CHECK(mutation.row_id == result.schema_id.value);

    REQUIRE(mutation.after);
    auto view = decode_catalog_schema(std::span<const std::byte>(mutation.after->payload.data(), mutation.after->payload.size()));
    REQUIRE(view);
    CHECK(view->schema_id == result.schema_id);
    CHECK(view->database_id == kSystemDatabaseId);
    CHECK(view->name == request.name);
}

TEST_CASE("Catalog DDL stages create table with columns")
{
    bored::txn::TransactionIdAllocatorStub allocator_stub{3000U};
    bored::txn::SnapshotManagerStub snapshot_stub{};
    CatalogTransaction transaction({&allocator_stub, &snapshot_stub});
    CatalogMutator mutator({&transaction});
    StubAllocator id_allocator{};

    CreateTableRequest request{};
    request.schema_id = SchemaId{42U};
    request.name = "metrics";
    request.table_type = CatalogTableType::Heap;
    request.root_page_id = 99U;
    request.columns = {
        ColumnDefinition{"id", CatalogColumnType::Int64},
        ColumnDefinition{"name", CatalogColumnType::Utf8},
        ColumnDefinition{"state", CatalogColumnType::UInt16, std::nullopt, static_cast<std::uint16_t>(5U)}
    };

    CreateTableResult result{};
    REQUIRE_FALSE(stage_create_table(mutator, id_allocator, request, result));

    CHECK(result.relation_id.is_valid());
    CHECK(id_allocator.table_calls == 1U);
    CHECK(id_allocator.column_calls == request.columns.size());
    REQUIRE(result.column_ids.size() == request.columns.size());

    const auto& mutations = mutator.staged_mutations();
    REQUIRE(mutations.size() == 1U + request.columns.size());

    const auto& table_mutation = mutations[0];
    CHECK(table_mutation.kind == CatalogMutationKind::Insert);
    CHECK(table_mutation.relation_id == kCatalogTablesRelationId);
    CHECK(table_mutation.row_id == result.relation_id.value);
    REQUIRE(table_mutation.after);

    auto table_view = decode_catalog_table(std::span<const std::byte>(table_mutation.after->payload.data(), table_mutation.after->payload.size()));
    REQUIRE(table_view);
    CHECK(table_view->relation_id == result.relation_id);
    CHECK(table_view->schema_id == request.schema_id);
    CHECK(table_view->root_page_id == request.root_page_id);
    CHECK(table_view->table_type == request.table_type);
    CHECK(table_view->name == request.name);

    std::vector<std::uint16_t> expected_ordinals;
    expected_ordinals.reserve(request.columns.size());
    std::uint16_t expected_seed = 1U;
    for (const auto& column : request.columns) {
        const auto ordinal = column.ordinal.value_or(expected_seed);
        expected_ordinals.push_back(ordinal);
        expected_seed = static_cast<std::uint16_t>(std::max<std::uint16_t>(expected_seed, static_cast<std::uint16_t>(ordinal + 1U)));
    }

    for (std::size_t index = 0; index < request.columns.size(); ++index) {
        const auto& column_mutation = mutations[index + 1U];
        CHECK(column_mutation.kind == CatalogMutationKind::Insert);
        CHECK(column_mutation.relation_id == kCatalogColumnsRelationId);
        REQUIRE(column_mutation.after);

        auto column_view = decode_catalog_column(std::span<const std::byte>(column_mutation.after->payload.data(), column_mutation.after->payload.size()));
        REQUIRE(column_view);
        CHECK(column_view->relation_id == result.relation_id);
        CHECK(column_view->column_type == request.columns[index].column_type);
        CHECK(column_view->name == request.columns[index].name);

        const auto expected_id = result.column_ids[index];
        CHECK(column_view->column_id == expected_id);
        CHECK(column_view->ordinal_position == expected_ordinals[index]);
    }
}

TEST_CASE("Catalog DDL stages create index with provided identifier")
{
    bored::txn::TransactionIdAllocatorStub allocator_stub{4000U};
    bored::txn::SnapshotManagerStub snapshot_stub{};
    CatalogTransaction transaction({&allocator_stub, &snapshot_stub});
    CatalogMutator mutator({&transaction});
    StubAllocator id_allocator{};

    CreateIndexRequest request{};
    request.relation_id = RelationId{6000U};
    request.name = "metrics_name_idx";
    request.index_type = CatalogIndexType::BTree;
    request.index_id = IndexId{9876U};
    request.root_page_id = 4096U;

    CreateIndexResult result{};
    REQUIRE_FALSE(stage_create_index(mutator, id_allocator, request, result));

    CHECK(result.index_id.value == request.index_id->value);
    CHECK(result.root_page_id == *request.root_page_id);
    CHECK(id_allocator.index_calls == 0U);

    const auto& mutations = mutator.staged_mutations();
    REQUIRE(mutations.size() == 1U);
    const auto& mutation = mutations[0];
    CHECK(mutation.relation_id == kCatalogIndexesRelationId);
    CHECK(mutation.row_id == result.index_id.value);
    REQUIRE(mutation.after);

    auto view = decode_catalog_index(std::span<const std::byte>(mutation.after->payload.data(), mutation.after->payload.size()));
    REQUIRE(view);
    CHECK(view->index_id == result.index_id);
    CHECK(view->relation_id == request.relation_id);
    CHECK(view->index_type == request.index_type);
    CHECK(view->root_page_id == *request.root_page_id);
    CHECK(view->name == request.name);
}

#include "bored/catalog/catalog_bootstrapper.hpp"

#include "bored/catalog/catalog_mvcc.hpp"
#include <array>
#include <span>

namespace bored::catalog {

namespace {

constexpr CatalogTupleDescriptor bootstrap_tuple() noexcept
{
    CatalogTupleDescriptor tuple{};
    tuple.xmin = kCatalogBootstrapTxnId;
    tuple.xmax = 0U;
    tuple.visibility_flags = to_value(CatalogVisibilityFlag::Frozen);
    return tuple;
}

std::error_code append_tuple(bored::storage::PageManager* manager,
                             std::span<std::byte> page,
                             std::span<const std::byte> payload,
                             std::uint64_t row_id)
{
    bored::storage::PageManager::TupleInsertResult insert{};
    auto ec = manager->insert_tuple(page, payload, row_id, insert);
    if (ec) {
        return ec;
    }
    return {};
}

}  // namespace

CatalogBootstrapper::CatalogBootstrapper(CatalogBootstrapConfig config)
    : config_{std::move(config)}
{
}

std::error_code CatalogBootstrapper::run(CatalogBootstrapArtifacts& artifacts) const
{
    artifacts.pages.clear();
    if (config_.page_manager == nullptr) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto make_page = [&artifacts](std::uint32_t page_id) {
        auto& slot = artifacts.pages[page_id];
        slot.fill(std::byte{0});
        return std::span<std::byte>(slot.data(), slot.size());
    };

    auto db_page = make_page(kCatalogDatabasesPageId);
    if (auto ec = config_.page_manager->initialize_page(db_page, bored::storage::PageType::Meta, kCatalogDatabasesPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_databases(db_page); ec) {
        return ec;
    }

    auto schema_page = make_page(kCatalogSchemasPageId);
    if (auto ec = config_.page_manager->initialize_page(schema_page, bored::storage::PageType::Meta, kCatalogSchemasPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_schemas(schema_page); ec) {
        return ec;
    }

    auto table_page = make_page(kCatalogTablesPageId);
    if (auto ec = config_.page_manager->initialize_page(table_page, bored::storage::PageType::Meta, kCatalogTablesPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_tables(table_page); ec) {
        return ec;
    }

    auto column_page = make_page(kCatalogColumnsPageId);
    if (auto ec = config_.page_manager->initialize_page(column_page, bored::storage::PageType::Meta, kCatalogColumnsPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_columns(column_page); ec) {
        return ec;
    }

    auto index_page = make_page(kCatalogIndexesPageId);
    if (auto ec = config_.page_manager->initialize_page(index_page, bored::storage::PageType::Meta, kCatalogIndexesPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_indexes(index_page); ec) {
        return ec;
    }

    auto constraint_page = make_page(kCatalogConstraintsPageId);
    if (auto ec = config_.page_manager->initialize_page(constraint_page, bored::storage::PageType::Meta, kCatalogConstraintsPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_constraints(constraint_page); ec) {
        return ec;
    }

    auto sequence_page = make_page(kCatalogSequencesPageId);
    if (auto ec = config_.page_manager->initialize_page(sequence_page, bored::storage::PageType::Meta, kCatalogSequencesPageId); ec) {
        return ec;
    }
    if (auto ec = bootstrap_sequences(sequence_page); ec) {
        return ec;
    }

    if (config_.flush_wal) {
        if (auto ec = config_.page_manager->flush_wal(); ec) {
            return ec;
        }
    }

    return {};
}

std::error_code CatalogBootstrapper::bootstrap_databases(std::span<std::byte> page) const
{
    CatalogDatabaseDescriptor descriptor{};
    descriptor.tuple = bootstrap_tuple();
    descriptor.database_id = kSystemDatabaseId;
    descriptor.default_schema_id = kSystemSchemaId;
    descriptor.name = "system";

    auto payload = serialize_catalog_database(descriptor);
    return append_tuple(config_.page_manager, page, payload, descriptor.database_id.value);
}

std::error_code CatalogBootstrapper::bootstrap_schemas(std::span<std::byte> page) const
{
    CatalogSchemaDescriptor descriptor{};
    descriptor.tuple = bootstrap_tuple();
    descriptor.schema_id = kSystemSchemaId;
    descriptor.database_id = kSystemDatabaseId;
    descriptor.name = "system";

    auto payload = serialize_catalog_schema(descriptor);
    return append_tuple(config_.page_manager, page, payload, descriptor.schema_id.value);
}

std::error_code CatalogBootstrapper::bootstrap_tables(std::span<std::byte> page) const
{
    const CatalogTupleDescriptor tuple = bootstrap_tuple();
    const std::array tables{
        CatalogTableDescriptor{tuple, kCatalogDatabasesRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogDatabasesPageId, "catalog_databases"},
        CatalogTableDescriptor{tuple, kCatalogSchemasRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogSchemasPageId, "catalog_schemas"},
        CatalogTableDescriptor{tuple, kCatalogTablesRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogTablesPageId, "catalog_tables"},
        CatalogTableDescriptor{tuple, kCatalogColumnsRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogColumnsPageId, "catalog_columns"},
        CatalogTableDescriptor{tuple, kCatalogIndexesRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogIndexesPageId, "catalog_indexes"},
        CatalogTableDescriptor{tuple, kCatalogConstraintsRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogConstraintsPageId, "catalog_constraints"},
        CatalogTableDescriptor{tuple, kCatalogSequencesRelationId, kSystemSchemaId, CatalogTableType::Catalog, kCatalogSequencesPageId, "catalog_sequences"}
    };

    for (const auto& table : tables) {
        auto payload = serialize_catalog_table(table);
        if (auto ec = append_tuple(config_.page_manager, page, payload, table.relation_id.value); ec) {
            return ec;
        }
    }

    return {};
}

std::error_code CatalogBootstrapper::bootstrap_columns(std::span<std::byte> page) const
{
    const CatalogTupleDescriptor tuple = bootstrap_tuple();
    const std::array<CatalogColumnDescriptor, 42> columns{
        CatalogColumnDescriptor{tuple, kCatalogDatabasesIdColumnId, kCatalogDatabasesRelationId, CatalogColumnType::Int64, 1U, "database_id"},
        CatalogColumnDescriptor{tuple, kCatalogDatabasesNameColumnId, kCatalogDatabasesRelationId, CatalogColumnType::Utf8, 2U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogSchemasIdColumnId, kCatalogSchemasRelationId, CatalogColumnType::Int64, 1U, "schema_id"},
        CatalogColumnDescriptor{tuple, kCatalogSchemasDatabaseColumnId, kCatalogSchemasRelationId, CatalogColumnType::Int64, 2U, "database_id"},
        CatalogColumnDescriptor{tuple, kCatalogSchemasNameColumnId, kCatalogSchemasRelationId, CatalogColumnType::Utf8, 3U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogTablesIdColumnId, kCatalogTablesRelationId, CatalogColumnType::Int64, 1U, "table_id"},
        CatalogColumnDescriptor{tuple, kCatalogTablesSchemaColumnId, kCatalogTablesRelationId, CatalogColumnType::Int64, 2U, "schema_id"},
        CatalogColumnDescriptor{tuple, kCatalogTablesTypeColumnId, kCatalogTablesRelationId, CatalogColumnType::UInt16, 3U, "table_type"},
        CatalogColumnDescriptor{tuple, kCatalogTablesPageColumnId, kCatalogTablesRelationId, CatalogColumnType::UInt32, 4U, "root_page_id"},
        CatalogColumnDescriptor{tuple, kCatalogTablesNameColumnId, kCatalogTablesRelationId, CatalogColumnType::Utf8, 5U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogColumnsIdColumnId, kCatalogColumnsRelationId, CatalogColumnType::Int64, 1U, "column_id"},
        CatalogColumnDescriptor{tuple, kCatalogColumnsTableColumnId, kCatalogColumnsRelationId, CatalogColumnType::Int64, 2U, "table_id"},
        CatalogColumnDescriptor{tuple, kCatalogColumnsTypeColumnId, kCatalogColumnsRelationId, CatalogColumnType::UInt32, 3U, "column_type"},
        CatalogColumnDescriptor{tuple, kCatalogColumnsOrdinalColumnId, kCatalogColumnsRelationId, CatalogColumnType::UInt16, 4U, "ordinal_position"},
        CatalogColumnDescriptor{tuple, kCatalogColumnsNameColumnId, kCatalogColumnsRelationId, CatalogColumnType::Utf8, 5U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesIdColumnId, kCatalogIndexesRelationId, CatalogColumnType::Int64, 1U, "index_id"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesTableColumnId, kCatalogIndexesRelationId, CatalogColumnType::Int64, 2U, "table_id"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesTypeColumnId, kCatalogIndexesRelationId, CatalogColumnType::UInt16, 3U, "index_type"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesRootPageColumnId, kCatalogIndexesRelationId, CatalogColumnType::UInt32, 4U, "root_page_id"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesComparatorColumnId, kCatalogIndexesRelationId, CatalogColumnType::Utf8, 5U, "comparator"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesFanoutColumnId, kCatalogIndexesRelationId, CatalogColumnType::UInt16, 6U, "max_fanout"},
        CatalogColumnDescriptor{tuple, kCatalogIndexesNameColumnId, kCatalogIndexesRelationId, CatalogColumnType::Utf8, 7U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsIdColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Int64, 1U, "constraint_id"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsTableColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Int64, 2U, "table_id"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsTypeColumnId, kCatalogConstraintsRelationId, CatalogColumnType::UInt16, 3U, "constraint_type"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsBackingIndexColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Int64, 4U, "backing_index_id"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsReferencedTableColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Int64, 5U, "referenced_table_id"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsKeyColumnsColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Utf8, 6U, "key_columns"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsReferencedColumnsColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Utf8, 7U, "referenced_columns"},
        CatalogColumnDescriptor{tuple, kCatalogConstraintsNameColumnId, kCatalogConstraintsRelationId, CatalogColumnType::Utf8, 8U, "name"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesIdColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 1U, "sequence_id"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesSchemaColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 2U, "schema_id"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesOwningTableColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 3U, "owning_table_id"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesOwningColumnColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 4U, "owning_column_id"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesStartValueColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 5U, "start_value"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesNextValueColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 6U, "next_value"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesIncrementColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 7U, "increment"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesMinValueColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 8U, "min_value"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesMaxValueColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 9U, "max_value"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesCacheSizeColumnId, kCatalogSequencesRelationId, CatalogColumnType::Int64, 10U, "cache_size"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesCycleFlagColumnId, kCatalogSequencesRelationId, CatalogColumnType::UInt16, 11U, "cycle_flag"},
        CatalogColumnDescriptor{tuple, kCatalogSequencesNameColumnId, kCatalogSequencesRelationId, CatalogColumnType::Utf8, 12U, "name"}
    };

    for (const auto& column : columns) {
        auto payload = serialize_catalog_column(column);
        if (auto ec = append_tuple(config_.page_manager, page, payload, column.column_id.value); ec) {
            return ec;
        }
    }

    return {};
}

std::error_code CatalogBootstrapper::bootstrap_indexes(std::span<std::byte> page) const
{
    const CatalogTupleDescriptor tuple = bootstrap_tuple();
    const std::array<CatalogIndexDescriptor, 7> indexes{
        CatalogIndexDescriptor{tuple, kCatalogDatabasesNameIndexId, kCatalogDatabasesRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_databases_name"},
        CatalogIndexDescriptor{tuple, kCatalogSchemasNameIndexId, kCatalogSchemasRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_schemas_name"},
        CatalogIndexDescriptor{tuple, kCatalogTablesNameIndexId, kCatalogTablesRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_tables_name"},
        CatalogIndexDescriptor{tuple, kCatalogColumnsNameIndexId, kCatalogColumnsRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_columns_name"},
        CatalogIndexDescriptor{tuple, kCatalogIndexesNameIndexId, kCatalogIndexesRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_indexes_name"},
        CatalogIndexDescriptor{tuple, kCatalogConstraintsNameIndexId, kCatalogConstraintsRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_constraints_name"},
        CatalogIndexDescriptor{tuple, kCatalogSequencesNameIndexId, kCatalogSequencesRelationId, CatalogIndexType::BTree, 0U, 0U, {}, "catalog_sequences_name"}
    };

    for (const auto& index : indexes) {
        auto payload = serialize_catalog_index(index);
        if (auto ec = append_tuple(config_.page_manager, page, payload, index.index_id.value); ec) {
            return ec;
        }
    }

    return {};
}

std::error_code CatalogBootstrapper::bootstrap_constraints(std::span<std::byte> page) const
{
    (void)page;
    return {};
}

std::error_code CatalogBootstrapper::bootstrap_sequences(std::span<std::byte> page) const
{
    (void)page;
    return {};
}

}  // namespace bored::catalog

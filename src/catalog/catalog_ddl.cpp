#include "bored/catalog/catalog_ddl.hpp"

#include <algorithm>
#include <limits>

namespace bored::catalog {

namespace {

[[nodiscard]] bool is_valid_name(std::string_view name) noexcept
{
    return !name.empty();
}

[[nodiscard]] std::error_code invalid_argument()
{
    return std::make_error_code(std::errc::invalid_argument);
}

[[nodiscard]] ColumnId next_column_id(CatalogIdentifierAllocator& allocator,
                                      const ColumnDefinition& definition)
{
    if (definition.column_id) {
        return *definition.column_id;
    }
    return allocator.allocate_column_id();
}

[[nodiscard]] std::uint16_t resolve_ordinal(std::uint16_t fallback,
                                            const ColumnDefinition& definition)
{
    if (definition.ordinal) {
        return *definition.ordinal;
    }
    return fallback;
}

}  // namespace

std::error_code stage_create_schema(CatalogMutator& mutator,
                                    CatalogIdentifierAllocator& allocator,
                                    const CreateSchemaRequest& request,
                                    CreateSchemaResult& result)
{
    if (!request.database_id.is_valid() || !is_valid_name(request.name)) {
        return invalid_argument();
    }

    SchemaId schema_id = request.schema_id ? *request.schema_id : allocator.allocate_schema_id();
    if (!schema_id.is_valid()) {
        return invalid_argument();
    }

    CatalogTupleDescriptor tuple = CatalogTupleBuilder::for_insert(mutator.transaction());

    CatalogSchemaDescriptor descriptor{};
    descriptor.tuple = tuple;
    descriptor.schema_id = schema_id;
    descriptor.database_id = request.database_id;
    descriptor.name = request.name;

    auto payload = serialize_catalog_schema(descriptor);
    mutator.stage_insert(kCatalogSchemasRelationId, schema_id.value, tuple, std::move(payload));

    result.schema_id = schema_id;
    return {};
}

std::error_code stage_create_table(CatalogMutator& mutator,
                                   CatalogIdentifierAllocator& allocator,
                                   const CreateTableRequest& request,
                                   CreateTableResult& result)
{
    if (!request.schema_id.is_valid() || !is_valid_name(request.name)) {
        return invalid_argument();
    }

    RelationId relation_id = request.relation_id ? *request.relation_id : allocator.allocate_table_id();
    if (!relation_id.is_valid()) {
        return invalid_argument();
    }

    CatalogTupleDescriptor tuple = CatalogTupleBuilder::for_insert(mutator.transaction());

    CatalogTableDescriptor descriptor{};
    descriptor.tuple = tuple;
    descriptor.relation_id = relation_id;
    descriptor.schema_id = request.schema_id;
    descriptor.table_type = request.table_type;
    descriptor.root_page_id = request.root_page_id;
    descriptor.name = request.name;

    auto payload = serialize_catalog_table(descriptor);
    mutator.stage_insert(kCatalogTablesRelationId, relation_id.value, tuple, std::move(payload));

    result.relation_id = relation_id;
    result.column_ids.clear();
    result.column_ids.reserve(request.columns.size());

    std::uint16_t ordinal_seed = 1U;
    for (const auto& column : request.columns) {
        if (!is_valid_name(column.name)) {
            return invalid_argument();
        }

        ColumnId column_id = next_column_id(allocator, column);
        if (!column_id.is_valid()) {
            return invalid_argument();
        }

        const std::uint16_t ordinal = resolve_ordinal(ordinal_seed, column);
        if (ordinal == 0U) {
            return invalid_argument();
        }

        CatalogTupleDescriptor column_tuple = CatalogTupleBuilder::for_insert(mutator.transaction());

        CatalogColumnDescriptor column_descriptor{};
        column_descriptor.tuple = column_tuple;
        column_descriptor.column_id = column_id;
        column_descriptor.relation_id = relation_id;
        column_descriptor.column_type = column.column_type;
        column_descriptor.ordinal_position = ordinal;
        column_descriptor.name = column.name;

        auto column_payload = serialize_catalog_column(column_descriptor);
        mutator.stage_insert(kCatalogColumnsRelationId, column_id.value, column_tuple, std::move(column_payload));

        result.column_ids.push_back(column_id);
        ordinal_seed = static_cast<std::uint16_t>(std::max<std::uint16_t>(ordinal_seed, static_cast<std::uint16_t>(ordinal + 1U)));
    }

    return {};
}

std::error_code stage_create_index(CatalogMutator& mutator,
                                   CatalogIdentifierAllocator& allocator,
                                   const CreateIndexRequest& request,
                                   CreateIndexResult& result)
{
    if (!request.relation_id.is_valid() || !is_valid_name(request.name)) {
        return invalid_argument();
    }

    IndexId index_id = request.index_id ? *request.index_id : allocator.allocate_index_id();
    if (!index_id.is_valid()) {
        return invalid_argument();
    }

    const auto root_page_id = request.root_page_id.value_or(0U);
    if (root_page_id == 0U) {
        return invalid_argument();
    }

    if (request.max_fanout == 0U) {
        return invalid_argument();
    }

    if (request.comparator.empty()) {
        return invalid_argument();
    }

    if (request.comparator.size() > std::numeric_limits<std::uint16_t>::max()) {
        return invalid_argument();
    }

    CatalogTupleDescriptor tuple = CatalogTupleBuilder::for_insert(mutator.transaction());

    CatalogIndexDescriptor descriptor{};
    descriptor.tuple = tuple;
    descriptor.index_id = index_id;
    descriptor.relation_id = request.relation_id;
    descriptor.index_type = request.index_type;
    descriptor.root_page_id = root_page_id;
    descriptor.max_fanout = request.max_fanout;
    descriptor.comparator = request.comparator;
    descriptor.name = request.name;
    descriptor.unique = request.unique;
    descriptor.covering_columns = request.covering_columns;
    descriptor.predicate = request.predicate;

    auto payload = serialize_catalog_index(descriptor);
    mutator.stage_insert(kCatalogIndexesRelationId, index_id.value, tuple, std::move(payload));

    result.index_id = index_id;
    result.root_page_id = root_page_id;
    result.max_fanout = request.max_fanout;
    result.comparator = request.comparator;
    result.unique = request.unique;
    result.covering_columns = request.covering_columns;
    result.predicate = request.predicate;
    return {};
}

}  // namespace bored::catalog

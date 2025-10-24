#pragma once

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_mutator.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace bored::catalog {

class CatalogIdentifierAllocator {
public:
    virtual ~CatalogIdentifierAllocator() = default;
    virtual SchemaId allocate_schema_id() = 0;
    virtual RelationId allocate_table_id() = 0;
    virtual IndexId allocate_index_id() = 0;
    virtual ColumnId allocate_column_id() = 0;
};

struct ColumnDefinition final {
    std::string name{};
    CatalogColumnType column_type = CatalogColumnType::Unknown;
    std::optional<ColumnId> column_id{};
    std::optional<std::uint16_t> ordinal{};
};

struct CreateSchemaRequest final {
    DatabaseId database_id{};
    std::string name{};
    std::optional<SchemaId> schema_id{};
};

struct CreateSchemaResult final {
    SchemaId schema_id{};
};

struct CreateTableRequest final {
    SchemaId schema_id{};
    std::string name{};
    CatalogTableType table_type = CatalogTableType::Heap;
    std::uint32_t root_page_id = 0U;
    std::vector<ColumnDefinition> columns{};
    std::optional<RelationId> relation_id{};
};

struct CreateTableResult final {
    RelationId relation_id{};
    std::vector<ColumnId> column_ids{};
};

struct CreateIndexRequest final {
    RelationId relation_id{};
    std::string name{};
    CatalogIndexType index_type = CatalogIndexType::Unknown;
    std::optional<IndexId> index_id{};
};

struct CreateIndexResult final {
    IndexId index_id{};
};

std::error_code stage_create_schema(CatalogMutator& mutator,
                                    CatalogIdentifierAllocator& allocator,
                                    const CreateSchemaRequest& request,
                                    CreateSchemaResult& result);

std::error_code stage_create_table(CatalogMutator& mutator,
                                   CatalogIdentifierAllocator& allocator,
                                   const CreateTableRequest& request,
                                   CreateTableResult& result);

std::error_code stage_create_index(CatalogMutator& mutator,
                                   CatalogIdentifierAllocator& allocator,
                                   const CreateIndexRequest& request,
                                   CreateIndexResult& result);

}  // namespace bored::catalog

#pragma once

#include "bored/catalog/catalog_ids.hpp"

#include <cstdint>
#include <string_view>

namespace bored::catalog {

struct CatalogTupleDescriptor final {
    std::uint64_t xmin = 0U;
    std::uint64_t xmax = 0U;
    std::uint32_t visibility_flags = 0U;
    std::uint32_t reserved = 0U;
};

enum class CatalogTableType : std::uint16_t {
    Heap = 0,
    Catalog = 1
};

enum class CatalogIndexType : std::uint16_t {
    Unknown = 0,
    BTree = 1
};

enum class CatalogColumnType : std::uint32_t {
    Unknown = 0,
    Int64 = 1,
    Utf8 = 2,
    UInt16 = 3,
    UInt32 = 4
};

struct CatalogDatabaseDescriptor final {
    CatalogTupleDescriptor tuple{};
    DatabaseId database_id{};
    SchemaId default_schema_id{};
    std::string_view name{};
};

struct CatalogSchemaDescriptor final {
    CatalogTupleDescriptor tuple{};
    SchemaId schema_id{};
    DatabaseId database_id{};
    std::string_view name{};
};

struct CatalogTableDescriptor final {
    CatalogTupleDescriptor tuple{};
    RelationId relation_id{};
    SchemaId schema_id{};
    CatalogTableType table_type = CatalogTableType::Catalog;
    std::uint32_t root_page_id = 0U;
    std::string_view name{};
};

struct CatalogColumnDescriptor final {
    CatalogTupleDescriptor tuple{};
    ColumnId column_id{};
    RelationId relation_id{};
    CatalogColumnType column_type = CatalogColumnType::Unknown;
    std::uint16_t ordinal_position = 0U;
    std::string_view name{};
};

struct CatalogIndexDescriptor final {
    CatalogTupleDescriptor tuple{};
    IndexId index_id{};
    RelationId relation_id{};
    CatalogIndexType index_type = CatalogIndexType::Unknown;
    std::uint32_t root_page_id = 0U;
    std::string_view name{};
};

}  // namespace bored::catalog

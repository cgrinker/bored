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

    constexpr CatalogDatabaseDescriptor() = default;
    constexpr CatalogDatabaseDescriptor(const CatalogTupleDescriptor& tuple_descriptor,
                                        DatabaseId database,
                                        SchemaId default_schema,
                                        std::string_view name_view) noexcept
        : tuple{tuple_descriptor}
        , database_id{database}
        , default_schema_id{default_schema}
        , name{name_view}
    {}
};

struct CatalogSchemaDescriptor final {
    CatalogTupleDescriptor tuple{};
    SchemaId schema_id{};
    DatabaseId database_id{};
    std::string_view name{};

    constexpr CatalogSchemaDescriptor() = default;
    constexpr CatalogSchemaDescriptor(const CatalogTupleDescriptor& tuple_descriptor,
                                      SchemaId schema,
                                      DatabaseId database,
                                      std::string_view name_view) noexcept
        : tuple{tuple_descriptor}
        , schema_id{schema}
        , database_id{database}
        , name{name_view}
    {}
};

struct CatalogTableDescriptor final {
    CatalogTupleDescriptor tuple{};
    RelationId relation_id{};
    SchemaId schema_id{};
    CatalogTableType table_type = CatalogTableType::Catalog;
    std::uint32_t root_page_id = 0U;
    std::string_view name{};

    constexpr CatalogTableDescriptor() = default;
    constexpr CatalogTableDescriptor(const CatalogTupleDescriptor& tuple_descriptor,
                                     RelationId relation,
                                     SchemaId schema,
                                     CatalogTableType type,
                                     std::uint32_t root_page,
                                     std::string_view name_view) noexcept
        : tuple{tuple_descriptor}
        , relation_id{relation}
        , schema_id{schema}
        , table_type{type}
        , root_page_id{root_page}
        , name{name_view}
    {}
};

struct CatalogColumnDescriptor final {
    CatalogTupleDescriptor tuple{};
    ColumnId column_id{};
    RelationId relation_id{};
    CatalogColumnType column_type = CatalogColumnType::Unknown;
    std::uint16_t ordinal_position = 0U;
    std::string_view name{};

    constexpr CatalogColumnDescriptor() = default;
    constexpr CatalogColumnDescriptor(const CatalogTupleDescriptor& tuple_descriptor,
                                      ColumnId column,
                                      RelationId relation,
                                      CatalogColumnType type,
                                      std::uint16_t ordinal,
                                      std::string_view name_view) noexcept
        : tuple{tuple_descriptor}
        , column_id{column}
        , relation_id{relation}
        , column_type{type}
        , ordinal_position{ordinal}
        , name{name_view}
    {}
};

struct CatalogIndexDescriptor final {
    CatalogTupleDescriptor tuple{};
    IndexId index_id{};
    RelationId relation_id{};
    CatalogIndexType index_type = CatalogIndexType::Unknown;
    std::uint32_t root_page_id = 0U;
    std::uint16_t max_fanout = 0U;
    std::string_view comparator{};
    std::string_view name{};

    constexpr CatalogIndexDescriptor() = default;
    constexpr CatalogIndexDescriptor(const CatalogTupleDescriptor& tuple_descriptor,
                                     IndexId index,
                                     RelationId relation,
                                     CatalogIndexType type,
                                     std::uint32_t root_page,
                                     std::uint16_t fanout,
                                     std::string_view comparator_view,
                                     std::string_view name_view) noexcept
        : tuple{tuple_descriptor}
        , index_id{index}
        , relation_id{relation}
        , index_type{type}
        , root_page_id{root_page}
        , max_fanout{fanout}
        , comparator{comparator_view}
        , name{name_view}
    {}
};

}  // namespace bored::catalog

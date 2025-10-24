#pragma once

#include "bored/catalog/catalog_relations.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace bored::catalog {

struct CatalogDatabaseView final {
    CatalogTupleDescriptor tuple{};
    DatabaseId database_id{};
    SchemaId default_schema_id{};
    std::string_view name{};
};

struct CatalogSchemaView final {
    CatalogTupleDescriptor tuple{};
    SchemaId schema_id{};
    DatabaseId database_id{};
    std::string_view name{};
};

struct CatalogTableView final {
    CatalogTupleDescriptor tuple{};
    RelationId relation_id{};
    SchemaId schema_id{};
    CatalogTableType table_type = CatalogTableType::Catalog;
    std::uint32_t root_page_id = 0U;
    std::string_view name{};
};

struct CatalogColumnView final {
    CatalogTupleDescriptor tuple{};
    ColumnId column_id{};
    RelationId relation_id{};
    CatalogColumnType column_type = CatalogColumnType::Unknown;
    std::uint16_t ordinal_position = 0U;
    std::string_view name{};
};

struct CatalogIndexView final {
    CatalogTupleDescriptor tuple{};
    IndexId index_id{};
    RelationId relation_id{};
    CatalogIndexType index_type = CatalogIndexType::Unknown;
    std::string_view name{};
};

[[nodiscard]] std::size_t catalog_database_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_schema_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_table_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_column_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_index_tuple_size(std::string_view name) noexcept;

[[nodiscard]] std::vector<std::byte> serialize_catalog_database(const CatalogDatabaseDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_schema(const CatalogSchemaDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_table(const CatalogTableDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_column(const CatalogColumnDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_index(const CatalogIndexDescriptor& descriptor);

[[nodiscard]] std::optional<CatalogDatabaseView> decode_catalog_database(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogSchemaView> decode_catalog_schema(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogTableView> decode_catalog_table(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogColumnView> decode_catalog_column(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogIndexView> decode_catalog_index(std::span<const std::byte> tuple);

}  // namespace bored::catalog

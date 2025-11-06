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
    std::uint32_t root_page_id = 0U;
    std::uint16_t max_fanout = 0U;
    std::string_view comparator{};
    std::string_view name{};
    bool unique = false;
    std::string_view covering_columns{};
    std::string_view predicate{};
};

struct CatalogViewView final {
    CatalogTupleDescriptor tuple{};
    RelationId relation_id{};
    std::string_view definition{};
};

struct CatalogConstraintView final {
    CatalogTupleDescriptor tuple{};
    ConstraintId constraint_id{};
    RelationId relation_id{};
    CatalogConstraintType constraint_type = CatalogConstraintType::Unknown;
    IndexId backing_index_id{};
    RelationId referenced_relation_id{};
    std::string_view key_columns{};
    std::string_view referenced_columns{};
    std::string_view name{};
};

struct CatalogSequenceView final {
    CatalogTupleDescriptor tuple{};
    SequenceId sequence_id{};
    SchemaId schema_id{};
    RelationId owning_relation_id{};
    ColumnId owning_column_id{};
    std::uint64_t start_value = 1U;
    std::uint64_t next_value = 1U;
    std::int64_t increment = 1;
    std::uint64_t min_value = 1U;
    std::uint64_t max_value = 0U;
    std::uint64_t cache_size = 1U;
    bool cycle = false;
    std::string_view name{};
};

[[nodiscard]] std::size_t catalog_database_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_schema_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_table_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_column_tuple_size(std::string_view name) noexcept;
[[nodiscard]] std::size_t catalog_index_tuple_size(std::string_view name,
                                                   std::string_view comparator,
                                                   std::string_view covering_columns,
                                                   std::string_view predicate) noexcept;
[[nodiscard]] std::size_t catalog_view_tuple_size(std::string_view definition) noexcept;
[[nodiscard]] std::size_t catalog_constraint_tuple_size(std::string_view name,
                                                       std::string_view key_columns,
                                                       std::string_view referenced_columns) noexcept;
[[nodiscard]] std::size_t catalog_sequence_tuple_size(std::string_view name) noexcept;

[[nodiscard]] std::vector<std::byte> serialize_catalog_database(const CatalogDatabaseDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_schema(const CatalogSchemaDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_table(const CatalogTableDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_column(const CatalogColumnDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_index(const CatalogIndexDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_view(const CatalogViewDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_constraint(const CatalogConstraintDescriptor& descriptor);
[[nodiscard]] std::vector<std::byte> serialize_catalog_sequence(const CatalogSequenceDescriptor& descriptor);

[[nodiscard]] std::optional<CatalogDatabaseView> decode_catalog_database(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogSchemaView> decode_catalog_schema(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogTableView> decode_catalog_table(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogColumnView> decode_catalog_column(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogIndexView> decode_catalog_index(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogViewView> decode_catalog_view(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogConstraintView> decode_catalog_constraint(std::span<const std::byte> tuple);
[[nodiscard]] std::optional<CatalogSequenceView> decode_catalog_sequence(std::span<const std::byte> tuple);

}  // namespace bored::catalog

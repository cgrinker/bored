#pragma once

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include <atomic>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace bored::catalog {

class CatalogAccessor final {
public:
    using TupleCallback = std::function<void(std::span<const std::byte> tuple)>;
    using RelationScanner = std::function<void(RelationId relation_id, const TupleCallback& callback)>;

    struct Config final {
        const CatalogTransaction* transaction = nullptr;
        RelationScanner scanner;
    };

    explicit CatalogAccessor(Config config);

    [[nodiscard]] const CatalogTransaction& transaction() const noexcept;

    [[nodiscard]] std::optional<CatalogDatabaseDescriptor> database(DatabaseId id) const;
    [[nodiscard]] std::optional<CatalogDatabaseDescriptor> database(std::string_view name) const;

    [[nodiscard]] std::optional<CatalogSchemaDescriptor> schema(SchemaId id) const;
    [[nodiscard]] std::vector<CatalogSchemaDescriptor> schemas(DatabaseId database_id) const;

    [[nodiscard]] std::optional<CatalogTableDescriptor> table(RelationId id) const;
    [[nodiscard]] std::vector<CatalogTableDescriptor> tables(SchemaId schema_id) const;

    [[nodiscard]] std::vector<CatalogColumnDescriptor> columns(RelationId relation_id) const;
    [[nodiscard]] std::optional<CatalogIndexDescriptor> index(IndexId id) const;
    [[nodiscard]] std::vector<CatalogIndexDescriptor> indexes(RelationId relation_id) const;
    [[nodiscard]] std::vector<CatalogIndexDescriptor> indexes_for_schema(SchemaId schema_id) const;

    static void invalidate_all() noexcept;
    static void invalidate_relation(RelationId relation_id) noexcept;
    [[nodiscard]] static std::uint64_t current_epoch(RelationId relation_id) noexcept;

private:
    struct DatabaseEntry final {
        CatalogTupleDescriptor tuple{};
        DatabaseId database_id{};
        SchemaId default_schema_id{};
        std::string name{};
    };

    struct SchemaEntry final {
        CatalogTupleDescriptor tuple{};
        SchemaId schema_id{};
        DatabaseId database_id{};
        std::string name{};
    };

    struct TableEntry final {
        CatalogTupleDescriptor tuple{};
        RelationId relation_id{};
        SchemaId schema_id{};
        CatalogTableType table_type = CatalogTableType::Catalog;
        std::uint32_t root_page_id = 0U;
        std::string name{};
    };

    struct ColumnEntry final {
        CatalogTupleDescriptor tuple{};
        ColumnId column_id{};
        RelationId relation_id{};
        CatalogColumnType column_type = CatalogColumnType::Unknown;
        std::uint16_t ordinal_position = 0U;
        std::string name{};
    };

    struct IndexEntry final {
        CatalogTupleDescriptor tuple{};
        IndexId index_id{};
        RelationId relation_id{};
        SchemaId schema_id{};
        CatalogIndexType index_type = CatalogIndexType::Unknown;
        std::string name{};
    };

    const CatalogTransaction* transaction_ = nullptr;
    RelationScanner scanner_;

    mutable bool databases_loaded_ = false;
    mutable std::vector<DatabaseEntry> databases_{};
    mutable std::unordered_map<std::uint64_t, std::size_t> database_index_{};
    mutable std::unordered_map<std::string, std::size_t> database_name_index_{};

    mutable bool schemas_loaded_ = false;
    mutable std::vector<SchemaEntry> schemas_{};
    mutable std::unordered_map<std::uint64_t, std::size_t> schema_index_{};

    mutable bool tables_loaded_ = false;
    mutable std::vector<TableEntry> tables_{};
    mutable std::unordered_map<std::uint64_t, std::size_t> table_index_{};
    mutable std::unordered_map<std::uint64_t, std::vector<std::size_t>> tables_by_schema_{};

    mutable bool columns_loaded_ = false;
    mutable std::vector<ColumnEntry> columns_{};
    mutable std::unordered_map<std::uint64_t, std::vector<std::size_t>> columns_by_relation_{};

    mutable bool indexes_loaded_ = false;
    mutable std::vector<IndexEntry> indexes_{};
    mutable std::unordered_map<std::uint64_t, std::size_t> index_index_{};
    mutable std::unordered_map<std::uint64_t, std::vector<std::size_t>> indexes_by_relation_{};
    mutable std::unordered_map<std::uint64_t, std::vector<std::size_t>> indexes_by_schema_{};

    mutable std::uint64_t databases_epoch_ = 0U;
    mutable std::uint64_t schemas_epoch_ = 0U;
    mutable std::uint64_t tables_epoch_ = 0U;
    mutable std::uint64_t columns_epoch_ = 0U;
    mutable std::uint64_t indexes_epoch_ = 0U;

    void ensure_databases_loaded() const;
    void ensure_schemas_loaded() const;
    void ensure_tables_loaded() const;
    void ensure_columns_loaded() const;
    void ensure_indexes_loaded() const;
};

}  // namespace bored::catalog

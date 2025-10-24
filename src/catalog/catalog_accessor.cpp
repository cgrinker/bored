#include "bored/catalog/catalog_accessor.hpp"

#include "bored/catalog/catalog_cache.hpp"

#include <stdexcept>
#include <string_view>
#include <utility>

namespace bored::catalog {

namespace {

std::string make_string(std::string_view value)
{
    return std::string(value.begin(), value.end());
}

}  // namespace

CatalogAccessor::CatalogAccessor(Config config)
    : transaction_{config.transaction}
    , scanner_{std::move(config.scanner)}
{
    if (transaction_ == nullptr) {
        throw std::invalid_argument{"CatalogAccessor requires an active transaction"};
    }
    if (!scanner_) {
        throw std::invalid_argument{"CatalogAccessor requires a relation scanner"};
    }
}

const CatalogTransaction& CatalogAccessor::transaction() const noexcept
{
    return *transaction_;
}

std::optional<CatalogDatabaseDescriptor> CatalogAccessor::database(DatabaseId id) const
{
    ensure_databases_loaded();
    auto it = database_index_.find(id.value);
    if (it == database_index_.end()) {
        return std::nullopt;
    }
    const auto& entry = databases_[it->second];
    return CatalogDatabaseDescriptor{entry.tuple, entry.database_id, entry.default_schema_id, entry.name};
}

std::optional<CatalogDatabaseDescriptor> CatalogAccessor::database(std::string_view name) const
{
    ensure_databases_loaded();
    std::string key{name};
    auto it = database_name_index_.find(key);
    if (it == database_name_index_.end()) {
        return std::nullopt;
    }
    const auto& entry = databases_[it->second];
    return CatalogDatabaseDescriptor{entry.tuple, entry.database_id, entry.default_schema_id, entry.name};
}

std::optional<CatalogSchemaDescriptor> CatalogAccessor::schema(SchemaId id) const
{
    ensure_schemas_loaded();
    auto it = schema_index_.find(id.value);
    if (it == schema_index_.end()) {
        return std::nullopt;
    }
    const auto& entry = schemas_[it->second];
    return CatalogSchemaDescriptor{entry.tuple, entry.schema_id, entry.database_id, entry.name};
}

std::vector<CatalogSchemaDescriptor> CatalogAccessor::schemas(DatabaseId database_id) const
{
    ensure_schemas_loaded();
    std::vector<CatalogSchemaDescriptor> result;
    for (const auto& entry : schemas_) {
        if (entry.database_id == database_id) {
            result.emplace_back(entry.tuple, entry.schema_id, entry.database_id, entry.name);
        }
    }
    return result;
}

std::optional<CatalogTableDescriptor> CatalogAccessor::table(RelationId id) const
{
    ensure_tables_loaded();
    auto it = table_index_.find(id.value);
    if (it == table_index_.end()) {
        return std::nullopt;
    }
    const auto& entry = tables_[it->second];
    return CatalogTableDescriptor{entry.tuple, entry.relation_id, entry.schema_id, entry.table_type, entry.root_page_id, entry.name};
}

std::vector<CatalogTableDescriptor> CatalogAccessor::tables(SchemaId schema_id) const
{
    ensure_tables_loaded();
    std::vector<CatalogTableDescriptor> result;
    auto it = tables_by_schema_.find(schema_id.value);
    if (it == tables_by_schema_.end()) {
        return result;
    }
    for (auto index : it->second) {
        const auto& entry = tables_[index];
        result.emplace_back(entry.tuple, entry.relation_id, entry.schema_id, entry.table_type, entry.root_page_id, entry.name);
    }
    return result;
}

std::vector<CatalogColumnDescriptor> CatalogAccessor::columns(RelationId relation_id) const
{
    ensure_columns_loaded();
    std::vector<CatalogColumnDescriptor> result;
    auto it = columns_by_relation_.find(relation_id.value);
    if (it == columns_by_relation_.end()) {
        return result;
    }
    for (auto index : it->second) {
        const auto& entry = columns_[index];
        result.emplace_back(entry.tuple, entry.column_id, entry.relation_id, entry.column_type, entry.ordinal_position, entry.name);
    }
    return result;
}

std::optional<CatalogIndexDescriptor> CatalogAccessor::index(IndexId id) const
{
    ensure_indexes_loaded();
    auto it = index_index_.find(id.value);
    if (it == index_index_.end()) {
        return std::nullopt;
    }
    const auto& entry = indexes_[it->second];
    return CatalogIndexDescriptor{entry.tuple, entry.index_id, entry.relation_id, entry.index_type, entry.name};
}

std::vector<CatalogIndexDescriptor> CatalogAccessor::indexes(RelationId relation_id) const
{
    ensure_indexes_loaded();
    std::vector<CatalogIndexDescriptor> result;
    auto it = indexes_by_relation_.find(relation_id.value);
    if (it == indexes_by_relation_.end()) {
        return result;
    }
    for (auto index : it->second) {
        const auto& entry = indexes_[index];
        result.emplace_back(entry.tuple, entry.index_id, entry.relation_id, entry.index_type, entry.name);
    }
    return result;
}

std::vector<CatalogIndexDescriptor> CatalogAccessor::indexes_for_schema(SchemaId schema_id) const
{
    ensure_indexes_loaded();
    std::vector<CatalogIndexDescriptor> result;
    auto it = indexes_by_schema_.find(schema_id.value);
    if (it == indexes_by_schema_.end()) {
        return result;
    }
    for (auto index : it->second) {
        const auto& entry = indexes_[index];
        result.emplace_back(entry.tuple, entry.index_id, entry.relation_id, entry.index_type, entry.name);
    }
    return result;
}

void CatalogAccessor::invalidate_all() noexcept
{
    CatalogCache::instance().invalidate_all();
}

void CatalogAccessor::invalidate_relation(RelationId relation_id) noexcept
{
    CatalogCache::instance().invalidate(relation_id);
}

std::uint64_t CatalogAccessor::current_epoch(RelationId relation_id) noexcept
{
    return CatalogCache::instance().epoch(relation_id);
}

void CatalogAccessor::ensure_databases_loaded() const
{
    auto& cache = CatalogCache::instance();
    const auto current_epoch = cache.epoch(kCatalogDatabasesRelationId);
    if (databases_loaded_ && databases_epoch_ == current_epoch) {
        return;
    }

    databases_.clear();
    database_index_.clear();
    database_name_index_.clear();

    auto relation = cache.materialize(kCatalogDatabasesRelationId, scanner_);
    if (relation) {
        for (const auto& tuple : relation->tuples) {
            auto tuple_span = std::span<const std::byte>(tuple.payload.data(), tuple.payload.size());
            auto view = decode_catalog_database(tuple_span);
            if (!view) {
                continue;
            }
            if (!transaction_->is_visible(view->tuple)) {
                continue;
            }

        DatabaseEntry entry{};
        entry.tuple = view->tuple;
        entry.database_id = view->database_id;
        entry.default_schema_id = view->default_schema_id;
        entry.name = make_string(view->name);

        const auto index = databases_.size();
        databases_.push_back(entry);
        database_index_[entry.database_id.value] = index;
        database_name_index_[entry.name] = index;
        }
    }

    databases_loaded_ = true;
    databases_epoch_ = current_epoch;
}

void CatalogAccessor::ensure_schemas_loaded() const
{
    auto& cache = CatalogCache::instance();
    const auto current_epoch = cache.epoch(kCatalogSchemasRelationId);
    if (schemas_loaded_ && schemas_epoch_ == current_epoch) {
        return;
    }

    ensure_databases_loaded();

    schemas_.clear();
    schema_index_.clear();

    auto relation = cache.materialize(kCatalogSchemasRelationId, scanner_);
    if (relation) {
        for (const auto& tuple : relation->tuples) {
            auto tuple_span = std::span<const std::byte>(tuple.payload.data(), tuple.payload.size());
            auto view = decode_catalog_schema(tuple_span);
            if (!view) {
                continue;
            }
            if (!transaction_->is_visible(view->tuple)) {
                continue;
            }

        SchemaEntry entry{};
        entry.tuple = view->tuple;
        entry.schema_id = view->schema_id;
        entry.database_id = view->database_id;
        entry.name = make_string(view->name);

        const auto index = schemas_.size();
        schemas_.push_back(entry);
        schema_index_[entry.schema_id.value] = index;
        }
    }

    schemas_loaded_ = true;
    schemas_epoch_ = current_epoch;
}

void CatalogAccessor::ensure_tables_loaded() const
{
    auto& cache = CatalogCache::instance();
    const auto current_epoch = cache.epoch(kCatalogTablesRelationId);
    if (tables_loaded_ && tables_epoch_ == current_epoch) {
        return;
    }

    ensure_schemas_loaded();

    tables_.clear();
    table_index_.clear();
    tables_by_schema_.clear();

    auto relation = cache.materialize(kCatalogTablesRelationId, scanner_);
    if (relation) {
        for (const auto& tuple : relation->tuples) {
            auto tuple_span = std::span<const std::byte>(tuple.payload.data(), tuple.payload.size());
            auto view = decode_catalog_table(tuple_span);
            if (!view) {
                continue;
            }
            if (!transaction_->is_visible(view->tuple)) {
                continue;
            }

        TableEntry entry{};
        entry.tuple = view->tuple;
        entry.relation_id = view->relation_id;
        entry.schema_id = view->schema_id;
        entry.table_type = view->table_type;
        entry.root_page_id = view->root_page_id;
        entry.name = make_string(view->name);

        const auto index = tables_.size();
        tables_.push_back(entry);
        table_index_[entry.relation_id.value] = index;
        tables_by_schema_[entry.schema_id.value].push_back(index);
        }
    }

    tables_loaded_ = true;
    tables_epoch_ = current_epoch;
    indexes_loaded_ = false;
}

void CatalogAccessor::ensure_columns_loaded() const
{
    auto& cache = CatalogCache::instance();
    const auto current_epoch = cache.epoch(kCatalogColumnsRelationId);
    if (columns_loaded_ && columns_epoch_ == current_epoch) {
        return;
    }

    ensure_tables_loaded();

    columns_.clear();
    columns_by_relation_.clear();

    auto relation = cache.materialize(kCatalogColumnsRelationId, scanner_);
    if (relation) {
        for (const auto& tuple : relation->tuples) {
            auto tuple_span = std::span<const std::byte>(tuple.payload.data(), tuple.payload.size());
            auto view = decode_catalog_column(tuple_span);
            if (!view) {
                continue;
            }
            if (!transaction_->is_visible(view->tuple)) {
                continue;
            }

        ColumnEntry entry{};
        entry.tuple = view->tuple;
        entry.column_id = view->column_id;
        entry.relation_id = view->relation_id;
        entry.column_type = view->column_type;
        entry.ordinal_position = view->ordinal_position;
        entry.name = make_string(view->name);

        const auto index = columns_.size();
        columns_.push_back(entry);
        columns_by_relation_[entry.relation_id.value].push_back(index);
        }
    }

    columns_loaded_ = true;
    columns_epoch_ = current_epoch;
    indexes_loaded_ = false;
}

void CatalogAccessor::ensure_indexes_loaded() const
{
    auto& cache = CatalogCache::instance();
    const auto current_epoch = cache.epoch(kCatalogIndexesRelationId);
    if (indexes_loaded_ && indexes_epoch_ == current_epoch) {
        return;
    }

    ensure_tables_loaded();

    indexes_.clear();
    index_index_.clear();
    indexes_by_relation_.clear();
    indexes_by_schema_.clear();

    auto relation = cache.materialize(kCatalogIndexesRelationId, scanner_);
    if (relation) {
        for (const auto& tuple : relation->tuples) {
            auto tuple_span = std::span<const std::byte>(tuple.payload.data(), tuple.payload.size());
            auto view = decode_catalog_index(tuple_span);
            if (!view) {
                continue;
            }
            if (!transaction_->is_visible(view->tuple)) {
                continue;
            }

            IndexEntry entry{};
            entry.tuple = view->tuple;
            entry.index_id = view->index_id;
            entry.relation_id = view->relation_id;
            entry.index_type = view->index_type;
            entry.name = make_string(view->name);

            auto table_it = table_index_.find(entry.relation_id.value);
            if (table_it != table_index_.end()) {
                entry.schema_id = tables_[table_it->second].schema_id;
            }

            const auto index = indexes_.size();
            indexes_.push_back(entry);
            index_index_[entry.index_id.value] = index;
            indexes_by_relation_[entry.relation_id.value].push_back(index);
            if (entry.schema_id.is_valid()) {
                indexes_by_schema_[entry.schema_id.value].push_back(index);
            }
        }
    }

    indexes_loaded_ = true;
    indexes_epoch_ = current_epoch;
}

}  // namespace bored::catalog

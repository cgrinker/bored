#include "bored/ddl/ddl_handlers.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/ddl/ddl_dependency_graph.hpp"
#include "bored/ddl/ddl_errors.hpp"
#include "bored/ddl/ddl_validation.hpp"

#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

namespace bored::ddl {

namespace {

using catalog::CatalogAccessor;
using catalog::CatalogColumnDescriptor;
using catalog::CatalogMutator;
using catalog::CatalogSchemaDescriptor;
using catalog::CatalogTableDescriptor;
using catalog::CatalogTupleBuilder;
using catalog::CatalogTupleDescriptor;
using catalog::CatalogIndexDescriptor;

DdlCommandResponse handle_drop_table(DdlCommandContext& context, const DropTableRequest& request);
DdlCommandResponse handle_drop_index(DdlCommandContext& context, const DropIndexRequest& request);

DdlCommandResponse make_context_failure(std::string message)
{
    return make_failure(make_error_code(DdlErrc::ExecutionFailed), std::move(message));
}

std::error_code map_stage_error(std::error_code ec) noexcept
{
    using std::errc;

    if (!ec) {
        return {};
    }
    if (ec == std::make_error_code(errc::invalid_argument)) {
        return make_error_code(DdlErrc::ValidationFailed);
    }

    if (ec == std::make_error_code(errc::value_too_large)) {
        return make_error_code(DdlErrc::ExecutionFailed);
    }

    return make_error_code(DdlErrc::ExecutionFailed);
}

bool schema_exists(const CatalogAccessor& accessor, catalog::DatabaseId database_id, std::string_view name)
{
    const auto schemas = accessor.schemas(database_id);
    return std::any_of(schemas.begin(), schemas.end(), [&](const CatalogSchemaDescriptor& schema) {
        return schema.name == name;
    });
}

std::optional<CatalogSchemaDescriptor> find_schema(const CatalogAccessor& accessor,
                                                    catalog::DatabaseId database_id,
                                                    std::string_view name)
{
    const auto schemas = accessor.schemas(database_id);
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const CatalogSchemaDescriptor& schema) {
        return schema.name == name;
    });
    if (it == schemas.end()) {
        return std::nullopt;
    }
    return *it;
}

std::optional<CatalogTableDescriptor> find_table(const CatalogAccessor& accessor,
                                                  catalog::SchemaId schema_id,
                                                  std::string_view name)
{
    const auto tables = accessor.tables(schema_id);
    auto it = std::find_if(tables.begin(), tables.end(), [&](const CatalogTableDescriptor& table) {
        return table.name == name;
    });
    if (it == tables.end()) {
        return std::nullopt;
    }
    return *it;
}

std::vector<std::byte> serialize_schema(const CatalogSchemaDescriptor& schema)
{
    catalog::CatalogSchemaDescriptor descriptor{};
    descriptor.tuple = schema.tuple;
    descriptor.schema_id = schema.schema_id;
    descriptor.database_id = schema.database_id;
    descriptor.name = schema.name;
    return catalog::serialize_catalog_schema(descriptor);
}

std::vector<std::byte> serialize_table(const CatalogTableDescriptor& table)
{
    catalog::CatalogTableDescriptor descriptor{};
    descriptor.tuple = table.tuple;
    descriptor.relation_id = table.relation_id;
    descriptor.schema_id = table.schema_id;
    descriptor.table_type = table.table_type;
    descriptor.root_page_id = table.root_page_id;
    descriptor.name = table.name;
    return catalog::serialize_catalog_table(descriptor);
}

std::vector<std::byte> serialize_column(const CatalogColumnDescriptor& column)
{
    catalog::CatalogColumnDescriptor descriptor{};
    descriptor.tuple = column.tuple;
    descriptor.column_id = column.column_id;
    descriptor.relation_id = column.relation_id;
    descriptor.column_type = column.column_type;
    descriptor.ordinal_position = column.ordinal_position;
    descriptor.name = column.name;
    return catalog::serialize_catalog_column(descriptor);
}

std::vector<std::byte> serialize_index(const CatalogIndexDescriptor& index)
{
    catalog::CatalogIndexDescriptor descriptor{};
    descriptor.tuple = index.tuple;
    descriptor.index_id = index.index_id;
    descriptor.relation_id = index.relation_id;
    descriptor.index_type = index.index_type;
    descriptor.root_page_id = index.root_page_id;
    descriptor.name = index.name;
    return catalog::serialize_catalog_index(descriptor);
}

[[nodiscard]] std::size_t max_column_ordinal(const std::vector<CatalogColumnDescriptor>& columns) noexcept
{
    std::size_t max_value = 0U;
    for (const auto& column : columns) {
        max_value = std::max<std::size_t>(max_value, column.ordinal_position);
    }
    return max_value;
}

[[nodiscard]] CatalogColumnDescriptor* find_column(std::vector<CatalogColumnDescriptor>& columns,
                                                   std::string_view name)
{
    for (auto& column : columns) {
        if (column.name == name) {
            return &column;
        }
    }
    return nullptr;
}

[[nodiscard]] const CatalogColumnDescriptor* find_column(const std::vector<CatalogColumnDescriptor>& columns,
                                                         std::string_view name)
{
    for (const auto& column : columns) {
        if (column.name == name) {
            return &column;
        }
    }
    return nullptr;
}

bool column_name_available(const std::vector<CatalogColumnDescriptor>& columns, std::string_view name)
{
    return find_column(columns, name) == nullptr;
}

bool index_name_available(const std::vector<CatalogIndexDescriptor>& indexes, std::string_view name)
{
    return std::none_of(indexes.begin(), indexes.end(), [&](const CatalogIndexDescriptor& index) {
        return index.name == name;
    });
}

[[nodiscard]] const CatalogIndexDescriptor* find_index(const std::vector<CatalogIndexDescriptor>& indexes,
                                                       std::string_view name) noexcept
{
    for (const auto& index : indexes) {
        if (index.name == name) {
            return &index;
        }
    }
    return nullptr;
}

std::error_code stage_table_rename(DdlCommandContext& context,
                                   CatalogTableDescriptor& table,
                                   std::string_view new_name)
{
    auto before_payload = serialize_table(table);

    CatalogTableDescriptor updated = table;
    updated.tuple = CatalogTupleBuilder::for_update(context.transaction, table.tuple);
    updated.name = new_name;

    auto after_payload = serialize_table(updated);
    context.mutator->stage_update(catalog::kCatalogTablesRelationId,
                                  table.relation_id.value,
                                  table.tuple,
                                  std::move(before_payload),
                                  updated.tuple,
                                  std::move(after_payload));

    table = std::move(updated);
    return {};
}

std::error_code stage_column_rename(DdlCommandContext& context,
                                    CatalogColumnDescriptor& column,
                                    std::string_view new_name)
{
    auto before_payload = serialize_column(column);

    CatalogColumnDescriptor updated = column;
    updated.tuple = CatalogTupleBuilder::for_update(context.transaction, column.tuple);
    updated.name = new_name;

    auto after_payload = serialize_column(updated);
    context.mutator->stage_update(catalog::kCatalogColumnsRelationId,
                                  column.column_id.value,
                                  column.tuple,
                                  std::move(before_payload),
                                  updated.tuple,
                                  std::move(after_payload));

    column = std::move(updated);
    return {};
}

std::error_code stage_column_add(DdlCommandContext& context,
                                 std::vector<CatalogColumnDescriptor>& columns,
                                 const AlterTableAddColumn& action,
                                 catalog::RelationId relation_id,
                                 std::uint16_t& next_ordinal)
{
    if (auto ec = validate_identifier(action.column.name); ec) {
        return ec;
    }
    if (!column_name_available(columns, action.column.name)) {
        if (action.if_not_exists) {
            return {};
        }
        return make_error_code(DdlErrc::ValidationFailed);
    }
    if (action.column.column_type == catalog::CatalogColumnType::Unknown) {
        return make_error_code(DdlErrc::ValidationFailed);
    }

    catalog::ColumnDefinition column_def = action.column;
    if (!column_def.column_id) {
        column_def.column_id = context.allocator.allocate_column_id();
    }
    if (!column_def.column_id->is_valid()) {
        return make_error_code(DdlErrc::ExecutionFailed);
    }

    const auto ordinal = column_def.ordinal.value_or(next_ordinal);
    if (ordinal == 0U) {
        return make_error_code(DdlErrc::ValidationFailed);
    }

    for (const auto& existing : columns) {
        if (existing.ordinal_position == ordinal) {
            return make_error_code(DdlErrc::ValidationFailed);
        }
    }

    CatalogColumnDescriptor descriptor{};
    descriptor.tuple = CatalogTupleBuilder::for_insert(context.transaction);
    descriptor.column_id = *column_def.column_id;
    descriptor.relation_id = relation_id;
    descriptor.column_type = column_def.column_type;
    descriptor.ordinal_position = ordinal;
    descriptor.name = column_def.name;

    auto payload = serialize_column(descriptor);
    context.mutator->stage_insert(catalog::kCatalogColumnsRelationId,
                                  descriptor.column_id.value,
                                  descriptor.tuple,
                                  std::move(payload));

    columns.push_back(descriptor);
    next_ordinal = static_cast<std::uint16_t>(std::max<std::uint16_t>(next_ordinal, static_cast<std::uint16_t>(ordinal + 1U)));
    return {};
}

std::error_code stage_column_drop(DdlCommandContext& context,
                                  std::vector<CatalogColumnDescriptor>& columns,
                                  const AlterTableDropColumn& action)
{
    auto* column = find_column(columns, action.column_name);
    if (column == nullptr) {
        if (action.if_exists) {
            return {};
        }
        return make_error_code(DdlErrc::ValidationFailed);
    }

    auto payload = serialize_column(*column);
    const auto column_id = column->column_id;
    context.mutator->stage_delete(catalog::kCatalogColumnsRelationId,
                                  column_id.value,
                                  column->tuple,
                                  std::move(payload));

    columns.erase(std::remove_if(columns.begin(), columns.end(), [&](const CatalogColumnDescriptor& entry) {
                        return entry.column_id == column_id;
                    }),
                    columns.end());
    return {};
}

[[nodiscard]] bool is_supported_index_column_type(catalog::CatalogColumnType type) noexcept
{
    switch (type) {
    case catalog::CatalogColumnType::Int64:
    case catalog::CatalogColumnType::UInt16:
    case catalog::CatalogColumnType::UInt32:
    case catalog::CatalogColumnType::Utf8:
        return true;
    default:
        return false;
    }
}

std::error_code stage_alter_actions(DdlCommandContext& context,
                                    CatalogTableDescriptor& table,
                                    std::vector<CatalogColumnDescriptor>& columns,
                                    const std::vector<AlterTableAction>& actions)
{
    std::uint16_t next_ordinal = static_cast<std::uint16_t>(max_column_ordinal(columns) + 1U);

    for (const auto& action : actions) {
        if (std::holds_alternative<AlterTableRenameTable>(action)) {
            const auto& rename = std::get<AlterTableRenameTable>(action);
            if (auto ec = validate_identifier(rename.new_table_name); ec) {
                return ec;
            }
            if (table.name == rename.new_table_name) {
                continue;
            }
            if (auto existing = find_table(*context.accessor, table.schema_id, rename.new_table_name); existing) {
                return make_error_code(DdlErrc::TableAlreadyExists);
            }
            if (auto ec = stage_table_rename(context, table, rename.new_table_name); ec) {
                return ec;
            }
            continue;
        }

        if (std::holds_alternative<AlterTableRenameColumn>(action)) {
            const auto& rename = std::get<AlterTableRenameColumn>(action);
            if (auto ec = validate_identifier(rename.new_column_name); ec) {
                return ec;
            }
            if (rename.column_name == rename.new_column_name) {
                continue;
            }
            auto* column = find_column(columns, rename.column_name);
            if (column == nullptr) {
                return make_error_code(DdlErrc::ValidationFailed);
            }
            if (!column_name_available(columns, rename.new_column_name)) {
                return make_error_code(DdlErrc::ValidationFailed);
            }
            if (auto ec = stage_column_rename(context, *column, rename.new_column_name); ec) {
                return ec;
            }
            continue;
        }

        if (std::holds_alternative<AlterTableAddColumn>(action)) {
            const auto& add = std::get<AlterTableAddColumn>(action);
            if (auto ec = stage_column_add(context, columns, add, table.relation_id, next_ordinal); ec) {
                return ec;
            }
            continue;
        }

        if (std::holds_alternative<AlterTableDropColumn>(action)) {
            const auto& drop = std::get<AlterTableDropColumn>(action);
            if (auto ec = stage_column_drop(context, columns, drop); ec) {
                return ec;
            }
            continue;
        }
    }

    return {};
}

DdlCommandResponse handle_create_schema(DdlCommandContext& context, const CreateSchemaRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl create schema missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl create schema missing catalog accessor");
    }

    if (!request.database_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target database id is invalid");
    }

    if (auto ec = validate_identifier(request.name); ec) {
        return make_failure(ec, "schema name is invalid");
    }

    auto database = context.accessor->database(request.database_id);
    if (!database) {
        return make_failure(make_error_code(DdlErrc::DatabaseNotFound), "database not found");
    }

    if (schema_exists(*context.accessor, request.database_id, request.name)) {
        if (request.if_not_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::SchemaAlreadyExists), "schema already exists");
    }

    catalog::CreateSchemaRequest stage_request{};
    stage_request.database_id = request.database_id;
    stage_request.name = request.name;

    catalog::CreateSchemaResult stage_result{};
    if (auto ec = catalog::stage_create_schema(*context.mutator, context.allocator, stage_request, stage_result); ec) {
        return make_failure(map_stage_error(ec), ec.message());
    }

    DdlCommandResult payload{std::in_place_type<catalog::CreateSchemaResult>, stage_result};
    return make_success(std::move(payload));
}

DdlCommandResponse handle_drop_schema(DdlCommandContext& context, const DropSchemaRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl drop schema missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl drop schema missing catalog accessor");
    }

    if (!request.database_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target database id is invalid");
    }

    if (auto ec = validate_identifier(request.name); ec) {
        return make_failure(ec, "schema name is invalid");
    }

    auto database = context.accessor->database(request.database_id);
    if (!database) {
        return make_failure(make_error_code(DdlErrc::DatabaseNotFound), "database not found");
    }

    auto schema = find_schema(*context.accessor, request.database_id, request.name);
    if (!schema) {
        if (request.if_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto tables = context.accessor->tables(schema->schema_id);
    const DdlDependencyGraph dependencies{*context.accessor};
    auto schema_indexes = dependencies.indexes_in_schema(schema->schema_id);

    if (!tables.empty()) {
        if (!request.cascade) {
            return make_failure(make_error_code(DdlErrc::ValidationFailed), "schema contains tables");
        }

        for (const auto& table : tables) {
            DropTableRequest cascade_request{};
            cascade_request.schema_id = schema->schema_id;
            cascade_request.name = std::string(table.name);
            cascade_request.cascade = true;
            cascade_request.if_exists = true;

            auto cascade_result = handle_drop_table(context, cascade_request);
            if (!cascade_result.success) {
                return cascade_result;
            }
        }
    } else if (!schema_indexes.empty()) {
        if (!request.cascade) {
            return make_failure(make_error_code(DdlErrc::ValidationFailed), "schema contains indexes");
        }

        for (const auto& index : schema_indexes) {
            DropIndexRequest cascade_index_request{};
            cascade_index_request.schema_id = schema->schema_id;
            cascade_index_request.index_name = std::string(index.name);
            cascade_index_request.if_exists = true;

            auto cascade_result = handle_drop_index(context, cascade_index_request);
            if (!cascade_result.success) {
                return cascade_result;
            }
        }
    }

    auto payload = serialize_schema(*schema);
    context.mutator->stage_delete(catalog::kCatalogSchemasRelationId, schema->schema_id.value, schema->tuple, std::move(payload));
    return make_success();
}

bool validate_table_columns(const CreateTableRequest& request, std::string& message)
{
    if (request.columns.empty()) {
        message = "table definition requires at least one column";
        return false;
    }

    std::unordered_set<std::string_view> seen_names;
    seen_names.reserve(request.columns.size());

    for (const auto& column : request.columns) {
        if (auto ec = validate_identifier(column.name); ec) {
            message = "column name is invalid";
            return false;
        }
        if (column.column_type == catalog::CatalogColumnType::Unknown) {
            message = "column type must be specified";
            return false;
        }
        if (!seen_names.insert(column.name).second) {
            message = "duplicate column names are not allowed";
            return false;
        }
    }

    return true;
}

DdlCommandResponse handle_create_table(DdlCommandContext& context, const CreateTableRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl create table missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl create table missing catalog accessor");
    }

    if (!request.schema_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target schema id is invalid");
    }

    if (auto ec = validate_identifier(request.name); ec) {
        return make_failure(ec, "table name is invalid");
    }

    auto schema = context.accessor->schema(request.schema_id);
    if (!schema) {
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto existing = find_table(*context.accessor, request.schema_id, request.name);
    if (existing) {
        if (request.if_not_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::TableAlreadyExists), "table already exists");
    }

    std::string validation_error;
    if (!validate_table_columns(request, validation_error)) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), validation_error);
    }

    catalog::CreateTableRequest stage_request{};
    stage_request.schema_id = request.schema_id;
    stage_request.name = request.name;
    stage_request.table_type = request.table_type;
    stage_request.root_page_id = request.root_page_id;
    stage_request.columns = request.columns;

    catalog::CreateTableResult stage_result{};
    if (auto ec = catalog::stage_create_table(*context.mutator, context.allocator, stage_request, stage_result); ec) {
        return make_failure(map_stage_error(ec), ec.message());
    }

    DdlCommandResult payload{std::in_place_type<catalog::CreateTableResult>, stage_result};
    return make_success(std::move(payload));
}

DdlCommandResponse handle_drop_table(DdlCommandContext& context, const DropTableRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl drop table missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl drop table missing catalog accessor");
    }

    if (!request.schema_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target schema id is invalid");
    }

    if (auto ec = validate_identifier(request.name); ec) {
        return make_failure(ec, "table name is invalid");
    }

    auto schema = context.accessor->schema(request.schema_id);
    if (!schema) {
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto table = find_table(*context.accessor, request.schema_id, request.name);
    if (!table) {
        if (request.if_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::TableNotFound), "table not found");
    }

    const DdlDependencyGraph dependencies{*context.accessor};
    auto dependent_indexes = dependencies.indexes_on_table(table->relation_id);
    for (const auto& index : dependent_indexes) {
        DropIndexRequest drop_index_request{};
        drop_index_request.schema_id = table->schema_id;
        drop_index_request.index_name = std::string(index.name);

        if (context.drop_index_cleanup) {
            if (auto ec = context.drop_index_cleanup(drop_index_request, index, *context.mutator); ec) {
                return make_failure(ec, ec.message());
            }
        }

        auto index_payload = serialize_index(index);
        context.mutator->stage_delete(catalog::kCatalogIndexesRelationId,
                                      index.index_id.value,
                                      index.tuple,
                                      std::move(index_payload));
    }

    auto columns = context.accessor->columns(table->relation_id);

    if (context.drop_table_cleanup) {
        const auto column_span = std::span<const catalog::CatalogColumnDescriptor>(columns.data(), columns.size());
        if (auto ec = context.drop_table_cleanup(request, *table, column_span, *context.mutator); ec) {
            return make_failure(ec, ec.message());
        }
    }

    for (const auto& column : columns) {
        auto column_payload = serialize_column(column);
        context.mutator->stage_delete(catalog::kCatalogColumnsRelationId,
                                      column.column_id.value,
                                      column.tuple,
                                      std::move(column_payload));
    }

    auto table_payload = serialize_table(*table);
    context.mutator->stage_delete(catalog::kCatalogTablesRelationId,
                                  table->relation_id.value,
                                  table->tuple,
                                  std::move(table_payload));

    return make_success();
}

DdlCommandResponse handle_alter_table(DdlCommandContext& context, const AlterTableRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl alter table missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl alter table missing catalog accessor");
    }

    if (!request.schema_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target schema id is invalid");
    }

    if (auto ec = validate_identifier(request.name); ec) {
        return make_failure(ec, "table name is invalid");
    }

    auto schema = context.accessor->schema(request.schema_id);
    if (!schema) {
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto table_opt = find_table(*context.accessor, request.schema_id, request.name);
    if (!table_opt) {
        return make_failure(make_error_code(DdlErrc::TableNotFound), "table not found");
    }

    CatalogTableDescriptor table = *table_opt;
    auto columns = context.accessor->columns(table.relation_id);

    if (auto ec = stage_alter_actions(context, table, columns, request.actions); ec) {
        if (!ec) {
            return make_failure(make_error_code(DdlErrc::ExecutionFailed), "alter table failed");
        }
        return make_failure(ec, ec.message());
    }

    return make_success();
}

DdlCommandResponse handle_create_index(DdlCommandContext& context, const CreateIndexRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl create index missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl create index missing catalog accessor");
    }

    if (!request.schema_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target schema id is invalid");
    }

    if (auto ec = validate_identifier(request.table_name); ec) {
        return make_failure(ec, "table name is invalid");
    }

    if (auto ec = validate_identifier(request.index_name); ec) {
        return make_failure(ec, "index name is invalid");
    }

    if (request.unique) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "unique indexes are not supported yet");
    }

    if (request.column_names.empty()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "create index requires at least one column");
    }
    if (request.column_names.size() != 1U) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "create index currently supports exactly one column");
    }

    const auto& column_name = request.column_names.front();
    if (auto ec = validate_identifier(column_name); ec) {
        return make_failure(ec, "index column name is invalid");
    }

    auto schema = context.accessor->schema(request.schema_id);
    if (!schema) {
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto table_opt = find_table(*context.accessor, request.schema_id, request.table_name);
    if (!table_opt) {
        return make_failure(make_error_code(DdlErrc::TableNotFound), "table not found");
    }

    const auto indexes = context.accessor->indexes(table_opt->relation_id);
    if (!index_name_available(indexes, request.index_name)) {
        if (request.if_not_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::IndexAlreadyExists), "index already exists");
    }

    auto columns = context.accessor->columns(table_opt->relation_id);
    const auto* column = find_column(columns, column_name);
    if (column == nullptr) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "column not found");
    }
    if (!is_supported_index_column_type(column->column_type)) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "column type is not supported for indexes");
    }

    CreateIndexStoragePlan storage_plan{};
    if (context.create_index_storage) {
        if (auto ec = context.create_index_storage(request, *table_opt, *column, storage_plan); ec) {
            return make_failure(ec, ec.message());
        }
    }

    if (storage_plan.root_page_id == 0U) {
        return make_failure(make_error_code(DdlErrc::ExecutionFailed), "index storage plan missing root page reservation");
    }

    catalog::CreateIndexRequest stage_request{};
    stage_request.relation_id = table_opt->relation_id;
    stage_request.name = request.index_name;
    stage_request.index_type = catalog::CatalogIndexType::BTree;
    stage_request.root_page_id = storage_plan.root_page_id;

    catalog::CreateIndexResult stage_result{};
    if (auto ec = catalog::stage_create_index(*context.mutator, context.allocator, stage_request, stage_result); ec) {
        return make_failure(map_stage_error(ec), ec.message());
    }

    if (storage_plan.finalize) {
        if (auto ec = storage_plan.finalize(stage_result, *context.mutator); ec) {
            return make_failure(ec, ec.message());
        }
    }

    DdlCommandResult payload{std::in_place_type<catalog::CreateIndexResult>, stage_result};
    return make_success(std::move(payload));
}

DdlCommandResponse handle_drop_index(DdlCommandContext& context, const DropIndexRequest& request)
{
    if (context.mutator == nullptr) {
        return make_context_failure("ddl drop index missing catalog mutator");
    }
    if (context.accessor == nullptr) {
        return make_context_failure("ddl drop index missing catalog accessor");
    }

    if (!request.schema_id.is_valid()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "target schema id is invalid");
    }

    if (auto ec = validate_identifier(request.index_name); ec) {
        return make_failure(ec, "index name is invalid");
    }

    auto schema = context.accessor->schema(request.schema_id);
    if (!schema) {
        return make_failure(make_error_code(DdlErrc::SchemaNotFound), "schema not found");
    }

    auto indexes = context.accessor->indexes_for_schema(request.schema_id);
    const auto* descriptor = find_index(indexes, request.index_name);
    if (descriptor == nullptr) {
        if (request.if_exists) {
            return make_success();
        }
        return make_failure(make_error_code(DdlErrc::IndexNotFound), "index not found");
    }

    if (context.drop_index_cleanup) {
        if (auto ec = context.drop_index_cleanup(request, *descriptor, *context.mutator); ec) {
            return make_failure(ec, ec.message());
        }
    }

    auto payload = serialize_index(*descriptor);
    context.mutator->stage_delete(catalog::kCatalogIndexesRelationId,
                                  descriptor->index_id.value,
                                  descriptor->tuple,
                                  std::move(payload));

    return make_success();
}

}  // namespace

void register_catalog_handlers(DdlCommandDispatcher& dispatcher)
{
    dispatcher.register_handler<CreateSchemaRequest>(handle_create_schema);
    dispatcher.register_handler<DropSchemaRequest>(handle_drop_schema);
    dispatcher.register_handler<CreateTableRequest>(handle_create_table);
    dispatcher.register_handler<DropTableRequest>(handle_drop_table);
    dispatcher.register_handler<AlterTableRequest>(handle_alter_table);
    dispatcher.register_handler<CreateIndexRequest>(handle_create_index);
    dispatcher.register_handler<DropIndexRequest>(handle_drop_index);
}

}  // namespace bored::ddl

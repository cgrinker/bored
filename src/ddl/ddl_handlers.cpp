#include "bored/ddl/ddl_handlers.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/ddl/ddl_errors.hpp"
#include "bored/ddl/ddl_validation.hpp"

#include <algorithm>
#include <optional>
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

    const auto tables = context.accessor->tables(schema->schema_id);
    if (!tables.empty()) {
        return make_failure(make_error_code(DdlErrc::ValidationFailed), "schema contains tables");
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

    const auto columns = context.accessor->columns(table->relation_id);
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

}  // namespace

void register_catalog_handlers(DdlCommandDispatcher& dispatcher)
{
    dispatcher.register_handler<CreateSchemaRequest>(handle_create_schema);
    dispatcher.register_handler<DropSchemaRequest>(handle_drop_schema);
    dispatcher.register_handler<CreateTableRequest>(handle_create_table);
    dispatcher.register_handler<DropTableRequest>(handle_drop_table);
}

}  // namespace bored::ddl

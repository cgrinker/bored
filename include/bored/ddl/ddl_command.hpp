#pragma once

#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_ids.hpp"
#include "bored/ddl/ddl_errors.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace bored::ddl {

enum class DdlVerb : std::uint8_t {
    CreateDatabase = 0,
    DropDatabase,
    CreateSchema,
    DropSchema,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    Count
};

struct CreateDatabaseRequest final {
    std::string name{};
    bool if_not_exists = false;
};

struct DropDatabaseRequest final {
    std::string name{};
    bool if_exists = false;
    bool cascade = false;
};

struct CreateSchemaRequest final {
    catalog::DatabaseId database_id{};
    std::string name{};
    bool if_not_exists = false;
};

struct DropSchemaRequest final {
    catalog::DatabaseId database_id{};
    std::string name{};
    bool if_exists = false;
    bool cascade = false;
};

struct AlterTableRenameTable final {
    std::string new_table_name{};
};

struct AlterTableRenameColumn final {
    std::string column_name{};
    std::string new_column_name{};
};

struct AlterTableAddColumn final {
    catalog::ColumnDefinition column{};
    bool if_not_exists = false;
};

struct AlterTableDropColumn final {
    std::string column_name{};
    bool if_exists = false;
};

using AlterTableAction = std::variant<AlterTableRenameTable, AlterTableRenameColumn, AlterTableAddColumn, AlterTableDropColumn>;

struct CreateTableRequest final {
    catalog::SchemaId schema_id{};
    std::string name{};
    std::vector<catalog::ColumnDefinition> columns{};
    bool if_not_exists = false;
};

struct DropTableRequest final {
    catalog::SchemaId schema_id{};
    std::string name{};
    bool if_exists = false;
    bool cascade = false;
};

struct AlterTableRequest final {
    catalog::SchemaId schema_id{};
    std::string name{};
    std::vector<AlterTableAction> actions{};
};

struct CreateIndexRequest final {
    catalog::SchemaId schema_id{};
    std::string table_name{};
    std::string index_name{};
    std::vector<std::string> column_names{};
    bool unique = false;
    bool if_not_exists = false;
};

struct DropIndexRequest final {
    catalog::SchemaId schema_id{};
    std::string index_name{};
    bool if_exists = false;
};

using DdlCommand = std::variant<CreateDatabaseRequest,
                                DropDatabaseRequest,
                                CreateSchemaRequest,
                                DropSchemaRequest,
                                CreateTableRequest,
                                DropTableRequest,
                                AlterTableRequest,
                                CreateIndexRequest,
                                DropIndexRequest>;

using DdlCommandResult = std::variant<std::monostate,
                                      catalog::CreateSchemaResult,
                                      catalog::CreateTableResult,
                                      catalog::CreateIndexResult>;

struct DdlCommandResponse final {
    bool success = false;
    std::error_code error{};
    std::string message{};
    DdlCommandResult result{};
};

struct DdlCommandContext final {
    catalog::CatalogTransaction& transaction;
    catalog::CatalogIdentifierAllocator& allocator;
};

namespace detail {

template <typename Request>
struct RequestTrait;

template <>
struct RequestTrait<CreateDatabaseRequest> {
    static constexpr DdlVerb verb = DdlVerb::CreateDatabase;
};

template <>
struct RequestTrait<DropDatabaseRequest> {
    static constexpr DdlVerb verb = DdlVerb::DropDatabase;
};

template <>
struct RequestTrait<CreateSchemaRequest> {
    static constexpr DdlVerb verb = DdlVerb::CreateSchema;
};

template <>
struct RequestTrait<DropSchemaRequest> {
    static constexpr DdlVerb verb = DdlVerb::DropSchema;
};

template <>
struct RequestTrait<CreateTableRequest> {
    static constexpr DdlVerb verb = DdlVerb::CreateTable;
};

template <>
struct RequestTrait<DropTableRequest> {
    static constexpr DdlVerb verb = DdlVerb::DropTable;
};

template <>
struct RequestTrait<AlterTableRequest> {
    static constexpr DdlVerb verb = DdlVerb::AlterTable;
};

template <>
struct RequestTrait<CreateIndexRequest> {
    static constexpr DdlVerb verb = DdlVerb::CreateIndex;
};

template <>
struct RequestTrait<DropIndexRequest> {
    static constexpr DdlVerb verb = DdlVerb::DropIndex;
};

}  // namespace detail

template <typename Request>
constexpr DdlVerb request_verb() noexcept
{
    return detail::RequestTrait<Request>::verb;
}

template <typename Request>
constexpr std::size_t request_verb_index() noexcept
{
    return static_cast<std::size_t>(request_verb<Request>());
}

inline DdlCommandResponse make_success(DdlCommandResult result = {})
{
    DdlCommandResponse response{};
    response.success = true;
    response.result = std::move(result);
    return response;
}

inline DdlCommandResponse make_failure(std::error_code error, std::string message = {})
{
    DdlCommandResponse response{};
    response.success = false;
    response.error = std::move(error);
    response.message = std::move(message);
    return response;
}

}  // namespace bored::ddl

#pragma once

#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_ids.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/ddl/ddl_errors.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <system_error>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace bored::catalog {
class CatalogAccessor;
class CatalogMutator;
class SequenceAllocator;
}

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
    CreateView,
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
    catalog::CatalogTableType table_type = catalog::CatalogTableType::Heap;
    std::uint32_t root_page_id = 0U;
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

using DropTableCleanupHook = std::function<std::error_code(const DropTableRequest&,
                                                           const catalog::CatalogTableDescriptor&,
                                                           std::span<const catalog::CatalogColumnDescriptor>,
                                                           catalog::CatalogMutator&)>;

using CatalogDirtyRelationHook = std::function<std::error_code(std::span<const catalog::RelationId>, std::uint64_t)>;

struct CreateIndexRequest;
struct DropIndexRequest;

struct CreateIndexStoragePlan final {
    std::uint32_t root_page_id = 0U;
    std::uint16_t max_fanout = 0U;
    std::string comparator{};
    std::function<std::error_code(const catalog::CreateIndexResult&, catalog::CatalogMutator&)> finalize{};
};

using CreateIndexStorageHook = std::function<std::error_code(const CreateIndexRequest&,
                                                             const catalog::CatalogTableDescriptor&,
                                                             const catalog::CatalogColumnDescriptor&,
                                                             CreateIndexStoragePlan&)>;

using DropIndexCleanupHook = std::function<std::error_code(const DropIndexRequest&,
                                                          const catalog::CatalogIndexDescriptor&,
                                                          catalog::CatalogMutator&)>;

struct CreateIndexRequest final {
    catalog::SchemaId schema_id{};
    std::string table_name{};
    std::string index_name{};
    std::vector<std::string> column_names{};
    std::uint16_t max_fanout = 0U;
    std::string comparator{};
    bool unique = false;
    std::vector<std::string> covering_column_names{};
    std::string predicate{};
    bool if_not_exists = false;
};

struct DropIndexRequest final {
    catalog::SchemaId schema_id{};
    std::string index_name{};
    bool if_exists = false;
};

struct CreateViewRequest final {
    catalog::SchemaId schema_id{};
    std::string name{};
    std::string definition{};
    bool if_not_exists = false;
};

using DdlCommand = std::variant<CreateDatabaseRequest,
                                DropDatabaseRequest,
                                CreateSchemaRequest,
                                DropSchemaRequest,
                                CreateTableRequest,
                                DropTableRequest,
                                AlterTableRequest,
                                CreateIndexRequest,
                                DropIndexRequest,
                                CreateViewRequest>;

using DdlCommandResult = std::variant<std::monostate,
                                      catalog::CreateSchemaResult,
                                      catalog::CreateTableResult,
                                      catalog::CreateIndexResult,
                                      catalog::CreateViewResult>;

enum class DdlDiagnosticSeverity : std::uint8_t {
    Info = 0,
    Warning,
    Error
};

struct DdlCommandResponse final {
    bool success = false;
    std::error_code error{};
    std::string message{};
    DdlDiagnosticSeverity severity = DdlDiagnosticSeverity::Error;
    std::vector<std::string> remediation_hints{};
    DdlCommandResult result{};
};

struct DdlCommandContext final {
    catalog::CatalogTransaction& transaction;
    catalog::CatalogIdentifierAllocator& allocator;
    catalog::CatalogMutator* mutator = nullptr;
    catalog::CatalogAccessor* accessor = nullptr;
    catalog::SequenceAllocator* sequence_allocator = nullptr;
    DropTableCleanupHook drop_table_cleanup{};
    CatalogDirtyRelationHook dirty_relation_notifier{};
    CreateIndexStorageHook create_index_storage{};
    DropIndexCleanupHook drop_index_cleanup{};
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

template <>
struct RequestTrait<CreateViewRequest> {
    static constexpr DdlVerb verb = DdlVerb::CreateView;
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

inline DdlDiagnosticSeverity default_diagnostic_severity(std::error_code error) noexcept
{
    if (!error) {
        return DdlDiagnosticSeverity::Info;
    }
    if (error.category() != ddl_error_category()) {
        return DdlDiagnosticSeverity::Error;
    }

    switch (static_cast<DdlErrc>(error.value())) {
    case DdlErrc::Success:
        return DdlDiagnosticSeverity::Info;
    case DdlErrc::InvalidIdentifier:
    case DdlErrc::InvalidOption:
    case DdlErrc::SchemaNotFound:
    case DdlErrc::SchemaAlreadyExists:
    case DdlErrc::TableNotFound:
    case DdlErrc::TableAlreadyExists:
    case DdlErrc::IndexNotFound:
    case DdlErrc::IndexAlreadyExists:
    case DdlErrc::DatabaseNotFound:
    case DdlErrc::DatabaseAlreadyExists:
    case DdlErrc::ValidationFailed:
        return DdlDiagnosticSeverity::Warning;
    default:
        return DdlDiagnosticSeverity::Error;
    }
}

inline std::vector<std::string> default_remediation_hints(std::error_code error)
{
    if (!error) {
        return {};
    }
    if (error.category() != ddl_error_category()) {
        return {"Inspect server logs for additional details."};
    }

    switch (static_cast<DdlErrc>(error.value())) {
    case DdlErrc::Success:
        return {};
    case DdlErrc::InvalidIdentifier:
        return {"Verify identifier names use supported characters and length constraints."};
    case DdlErrc::InvalidOption:
        return {"Review command options and remove unsupported flags or correct their values."};
    case DdlErrc::SchemaNotFound:
        return {"Ensure the target schema exists or create it before running this command."};
    case DdlErrc::SchemaAlreadyExists:
        return {"Use IF NOT EXISTS or drop the existing schema before retrying."};
    case DdlErrc::TableNotFound:
        return {"Confirm the table name and schema are correct."};
    case DdlErrc::TableAlreadyExists:
        return {"Use IF NOT EXISTS, drop the table, or choose a different table name."};
    case DdlErrc::IndexNotFound:
        return {"Confirm the index name exists on the target table."};
    case DdlErrc::IndexAlreadyExists:
        return {"Use IF NOT EXISTS, drop the index, or choose a different index name."};
    case DdlErrc::DatabaseNotFound:
        return {"Verify the database exists and is accessible to this session."};
    case DdlErrc::DatabaseAlreadyExists:
        return {"Use IF NOT EXISTS or drop the existing database before retrying."};
    case DdlErrc::HandlerMissing:
        return {"Register a handler for this DDL verb before dispatching commands."};
    case DdlErrc::ValidationFailed:
        return {"Review the validation message and adjust the command parameters."};
    case DdlErrc::ExecutionFailed:
        return {"Check storage and catalog logs for execution errors, then retry after resolving the underlying issue."};
    default:
        return {"Inspect server logs for additional details."};
    }
}

inline DdlCommandResponse make_success(DdlCommandResult result = {})
{
    DdlCommandResponse response{};
    response.success = true;
    response.severity = DdlDiagnosticSeverity::Info;
    response.remediation_hints.clear();
    response.result = std::move(result);
    return response;
}

inline DdlCommandResponse make_failure(std::error_code error, std::string message = {})
{
    DdlCommandResponse response{};
    response.success = false;
    response.error = std::move(error);
    response.message = std::move(message);
    response.severity = default_diagnostic_severity(response.error);
    response.remediation_hints = default_remediation_hints(response.error);
    return response;
}

inline DdlCommandResponse make_failure(std::error_code error,
                                       std::string message,
                                       DdlDiagnosticSeverity severity,
                                       std::vector<std::string> remediation_hints)
{
    DdlCommandResponse response{};
    response.success = false;
    response.error = std::move(error);
    response.message = std::move(message);
    response.severity = severity;
    response.remediation_hints = std::move(remediation_hints);
    return response;
}

}  // namespace bored::ddl

#include "bored/parser/ddl_command_builder.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_ddl.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iterator>
#include <string>
#include <utility>
#include <variant>

namespace bored::parser {
namespace {

struct TranslationOutcome final {
    std::vector<ddl::DdlCommand> commands{};
    std::vector<ParserDiagnostic> diagnostics{};
};

ParserDiagnostic make_diagnostic(ParserSeverity severity,
                                 std::string message,
                                 const ScriptStatement& statement,
                                 std::vector<std::string> hints = {})
{
    ParserDiagnostic diagnostic{};
    diagnostic.severity = severity;
    diagnostic.message = std::move(message);
    diagnostic.statement = statement.text;
    diagnostic.remediation_hints = std::move(hints);
    return diagnostic;
}

bool has_error(const std::vector<ParserDiagnostic>& diagnostics) noexcept
{
    return std::any_of(diagnostics.begin(), diagnostics.end(), [](const ParserDiagnostic& diagnostic) {
        return diagnostic.severity == ParserSeverity::Error;
    });
}

std::string uppercase_copy(std::string_view input)
{
    std::string upper;
    upper.reserve(input.size());
    std::transform(input.begin(), input.end(), std::back_inserter(upper), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return upper;
}

std::optional<catalog::DatabaseId> resolve_database_id(const DdlCommandBuilderConfig& config,
                                                       std::string_view name,
                                                       const ScriptStatement& statement,
                                                       std::vector<ParserDiagnostic>& diagnostics)
{
    if (!name.empty()) {
        if (config.database_lookup) {
            if (auto resolved = config.database_lookup(name)) {
                if (resolved->is_valid()) {
                    return resolved;
                }
            }
        }

        if (config.accessor != nullptr) {
            if (auto descriptor = config.accessor->database(name)) {
                return descriptor->database_id;
            }
        }

        std::string message = "Database '";
        message.append(name);
        message.append("' not found");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                              std::move(message),
                                              statement,
                                              {"Ensure the database exists and is visible to the current session."}));
        return std::nullopt;
    }

    if (config.default_database_id && config.default_database_id->is_valid()) {
        return *config.default_database_id;
    }

    diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                          "No default database configured for implicit database resolution",
                                          statement,
                                          {"Set DdlCommandBuilderConfig::default_database_id or qualify the database explicitly."}));
    return std::nullopt;
}

std::optional<catalog::SchemaId> resolve_schema_id(const DdlCommandBuilderConfig& config,
                                                   std::string_view schema_name,
                                                   catalog::DatabaseId database_id,
                                                   const ScriptStatement& statement,
                                                   std::vector<ParserDiagnostic>& diagnostics)
{
    if (!schema_name.empty()) {
        if (config.schema_lookup) {
            if (auto resolved = config.schema_lookup(database_id, schema_name)) {
                if (resolved->is_valid()) {
                    return resolved;
                }
            }
        }

        if (config.accessor != nullptr) {
            const auto schemas = config.accessor->schemas(database_id);
            for (const auto& schema : schemas) {
                if (schema.name == schema_name) {
                    return schema.schema_id;
                }
            }
        }

        std::string message = "Schema '";
        message.append(schema_name);
        message.append("' not found");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                              std::move(message),
                                              statement,
                                              {"Verify the schema exists in the target database."}));
        return std::nullopt;
    }

    if (config.default_schema_id && config.default_schema_id->is_valid()) {
        return *config.default_schema_id;
    }

    if (config.accessor != nullptr) {
        if (auto descriptor = config.accessor->database(database_id)) {
            if (descriptor->default_schema_id.is_valid()) {
                return descriptor->default_schema_id;
            }
        }
    }

    diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                          "No default schema configured for unqualified statement",
                                          statement,
                                          {"Set DdlCommandBuilderConfig::default_schema_id or qualify the schema explicitly."}));
    return std::nullopt;
}

std::optional<catalog::CatalogColumnType> map_column_type(std::string_view type_name)
{
    const auto upper = uppercase_copy(type_name);

    if (upper == "INT" || upper == "INTEGER" || upper == "BIGINT" || upper == "INT8" || upper == "INT4" || upper == "INT64") {
        return catalog::CatalogColumnType::Int64;
    }
    if (upper == "SMALLINT" || upper == "INT2" || upper == "INT16" || upper == "UINT16") {
        return catalog::CatalogColumnType::UInt16;
    }
    if (upper == "UINT32" || upper == "INT32") {
        return catalog::CatalogColumnType::UInt32;
    }
    if (upper == "TEXT" || upper == "VARCHAR" || upper == "CHAR" || upper == "JSON" || upper == "STRING" ||
        upper == "UUID" || upper == "TIMESTAMP" || upper == "DATETIME" || upper == "DATE") {
        return catalog::CatalogColumnType::Utf8;
    }

    return std::nullopt;
}

void append_column_feature_warnings(const ColumnDefinition& column,
                                    const ScriptStatement& statement,
                                    std::vector<ParserDiagnostic>& diagnostics)
{
    if (column.primary_key) {
        std::string message = "PRIMARY KEY constraints are not currently enforced; column '";
        message.append(column.name.value);
        message.append("' will be created without a key.");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                              std::move(message),
                                              statement,
                                              {"Define the PRIMARY KEY using supported catalog APIs after table creation."}));
    }

    if (column.unique) {
        std::string message = "UNIQUE constraints are not currently enforced; column '";
        message.append(column.name.value);
        message.append("' will allow duplicate values.");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                              std::move(message),
                                              statement,
                                              {"Create a UNIQUE index after table creation to enforce uniqueness."}));
    }

    if (column.default_expression) {
        std::string message = "Default expressions are not yet supported; column '";
        message.append(column.name.value);
        message.append("' will not receive an automatic default.");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                              std::move(message),
                                              statement,
                                              {"Update application code to supply explicit values until defaults are implemented."}));
    }

    if (!column.primary_key && column.not_null) {
        std::string message = "NOT NULL constraints are not currently enforced; column '";
        message.append(column.name.value);
        message.append("' will permit null values.");
        diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                              std::move(message),
                                              statement,
                                              {"Validate input at the application layer until NOT NULL enforcement is available."}));
    }
}

TranslationOutcome translate_create_database(const CreateDatabaseStatement& ast, const ScriptStatement& statement)
{
    TranslationOutcome outcome{};

    ddl::CreateDatabaseRequest request{};
    request.name = ast.name.value;
    request.if_not_exists = ast.if_not_exists;
    outcome.commands.emplace_back(std::move(request));

    return outcome;
}

TranslationOutcome translate_drop_database(const DropDatabaseStatement& ast, const ScriptStatement& statement)
{
    TranslationOutcome outcome{};

    ddl::DropDatabaseRequest request{};
    request.name = ast.name.value;
    request.if_exists = ast.if_exists;
    request.cascade = ast.cascade;
    outcome.commands.emplace_back(std::move(request));

    return outcome;
}

TranslationOutcome translate_create_schema(const CreateSchemaStatement& ast,
                                           const ScriptStatement& statement,
                                           const DdlCommandBuilderConfig& config)
{
    TranslationOutcome outcome{};

    const auto database_id = resolve_database_id(config, ast.database.value, statement, outcome.diagnostics);
    if (!database_id) {
        return outcome;
    }

    ddl::CreateSchemaRequest request{};
    request.database_id = *database_id;
    request.name = ast.name.value;
    request.if_not_exists = ast.if_not_exists;

    if (ast.authorization) {
        outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                                      "AUTHORIZATION clauses are ignored during schema creation.",
                                                      statement,
                                                      {"Grant privileges via the catalog roles subsystem after schema creation."}));
    }

    if (!ast.embedded_statements.empty()) {
        outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Warning,
                                                      "Embedded CREATE statements inside CREATE SCHEMA are not executed automatically.",
                                                      statement,
                                                      {"Execute embedded statements separately after the schema is created."}));
    }

    outcome.commands.emplace_back(std::move(request));
    return outcome;
}

TranslationOutcome translate_drop_schema(const DropSchemaStatement& ast,
                                         const ScriptStatement& statement,
                                         const DdlCommandBuilderConfig& config)
{
    TranslationOutcome outcome{};

    const bool cascade = ast.behavior == DropSchemaStatement::Behavior::Cascade;

    for (const auto& schema_name : ast.schemas) {
        const auto database_id = resolve_database_id(config, schema_name.database.value, statement, outcome.diagnostics);
        if (!database_id) {
            continue;
        }

        ddl::DropSchemaRequest request{};
        request.database_id = *database_id;
        request.name = schema_name.name.value;
        request.if_exists = ast.if_exists || schema_name.if_exists;
        request.cascade = cascade;
        outcome.commands.emplace_back(std::move(request));
    }

    if (outcome.commands.empty() && has_error(outcome.diagnostics)) {
        outcome.commands.clear();
    }

    return outcome;
}

TranslationOutcome translate_create_table(const CreateTableStatement& ast,
                                          const ScriptStatement& statement,
                                          const DdlCommandBuilderConfig& config)
{
    TranslationOutcome outcome{};

    const auto database_id = resolve_database_id(config, {}, statement, outcome.diagnostics);
    if (!database_id) {
        return outcome;
    }

    const auto schema_id = resolve_schema_id(config, ast.schema.value, *database_id, statement, outcome.diagnostics);
    if (!schema_id) {
        return outcome;
    }

    ddl::CreateTableRequest request{};
    request.schema_id = *schema_id;
    request.name = ast.name.value;
    request.if_not_exists = ast.if_not_exists;

    bool column_failure = false;
    request.columns.reserve(ast.columns.size());
    for (std::size_t index = 0; index < ast.columns.size(); ++index) {
        const auto& column_ast = ast.columns[index];
        const auto column_type = map_column_type(column_ast.type_name.value);
        if (!column_type) {
            std::string message = "Unsupported column type '";
            message.append(column_ast.type_name.value);
            message.append("'");
            outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                                          std::move(message),
                                                          statement,
                                                          {"Use INT, SMALLINT, UINT32, or text-compatible types (TEXT, VARCHAR, JSON)."}));
            column_failure = true;
            continue;
        }

        catalog::ColumnDefinition column{};
        column.name = column_ast.name.value;
        column.column_type = *column_type;
        column.ordinal = static_cast<std::uint16_t>(index + 1U);
        request.columns.push_back(std::move(column));
        append_column_feature_warnings(column_ast, statement, outcome.diagnostics);
    }

    if (column_failure || request.columns.empty()) {
        return outcome;
    }

    outcome.commands.emplace_back(std::move(request));
    return outcome;
}

TranslationOutcome translate_drop_table(const DropTableStatement& ast,
                                        const ScriptStatement& statement,
                                        const DdlCommandBuilderConfig& config)
{
    TranslationOutcome outcome{};

    const auto database_id = resolve_database_id(config, {}, statement, outcome.diagnostics);
    if (!database_id) {
        return outcome;
    }

    const auto schema_id = resolve_schema_id(config, ast.schema.value, *database_id, statement, outcome.diagnostics);
    if (!schema_id) {
        return outcome;
    }

    ddl::DropTableRequest request{};
    request.schema_id = *schema_id;
    request.name = ast.name.value;
    request.if_exists = ast.if_exists;
    request.cascade = ast.cascade;

    outcome.commands.emplace_back(std::move(request));
    return outcome;
}

TranslationOutcome translate_statement(const ScriptStatement& statement, const DdlCommandBuilderConfig& config)
{
    TranslationOutcome outcome{};

    if (statement.ast.valueless_by_exception()) {
        outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                                      "Parser produced an empty AST for the statement.",
                                                      statement,
                                                      {"Retry the statement and capture diagnostics if the issue persists."}));
        return outcome;
    }

    if (std::holds_alternative<std::monostate>(statement.ast)) {
        outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                                      "Statement could not be classified for DDL translation.",
                                                      statement,
                                                      {"Ensure the SQL statement is a supported CREATE or DROP command."}));
        return outcome;
    }

    if (std::holds_alternative<CreateDatabaseStatement>(statement.ast)) {
        return translate_create_database(std::get<CreateDatabaseStatement>(statement.ast), statement);
    }

    if (std::holds_alternative<DropDatabaseStatement>(statement.ast)) {
        return translate_drop_database(std::get<DropDatabaseStatement>(statement.ast), statement);
    }

    if (std::holds_alternative<CreateSchemaStatement>(statement.ast)) {
        return translate_create_schema(std::get<CreateSchemaStatement>(statement.ast), statement, config);
    }

    if (std::holds_alternative<DropSchemaStatement>(statement.ast)) {
        return translate_drop_schema(std::get<DropSchemaStatement>(statement.ast), statement, config);
    }

    if (std::holds_alternative<CreateTableStatement>(statement.ast)) {
        return translate_create_table(std::get<CreateTableStatement>(statement.ast), statement, config);
    }

    if (std::holds_alternative<DropTableStatement>(statement.ast)) {
        return translate_drop_table(std::get<DropTableStatement>(statement.ast), statement, config);
    }

    if (std::holds_alternative<CreateViewStatement>(statement.ast)) {
        outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                                      "CREATE VIEW translation is not yet supported.",
                                                      statement,
                                                      {"Execute the statement via the legacy DDL path or implement view support."}));
        return outcome;
    }

    outcome.diagnostics.push_back(make_diagnostic(ParserSeverity::Error,
                                                  "Unsupported DDL statement encountered during translation.",
                                                  statement,
                                                  {"Restrict the script to CREATE/DROP DATABASE, SCHEMA, or TABLE statements."}));
    return outcome;
}

}  // namespace

DdlCommandBuilderResult build_ddl_commands(const ScriptStatement& statement,
                                           const DdlCommandBuilderConfig& config)
{
    DdlCommandBuilderResult result{};
    result.diagnostics.insert(result.diagnostics.end(), statement.diagnostics.begin(), statement.diagnostics.end());

    if (!statement.success) {
        return result;
    }

    auto outcome = translate_statement(statement, config);
    result.diagnostics.insert(result.diagnostics.end(), outcome.diagnostics.begin(), outcome.diagnostics.end());

    if (!has_error(outcome.diagnostics)) {
        result.commands = std::move(outcome.commands);
    }

    return result;
}

DdlCommandBuilderResult build_ddl_commands(const ScriptParseResult& script,
                                           const DdlCommandBuilderConfig& config)
{
    DdlCommandBuilderResult result{};

    for (const auto& statement : script.statements) {
        auto partial = build_ddl_commands(statement, config);
        result.diagnostics.insert(result.diagnostics.end(), partial.diagnostics.begin(), partial.diagnostics.end());
        result.commands.insert(result.commands.end(), partial.commands.begin(), partial.commands.end());
    }

    return result;
}

}  // namespace bored::parser

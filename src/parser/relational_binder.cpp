#include "bored/parser/relational/binder.hpp"

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::parser::relational {
namespace {

using AliasMap = std::unordered_map<std::string, const Expression*>;

std::string normalise_identifier(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (char ch : text) {
        result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return result;
}

std::string scalar_type_name(ScalarType type)
{
    switch (type) {
    case ScalarType::Boolean:
        return "BOOLEAN";
    case ScalarType::Int64:
        return "INT64";
    case ScalarType::UInt32:
        return "UINT32";
    case ScalarType::Decimal:
        return "DECIMAL";
    case ScalarType::Utf8:
        return "UTF8";
    default:
        return "UNKNOWN";
    }
}

ScalarType to_scalar_type(catalog::CatalogColumnType column_type)
{
    switch (column_type) {
    case catalog::CatalogColumnType::Int64:
        return ScalarType::Int64;
    case catalog::CatalogColumnType::UInt32:
        return ScalarType::UInt32;
    case catalog::CatalogColumnType::Utf8:
        return ScalarType::Utf8;
    default:
        return ScalarType::Unknown;
    }
}

ScalarType literal_scalar_type(const LiteralExpression& literal)
{
    switch (literal.tag) {
    case LiteralTag::Boolean:
        return ScalarType::Boolean;
    case LiteralTag::Integer:
        return ScalarType::Int64;
    case LiteralTag::Decimal:
        return ScalarType::Decimal;
    case LiteralTag::String:
        return ScalarType::Utf8;
    default:
        return ScalarType::Unknown;
    }
}

bool is_nullable_literal(const LiteralExpression& literal)
{
    return literal.tag == LiteralTag::Null;
}

bool is_numeric(ScalarType type)
{
    return type == ScalarType::Int64 || type == ScalarType::UInt32 || type == ScalarType::Decimal;
}

struct ComparisonAnalysis final {
    bool comparable = false;
    std::optional<ScalarType> common_type{};
};

ComparisonAnalysis analyse_comparison(ScalarType lhs, ScalarType rhs)
{
    ComparisonAnalysis analysis{};

    if (lhs == ScalarType::Unknown || rhs == ScalarType::Unknown) {
        analysis.comparable = true;
        return analysis;
    }

    if (lhs == rhs) {
        analysis.comparable = true;
        analysis.common_type = lhs;
        return analysis;
    }

    if (is_numeric(lhs) && is_numeric(rhs)) {
        analysis.comparable = true;
        if (lhs == ScalarType::Decimal || rhs == ScalarType::Decimal) {
            analysis.common_type = ScalarType::Decimal;
        } else if (lhs == ScalarType::Int64 || rhs == ScalarType::Int64) {
            analysis.common_type = ScalarType::Int64;
        } else {
            analysis.common_type = ScalarType::UInt32;
        }
        return analysis;
    }

    analysis.comparable = false;
    return analysis;
}

struct ColumnMatch final {
    const ColumnBinding* binding = nullptr;
    const TableBinding* table_binding = nullptr;
};

struct BoundTable final {
    TableReference* node = nullptr;
    TableBinding binding{};
    std::unordered_map<std::string, ColumnBinding> columns{};
};

class Scope final {
public:
    void register_table(TableReference& node,
                        const TableMetadata& metadata,
                        const TableBinding& binding)
    {
        BoundTable table{};
        table.node = &node;
        table.binding = binding;
        table.columns.reserve(metadata.columns.size());
        for (const auto& column : metadata.columns) {
            ColumnBinding column_binding{};
            column_binding.database_id = metadata.database_id;
            column_binding.schema_id = metadata.schema_id;
            column_binding.relation_id = metadata.relation_id;
            column_binding.column_id = column.column_id;
            column_binding.column_type = column.column_type;
            column_binding.schema_name = metadata.schema_name;
            column_binding.table_name = metadata.table_name;
            column_binding.table_alias = binding.table_alias;
            column_binding.column_name = column.name;
            const auto key = normalise_identifier(column.name);
            table.columns.emplace(key, std::move(column_binding));
        }

        tables_.push_back(std::move(table));
        const auto index = tables_.size() - 1U;

        register_symbol(binding.table_name, index);
        if (!binding.schema_name.empty()) {
            register_symbol(binding.schema_name + "." + binding.table_name, index);
        }
        if (binding.table_alias && !binding.table_alias->empty()) {
            register_symbol(*binding.table_alias, index);
        }
    }

    [[nodiscard]] std::vector<const BoundTable*> tables() const
    {
        std::vector<const BoundTable*> result;
        result.reserve(tables_.size());
        for (const auto& table : tables_) {
            result.push_back(&table);
        }
        return result;
    }

    [[nodiscard]] std::vector<ColumnMatch> resolve_column(std::string_view column_name) const
    {
        std::vector<ColumnMatch> matches;
        const auto key = normalise_identifier(column_name);
        for (const auto& table : tables_) {
            auto it = table.columns.find(key);
            if (it != table.columns.end()) {
                matches.push_back(ColumnMatch{&it->second, &table.binding});
            }
        }
        return matches;
    }

    [[nodiscard]] std::vector<ColumnMatch> resolve_column(std::string_view qualifier,
                                                          std::string_view column_name) const
    {
        std::vector<ColumnMatch> matches;
        const auto key = normalise_identifier(column_name);
        const auto indexes = lookup_tables(qualifier);
        for (auto index : indexes) {
            const auto& table = tables_[index];
            auto it = table.columns.find(key);
            if (it != table.columns.end()) {
                matches.push_back(ColumnMatch{&it->second, &table.binding});
            }
        }
        return matches;
    }

    [[nodiscard]] std::vector<const TableBinding*> resolve_table(std::string_view name) const
    {
        std::vector<const TableBinding*> result;
        const auto indexes = lookup_tables(name);
        for (auto index : indexes) {
            result.push_back(&tables_[index].binding);
        }
        return result;
    }

    [[nodiscard]] static std::string display_name(const TableBinding& binding)
    {
        if (binding.table_alias && !binding.table_alias->empty()) {
            return *binding.table_alias;
        }
        if (!binding.schema_name.empty()) {
            return binding.schema_name + "." + binding.table_name;
        }
        return binding.table_name;
    }

private:
    void register_symbol(std::string_view symbol, std::size_t index)
    {
        if (symbol.empty()) {
            return;
        }
        const auto key = normalise_identifier(symbol);
        table_symbols_[key].push_back(index);
    }

    [[nodiscard]] std::vector<std::size_t> lookup_tables(std::string_view name) const
    {
        std::vector<std::size_t> indexes;
        const auto key = normalise_identifier(name);
        auto it = table_symbols_.find(key);
        if (it == table_symbols_.end()) {
            return indexes;
        }
        std::unordered_set<std::size_t> unique;
        for (auto index : it->second) {
            if (unique.insert(index).second) {
                indexes.push_back(index);
            }
        }
        return indexes;
    }

    std::vector<BoundTable> tables_{};
    std::unordered_map<std::string, std::vector<std::size_t>> table_symbols_{};
};

struct TableNameParts final {
    std::optional<std::string_view> schema;
    std::string_view table;
};

std::optional<TableNameParts> parse_table_name(const QualifiedName& name)
{
    if (name.parts.empty()) {
        return std::nullopt;
    }

    if (name.parts.size() == 1U) {
        return TableNameParts{std::nullopt, name.parts.front().value};
    }

    if (name.parts.size() == 2U) {
        return TableNameParts{name.parts.front().value, name.parts.back().value};
    }

    return std::nullopt;
}

std::string format_table_label(std::optional<std::string_view> schema, std::string_view table)
{
    if (schema && !schema->empty()) {
        return std::string(*schema) + "." + std::string(table);
    }
    return std::string(table);
}

class RelationalBinder final {
public:
    explicit RelationalBinder(BinderConfig config)
        : config_(std::move(config))
    {
    }

    BindingResult bind(SelectStatement& statement) const
    {
        BindingResult result{};
        if (!ensure_catalog(result)) {
            return result;
        }

        if (statement.query == nullptr) {
            return result;
        }

        Scope scope{};
        bind_from_clause(*statement.query, scope, result);
        bind_join_clauses(*statement.query, scope, result);
        const auto aliases = bind_select_list(*statement.query, scope, result);
        bind_where_clause(*statement.query, scope, result);
        bind_group_by(*statement.query, scope, aliases, result);
        bind_order_by(*statement.query, scope, aliases, result);
        bind_limit(*statement.query, scope, result);
        return result;
    }

    BindingResult bind(InsertStatement& statement) const
    {
        BindingResult result{};
        if (!ensure_catalog(result)) {
            return result;
        }

        if (statement.target == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "INSERT statement requires a target table";
            diagnostic.remediation_hints = {"Specify a table after INSERT INTO."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        Scope scope{};
        bind_table_reference(*statement.target, scope, result);
        if (!result.success()) {
            return result;
        }

        const auto tables = scope.tables();
        if (tables.empty()) {
            return result;
        }
        const auto& bound_table = *tables.front();

        if (statement.columns.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "INSERT statement must specify a column list";
            diagnostic.remediation_hints = {"List all target columns inside parentheses after the table name."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        std::unordered_set<std::string> seen_columns;
        for (auto& column : statement.columns) {
            if (column.name.value.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "INSERT column list contains an empty identifier";
                diagnostic.remediation_hints = {"Ensure each column name is a valid identifier."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            const auto key = normalise_identifier(column.name.value);
            if (!seen_columns.insert(key).second) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Duplicate column '" + column.name.value + "' in INSERT column list";
                diagnostic.remediation_hints = {"Remove duplicate column references from the column list."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            auto matches = scope.resolve_column(column.name.value);
            if (matches.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column '" + column.name.value + "' not found in target table";
                diagnostic.remediation_hints = {"Verify the column exists on the target table."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }
            if (matches.size() > 1U) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column reference '" + column.name.value + "' is ambiguous";
                diagnostic.remediation_hints = {"Qualify the column with a table alias."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            column.binding = *matches.front().binding;
        }

        if (!result.success()) {
            return result;
        }

        if (statement.columns.size() != bound_table.columns.size()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "INSERT must provide values for all columns in the target table";
            diagnostic.remediation_hints = {"List every column defined on the table in the INSERT column list."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        if (statement.rows.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "INSERT statement must include at least one VALUES row";
            diagnostic.remediation_hints = {"Specify one or more parenthesized value lists after VALUES."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        for (auto& row : statement.rows) {
            if (row.values.size() != statement.columns.size()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "VALUES list does not match INSERT column count";
                diagnostic.remediation_hints = {"Ensure each VALUES row has exactly one literal per listed column."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }
            for (auto* expression : row.values) {
                if (expression == nullptr) {
                    ParserDiagnostic diagnostic{};
                    diagnostic.severity = ParserSeverity::Error;
                    diagnostic.message = "VALUES list contains an empty expression";
                    diagnostic.remediation_hints = {"Provide a literal for each position in the VALUES row."};
                    result.diagnostics.push_back(std::move(diagnostic));
                    continue;
                }
                bind_expression(*expression, scope, result);
            }
        }

        return result;
    }

    BindingResult bind(UpdateStatement& statement) const
    {
        BindingResult result{};
        if (!ensure_catalog(result)) {
            return result;
        }

        if (statement.target == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "UPDATE statement requires a target table";
            diagnostic.remediation_hints = {"Provide a table name immediately after UPDATE."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        Scope scope{};
        bind_table_reference(*statement.target, scope, result);
        if (!result.success()) {
            return result;
        }

        if (statement.assignments.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "UPDATE statement must include at least one SET assignment";
            diagnostic.remediation_hints = {"Add a SET clause listing the columns to update."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        std::unordered_set<std::string> seen_targets;
        for (auto& assignment : statement.assignments) {
            if (assignment.column.value.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "SET clause contains an empty column reference";
                diagnostic.remediation_hints = {"Ensure each assignment references a valid column."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            const auto key = normalise_identifier(assignment.column.value);
            if (!seen_targets.insert(key).second) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column '" + assignment.column.value + "' is assigned multiple times";
                diagnostic.remediation_hints = {"Remove duplicate assignments for the same column."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            auto matches = scope.resolve_column(assignment.column.value);
            if (matches.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column '" + assignment.column.value + "' not found in target table";
                diagnostic.remediation_hints = {"Verify the column exists on the table being updated."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }
            if (matches.size() > 1U) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column reference '" + assignment.column.value + "' is ambiguous";
                diagnostic.remediation_hints = {"Qualify the column with a table alias."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            assignment.binding = *matches.front().binding;

            if (assignment.value == nullptr) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "SET assignment for column '" + assignment.column.value + "' is missing a value";
                diagnostic.remediation_hints = {"Provide an expression on the right-hand side of the assignment."};
                result.diagnostics.push_back(std::move(diagnostic));
                continue;
            }

            bind_expression(*assignment.value, scope, result);
        }

        if (statement.where == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "UPDATE statement requires a WHERE clause";
            diagnostic.remediation_hints = {"Add a WHERE predicate to target specific rows."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        bind_expression(*statement.where, scope, result);
        return result;
    }

    BindingResult bind(DeleteStatement& statement) const
    {
        BindingResult result{};
        if (!ensure_catalog(result)) {
            return result;
        }

        if (statement.target == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "DELETE statement requires a target table";
            diagnostic.remediation_hints = {"Provide a table name after DELETE FROM."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        Scope scope{};
        bind_table_reference(*statement.target, scope, result);
        if (!result.success()) {
            return result;
        }

        if (statement.where == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "DELETE statement requires a WHERE clause";
            diagnostic.remediation_hints = {"Add a WHERE condition to control which rows are deleted."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        bind_expression(*statement.where, scope, result);
        return result;
    }

private:
    bool ensure_catalog(BindingResult& result) const
    {
        if (config_.catalog != nullptr) {
            return true;
        }

        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Binder catalog is not configured";
        diagnostic.remediation_hints = {"Provide a catalog adapter before binding."};
        result.diagnostics.push_back(std::move(diagnostic));
        return false;
    }

    void bind_from_clause(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        if (query.from_tables.empty()) {
            return;
        }
        for (auto* table : query.from_tables) {
            if (table == nullptr) {
                continue;
            }
            bind_table_reference(*table, scope, result);
        }
    }

    void bind_join_clauses(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        for (auto& join : query.joins) {
            if (join.predicate != nullptr) {
                bind_expression(*join.predicate, scope, result);
            } else if (join.type != JoinType::Cross) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "JOIN clause requires an ON predicate";
                diagnostic.remediation_hints = {"Provide an ON expression or use CROSS JOIN for predicate-free joins."};
                result.diagnostics.push_back(std::move(diagnostic));
            }
        }
    }

    void bind_table_reference(TableReference& table, Scope& scope, BindingResult& result) const
    {
        auto parts = parse_table_name(table.name);
        if (!parts.has_value()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Table references may only use optional schema qualifiers (schema.table)";
            diagnostic.remediation_hints = {"Rewrite the query to avoid multi-part table references."};
            result.diagnostics.push_back(std::move(diagnostic));
            return;
        }

        std::optional<std::string_view> schema = parts->schema;
        std::optional<std::string> fallback_schema_storage{};
        if (!schema) {
            if (config_.default_schema) {
                fallback_schema_storage = *config_.default_schema;
                schema = std::string_view(*fallback_schema_storage);
            }
        }

        auto metadata = config_.catalog->lookup_table(schema, parts->table);
        if (!metadata.has_value()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Table '" + format_table_label(schema, parts->table) + "' not found";
            diagnostic.remediation_hints = {"Verify the table exists and is visible to the current user."};
            result.diagnostics.push_back(std::move(diagnostic));
            return;
        }

        TableBinding binding{};
        binding.database_id = metadata->database_id;
        binding.schema_id = metadata->schema_id;
        binding.relation_id = metadata->relation_id;
        binding.schema_name = metadata->schema_name;
        binding.table_name = metadata->table_name;
        if (table.alias) {
            binding.table_alias = table.alias->value;
        }

        table.binding = binding;
        scope.register_table(table, *metadata, binding);
    }

    AliasMap bind_select_list(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        AliasMap aliases{};
        for (auto* item : query.select_items) {
            if (item == nullptr || item->expression == nullptr) {
                continue;
            }
            bind_expression(*item->expression, scope, result);
            if (item->alias.has_value()) {
                const auto key = normalise_identifier(item->alias->value);
                const auto inserted = aliases.emplace(key, item->expression).second;
                if (!inserted) {
                    ParserDiagnostic diagnostic{};
                    diagnostic.severity = ParserSeverity::Error;
                    diagnostic.message = "Select item alias '" + item->alias->value + "' is ambiguous";
                    diagnostic.remediation_hints = {"Rename one of the select item aliases to be unique."};
                    result.diagnostics.push_back(std::move(diagnostic));
                }
            }
        }
        return aliases;
    }

    void bind_where_clause(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        if (query.where != nullptr) {
            bind_expression(*query.where, scope, result);
        }
    }

    void bind_group_by(QuerySpecification& query,
                       Scope& scope,
                       const AliasMap& aliases,
                       BindingResult& result) const
    {
        for (auto* expression : query.group_by) {
            if (expression == nullptr) {
                continue;
            }
            bind_expression_with_alias(*expression, scope, aliases, result);
        }
    }

    void bind_order_by(QuerySpecification& query,
                       Scope& scope,
                       const AliasMap& aliases,
                       BindingResult& result) const
    {
        for (auto* item : query.order_by) {
            if (item == nullptr || item->expression == nullptr) {
                continue;
            }
            bind_expression_with_alias(*item->expression, scope, aliases, result);
        }
    }

    void bind_limit(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        if (query.limit == nullptr) {
            return;
        }
        if (query.limit->row_count != nullptr) {
            bind_expression(*query.limit->row_count, scope, result);
        }
        if (query.limit->offset != nullptr) {
            bind_expression(*query.limit->offset, scope, result);
        }
    }

    void bind_expression_with_alias(Expression& expression,
                                    Scope& scope,
                                    const AliasMap& aliases,
                                    BindingResult& result) const
    {
        if (expression.kind == NodeKind::IdentifierExpression) {
            auto& identifier = static_cast<IdentifierExpression&>(expression);
            if (identifier.name.parts.size() == 1U) {
                const auto key = normalise_identifier(identifier.name.parts.front().value);
                auto it = aliases.find(key);
                if (it != aliases.end()) {
                    const auto* target = it->second;
                    if (target != nullptr && target->inferred_type.has_value()) {
                        set_expression_type(identifier,
                                            target->inferred_type->type,
                                            target->inferred_type->nullable);
                        if (target->required_coercion.has_value()) {
                            identifier.required_coercion = target->required_coercion;
                        }
                    } else {
                        set_expression_type(identifier, ScalarType::Unknown, true);
                    }
                    return;
                }
            }
        }

        bind_expression(expression, scope, result);
    }

    void bind_expression(Expression& expression, Scope& scope, BindingResult& result) const
    {
        switch (expression.kind) {
        case NodeKind::IdentifierExpression:
            bind_identifier(static_cast<IdentifierExpression&>(expression), scope, result);
            break;
        case NodeKind::LiteralExpression:
            bind_literal(static_cast<LiteralExpression&>(expression));
            break;
        case NodeKind::BinaryExpression: {
            auto& binary = static_cast<BinaryExpression&>(expression);
            if (binary.left != nullptr) {
                bind_expression(*binary.left, scope, result);
            }
            if (binary.right != nullptr) {
                bind_expression(*binary.right, scope, result);
            }
            infer_binary_expression(binary, result);
            break;
        }
        case NodeKind::StarExpression:
            bind_star(static_cast<StarExpression&>(expression), scope, result);
            break;
        default:
            break;
        }
    }

    void bind_identifier(IdentifierExpression& expression, Scope& scope, BindingResult& result) const
    {
        if (expression.name.parts.empty()) {
            return;
        }

        if (expression.name.parts.size() == 1U) {
            const auto column_name = expression.name.parts.front().value;
            auto matches = scope.resolve_column(column_name);
            if (matches.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column '" + column_name + "' not found in scope";
                diagnostic.remediation_hints = {"Ensure the column exists or add the appropriate table to the FROM clause."};
                result.diagnostics.push_back(std::move(diagnostic));
                return;
            }
            if (matches.size() > 1U) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column reference '" + column_name + "' is ambiguous";
                std::string hint = "Qualify the column using one of: ";
                bool first = true;
                for (const auto& match : matches) {
                    if (!first) {
                        hint.append(", ");
                    }
                    hint.append(Scope::display_name(*match.table_binding));
                    first = false;
                }
                diagnostic.remediation_hints = {std::move(hint)};
                result.diagnostics.push_back(std::move(diagnostic));
                return;
            }

            expression.binding = *matches.front().binding;
            const auto scalar_type = to_scalar_type(matches.front().binding->column_type);
            set_expression_type(expression, scalar_type, true);
            return;
        }

        if (expression.name.parts.size() == 2U) {
            const auto qualifier = expression.name.parts.front().value;
            const auto column_name = expression.name.parts.back().value;
            auto matches = scope.resolve_column(qualifier, column_name);
            if (matches.empty()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column '" + column_name + "' not found on table '" + qualifier + "'";
                diagnostic.remediation_hints = {"Verify the qualifier references an existing table alias."};
                result.diagnostics.push_back(std::move(diagnostic));
                return;
            }
            if (matches.size() > 1U) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Column reference '" + qualifier + "." + column_name + "' is ambiguous";
                diagnostic.remediation_hints = {"Qualify the column with a unique table alias."};
                result.diagnostics.push_back(std::move(diagnostic));
                return;
            }

            expression.binding = *matches.front().binding;
            const auto scalar_type = to_scalar_type(matches.front().binding->column_type);
            set_expression_type(expression, scalar_type, true);
            return;
        }

        ParserDiagnostic diagnostic{};
        diagnostic.severity = ParserSeverity::Error;
        diagnostic.message = "Column references with more than one qualifier are not supported";
        diagnostic.remediation_hints = {"Use optional table aliases when qualifying columns."};
        result.diagnostics.push_back(std::move(diagnostic));
    }

    void bind_literal(LiteralExpression& expression) const
    {
        const auto type = literal_scalar_type(expression);
        const bool nullable = is_nullable_literal(expression);
        set_expression_type(expression, type, nullable);
    }

    void bind_star(StarExpression& expression, Scope& scope, BindingResult& result) const
    {
        if (expression.qualifier.parts.empty()) {
            return;
        }

        if (expression.qualifier.parts.size() > 1U) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Star expressions support at most one qualifier (table alias)";
            diagnostic.remediation_hints = {"Reduce the qualifier to a single table alias."};
            result.diagnostics.push_back(std::move(diagnostic));
            return;
        }

        const auto qualifier = expression.qualifier.parts.front().value;
        auto tables = scope.resolve_table(qualifier);
        if (tables.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Table alias '" + qualifier + "' not found for star expression";
            diagnostic.remediation_hints = {"Ensure the alias is defined in the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return;
        }
        if (tables.size() > 1U) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Table alias '" + qualifier + "' is ambiguous for star expression";
            diagnostic.remediation_hints = {"Provide a unique alias for each table in the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return;
        }

        expression.binding = *tables.front();
    }

    void infer_binary_expression(BinaryExpression& expression, BindingResult& result) const
    {
        auto left_type = ScalarType::Unknown;
        bool left_nullable = true;
        auto right_type = ScalarType::Unknown;
        bool right_nullable = true;
        if (expression.left != nullptr && expression.left->inferred_type.has_value()) {
            left_type = expression.left->inferred_type->type;
            left_nullable = expression.left->inferred_type->nullable;
        }
        if (expression.right != nullptr && expression.right->inferred_type.has_value()) {
            right_type = expression.right->inferred_type->type;
            right_nullable = expression.right->inferred_type->nullable;
        }
        switch (expression.op) {
        case BinaryOperator::Equal:
        case BinaryOperator::NotEqual:
        case BinaryOperator::Less:
        case BinaryOperator::LessOrEqual:
        case BinaryOperator::Greater:
        case BinaryOperator::GreaterOrEqual: {
            const auto analysis = analyse_comparison(left_type, right_type);
            if (!analysis.comparable) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Type mismatch: cannot compare " + scalar_type_name(left_type) + " to " + scalar_type_name(right_type);
                diagnostic.remediation_hints = {"Cast one side to a compatible type or adjust the query predicate."};
                result.diagnostics.push_back(std::move(diagnostic));
            }

            if (analysis.common_type.has_value()) {
                const auto target_type = *analysis.common_type;
                if (expression.left != nullptr && expression.left->inferred_type.has_value() &&
                    expression.left->inferred_type->type != target_type) {
                    set_expression_coercion(*expression.left, target_type, left_nullable);
                }
                if (expression.right != nullptr && expression.right->inferred_type.has_value() &&
                    expression.right->inferred_type->type != target_type) {
                    set_expression_coercion(*expression.right, target_type, right_nullable);
                }
            }

            set_expression_type(expression, ScalarType::Boolean, true);
            break;
        }
        case BinaryOperator::Add:
        case BinaryOperator::Subtract: {
            if (!is_numeric(left_type) || !is_numeric(right_type)) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Type mismatch: arithmetic requires numeric operands but saw " + scalar_type_name(left_type) + " and " + scalar_type_name(right_type);
                diagnostic.remediation_hints = {"Cast operands to numeric types or adjust the SET expression."};
                result.diagnostics.push_back(std::move(diagnostic));
                set_expression_type(expression, ScalarType::Unknown, true);
                break;
            }

            ScalarType target_type = ScalarType::Int64;
            if (left_type == ScalarType::Decimal || right_type == ScalarType::Decimal) {
                target_type = ScalarType::Decimal;
            } else if (left_type == ScalarType::Int64 || right_type == ScalarType::Int64) {
                target_type = ScalarType::Int64;
            } else {
                target_type = ScalarType::UInt32;
            }

            if (expression.left != nullptr && expression.left->inferred_type.has_value() &&
                expression.left->inferred_type->type != target_type) {
                set_expression_coercion(*expression.left, target_type, left_nullable);
            }
            if (expression.right != nullptr && expression.right->inferred_type.has_value() &&
                expression.right->inferred_type->type != target_type) {
                set_expression_coercion(*expression.right, target_type, right_nullable);
            }

            const bool nullable = left_nullable || right_nullable;
            set_expression_type(expression, target_type, nullable);
            break;
        }
        default:
            set_expression_type(expression, ScalarType::Unknown, true);
            break;
        }
    }

    void set_expression_type(Expression& expression, ScalarType type, bool nullable) const
    {
        TypeAnnotation annotation{};
        annotation.type = type;
        annotation.nullable = nullable;
        expression.inferred_type = annotation;
    }

    void set_expression_coercion(Expression& expression, ScalarType type, bool nullable) const
    {
        CoercionRequirement requirement{};
        requirement.target_type = type;
        requirement.nullable = nullable;
        expression.required_coercion = requirement;
    }

    BinderConfig config_;
};

}  // namespace

BindingResult bind_select(const BinderConfig& config, SelectStatement& statement)
{
    RelationalBinder binder(config);
    return binder.bind(statement);
}

BindingResult bind_insert(const BinderConfig& config, InsertStatement& statement)
{
    RelationalBinder binder(config);
    return binder.bind(statement);
}

BindingResult bind_update(const BinderConfig& config, UpdateStatement& statement)
{
    RelationalBinder binder(config);
    return binder.bind(statement);
}

BindingResult bind_delete(const BinderConfig& config, DeleteStatement& statement)
{
    RelationalBinder binder(config);
    return binder.bind(statement);
}

}  // namespace bored::parser::relational

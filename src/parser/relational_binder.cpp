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
        if (config_.catalog == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Binder catalog is not configured";
            diagnostic.remediation_hints = {"Provide a catalog adapter before binding."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        if (statement.query == nullptr) {
            return result;
        }

        Scope scope{};
        bind_from_clause(*statement.query, scope, result);
        bind_select_list(*statement.query, scope, result);
        bind_where_clause(*statement.query, scope, result);
        bind_order_by(*statement.query, scope, result);
        bind_limit(*statement.query, scope, result);
        return result;
    }

private:
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

    void bind_select_list(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        for (auto* item : query.select_items) {
            if (item == nullptr || item->expression == nullptr) {
                continue;
            }
            bind_expression(*item->expression, scope, result);
        }
    }

    void bind_where_clause(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        if (query.where != nullptr) {
            bind_expression(*query.where, scope, result);
        }
    }

    void bind_order_by(QuerySpecification& query, Scope& scope, BindingResult& result) const
    {
        for (auto* item : query.order_by) {
            if (item == nullptr || item->expression == nullptr) {
                continue;
            }
            bind_expression(*item->expression, scope, result);
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

}  // namespace bored::parser::relational

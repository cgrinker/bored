#include "bored/parser/relational/logical_lowering.hpp"

#include <sstream>
#include <string>
#include <utility>

namespace bored::parser::relational {
namespace {

std::string projection_label(const SelectItem& item, std::size_t index)
{
    if (item.alias.has_value()) {
        return item.alias->value;
    }

    if (item.expression != nullptr && item.expression->kind == NodeKind::IdentifierExpression) {
        const auto& identifier = static_cast<const IdentifierExpression&>(*item.expression);
        return format_qualified_name(identifier.name);
    }

    std::ostringstream stream;
    stream << "column_" << (index + 1U);
    return stream.str();
}

std::vector<LogicalColumn> propagate_schema(const LogicalOperatorPtr& input)
{
    if (input == nullptr) {
        return {};
    }
    return input->output_schema;
}

class LoweringContext final {
public:
    explicit LoweringContext(const SelectStatement& statement)
        : statement_(statement)
    {
    }

    LoweringResult run()
    {
        LoweringResult result{};
        if (statement_.query == nullptr) {
            return result;
        }

        if (statement_.query->select_items.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering requires at least one projection";
            diagnostic.remediation_hints = {"Add at least one item to the SELECT list."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        auto plan = lower_query(*statement_.query, result);
        if (plan == nullptr) {
            return result;
        }

        result.plan = std::move(plan);
        return result;
    }

private:
    LogicalOperatorPtr lower_query(const QuerySpecification& query, LoweringResult& result)
    {
        auto current = lower_from(query, result);
        if (current == nullptr) {
            return nullptr;
        }

        if (query.where != nullptr) {
            auto filter = std::make_unique<LogicalFilter>();
            filter->predicate = query.where;
            filter->output_schema = propagate_schema(current);
            filter->input = std::move(current);
            current = std::move(filter);
        }

        if (!query.group_by.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "GROUP BY lowering is not implemented";
            diagnostic.remediation_hints = {"Remove the GROUP BY clause or extend the lowering pass."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        auto project = lower_projection(query, std::move(current));
        if (project == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering failed to build projection";
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }
        current = std::move(project);

        if (!query.order_by.empty()) {
            auto sort = std::make_unique<LogicalSort>();
            sort->output_schema = propagate_schema(current);
            for (const auto* item : query.order_by) {
                if (item == nullptr || item->expression == nullptr) {
                    continue;
                }
                LogicalSort::SortKey key{};
                key.expression = item->expression;
                key.direction = item->direction;
                sort->keys.push_back(key);
            }
            sort->input = std::move(current);
            current = std::move(sort);
        }

        if (query.limit != nullptr && (query.limit->row_count != nullptr || query.limit->offset != nullptr)) {
            auto limit = std::make_unique<LogicalLimit>();
            limit->output_schema = propagate_schema(current);
            if (query.limit->row_count != nullptr) {
                limit->row_count = query.limit->row_count;
            }
            if (query.limit->offset != nullptr) {
                limit->offset = query.limit->offset;
            }
            limit->input = std::move(current);
            current = std::move(limit);
        }

        return current;
    }

    LogicalOperatorPtr lower_from(const QuerySpecification& query, LoweringResult& result)
    {
        if (query.from_tables.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering requires a FROM clause";
            diagnostic.remediation_hints = {"Add a table to the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        if (query.from_tables.size() != 1U) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering currently supports only a single table";
            diagnostic.remediation_hints = {"Rewrite the query without joins until join lowering is implemented."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        auto* table = query.from_tables.front();
        if (table == nullptr || !table->binding.has_value()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Table binding missing for SELECT lowering";
            diagnostic.remediation_hints = {"Ensure the binder has resolved table references before lowering."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        auto scan = std::make_unique<LogicalScan>();
        scan->table = *table->binding;
        // Without catalog metadata we conservatively leave the scan schema empty.
        return scan;
    }

    std::unique_ptr<LogicalProject> lower_projection(const QuerySpecification& query,
                                                     LogicalOperatorPtr input)
    {
        auto project = std::make_unique<LogicalProject>();
        project->input = std::move(input);

        std::size_t index = 0U;
        for (const auto* item : query.select_items) {
            if (item == nullptr || item->expression == nullptr) {
                ++index;
                continue;
            }

            LogicalProject::Projection projection{};
            projection.expression = item->expression;
            if (item->alias.has_value()) {
                projection.alias = item->alias->value;
            }
            project->projections.push_back(projection);

            LogicalColumn column{};
            column.name = projection_label(*item, index);
            if (item->expression->inferred_type.has_value()) {
                column.type = item->expression->inferred_type->type;
                column.nullable = item->expression->inferred_type->nullable;
            }
            project->output_schema.push_back(column);
            ++index;
        }

        return project;
    }

    const SelectStatement& statement_;
};

}  // namespace

LoweringResult lower_select(const SelectStatement& statement)
{
    LoweringContext context(statement);
    return context.run();
}

}  // namespace bored::parser::relational

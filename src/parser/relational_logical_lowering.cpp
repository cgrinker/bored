#include "bored/parser/relational/logical_lowering.hpp"

#include <cctype>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
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

std::string normalise_identifier(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (char ch : text) {
        result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return result;
}

std::vector<LogicalColumn> propagate_schema(const LogicalOperatorPtr& input)
{
    if (input == nullptr) {
        return {};
    }
    return input->output_schema;
}

std::vector<LogicalColumn> merge_schemas(const LogicalOperatorPtr& left, const LogicalOperatorPtr& right)
{
    auto schema = propagate_schema(left);
    const auto right_schema = propagate_schema(right);
    schema.insert(schema.end(), right_schema.begin(), right_schema.end());
    return schema;
}

class LoweringContext final {
public:
    LoweringContext(const SelectStatement& statement, const LoweringConfig& config)
        : statement_(statement)
        , config_(config)
    {
        if (statement_.with != nullptr) {
            for (auto* cte : statement_.with->expressions) {
                if (cte == nullptr) {
                    continue;
                }
                const auto key = normalise_identifier(cte->name.value);
                if (!key.empty()) {
                    cte_definitions_.emplace(key, cte);
                }
            }
        }
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
        if (config_.plan_sink && result.plan != nullptr) {
            config_.plan_sink(*result.plan);
        }
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

        std::vector<LogicalOperatorPtr> table_nodes(query.from_tables.size());
        for (std::size_t index = 0; index < query.from_tables.size(); ++index) {
            auto* table = query.from_tables[index];
            if (table == nullptr) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "FROM clause contains an invalid table reference";
                diagnostic.remediation_hints = {"Ensure each table reference in the FROM clause is well-formed."};
                result.diagnostics.push_back(std::move(diagnostic));
                return nullptr;
            }
            if (!table->binding.has_value()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Table binding missing for SELECT lowering";
                diagnostic.remediation_hints = {"Ensure the binder has resolved table references before lowering."};
                result.diagnostics.push_back(std::move(diagnostic));
                return nullptr;
            }

            LogicalOperatorPtr node;
            const auto table_key = normalise_identifier(table->binding->table_name);
            if (cte_definitions_.find(table_key) != cte_definitions_.end()) {
                node = lower_cte_reference(*table, result);
            } else {
                auto scan = std::make_unique<LogicalScan>();
                scan->table = *table->binding;
                node = std::move(scan);
            }
            if (node == nullptr) {
                return nullptr;
            }
            table_nodes[index] = std::move(node);
        }

        const auto make_reference_error = [&](std::string message) -> LogicalOperatorPtr {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = std::move(message);
            diagnostic.remediation_hints = {"Verify JOIN clauses reference tables that appear earlier in the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        };

        auto take_table = [&](std::size_t index) -> LogicalOperatorPtr {
            if (index >= table_nodes.size()) {
                std::ostringstream stream;
                stream << "JOIN references table index " << index << " which is out of range";
                return make_reference_error(stream.str());
            }
            auto& entry = table_nodes[index];
            if (entry == nullptr) {
                std::ostringstream stream;
                stream << "JOIN references table index " << index << " that is already consumed";
                return make_reference_error(stream.str());
            }
            return std::move(entry);
        };

        std::vector<LogicalOperatorPtr> join_nodes(query.joins.size());

        auto take_join = [&](std::size_t index) -> LogicalOperatorPtr {
            if (index >= join_nodes.size()) {
                std::ostringstream stream;
                stream << "JOIN references join index " << index << " which is out of range";
                return make_reference_error(stream.str());
            }
            auto& entry = join_nodes[index];
            if (entry == nullptr) {
                std::ostringstream stream;
                stream << "JOIN references join index " << index << " that is not yet available";
                return make_reference_error(stream.str());
            }
            return std::move(entry);
        };

        for (std::size_t join_index = 0; join_index < query.joins.size(); ++join_index) {
            const auto& clause = query.joins[join_index];

            LogicalOperatorPtr left_input;
            if (clause.left_kind == JoinClause::InputKind::Join) {
                left_input = take_join(clause.left_index);
            } else {
                left_input = take_table(clause.left_index);
            }
            if (left_input == nullptr) {
                return nullptr;
            }

            auto right_input = take_table(clause.right_index);
            if (right_input == nullptr) {
                return nullptr;
            }

            auto join = std::make_unique<LogicalJoin>();
            join->join_type = clause.type;
            join->predicate = clause.predicate;
            join->left = std::move(left_input);
            join->right = std::move(right_input);
            join->output_schema = merge_schemas(join->left, join->right);

            join_nodes[join_index] = std::move(join);
        }

        LogicalOperatorPtr current;
        if (!join_nodes.empty()) {
            current = std::move(join_nodes.back());
            if (current == nullptr) {
                return make_reference_error("JOIN tree missing final join result");
            }
        }

        for (auto& table_node : table_nodes) {
            if (table_node == nullptr) {
                continue;
            }
            if (current == nullptr) {
                current = std::move(table_node);
                continue;
            }

            auto join = std::make_unique<LogicalJoin>();
            join->join_type = JoinType::Cross;
            join->predicate = nullptr;
            join->left = std::move(current);
            join->right = std::move(table_node);
            join->output_schema = merge_schemas(join->left, join->right);
            current = std::move(join);
        }

        if (current == nullptr) {
            return make_reference_error("SELECT lowering requires at least one resolved table");
        }

        return current;
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
    const LoweringConfig& config_;
    std::unordered_map<std::string, const CommonTableExpression*> cte_definitions_{};
    std::unordered_set<std::string> active_ctes_{};

    LogicalOperatorPtr lower_cte_reference(const TableReference& table, LoweringResult& result)
    {
        if (!table.binding.has_value()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE reference is missing binding information";
            diagnostic.remediation_hints = {
                "Ensure the binder resolves WITH clause references before lowering."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        const auto& binding = *table.binding;
        const auto key = normalise_identifier(binding.table_name);
        auto it = cte_definitions_.find(key);
        if (it == cte_definitions_.end()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE '" + binding.table_name + "' is not available during lowering";
            diagnostic.remediation_hints = {
                "Verify the WITH clause defines the referenced CTE before it is used."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        if (active_ctes_.count(key) != 0U) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Recursive CTE reference '" + binding.table_name + "' is not supported";
            diagnostic.remediation_hints = {
                "Rewrite the query to avoid referencing a CTE within its own definition."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        const auto* cte = it->second;
        if (cte == nullptr || cte->query == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE '" + binding.table_name + "' has an empty query body";
            diagnostic.remediation_hints = {
                "Provide a SELECT statement inside the CTE definition."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        active_ctes_.insert(key);
        auto plan = lower_query(*cte->query, result);
        active_ctes_.erase(key);
        return plan;
    }
};

}  // namespace

LoweringResult lower_select(const SelectStatement& statement, const LoweringConfig& config)
{
    LoweringContext context(statement, config);
    return context.run();
}

LoweringResult lower_select(const SelectStatement& statement)
{
    LoweringConfig config{};
    return lower_select(statement, config);
}

}  // namespace bored::parser::relational
